#!/bin/bash
#
# Deploy Flask API to Oracle Cloud server
# Uses SSH ControlMaster so you only enter your passphrase once.

set -e  # Exit on error

# Load environment variables from .env
if [ -f .env ]; then
    export $(grep -v '^#' .env | xargs)
fi

# Get server config from Python
read SERVER REMOTE_DIR <<< $(python3 -c "from config.sheets import SERVER_CONFIG; print(f\"{SERVER_CONFIG['ssh_user']}@{SERVER_CONFIG['production_host']} {SERVER_CONFIG['remote_dir']}\")")

echo "Deploying Flask API to $SERVER..."

# ---- SSH connection multiplexing (one passphrase for the whole deploy) ----
CTRL_SOCKET="/tmp/deploy-ssh-$$"
ssh -fNM -S "$CTRL_SOCKET" "$SERVER"
trap 'ssh -S "$CTRL_SOCKET" -O exit "$SERVER" 2>/dev/null' EXIT
SSH="ssh -S $CTRL_SOCKET"
SCP="scp -o ControlPath=$CTRL_SOCKET"

# ---- Upload src, lib, and config directories ----
echo "Uploading Python packages..."
$SSH $SERVER "cd $REMOTE_DIR && for d in src lib config; do [ -d \$d ] && cp -r \$d \$d.backup || true; done"

echo "  - Creating archive..."
tar czf /tmp/deploy.tar.gz --exclude='__pycache__' --exclude='*.pyc' --exclude='.git' -C . src/ lib/ config/

echo "  - Uploading archive..."
$SCP /tmp/deploy.tar.gz $SERVER:$REMOTE_DIR/

echo "  - Extracting on server..."
$SSH $SERVER "cd $REMOTE_DIR && rm -rf src lib config && tar xzf deploy.tar.gz && rm deploy.tar.gz"

echo "  - Verifying update timestamps..."
$SSH $SERVER "stat -c '%n %y' $REMOTE_DIR/src/api.py $REMOTE_DIR/lib/sheets.py $REMOTE_DIR/config/sheets.py"

# Verify critical files are present
echo "  - Verifying upload..."
FILE_COUNT=$($SSH $SERVER "ls $REMOTE_DIR/src/*.py $REMOTE_DIR/lib/*.py $REMOTE_DIR/config/*.py 2>/dev/null | wc -l")
if [ "$FILE_COUNT" -lt 8 ]; then
    echo "ERROR: Critical files missing ($FILE_COUNT found)! Restoring backups..."
    $SSH $SERVER "cd $REMOTE_DIR && for d in src lib config; do rm -rf \$d && [ -d \$d.backup ] && mv \$d.backup \$d || true; done"
    rm -f /tmp/deploy.tar.gz
    exit 1
fi

# Cleanup backups
rm -f /tmp/deploy.tar.gz
$SSH $SERVER "cd $REMOTE_DIR && rm -rf src.backup lib.backup config.backup"
echo "  All files uploaded successfully"

# ---- Upload google credentials ----
echo "Uploading credentials..."
$SCP google-credentials.json $SERVER:$REMOTE_DIR/

# ---- Sync Python dependencies ----
echo "Syncing Python dependencies..."
$SCP requirements.txt $SERVER:$REMOTE_DIR/
$SSH $SERVER "cd $REMOTE_DIR && ./venv/bin/pip install -q -r requirements.txt 2>&1 | tail -5"

# ---- Restart Flask API ----
echo "Restarting Flask API..."
$SSH $SERVER "sudo systemctl stop flask-api; sudo fuser -k 5000/tcp 2>/dev/null || true; sleep 1; sudo systemctl start flask-api"

# Wait for startup
sleep 2

# Check status
echo "Checking service status..."
$SSH $SERVER 'sudo systemctl status flask-api --no-pager | head -15'

echo "Deployment complete!"
