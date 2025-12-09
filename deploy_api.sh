#!/bin/bash
#
# Deploy Flask API to Oracle Cloud server

set -e  # Exit on error

# Load environment variables from .env
if [ -f .env ]; then
    export $(grep -v '^#' .env | xargs)
fi

# Get server config from Python
read SERVER REMOTE_DIR <<< $(python3 -c "from src.config import SERVER_CONFIG; print(f\"{SERVER_CONFIG['ssh_user']}@{SERVER_CONFIG['production_host']} {SERVER_CONFIG['remote_dir']}\")")

echo "Deploying Flask API to $SERVER..."

# Upload src directory
echo "Uploading src directory..."
# Create backup of existing src
ssh $SERVER "cd $REMOTE_DIR && [ -d src ] && cp -r src src.backup || true"

# Upload all Python files in src/ using tar (more reliable than individual scp)
echo "  - Creating archive..."
tar czf /tmp/src.tar.gz --exclude='__pycache__' --exclude='*.pyc' --exclude='.git' -C . src/

echo "  - Uploading archive..."
scp /tmp/src.tar.gz $SERVER:$REMOTE_DIR/

echo "  - Extracting on server..."
ssh $SERVER "cd $REMOTE_DIR && rm -rf src && tar xzf src.tar.gz && rm src.tar.gz"

echo "  - Verifying update timestamp..."
ssh $SERVER "stat -c '%y' $REMOTE_DIR/src/sheets_sync.py"

# Verify critical files are present
echo "  - Verifying upload..."
ssh $SERVER "ls -la $REMOTE_DIR/src/*.py 2>/dev/null | wc -l" | {
    read count
    if [ "$count" -lt 5 ]; then
        echo "ERROR: Critical files missing! Restoring backup..."
        ssh $SERVER "cd $REMOTE_DIR && rm -rf src && [ -d src.backup ] && mv src.backup src || true"
        rm -f /tmp/src.tar.gz
        exit 1
    fi
}

# Cleanup
rm -f /tmp/src.tar.gz
ssh $SERVER "cd $REMOTE_DIR && rm -rf src.backup"
echo "  All files uploaded successfully"

# Upload google credentials
echo "Uploading credentials..."
scp google-credentials.json $SERVER:$REMOTE_DIR/

# Restart Flask API
echo "Restarting Flask API..."
ssh $SERVER 'sudo systemctl restart flask-api'

# Check status
echo "Checking service status..."
ssh $SERVER 'sudo systemctl status flask-api --no-pager | head -15'

echo "Deployment complete!"
