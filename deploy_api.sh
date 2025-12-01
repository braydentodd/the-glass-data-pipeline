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
echo "Uploading..."
tar czf /tmp/src.tar.gz --exclude='__pycache__' --exclude='*.pyc' --exclude='.git' ./src/
scp /tmp/src.tar.gz $SERVER:$REMOTE_DIR/
ssh $SERVER "cd $REMOTE_DIR && rm -rf src && tar xzf src.tar.gz && rm src.tar.gz"
rm /tmp/src.tar.gz

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
