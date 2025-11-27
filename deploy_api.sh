#!/bin/bash
#
# Deploy Flask API to Oracle Cloud server
# Configuration is centralized in src/config.py (SERVER_CONFIG)
#

set -e  # Exit on error

SERVER="ubuntu@150.136.255.23"
REMOTE_DIR="/home/ubuntu/the-glass-api"

echo "📦 Deploying Flask API to $SERVER..."

# Upload src directory using tar (more reliable than rsync)
echo "→ Uploading Python code..."
tar czf /tmp/src.tar.gz --exclude='__pycache__' --exclude='*.pyc' --exclude='.git' ./src/
scp /tmp/src.tar.gz $SERVER:$REMOTE_DIR/
ssh $SERVER "cd $REMOTE_DIR && rm -rf src && tar xzf src.tar.gz && rm src.tar.gz"
rm /tmp/src.tar.gz

# Upload google credentials
echo "→ Uploading credentials..."
scp google-credentials.json $SERVER:$REMOTE_DIR/

# Restart Flask API
echo "→ Restarting Flask API..."
ssh $SERVER 'sudo systemctl restart flask-api'

# Check status
echo "→ Checking service status..."
ssh $SERVER 'sudo systemctl status flask-api --no-pager | head -15'

echo "✅ Deployment complete!"
