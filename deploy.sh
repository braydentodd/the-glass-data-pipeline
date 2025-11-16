#!/bin/bash
#
# Simple deployment script for The Glass
# Deploys Python code to Oracle Cloud server
#

set -e  # Exit on error

SERVER="ubuntu@150.136.255.23"
REMOTE_DIR="/home/ubuntu/the-glass-api"

echo "📦 Deploying The Glass to $SERVER..."

# Upload src directory
echo "→ Uploading Python code..."
rsync -av --delete \
    --exclude='__pycache__' \
    --exclude='*.pyc' \
    --exclude='.git' \
    ./src/ $SERVER:$REMOTE_DIR/src/

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
