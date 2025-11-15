#!/bin/bash

# Deploy sync script and dependencies to remote server

SERVER="ubuntu@150.136.255.23"
REMOTE_DIR="/home/ubuntu/the-glass-api"

# Load environment variables from .env
if [ -f .env ]; then
    export $(grep -v '^#' .env | xargs)
else
    echo "❌ .env file not found"
    exit 1
fi

echo "Deploying sync scripts to $SERVER..."

# Copy necessary files for sync
scp sync_sheets.sh "$SERVER:$REMOTE_DIR/"
scp google-credentials.json "$SERVER:$REMOTE_DIR/"
scp -r src/*.py "$SERVER:$REMOTE_DIR/src/"

if [ $? -eq 0 ]; then
    echo "✅ Files uploaded successfully"
    
    # Make sync_sheets.sh executable and set up .env on remote
    echo "Setting permissions and environment..."
    ssh "$SERVER" "chmod +x $REMOTE_DIR/sync_sheets.sh && echo 'DB_PASSWORD=$DB_PASSWORD' > $REMOTE_DIR/.env"
    
    if [ $? -eq 0 ]; then
        echo "✅ Permissions set and environment configured"
        echo "✅ Deployment complete!"
    else
        echo "❌ Failed to set permissions or configure environment"
        exit 1
    fi
else
    echo "❌ Failed to upload files"
    exit 1
fi
