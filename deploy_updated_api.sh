#!/bin/bash

# Deploy updated API to remote server
# This script copies the updated api.py file and restarts the service

SERVER="ubuntu@150.136.255.23"
REMOTE_DIR="/home/ubuntu/the-glass-api"

echo "Deploying updated API to $SERVER..."

# Copy the updated api.py file
scp src/api.py "$SERVER:$REMOTE_DIR/src/api.py"

if [ $? -eq 0 ]; then
    echo "✅ File uploaded successfully"
    
    # Restart the Flask API service
    echo "Restarting Flask API service..."
    ssh "$SERVER" "sudo systemctl restart flask-api.service"
    
    if [ $? -eq 0 ]; then
        echo "✅ API service restarted successfully"
        echo "✅ Deployment complete!"
    else
        echo "❌ Failed to restart API service"
        exit 1
    fi
else
    echo "❌ Failed to upload file"
    exit 1
fi
