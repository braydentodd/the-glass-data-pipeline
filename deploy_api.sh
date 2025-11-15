#!/bin/bash

# Deploy Flask API to cloud server
# This script uploads and sets up the Flask API on the remote server

SERVER="150.136.255.23"
USER="ubuntu"
REMOTE_DIR="/home/ubuntu/the-glass-api"

echo "📦 Deploying Flask API to $SERVER..."

# Upload deployment package
echo "Uploading files..."
scp api_deploy.tar.gz flask-api.service ${USER}@${SERVER}:/tmp/

# SSH and setup
ssh ${USER}@${SERVER} << 'ENDSSH'
set -e

echo "Setting up API directory..."
mkdir -p /home/ubuntu/the-glass-api
cd /home/ubuntu/the-glass-api

echo "Extracting files..."
tar -xzf /tmp/api_deploy.tar.gz

echo "Setting up Python virtual environment..."
python3 -m venv venv
source venv/bin/activate

echo "Installing Python dependencies..."
pip install -r requirements.txt

echo "Setting up systemd service..."
sudo cp /tmp/flask-api.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable flask-api
sudo systemctl restart flask-api

echo "Checking service status..."
sudo systemctl status flask-api --no-pager

echo "Opening firewall port 5001..."
sudo firewall-cmd --permanent --add-port=5001/tcp || true
sudo firewall-cmd --reload || true

ENDSSH

echo "✅ Deployment complete!"
echo ""
echo "Test the API:"
echo "  curl http://${SERVER}:5001/health"
