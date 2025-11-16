#!/bin/bash
#
# Deploy Google Apps Script to Google using clasp
# Configuration is loaded dynamically from Flask API (src/config.py)
#

set -e  # Exit on error

echo "📜 Deploying Google Apps Script..."

# Check if clasp is installed
if ! command -v clasp &> /dev/null; then
    echo "❌ Error: clasp is not installed"
    echo "   Install with: npm install -g @google/clasp"
    exit 1
fi

# Push to Google Apps Script
echo "→ Pushing to Google Apps Script..."
clasp push

echo "✅ Apps Script deployed successfully!"
echo ""
echo "To open in browser: clasp open"
