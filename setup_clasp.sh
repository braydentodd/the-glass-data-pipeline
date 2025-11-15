#!/bin/bash

# Install clasp globally (if not already installed)
echo "Installing clasp (Google Apps Script CLI)..."
npm install -g @google/clasp

# Login to Google (opens browser)
echo "Logging in to Google..."
clasp login

# Clone your existing Apps Script project
echo "Cloning your Apps Script project..."
echo "You'll need the Script ID from your Apps Script project."
echo "Get it from: Extensions > Apps Script > Project Settings > Script ID"
echo ""
read -p "Enter your Script ID: " SCRIPT_ID

clasp clone "$SCRIPT_ID"

echo ""
echo "✅ Setup complete! Now you can:"
echo "   1. Edit google_apps_script.gs locally"
echo "   2. Run 'clasp push' to upload changes"
echo "   3. Run 'clasp open' to open in browser"
