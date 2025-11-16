"""
Read document properties from Google Sheets using Apps Script API
This script helps transfer settings from the Google Sheets UI to environment variables
"""

import os
import sys
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

# Add parent directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from src.config import GOOGLE_SHEETS_CONFIG

def get_document_properties():
    """Fetch document properties from Google Sheets"""
    
    # Script ID from the Apps Script project
    SCRIPT_ID = 'AKfycbyBsWfwJbvMi9eqNm5pYKXDKq8LqxCWx4qEUlhYBQLi9R_Wo9vE0ZmjdG4MlJdpWlrp'
    
    # Set up credentials
    credentials = Credentials.from_service_account_file(
        GOOGLE_SHEETS_CONFIG['credentials_file'],
        scopes=[
            'https://www.googleapis.com/auth/spreadsheets.readonly',
            'https://www.googleapis.com/auth/script.projects'
        ]
    )
    
    # Build the Apps Script API service
    service = build('script', 'v1', credentials=credentials)
    
    # Create request body to execute a function
    request_body = {
        'function': 'getDocumentProperties',
        'devMode': False
    }
    
    try:
        # Execute the Apps Script function
        response = service.scripts().run(body=request_body, scriptId=SCRIPT_ID).execute()
        
        if 'error' in response:
            print(f"Error: {response['error']}")
            return None
        
        if 'response' in response and 'result' in response['response']:
            return response['response']['result']
        
        return None
        
    except Exception as e:
        print(f"Error calling Apps Script API: {e}")
        return None

def main():
    """Main function to read and display properties"""
    print("Reading document properties from Google Sheets...")
    
    properties = get_document_properties()
    
    if properties:
        print("\nDocument Properties:")
        print("=" * 50)
        for key, value in properties.items():
            print(f"{key} = {value}")
        
        # Export as environment variables
        print("\n" + "=" * 50)
        print("Export these as environment variables:")
        print("=" * 50)
        for key, value in properties.items():
            if key in ['HISTORICAL_MODE', 'HISTORICAL_YEARS', 'HISTORICAL_SEASONS', 'INCLUDE_CURRENT_YEAR']:
                print(f"export {key}='{value}'")
    else:
        print("Failed to read document properties")
        print("\nUsing defaults:")
        print("export HISTORICAL_MODE='years'")
        print("export HISTORICAL_YEARS='3'")
        print("export INCLUDE_CURRENT_YEAR='false'")

if __name__ == '__main__':
    main()
