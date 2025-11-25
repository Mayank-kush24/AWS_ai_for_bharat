# Google Sheets Export Setup Guide

This guide explains how to set up Google Sheets API integration for exporting data from the dashboard.

## Prerequisites

1. A Google Cloud Project
2. Google Sheets API enabled
3. A service account with Google Sheets API access

## Step 1: Create a Google Cloud Project

1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Create a new project or select an existing one
3. Enable the **Google Sheets API**:
   - Navigate to "APIs & Services" > "Library"
   - Search for "Google Sheets API"
   - Click "Enable"

## Step 2: Create a Service Account

1. Go to "APIs & Services" > "Credentials"
2. Click "Create Credentials" > "Service Account"
3. Fill in the service account details:
   - Name: `aws-ai-bharat-export` (or any name you prefer)
   - Description: `Service account for exporting data to Google Sheets`
4. Click "Create and Continue"
5. Skip the optional steps and click "Done"

## Step 3: Create and Download Service Account Key

1. Click on the service account you just created
2. Go to the "Keys" tab
3. Click "Add Key" > "Create new key"
4. Select "JSON" format
5. Click "Create" - this will download a JSON file
6. Save this file securely (e.g., `credentials/google-sheets-credentials.json`)

## Step 4: Share Google Sheet with Service Account

1. Open your Google Sheet (or create a new one)
2. Click "Share" button
3. Add the service account email (found in the JSON file as `client_email`)
4. Give it "Editor" permissions
5. Click "Send"

## Step 5: Configure Environment Variables

Add the following to your `.env` file:

```env
# Google Sheets Configuration
GOOGLE_SHEETS_CREDENTIALS_PATH=credentials/google-sheets-credentials.json
GOOGLE_SHEET_ID=your-google-sheet-id-here
```

### Finding Your Google Sheet ID

The Google Sheet ID is found in the URL:
```
https://docs.google.com/spreadsheets/d/SHEET_ID_HERE/edit
```

For example, if your URL is:
```
https://docs.google.com/spreadsheets/d/1a2b3c4d5e6f7g8h9i0j/edit
```

Then your `GOOGLE_SHEET_ID` is: `1a2b3c4d5e6f7g8h9i0j`

## Step 6: Install Dependencies

Make sure you have installed the required Python packages:

```bash
pip install -r requirements.txt
```

This will install:
- `google-api-python-client`
- `google-auth`
- `google-auth-httplib2`

## Step 7: Test the Export

1. Start your Flask application
2. Navigate to the Dashboard
3. Click the "Export to Google Sheet" button
4. Enter your Google Sheet ID (or leave empty if set in `.env`)
5. Wait for the export to complete
6. Check your Google Sheet - it should contain:
   - Column A: Workshop Number
   - Column B: Time Slot
   - Column C: Occupation
   - Column D: Count

## Troubleshooting

### Error: "Credentials file not found"
- Make sure the path in `GOOGLE_SHEETS_CREDENTIALS_PATH` is correct
- Use an absolute path if relative paths don't work

### Error: "Permission denied" or "The caller does not have permission"
- Make sure you've shared the Google Sheet with the service account email
- Verify the service account has "Editor" permissions

### Error: "Google Sheet ID not provided"
- Either set `GOOGLE_SHEET_ID` in your `.env` file
- Or provide it when clicking the export button

### Error: "No data found to export"
- Make sure you have data in the `form_response` table
- Verify that `form_name` values start with "Workshop " (e.g., "Workshop 1", "Workshop 2")

## Security Notes

- **Never commit** the service account JSON file to version control
- Add `credentials/` to your `.gitignore`
- Keep the service account key secure
- Rotate keys periodically if compromised

## File Structure

```
project/
├── credentials/
│   └── google-sheets-credentials.json  # Service account key (DO NOT COMMIT)
├── .env                                 # Environment variables
├── google_sheets_utils.py               # Google Sheets utility module
├── app_web.py                           # Flask app with export endpoint
└── requirements.txt                     # Python dependencies
```

