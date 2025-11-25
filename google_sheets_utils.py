"""
Google Sheets API Utility Module
Handles authentication and data export to Google Sheets
"""
import os
import json
from typing import List, Dict, Any, Optional
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import logging

logger = logging.getLogger(__name__)

class GoogleSheetsExporter:
    """Utility class for exporting data to Google Sheets"""
    
    def __init__(self, credentials_path: Optional[str] = None, sheet_id: Optional[str] = None):
        """
        Initialize Google Sheets exporter
        
        Args:
            credentials_path: Path to service account JSON credentials file
            sheet_id: Google Sheet ID (can be set later)
        """
        self.credentials_path = credentials_path or os.getenv('GOOGLE_SHEETS_CREDENTIALS_PATH')
        self.sheet_id = sheet_id or os.getenv('GOOGLE_SHEET_ID')
        self.service = None
        self._authenticate()
    
    def _authenticate(self):
        """Authenticate with Google Sheets API using service account"""
        try:
            if not self.credentials_path:
                raise ValueError("Google Sheets credentials path not provided. Set GOOGLE_SHEETS_CREDENTIALS_PATH environment variable or pass credentials_path.")
            
            if not os.path.exists(self.credentials_path):
                raise FileNotFoundError(f"Credentials file not found: {self.credentials_path}")
            
            # Load credentials
            credentials = service_account.Credentials.from_service_account_file(
                self.credentials_path,
                scopes=['https://www.googleapis.com/auth/spreadsheets']
            )
            
            # Build the service
            self.service = build('sheets', 'v4', credentials=credentials)
            logger.info("Successfully authenticated with Google Sheets API")
            
        except Exception as e:
            logger.error(f"Failed to authenticate with Google Sheets API: {str(e)}")
            raise
    
    def set_sheet_id(self, sheet_id: str):
        """Set or update the Google Sheet ID"""
        self.sheet_id = sheet_id
    
    def clear_sheet(self, range_name: str = 'Sheet1!A:Z'):
        """
        Clear all data from a sheet range
        
        Args:
            range_name: A1 notation range to clear (e.g., 'Sheet1!A1:Z1000')
        """
        try:
            if not self.sheet_id:
                raise ValueError("Google Sheet ID not set")
            
            self.service.spreadsheets().values().clear(
                spreadsheetId=self.sheet_id,
                range=range_name
            ).execute()
            logger.info(f"Cleared range: {range_name}")
            
        except HttpError as e:
            logger.error(f"Error clearing sheet: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error clearing sheet: {str(e)}")
            raise
    
    def write_data(self, data: List[List[Any]], range_name: str = 'Sheet1!A1', 
                   value_input_option: str = 'USER_ENTERED', clear_first: bool = True):
        """
        Write data to Google Sheet
        
        Args:
            data: 2D list of values to write (rows x columns)
            range_name: Starting cell in A1 notation (e.g., 'Sheet1!A1')
            value_input_option: 'USER_ENTERED' or 'RAW'
            clear_first: Whether to clear the sheet before writing
        
        Returns:
            dict: Response from Google Sheets API
        """
        try:
            if not self.sheet_id:
                raise ValueError("Google Sheet ID not set")
            
            if not data:
                logger.warning("No data to write")
                return {'updatedCells': 0}
            
            # Extract sheet name and starting cell from range_name
            if '!' in range_name:
                sheet_name, start_cell = range_name.split('!')
            else:
                sheet_name = 'Sheet1'
                start_cell = range_name
            
            # Clear sheet if requested
            if clear_first:
                full_range = f"{sheet_name}!A:Z"
                self.clear_sheet(full_range)
            
            # Prepare the data
            body = {
                'values': data
            }
            
            # Write data
            result = self.service.spreadsheets().values().update(
                spreadsheetId=self.sheet_id,
                range=range_name,
                valueInputOption=value_input_option,
                body=body
            ).execute()
            
            logger.info(f"Successfully wrote {result.get('updatedCells', 0)} cells to {range_name}")
            return result
            
        except HttpError as e:
            logger.error(f"Error writing to sheet: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error writing to sheet: {str(e)}")
            raise
    
    def append_data(self, data: List[List[Any]], range_name: str = 'Sheet1!A1',
                    value_input_option: str = 'USER_ENTERED'):
        """
        Append data to Google Sheet (without clearing existing data)
        
        Args:
            data: 2D list of values to append
            range_name: Starting range in A1 notation
            value_input_option: 'USER_ENTERED' or 'RAW'
        
        Returns:
            dict: Response from Google Sheets API
        """
        try:
            if not self.sheet_id:
                raise ValueError("Google Sheet ID not set")
            
            if not data:
                logger.warning("No data to append")
                return {'updatedCells': 0}
            
            body = {
                'values': data
            }
            
            result = self.service.spreadsheets().values().append(
                spreadsheetId=self.sheet_id,
                range=range_name,
                valueInputOption=value_input_option,
                body=body
            ).execute()
            
            logger.info(f"Successfully appended {result.get('updatedCells', 0)} cells to {range_name}")
            return result
            
        except HttpError as e:
            logger.error(f"Error appending to sheet: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error appending to sheet: {str(e)}")
            raise

