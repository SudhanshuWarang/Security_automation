"""
Google Sheets Manager module.
Handles saving lead data to Google Sheets.
"""

import os
from typing import List, Dict, Any
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from loguru import logger


class GoogleSheetsManager:
    """Handles Google Sheets operations for lead data storage."""
    
    def __init__(self, config):
        """
        Initialize the Google Sheets manager.
        
        Args:
            config: Configuration object
        """
        self.config = config
        self.service = None
        self._authenticate()
    
    def _authenticate(self):
        """Authenticate with Google Sheets API."""
        try:
            # Check if credentials file exists
            if not os.path.exists(self.config.google_sheets_credentials_file):
                logger.error(f"Google Sheets credentials file not found: {self.config.google_sheets_credentials_file}")
                raise FileNotFoundError(f"Credentials file not found: {self.config.google_sheets_credentials_file}")
            
            # Load credentials
            credentials = Credentials.from_service_account_file(
                self.config.google_sheets_credentials_file,
                scopes=['https://www.googleapis.com/auth/spreadsheets']
            )
            
            # Build the service
            self.service = build('sheets', 'v4', credentials=credentials)
            
            logger.info("Google Sheets authentication successful")
            
        except Exception as e:
            logger.error(f"Google Sheets authentication failed: {str(e)}")
            raise
    
    def save_leads(self, leads_data: List[Dict[str, Any]]) -> bool:
        """
        Save leads data to Google Sheets.
        
        Args:
            leads_data: List of lead data dictionaries
            
        Returns:
            True if successful, False otherwise
        """
        try:
            if not self.service:
                logger.error("Google Sheets service not initialized")
                return False
            
            # Prepare data for sheets
            sheet_data = self._prepare_sheet_data(leads_data)
            
            # Append data to sheets
            result = self._append_to_sheet(sheet_data)
            
            if result:
                logger.info(f"Successfully saved {len(leads_data)} leads to Google Sheets")
                return True
            else:
                logger.error("Failed to save leads to Google Sheets")
                return False
                
        except Exception as e:
            logger.error(f"Error saving leads to Google Sheets: {str(e)}")
            return False
    
    def _prepare_sheet_data(self, leads_data: List[Dict[str, Any]]) -> List[List[str]]:
        """
        Prepare lead data for Google Sheets format.
        
        Args:
            leads_data: List of lead data dictionaries
            
        Returns:
            List of rows for Google Sheets
        """
        sheet_data = []
        
        for lead in leads_data:
            row = [
                lead.get('first_name', ''),
                lead.get('last_name', ''),
                lead.get('full_name', ''),
                lead.get('title', ''),
                lead.get('email', ''),
                lead.get('company_name', ''),
                lead.get('company_name_original', ''),
                lead.get('company_website', ''),
                lead.get('company_domain', ''),
                lead.get('company_linkedin_url', ''),
                lead.get('job_title', ''),
                lead.get('job_location', ''),
                lead.get('employee_count', ''),
                lead.get('company_industry', ''),
                lead.get('job_url', ''),
                lead.get('posted_date', ''),
                lead.get('scraped_at', ''),
                lead.get('email_confidence', ''),
                lead.get('compliment', ''),
                lead.get('status', 'new')
            ]
            
            # Clean and format data
            row = [str(cell).strip() if cell is not None else '' for cell in row]
            sheet_data.append(row)
        
        return sheet_data
    
    def _append_to_sheet(self, sheet_data: List[List[str]]) -> bool:
        """
        Append data to Google Sheets.
        
        Args:
            sheet_data: Data to append
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Prepare the request
            body = {
                'values': sheet_data
            }
            
            # Make the API call
            result = self.service.spreadsheets().values().append(
                spreadsheetId=self.config.google_sheets_spreadsheet_id,
                range=self.config.google_sheets_range,
                valueInputOption='RAW',
                insertDataOption='INSERT_ROWS',
                body=body
            ).execute()
            
            logger.info(f"Appended {len(sheet_data)} rows to Google Sheets")
            return True
            
        except HttpError as e:
            logger.error(f"Google Sheets API error: {str(e)}")
            return False
        except Exception as e:
            logger.error(f"Error appending to Google Sheets: {str(e)}")
            return False
    
    def get_sheet_headers(self) -> List[str]:
        """
        Get the headers from the Google Sheet.
        
        Returns:
            List of header strings
        """
        try:
            if not self.service:
                return []
            
            # Get the first row (headers)
            result = self.service.spreadsheets().values().get(
                spreadsheetId=self.config.google_sheets_spreadsheet_id,
                range=f"{self.config.google_sheets_range.split(':')[0]}:{self.config.google_sheets_range.split(':')[0]}"
            ).execute()
            
            if 'values' in result and result['values']:
                return result['values'][0]
            else:
                return []
                
        except Exception as e:
            logger.error(f"Error getting sheet headers: {str(e)}")
            return []
    
    def create_sheet_with_headers(self) -> bool:
        """
        Create a new sheet with proper headers.
        
        Returns:
            True if successful, False otherwise
        """
        try:
            headers = [
                'First Name',
                'Last Name',
                'Full Name',
                'Title',
                'Email',
                'Company Name',
                'Company Name Original',
                'Company Website',
                'Company Domain',
                'Company LinkedIn URL',
                'Job Title',
                'Job Location',
                'Employee Count',
                'Company Industry',
                'Job URL',
                'Posted Date',
                'Scraped At',
                'Email Confidence',
                'Compliment',
                'Status'
            ]
            
            # Clear existing data and add headers
            body = {
                'values': [headers]
            }
            
            # Clear the sheet first
            self.service.spreadsheets().values().clear(
                spreadsheetId=self.config.google_sheets_spreadsheet_id,
                range=self.config.google_sheets_range
            ).execute()
            
            # Add headers
            self.service.spreadsheets().values().update(
                spreadsheetId=self.config.google_sheets_spreadsheet_id,
                range=f"{self.config.google_sheets_range.split(':')[0]}:{self.config.google_sheets_range.split(':')[0]}",
                valueInputOption='RAW',
                body=body
            ).execute()
            
            logger.info("Created Google Sheet with headers")
            return True
            
        except Exception as e:
            logger.error(f"Error creating sheet with headers: {str(e)}")
            return False
    
    def get_existing_leads(self) -> List[Dict[str, Any]]:
        """
        Get existing leads from Google Sheets.
        
        Returns:
            List of existing lead data
        """
        try:
            if not self.service:
                return []
            
            # Get all data from sheet
            result = self.service.spreadsheets().values().get(
                spreadsheetId=self.config.google_sheets_spreadsheet_id,
                range=self.config.google_sheets_range
            ).execute()
            
            if 'values' not in result or not result['values']:
                return []
            
            values = result['values']
            if len(values) < 2:  # Only headers or empty
                return []
            
            # Get headers
            headers = values[0]
            
            # Convert to list of dictionaries
            leads = []
            for row in values[1:]:  # Skip header row
                lead = {}
                for i, header in enumerate(headers):
                    if i < len(row):
                        lead[header.lower().replace(' ', '_')] = row[i]
                    else:
                        lead[header.lower().replace(' ', '_')] = ''
                leads.append(lead)
            
            return leads
            
        except Exception as e:
            logger.error(f"Error getting existing leads: {str(e)}")
            return []
    
    def check_duplicate_leads(self, new_leads: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Check for duplicate leads and return only new ones.
        
        Args:
            new_leads: List of new lead data
            
        Returns:
            List of non-duplicate leads
        """
        try:
            existing_leads = self.get_existing_leads()
            
            if not existing_leads:
                return new_leads
            
            # Create set of existing email + company combinations
            existing_combinations = set()
            for lead in existing_leads:
                email = lead.get('email', '').lower()
                company = lead.get('company_name', '').lower()
                if email and company:
                    existing_combinations.add(f"{email}_{company}")
            
            # Filter out duplicates
            unique_leads = []
            for lead in new_leads:
                email = lead.get('email', '').lower()
                company = lead.get('company_name', '').lower()
                
                if email and company:
                    combination = f"{email}_{company}"
                    if combination not in existing_combinations:
                        unique_leads.append(lead)
                        existing_combinations.add(combination)
                else:
                    # If no email, check by company name only
                    if company and company not in [l.get('company_name', '').lower() for l in existing_leads]:
                        unique_leads.append(lead)
            
            logger.info(f"Found {len(new_leads) - len(unique_leads)} duplicate leads")
            return unique_leads
            
        except Exception as e:
            logger.error(f"Error checking for duplicates: {str(e)}")
            return new_leads
    
    def test_connection(self) -> bool:
        """
        Test the Google Sheets connection.
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            if not self.service:
                return False
            
            # Try to get sheet metadata
            result = self.service.spreadsheets().get(
                spreadsheetId=self.config.google_sheets_spreadsheet_id
            ).execute()
            
            logger.info("Google Sheets connection successful")
            return True
            
        except Exception as e:
            logger.error(f"Google Sheets connection failed: {str(e)}")
            return False 