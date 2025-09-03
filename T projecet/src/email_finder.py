"""
Email Finder module.
Uses Prospeo API to find email addresses for job posters.
"""

import requests
import time
from typing import Dict, Any, Optional
from loguru import logger


class EmailFinder:
    """Handles email discovery using Prospeo API."""
    
    def __init__(self, config):
        """
        Initialize the email finder.
        
        Args:
            config: Configuration object
        """
        self.config = config
        self.session = requests.Session()
        self.session.headers.update({
            'Authorization': f'Bearer {self.config.prospeo_api_key}',
            'Content-Type': 'application/json'
        })
    
    def find_email(self, lead_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Find email address for a lead using Prospeo API.
        
        Args:
            lead_data: Lead data containing name and company information
            
        Returns:
            Email data dictionary or None if not found
        """
        try:
            # Extract name parts
            full_name = lead_data.get('job_poster_name', '')
            if not full_name:
                logger.warning("No job poster name provided")
                return None
            
            name_parts = full_name.split()
            if len(name_parts) < 2:
                logger.warning(f"Insufficient name parts: {full_name}")
                return None
            
            first_name = name_parts[0]
            last_name = ' '.join(name_parts[1:])
            company_website = lead_data.get('company_website', '')
            
            if not company_website:
                logger.warning(f"No company website for {full_name}")
                return None
            
            # Call Prospeo API
            email_data = self._call_prospeo_api(first_name, last_name, company_website)
            
            if email_data and email_data.get('email'):
                logger.info(f"Found email for {full_name}: {email_data['email']}")
                return email_data
            else:
                logger.warning(f"No email found for {full_name}")
                return None
                
        except Exception as e:
            logger.error(f"Error finding email for {lead_data.get('job_poster_name', 'Unknown')}: {str(e)}")
            return None
    
    def _call_prospeo_api(self, first_name: str, last_name: str, company_website: str) -> Optional[Dict[str, Any]]:
        """
        Call Prospeo API to find email address.
        
        Args:
            first_name: First name of the person
            last_name: Last name of the person
            company_website: Company website domain
            
        Returns:
            Email data from API or None
        """
        try:
            # Prepare the API payload
            payload = {
                'firstName': first_name,
                'lastName': last_name,
                'domain': self._extract_domain(company_website)
            }
            
            # Make API call
            url = f"{self.config.prospeo_base_url}/api/v1/email-finder"
            response = self.session.post(url, json=payload, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                
                # Check if email is valid
                if data.get('email') and data.get('status') == 'VALID':
                    return {
                        'email': data['email'],
                        'confidence': data.get('confidence', 'unknown'),
                        'status': data.get('status', 'unknown'),
                        'source': 'prospeo'
                    }
                else:
                    logger.debug(f"Email not found or invalid for {first_name} {last_name}")
                    return None
                    
            elif response.status_code == 429:
                logger.warning("Rate limit exceeded for Prospeo API")
                time.sleep(60)  # Wait 1 minute before retrying
                return None
            else:
                logger.error(f"Prospeo API error: {response.status_code} - {response.text}")
                return None
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Request error calling Prospeo API: {str(e)}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error calling Prospeo API: {str(e)}")
            return None
    
    def _extract_domain(self, url: str) -> str:
        """
        Extract domain from URL.
        
        Args:
            url: URL to extract domain from
            
        Returns:
            Domain name
        """
        if not url:
            return ''
        
        # Remove protocol
        domain = url.replace('https://', '').replace('http://', '')
        
        # Remove path and query parameters
        domain = domain.split('/')[0]
        
        # Remove www. prefix
        domain = domain.replace('www.', '')
        
        return domain
    
    def validate_email(self, email: str) -> bool:
        """
        Validate email format.
        
        Args:
            email: Email to validate
            
        Returns:
            True if valid, False otherwise
        """
        if not email:
            return False
        
        # Basic email validation
        import re
        pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        return bool(re.match(pattern, email))
    
    def find_emails_batch(self, leads_data: list) -> list:
        """
        Find emails for multiple leads in batch.
        
        Args:
            leads_data: List of lead data dictionaries
            
        Returns:
            List of leads with email data added
        """
        results = []
        
        for lead in leads_data:
            email_data = self.find_email(lead)
            
            if email_data:
                lead['email'] = email_data['email']
                lead['email_confidence'] = email_data.get('confidence', 'unknown')
                lead['email_status'] = email_data.get('status', 'unknown')
            else:
                lead['email'] = ''
                lead['email_confidence'] = ''
                lead['email_status'] = 'not_found'
            
            results.append(lead)
            
            # Rate limiting
            time.sleep(self.config.wait_time_between_requests)
        
        return results
    
    def test_api_connection(self) -> bool:
        """
        Test the Prospeo API connection.
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            url = f"{self.config.prospeo_base_url}/api/v1/health"
            response = self.session.get(url, timeout=10)
            
            if response.status_code == 200:
                logger.info("Prospeo API connection successful")
                return True
            else:
                logger.error(f"Prospeo API connection failed: {response.status_code}")
                return False
                
        except Exception as e:
            logger.error(f"Error testing Prospeo API connection: {str(e)}")
            return False 