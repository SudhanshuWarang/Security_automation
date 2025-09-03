"""
HeyReach Manager module.
Handles sending leads to HeyReach LinkedIn messaging campaigns.
"""

import requests
import time
from typing import Dict, Any, Optional
from loguru import logger


class HeyReachManager:
    """Handles HeyReach API operations for LinkedIn messaging campaigns."""
    
    def __init__(self, config):
        """
        Initialize the HeyReach manager.
        
        Args:
            config: Configuration object
        """
        self.config = config
        self.session = requests.Session()
        self.session.headers.update({
            'Authorization': f'Bearer {self.config.heyreach_api_key}',
            'Content-Type': 'application/json'
        })
    
    def add_lead(self, lead_data: Dict[str, Any]) -> bool:
        """
        Add a lead to HeyReach LinkedIn campaign.
        
        Args:
            lead_data: Lead data dictionary
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Prepare the lead data for HeyReach
            heyreach_data = self._prepare_lead_data(lead_data)
            
            # Add lead to campaign
            result = self._add_lead_to_campaign(heyreach_data)
            
            if result:
                logger.info(f"Successfully added lead {lead_data.get('full_name', 'Unknown')} to HeyReach")
                return True
            else:
                logger.error(f"Failed to add lead {lead_data.get('full_name', 'Unknown')} to HeyReach")
                return False
                
        except Exception as e:
            logger.error(f"Error adding lead to HeyReach: {str(e)}")
            return False
    
    def _prepare_lead_data(self, lead_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Prepare lead data for HeyReach API.
        
        Args:
            lead_data: Original lead data
            
        Returns:
            Formatted data for HeyReach
        """
        heyreach_data = {
            'campaign_id': self.config.heyreach_campaign_id,
            'first_name': lead_data.get('first_name', ''),
            'last_name': lead_data.get('last_name', ''),
            'email': lead_data.get('email', ''),
            'company': lead_data.get('company_name', ''),
            'title': lead_data.get('title', ''),
            'website': lead_data.get('company_website', ''),
            'linkedin_url': lead_data.get('company_linkedin_url', ''),
            'location': lead_data.get('job_location', ''),
            'industry': lead_data.get('company_industry', ''),
            'employee_count': lead_data.get('employee_count', ''),
            'custom_fields': {
                'compliment': lead_data.get('compliment', ''),
                'job_title': lead_data.get('job_title', ''),
                'job_url': lead_data.get('job_url', ''),
                'posted_date': lead_data.get('posted_date', ''),
                'scraped_at': lead_data.get('scraped_at', ''),
                'email_confidence': lead_data.get('email_confidence', ''),
                'company_domain': lead_data.get('company_domain', ''),
                'company_name_original': lead_data.get('company_name_original', ''),
                'full_name': lead_data.get('full_name', '')
            }
        }
        
        # Remove empty values
        heyreach_data = {k: v for k, v in heyreach_data.items() if v}
        heyreach_data['custom_fields'] = {k: v for k, v in heyreach_data['custom_fields'].items() if v}
        
        return heyreach_data
    
    def _add_lead_to_campaign(self, lead_data: Dict[str, Any]) -> bool:
        """
        Add lead to HeyReach campaign via API.
        
        Args:
            lead_data: Formatted lead data
            
        Returns:
            True if successful, False otherwise
        """
        try:
            url = f"{self.config.heyreach_base_url}/api/v1/leads"
            
            response = self.session.post(url, json=lead_data, timeout=30)
            
            if response.status_code == 200 or response.status_code == 201:
                logger.debug(f"Lead added to HeyReach successfully")
                return True
            elif response.status_code == 409:
                logger.warning(f"Lead already exists in HeyReach campaign")
                return True  # Consider this a success
            elif response.status_code == 429:
                logger.warning("HeyReach rate limit exceeded")
                time.sleep(60)  # Wait 1 minute
                return False
            else:
                logger.error(f"HeyReach API error: {response.status_code} - {response.text}")
                return False
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Request error adding lead to HeyReach: {str(e)}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error adding lead to HeyReach: {str(e)}")
            return False
    
    def add_leads_batch(self, leads_data: list) -> Dict[str, int]:
        """
        Add multiple leads to HeyReach in batch.
        
        Args:
            leads_data: List of lead data dictionaries
            
        Returns:
            Dictionary with success/failure counts
        """
        results = {
            'success': 0,
            'failed': 0,
            'duplicates': 0
        }
        
        for lead in leads_data:
            try:
                success = self.add_lead(lead)
                if success:
                    results['success'] += 1
                else:
                    results['failed'] += 1
                
                # Rate limiting
                time.sleep(self.config.wait_time_between_requests)
                
            except Exception as e:
                logger.error(f"Error processing lead {lead.get('full_name', 'Unknown')}: {str(e)}")
                results['failed'] += 1
        
        logger.info(f"HeyReach batch results: {results}")
        return results
    
    def get_campaign_info(self) -> Optional[Dict[str, Any]]:
        """
        Get information about the HeyReach campaign.
        
        Returns:
            Campaign information or None
        """
        try:
            url = f"{self.config.heyreach_base_url}/api/v1/campaigns/{self.config.heyreach_campaign_id}"
            
            response = self.session.get(url, timeout=30)
            
            if response.status_code == 200:
                return response.json()
            else:
                logger.error(f"Error getting campaign info: {response.status_code}")
                return None
                
        except Exception as e:
            logger.error(f"Error getting HeyReach campaign info: {str(e)}")
            return None
    
    def get_campaign_leads(self, limit: int = 100) -> list:
        """
        Get leads from the HeyReach campaign.
        
        Args:
            limit: Maximum number of leads to retrieve
            
        Returns:
            List of campaign leads
        """
        try:
            url = f"{self.config.heyreach_base_url}/api/v1/campaigns/{self.config.heyreach_campaign_id}/leads"
            params = {'limit': limit}
            
            response = self.session.get(url, params=params, timeout=30)
            
            if response.status_code == 200:
                return response.json().get('leads', [])
            else:
                logger.error(f"Error getting campaign leads: {response.status_code}")
                return []
                
        except Exception as e:
            logger.error(f"Error getting HeyReach campaign leads: {str(e)}")
            return []
    
    def test_connection(self) -> bool:
        """
        Test the HeyReach API connection.
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            # Try to get campaign info
            campaign_info = self.get_campaign_info()
            
            if campaign_info:
                logger.info("HeyReach API connection successful")
                return True
            else:
                logger.error("HeyReach API connection failed")
                return False
                
        except Exception as e:
            logger.error(f"Error testing HeyReach API connection: {str(e)}")
            return False
    
    def validate_lead_data(self, lead_data: Dict[str, Any]) -> bool:
        """
        Validate lead data before sending to HeyReach.
        
        Args:
            lead_data: Lead data to validate
            
        Returns:
            True if valid, False otherwise
        """
        required_fields = ['first_name', 'last_name', 'company']
        
        for field in required_fields:
            if not lead_data.get(field):
                logger.warning(f"Missing required field: {field}")
                return False
        
        # For HeyReach, email is optional but preferred
        email = lead_data.get('email', '')
        if email and not self._validate_email(email):
            logger.warning(f"Invalid email format: {email}")
            return False
        
        return True
    
    def _validate_email(self, email: str) -> bool:
        """
        Validate email format.
        
        Args:
            email: Email to validate
            
        Returns:
            True if valid, False otherwise
        """
        if not email:
            return False
        
        import re
        pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        return bool(re.match(pattern, email))
    
    def get_campaign_statistics(self) -> Dict[str, Any]:
        """
        Get statistics for the HeyReach campaign.
        
        Returns:
            Dictionary with campaign statistics
        """
        try:
            campaign_info = self.get_campaign_info()
            
            if not campaign_info:
                return {}
            
            stats = {
                'campaign_name': campaign_info.get('name', ''),
                'total_leads': campaign_info.get('total_leads', 0),
                'active_leads': campaign_info.get('active_leads', 0),
                'sent_messages': campaign_info.get('sent_messages', 0),
                'opened_messages': campaign_info.get('opened_messages', 0),
                'clicked_messages': campaign_info.get('clicked_messages', 0),
                'replied_messages': campaign_info.get('replied_messages', 0),
                'accepted_connections': campaign_info.get('accepted_connections', 0),
                'status': campaign_info.get('status', '')
            }
            
            return stats
            
        except Exception as e:
            logger.error(f"Error getting campaign statistics: {str(e)}")
            return {}
    
    def send_test_message(self, lead_data: Dict[str, Any]) -> bool:
        """
        Send a test message to verify campaign setup.
        
        Args:
            lead_data: Lead data for test message
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Prepare test message data
            test_data = {
                'campaign_id': self.config.heyreach_campaign_id,
                'lead_id': 'test',
                'message_type': 'test',
                'content': f"Test message for {lead_data.get('full_name', 'Test Lead')}"
            }
            
            url = f"{self.config.heyreach_base_url}/api/v1/messages/test"
            
            response = self.session.post(url, json=test_data, timeout=30)
            
            if response.status_code == 200:
                logger.info("Test message sent successfully")
                return True
            else:
                logger.error(f"Test message failed: {response.status_code}")
                return False
                
        except Exception as e:
            logger.error(f"Error sending test message: {str(e)}")
            return False 