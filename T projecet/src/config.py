"""
Configuration module for LinkedIn Lead Automation.
Handles all environment variables and settings.
"""

import os
from typing import List


class Config:
    """Configuration class for the LinkedIn Lead Automation system."""
    
    def __init__(self):
        """Initialize configuration from environment variables."""
        # API Keys
        self.apify_api_key = os.getenv('APIFY_API_KEY')
        self.prospeo_api_key = os.getenv('PROSPEO_API_KEY')
        self.openai_api_key = os.getenv('OPENAI_API_KEY')
        self.smartlead_api_key = os.getenv('SMARTLEAD_API_KEY')
        self.heyreach_api_key = os.getenv('HEYREACH_API_KEY')
        
        # Google Sheets Configuration
        self.google_sheets_credentials_file = os.getenv('GOOGLE_SHEETS_CREDENTIALS_FILE', 'credentials.json')
        self.google_sheets_spreadsheet_id = os.getenv('GOOGLE_SHEETS_SPREADSHEET_ID')
        self.google_sheets_range = os.getenv('GOOGLE_SHEETS_RANGE', 'A:Z')
        
        # Smartlead Configuration
        self.smartlead_campaign_id = os.getenv('SMARTLEAD_CAMPAIGN_ID')
        self.smartlead_base_url = os.getenv('SMARTLEAD_BASE_URL', 'https://api.smartlead.co')
        
        # HeyReach Configuration
        self.heyreach_campaign_id = os.getenv('HEYREACH_CAMPAIGN_ID')
        self.heyreach_base_url = os.getenv('HEYREACH_BASE_URL', 'https://api.heyreach.com')
        
        # Workflow Configuration
        self.min_employee_count = int(os.getenv('MIN_EMPLOYEE_COUNT', '200'))
        self.max_leads_per_run = int(os.getenv('MAX_LEADS_PER_RUN', '100'))
        self.wait_time_between_requests = float(os.getenv('WAIT_TIME_BETWEEN_REQUESTS', '1'))
        self.enable_debug_mode = os.getenv('ENABLE_DEBUG_MODE', 'false').lower() == 'true'
        
        # LinkedIn Search Configuration
        self.linkedin_keywords = os.getenv('LINKEDIN_KEYWORDS', 'SDR,Sales Development Representative').split(',')
        self.linkedin_time_range = os.getenv('LINKEDIN_TIME_RANGE', 'r604800')
        self.linkedin_geo_id = os.getenv('LINKEDIN_GEO_ID', '103644278')
        
        # Apify Configuration
        self.apify_actor_id = 'hKByXkMQaC5Qt9UMN'
        self.apify_base_url = 'https://api.apify.com/v2/acts'
        
        # OpenAI Configuration
        self.openai_model = 'gpt-3.5-turbo'
        self.openai_max_tokens = 150
        self.openai_temperature = 0.7
        
        # Prospeo Configuration
        self.prospeo_base_url = 'https://api.prospeo.io'
        self.prospeo_validation_level = 'VALID'
        
        # Validation
        self._validate_config()
    
    def _validate_config(self):
        """Validate that all required configuration is present."""
        required_keys = [
            'apify_api_key',
            'prospeo_api_key', 
            'openai_api_key',
            'google_sheets_spreadsheet_id',
            'smartlead_api_key',
            'smartlead_campaign_id',
            'heyreach_api_key',
            'heyreach_campaign_id'
        ]
        
        missing_keys = []
        for key in required_keys:
            if not getattr(self, key):
                missing_keys.append(key)
        
        if missing_keys:
            raise ValueError(f"Missing required configuration keys: {', '.join(missing_keys)}")
    
    def get_linkedin_search_urls(self) -> List[str]:
        """Generate LinkedIn search URLs based on keywords."""
        urls = []
        for keyword in self.linkedin_keywords:
            url = (
                f"https://www.linkedin.com/jobs/search-results/"
                f"?f_TPR={self.linkedin_time_range}"
                f"&geoId={self.linkedin_geo_id}"
                f"&keywords={keyword.strip()}"
            )
            urls.append(url)
        return urls
    
    def get_apify_payload(self) -> dict:
        """Generate the payload for Apify API calls."""
        return {
            "count": self.max_leads_per_run,
            "scrapeCompany": True,
            "urls": self.get_linkedin_search_urls()
        }
    
    def __str__(self):
        """String representation of configuration (without sensitive data)."""
        return f"""
        Configuration:
        - Min Employee Count: {self.min_employee_count}
        - Max Leads Per Run: {self.max_leads_per_run}
        - Wait Time: {self.wait_time_between_requests}s
        - Debug Mode: {self.enable_debug_mode}
        - LinkedIn Keywords: {self.linkedin_keywords}
        - LinkedIn Time Range: {self.linkedin_time_range}
        - LinkedIn Geo ID: {self.linkedin_geo_id}
        """ 