"""
Data Processor module.
Handles filtering, cleaning, and deduplication of job data.
"""

import re
from typing import List, Dict, Any
from loguru import logger


class DataProcessor:
    """Handles data processing, filtering, and cleaning operations."""
    
    def __init__(self, config):
        """
        Initialize the data processor.
        
        Args:
            config: Configuration object
        """
        self.config = config
        self.company_suffixes = [
            'Inc', 'LLC', 'Ltd', 'LTD', 'Corp', 'Corporation', 'Company', 'Co',
            'Limited', 'Incorporated', 'Group', 'Partners', 'Associates',
            'International', 'Intl', 'Technologies', 'Tech', 'Software', 'Systems',
            'Solutions', 'Services', 'Enterprises', 'Ventures', 'Holdings'
        ]
    
    def filter_and_clean(self, job_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Filter and clean job data according to business rules.
        
        Args:
            job_data: Raw job data from scraper
            
        Returns:
            Filtered and cleaned job data
        """
        logger.info(f"Processing {len(job_data)} job entries")
        
        # Step 1: Filter by employee count
        filtered_data = self._filter_by_employee_count(job_data)
        logger.info(f"After employee count filter: {len(filtered_data)} entries")
        
        # Step 2: Remove duplicates
        deduplicated_data = self._remove_duplicates(filtered_data)
        logger.info(f"After deduplication: {len(deduplicated_data)} entries")
        
        # Step 3: Clean company names
        cleaned_data = self._clean_company_names(deduplicated_data)
        
        # Step 4: Validate required fields
        validated_data = self._validate_required_fields(cleaned_data)
        logger.info(f"After validation: {len(validated_data)} entries")
        
        return validated_data
    
    def _filter_by_employee_count(self, job_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Filter jobs by minimum employee count.
        
        Args:
            job_data: Job data to filter
            
        Returns:
            Filtered job data
        """
        filtered_data = []
        
        for job in job_data:
            employee_count = job.get('employee_count')
            
            if employee_count is None:
                logger.warning(f"No employee count for {job.get('company_name', 'Unknown')}")
                continue
            
            try:
                # Convert to integer if it's a string
                if isinstance(employee_count, str):
                    # Extract numbers from strings like "500-1000 employees"
                    numbers = re.findall(r'\d+', employee_count)
                    if numbers:
                        employee_count = int(numbers[0])
                    else:
                        continue
                else:
                    employee_count = int(employee_count)
                
                if employee_count >= self.config.min_employee_count:
                    filtered_data.append(job)
                    
            except (ValueError, TypeError):
                logger.warning(f"Invalid employee count format: {employee_count}")
                continue
        
        return filtered_data
    
    def _remove_duplicates(self, job_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Remove duplicate job entries based on company LinkedIn URL.
        
        Args:
            job_data: Job data to deduplicate
            
        Returns:
            Deduplicated job data
        """
        seen_urls = set()
        unique_data = []
        
        for job in job_data:
            company_url = job.get('company_linkedin_url')
            
            if not company_url:
                logger.warning(f"No company LinkedIn URL for {job.get('company_name', 'Unknown')}")
                continue
            
            if company_url not in seen_urls:
                seen_urls.add(company_url)
                unique_data.append(job)
        
        return unique_data
    
    def _clean_company_names(self, job_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Clean company names by removing common suffixes.
        
        Args:
            job_data: Job data to clean
            
        Returns:
            Job data with cleaned company names
        """
        for job in job_data:
            company_name = job.get('company_name', '')
            if company_name:
                cleaned_name = self._clean_company_name(company_name)
                job['company_name_cleaned'] = cleaned_name
                job['company_name_original'] = company_name
        
        return job_data
    
    def _clean_company_name(self, company_name: str) -> str:
        """
        Clean a single company name by removing suffixes.
        
        Args:
            company_name: Company name to clean
            
        Returns:
            Cleaned company name
        """
        if not company_name:
            return company_name
        
        # Remove common suffixes
        for suffix in self.company_suffixes:
            # Match suffix with word boundaries
            pattern = rf'\b{suffix}\b'
            company_name = re.sub(pattern, '', company_name, flags=re.IGNORECASE)
        
        # Clean up extra whitespace and punctuation
        company_name = re.sub(r'\s+', ' ', company_name.strip())
        company_name = re.sub(r'[,\s]+$', '', company_name)
        
        return company_name
    
    def _validate_required_fields(self, job_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Validate that required fields are present.
        
        Args:
            job_data: Job data to validate
            
        Returns:
            Validated job data
        """
        required_fields = ['company_name', 'job_poster_name', 'company_website']
        validated_data = []
        
        for job in job_data:
            missing_fields = []
            
            for field in required_fields:
                if not job.get(field):
                    missing_fields.append(field)
            
            if missing_fields:
                logger.warning(f"Missing required fields for {job.get('company_name', 'Unknown')}: {missing_fields}")
                continue
            
            validated_data.append(job)
        
        return validated_data
    
    def extract_name_parts(self, full_name: str) -> Dict[str, str]:
        """
        Extract first and last name from full name.
        
        Args:
            full_name: Full name to parse
            
        Returns:
            Dictionary with 'first_name' and 'last_name'
        """
        if not full_name:
            return {'first_name': '', 'last_name': ''}
        
        # Split by spaces and clean up
        name_parts = [part.strip() for part in full_name.split() if part.strip()]
        
        if len(name_parts) == 0:
            return {'first_name': '', 'last_name': ''}
        elif len(name_parts) == 1:
            return {'first_name': name_parts[0], 'last_name': ''}
        else:
            return {
                'first_name': name_parts[0],
                'last_name': ' '.join(name_parts[1:])
            }
    
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
        
        # Basic email validation pattern
        pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        return bool(re.match(pattern, email))
    
    def extract_domain_from_url(self, url: str) -> str:
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
        domain = re.sub(r'^https?://', '', url)
        
        # Remove path and query parameters
        domain = domain.split('/')[0]
        
        # Remove www. prefix
        domain = re.sub(r'^www\.', '', domain)
        
        return domain
    
    def format_lead_data(self, job_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Format job data into lead data structure.
        
        Args:
            job_data: Job data to format
            
        Returns:
            Formatted lead data
        """
        name_parts = self.extract_name_parts(job_data.get('job_poster_name', ''))
        
        lead_data = {
            'first_name': name_parts['first_name'],
            'last_name': name_parts['last_name'],
            'full_name': job_data.get('job_poster_name', ''),
            'title': job_data.get('job_poster_title', ''),
            'company_name': job_data.get('company_name_cleaned', job_data.get('company_name', '')),
            'company_name_original': job_data.get('company_name_original', job_data.get('company_name', '')),
            'company_website': job_data.get('company_website', ''),
            'company_domain': self.extract_domain_from_url(job_data.get('company_website', '')),
            'company_linkedin_url': job_data.get('company_linkedin_url', ''),
            'job_title': job_data.get('job_title', ''),
            'job_location': job_data.get('job_location', ''),
            'employee_count': job_data.get('employee_count', ''),
            'company_industry': job_data.get('company_industry', ''),
            'job_url': job_data.get('job_url', ''),
            'posted_date': job_data.get('posted_date', ''),
            'scraped_at': job_data.get('scraped_at', ''),
            'email': '',  # Will be filled by email finder
            'email_confidence': '',
            'compliment': '',  # Will be filled by AI generator
            'status': 'new'
        }
        
        return lead_data 