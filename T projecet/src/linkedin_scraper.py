"""
LinkedIn Job Scraper module.
Handles scraping of LinkedIn job postings using Apify API.
"""

import requests
import time
from typing import List, Dict, Any
from loguru import logger


class LinkedInScraper:
    """Handles scraping of LinkedIn job postings using Apify API."""
    
    def __init__(self, config):
        """
        Initialize the LinkedIn scraper.
        
        Args:
            config: Configuration object
        """
        self.config = config
        self.session = requests.Session()
        self.session.headers.update({
            'Authorization': f'Bearer {self.config.apify_api_key}',
            'Content-Type': 'application/json'
        })
    
    def scrape_jobs(self) -> List[Dict[str, Any]]:
        """
        Scrape LinkedIn job postings using Apify.
        
        Returns:
            List of job data dictionaries
        """
        logger.info("Starting LinkedIn job scraping")
        
        try:
            # Prepare the API call
            url = f"{self.config.apify_base_url}/{self.config.apify_actor_id}/run-sync-get-dataset-items"
            payload = self.config.get_apify_payload()
            
            logger.info(f"Calling Apify API with payload: {payload}")
            
            # Make the API call
            response = self.session.post(url, json=payload, timeout=300)
            response.raise_for_status()
            
            job_data = response.json()
            logger.info(f"Successfully scraped {len(job_data)} job postings")
            
            # Process and clean the data
            processed_data = self._process_job_data(job_data)
            
            return processed_data
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error calling Apify API: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error during job scraping: {str(e)}")
            raise
    
    def _process_job_data(self, raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Process and clean raw job data from Apify.
        
        Args:
            raw_data: Raw job data from Apify
            
        Returns:
            Processed job data
        """
        processed_data = []
        
        for job in raw_data:
            try:
                processed_job = {
                    'job_id': job.get('jobId'),
                    'job_title': job.get('title'),
                    'company_name': job.get('companyName'),
                    'company_linkedin_url': job.get('companyLinkedinUrl'),
                    'company_website': job.get('companyWebsite'),
                    'job_poster_name': job.get('jobPosterName'),
                    'job_poster_title': job.get('jobPosterTitle'),
                    'job_location': job.get('location'),
                    'job_url': job.get('jobUrl'),
                    'posted_date': job.get('postedDate'),
                    'employee_count': job.get('employeeCount'),
                    'company_industry': job.get('companyIndustry'),
                    'company_description': job.get('companyDescription'),
                    'job_description': job.get('description'),
                    'requirements': job.get('requirements'),
                    'benefits': job.get('benefits'),
                    'salary_range': job.get('salaryRange'),
                    'job_type': job.get('jobType'),
                    'experience_level': job.get('experienceLevel'),
                    'remote_work': job.get('remoteWork'),
                    'scraped_at': job.get('scrapedAt')
                }
                
                # Remove None values
                processed_job = {k: v for k, v in processed_job.items() if v is not None}
                
                processed_data.append(processed_job)
                
            except Exception as e:
                logger.warning(f"Error processing job data: {str(e)}")
                continue
        
        logger.info(f"Processed {len(processed_data)} jobs from {len(raw_data)} raw entries")
        return processed_data
    
    def get_job_details(self, job_id: str) -> Dict[str, Any]:
        """
        Get detailed information for a specific job.
        
        Args:
            job_id: LinkedIn job ID
            
        Returns:
            Detailed job information
        """
        try:
            url = f"{self.config.apify_base_url}/{self.config.apify_actor_id}/run-sync-get-dataset-items"
            payload = {
                "count": 1,
                "scrapeCompany": True,
                "urls": [f"https://www.linkedin.com/jobs/view/{job_id}"]
            }
            
            response = self.session.post(url, json=payload, timeout=60)
            response.raise_for_status()
            
            job_data = response.json()
            if job_data:
                return self._process_job_data(job_data)[0]
            
            return {}
            
        except Exception as e:
            logger.error(f"Error getting job details for {job_id}: {str(e)}")
            return {}
    
    def search_jobs_by_keyword(self, keyword: str, limit: int = 50) -> List[Dict[str, Any]]:
        """
        Search for jobs by specific keyword.
        
        Args:
            keyword: Search keyword
            limit: Maximum number of results
            
        Returns:
            List of job data
        """
        try:
            url = f"{self.config.apify_base_url}/{self.config.apify_actor_id}/run-sync-get-dataset-items"
            payload = {
                "count": limit,
                "scrapeCompany": True,
                "urls": [
                    f"https://www.linkedin.com/jobs/search-results/"
                    f"?f_TPR={self.config.linkedin_time_range}"
                    f"&geoId={self.config.linkedin_geo_id}"
                    f"&keywords={keyword}"
                ]
            }
            
            response = self.session.post(url, json=payload, timeout=300)
            response.raise_for_status()
            
            job_data = response.json()
            return self._process_job_data(job_data)
            
        except Exception as e:
            logger.error(f"Error searching jobs by keyword '{keyword}': {str(e)}")
            return []
    
    def get_company_info(self, company_linkedin_url: str) -> Dict[str, Any]:
        """
        Get detailed company information.
        
        Args:
            company_linkedin_url: LinkedIn company URL
            
        Returns:
            Company information
        """
        try:
            url = f"{self.config.apify_base_url}/{self.config.apify_actor_id}/run-sync-get-dataset-items"
            payload = {
                "count": 1,
                "scrapeCompany": True,
                "urls": [company_linkedin_url]
            }
            
            response = self.session.post(url, json=payload, timeout=60)
            response.raise_for_status()
            
            company_data = response.json()
            if company_data:
                return company_data[0]
            
            return {}
            
        except Exception as e:
            logger.error(f"Error getting company info for {company_linkedin_url}: {str(e)}")
            return {} 