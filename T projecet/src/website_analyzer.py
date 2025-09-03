"""
Website Analyzer module.
Fetches and processes company website content for AI analysis.
"""

import requests
import html2text
from bs4 import BeautifulSoup
from typing import Optional, Dict, Any
from loguru import logger
import time


class WebsiteAnalyzer:
    """Handles fetching and processing of company website content."""
    
    def __init__(self, config):
        """
        Initialize the website analyzer.
        
        Args:
            config: Configuration object
        """
        self.config = config
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        
        # Initialize HTML to text converter
        self.html_converter = html2text.HTML2Text()
        self.html_converter.ignore_links = False
        self.html_converter.ignore_images = True
        self.html_converter.body_width = 0  # No line wrapping
    
    def get_website_content(self, url: str) -> Optional[str]:
        """
        Fetch and process website content.
        
        Args:
            url: Website URL to fetch
            
        Returns:
            Processed text content or None if failed
        """
        if not url:
            logger.warning("No URL provided")
            return None
        
        try:
            # Fetch the website
            html_content = self._fetch_website(url)
            if not html_content:
                return None
            
            # Convert HTML to text
            text_content = self._convert_html_to_text(html_content)
            if not text_content:
                return None
            
            # Clean and process the text
            cleaned_content = self._clean_text_content(text_content)
            
            logger.info(f"Successfully processed website content for {url}")
            return cleaned_content
            
        except Exception as e:
            logger.error(f"Error processing website {url}: {str(e)}")
            return None
    
    def _fetch_website(self, url: str) -> Optional[str]:
        """
        Fetch website HTML content.
        
        Args:
            url: Website URL
            
        Returns:
            HTML content or None if failed
        """
        try:
            # Ensure URL has protocol
            if not url.startswith(('http://', 'https://')):
                url = 'https://' + url
            
            logger.debug(f"Fetching website: {url}")
            
            response = self.session.get(url, timeout=30, allow_redirects=True)
            response.raise_for_status()
            
            # Check if response is HTML
            content_type = response.headers.get('content-type', '').lower()
            if 'text/html' not in content_type:
                logger.warning(f"Non-HTML content type: {content_type}")
                return None
            
            return response.text
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching website {url}: {str(e)}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error fetching website {url}: {str(e)}")
            return None
    
    def _convert_html_to_text(self, html_content: str) -> Optional[str]:
        """
        Convert HTML content to readable text.
        
        Args:
            html_content: Raw HTML content
            
        Returns:
            Converted text content
        """
        try:
            # Use html2text to convert HTML to markdown
            text_content = self.html_converter.handle(html_content)
            
            if not text_content or len(text_content.strip()) < 50:
                logger.warning("Converted text content is too short")
                return None
            
            return text_content
            
        except Exception as e:
            logger.error(f"Error converting HTML to text: {str(e)}")
            return None
    
    def _clean_text_content(self, text_content: str) -> str:
        """
        Clean and process text content.
        
        Args:
            text_content: Raw text content
            
        Returns:
            Cleaned text content
        """
        import re
        
        # Remove excessive whitespace
        text_content = re.sub(r'\s+', ' ', text_content)
        
        # Remove common unwanted elements
        unwanted_patterns = [
            r'Cookie Policy.*?Accept',
            r'Privacy Policy.*?Accept',
            r'Terms of Service.*?Accept',
            r'This website uses cookies.*?Accept',
            r'We use cookies.*?Accept',
            r'By continuing to use.*?Accept',
            r'Accept All Cookies',
            r'Reject All',
            r'Cookie Settings',
            r'Close',
            r'Skip to main content',
            r'Skip to navigation',
            r'Menu',
            r'Search',
            r'Login',
            r'Sign up',
            r'Subscribe',
            r'Newsletter',
            r'Follow us',
            r'Share this',
            r'Print',
            r'Email',
            r'Facebook',
            r'Twitter',
            r'LinkedIn',
            r'Instagram',
            r'YouTube',
            r'Contact us',
            r'About us',
            r'Careers',
            r'Blog',
            r'News',
            r'Press',
            r'Media',
            r'Investors',
            r'Partners',
            r'Support',
            r'Help',
            r'FAQ',
            r'Terms',
            r'Privacy',
            r'Legal',
            r'Copyright',
            r'All rights reserved',
            r'Powered by',
            r'Built with',
            r'Hosted by'
        ]
        
        for pattern in unwanted_patterns:
            text_content = re.sub(pattern, '', text_content, flags=re.IGNORECASE | re.DOTALL)
        
        # Remove excessive line breaks
        text_content = re.sub(r'\n\s*\n\s*\n', '\n\n', text_content)
        
        # Remove leading/trailing whitespace
        text_content = text_content.strip()
        
        # Limit content length for AI processing
        max_length = 8000  # Reasonable limit for AI processing
        if len(text_content) > max_length:
            # Try to find a good cutoff point
            sentences = text_content.split('. ')
            truncated_content = ''
            
            for sentence in sentences:
                if len(truncated_content + sentence) < max_length:
                    truncated_content += sentence + '. '
                else:
                    break
            
            text_content = truncated_content.strip()
            logger.info(f"Content truncated to {len(text_content)} characters")
        
        return text_content
    
    def extract_company_info(self, url: str) -> Dict[str, Any]:
        """
        Extract key company information from website.
        
        Args:
            url: Company website URL
            
        Returns:
            Dictionary with extracted company information
        """
        try:
            html_content = self._fetch_website(url)
            if not html_content:
                return {}
            
            soup = BeautifulSoup(html_content, 'html.parser')
            
            company_info = {
                'title': '',
                'description': '',
                'keywords': '',
                'company_name': '',
                'industry': '',
                'founded_year': '',
                'headquarters': '',
                'mission': '',
                'products': [],
                'services': []
            }
            
            # Extract title
            title_tag = soup.find('title')
            if title_tag:
                company_info['title'] = title_tag.get_text().strip()
            
            # Extract meta description
            meta_desc = soup.find('meta', attrs={'name': 'description'})
            if meta_desc:
                company_info['description'] = meta_desc.get('content', '').strip()
            
            # Extract meta keywords
            meta_keywords = soup.find('meta', attrs={'name': 'keywords'})
            if meta_keywords:
                company_info['keywords'] = meta_keywords.get('content', '').strip()
            
            # Extract company name from various sources
            company_name = self._extract_company_name(soup)
            if company_name:
                company_info['company_name'] = company_name
            
            return company_info
            
        except Exception as e:
            logger.error(f"Error extracting company info from {url}: {str(e)}")
            return {}
    
    def _extract_company_name(self, soup: BeautifulSoup) -> str:
        """
        Extract company name from various HTML elements.
        
        Args:
            soup: BeautifulSoup object
            
        Returns:
            Company name or empty string
        """
        # Try different selectors for company name
        selectors = [
            'h1',
            '.company-name',
            '.brand',
            '.logo',
            'header h1',
            'header .company-name',
            '.site-title',
            '.brand-name'
        ]
        
        for selector in selectors:
            element = soup.select_one(selector)
            if element:
                text = element.get_text().strip()
                if text and len(text) < 100:  # Reasonable company name length
                    return text
        
        return ''
    
    def test_website_accessibility(self, url: str) -> Dict[str, Any]:
        """
        Test if a website is accessible and returns useful content.
        
        Args:
            url: Website URL to test
            
        Returns:
            Dictionary with accessibility test results
        """
        try:
            # Test basic connectivity
            response = self.session.get(url, timeout=10, allow_redirects=True)
            
            test_results = {
                'accessible': response.status_code == 200,
                'status_code': response.status_code,
                'content_type': response.headers.get('content-type', ''),
                'content_length': len(response.content),
                'has_html': 'text/html' in response.headers.get('content-type', '').lower(),
                'redirects': len(response.history),
                'final_url': response.url
            }
            
            # Test content quality
            if test_results['has_html'] and test_results['content_length'] > 1000:
                text_content = self._convert_html_to_text(response.text)
                if text_content:
                    test_results['text_content_length'] = len(text_content)
                    test_results['has_meaningful_content'] = len(text_content) > 200
                else:
                    test_results['text_content_length'] = 0
                    test_results['has_meaningful_content'] = False
            else:
                test_results['text_content_length'] = 0
                test_results['has_meaningful_content'] = False
            
            return test_results
            
        except Exception as e:
            logger.error(f"Error testing website accessibility for {url}: {str(e)}")
            return {
                'accessible': False,
                'error': str(e)
            } 