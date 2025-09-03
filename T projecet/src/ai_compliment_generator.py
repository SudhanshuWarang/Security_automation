"""
AI Compliment Generator module.
Uses OpenAI to generate personalized compliments based on company website content.
"""

import openai
from typing import Optional, Dict, Any
from loguru import logger


class AIComplimentGenerator:
    """Handles AI-powered compliment generation using OpenAI."""
    
    def __init__(self, config):
        """
        Initialize the AI compliment generator.
        
        Args:
            config: Configuration object
        """
        self.config = config
        openai.api_key = self.config.openai_api_key
        
        # Predefined compliment templates for fallback
        self.fallback_compliments = [
            "I've been following your company's growth and I'm impressed by your innovative approach.",
            "Your company's mission really resonates with me, and I love what you're building.",
            "I've heard great things about your team and the work you're doing.",
            "Your company's recent achievements have caught my attention, and I'm excited about your vision.",
            "I appreciate the innovative solutions your company is developing.",
            "Your company's commitment to excellence is truly inspiring.",
            "I've been impressed by your company's market presence and growth trajectory.",
            "Your company's approach to solving industry challenges is remarkable.",
            "I admire your company's dedication to customer success.",
            "Your company's culture and values really stand out in the industry."
        ]
    
    def generate_compliment(self, company_name: str, website_content: str) -> str:
        """
        Generate a personalized compliment based on company information.
        
        Args:
            company_name: Name of the company
            website_content: Processed website content
            
        Returns:
            Generated compliment
        """
        try:
            # Try to generate AI-powered compliment
            ai_compliment = self._generate_ai_compliment(company_name, website_content)
            
            if ai_compliment:
                logger.info(f"Generated AI compliment for {company_name}")
                return ai_compliment
            else:
                # Fallback to predefined compliment
                fallback_compliment = self._get_fallback_compliment(company_name)
                logger.info(f"Using fallback compliment for {company_name}")
                return fallback_compliment
                
        except Exception as e:
            logger.error(f"Error generating compliment for {company_name}: {str(e)}")
            return self._get_fallback_compliment(company_name)
    
    def _generate_ai_compliment(self, company_name: str, website_content: str) -> Optional[str]:
        """
        Generate AI-powered compliment using OpenAI.
        
        Args:
            company_name: Name of the company
            website_content: Processed website content
            
        Returns:
            Generated compliment or None if failed
        """
        try:
            # Prepare the prompt
            prompt = self._create_compliment_prompt(company_name, website_content)
            
            # Call OpenAI API
            response = openai.ChatCompletion.create(
                model=self.config.openai_model,
                messages=[
                    {
                        "role": "system",
                        "content": "You are a professional business development specialist. Your task is to generate personalized, genuine compliments about companies based on their website content. The compliments should be specific, professional, and authentic. Keep them concise (1-2 sentences) and avoid generic statements."
                    },
                    {
                        "role": "user",
                        "content": prompt
                    }
                ],
                max_tokens=self.config.openai_max_tokens,
                temperature=self.config.openai_temperature
            )
            
            # Extract the generated compliment
            compliment = response.choices[0].message.content.strip()
            
            # Validate the compliment
            if self._validate_compliment(compliment):
                return compliment
            else:
                logger.warning(f"Generated compliment validation failed for {company_name}")
                return None
                
        except openai.error.RateLimitError:
            logger.warning("OpenAI rate limit exceeded")
            return None
        except openai.error.APIError as e:
            logger.error(f"OpenAI API error: {str(e)}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error generating AI compliment: {str(e)}")
            return None
    
    def _create_compliment_prompt(self, company_name: str, website_content: str) -> str:
        """
        Create a prompt for AI compliment generation.
        
        Args:
            company_name: Name of the company
            website_content: Processed website content
            
        Returns:
            Formatted prompt
        """
        prompt = f"""
        Based on the following information about {company_name}, generate a personalized, genuine compliment that could be used in a cold outreach message.

        Company Name: {company_name}
        Website Content: {website_content[:4000]}  # Limit content length

        Requirements:
        - Make it specific to what the company does
        - Keep it professional and authentic
        - Avoid generic statements
        - Focus on their achievements, innovation, or unique approach
        - Keep it to 1-2 sentences maximum
        - Make it sound natural and conversational

        Generate only the compliment text, nothing else:
        """
        
        return prompt.strip()
    
    def _validate_compliment(self, compliment: str) -> bool:
        """
        Validate the generated compliment.
        
        Args:
            compliment: Generated compliment to validate
            
        Returns:
            True if valid, False otherwise
        """
        if not compliment:
            return False
        
        # Check length
        if len(compliment) < 20 or len(compliment) > 300:
            return False
        
        # Check for common issues
        invalid_phrases = [
            "I'm sorry",
            "I cannot",
            "I don't have",
            "I'm unable",
            "I cannot provide",
            "I don't have access",
            "I'm not able",
            "I cannot generate",
            "I don't have information",
            "I cannot create"
        ]
        
        for phrase in invalid_phrases:
            if phrase.lower() in compliment.lower():
                return False
        
        # Check for excessive punctuation
        if compliment.count('!') > 2 or compliment.count('?') > 1:
            return False
        
        return True
    
    def _get_fallback_compliment(self, company_name: str) -> str:
        """
        Get a fallback compliment when AI generation fails.
        
        Args:
            company_name: Name of the company
            
        Returns:
            Fallback compliment
        """
        import random
        
        # Select a random fallback compliment
        compliment = random.choice(self.fallback_compliments)
        
        # Personalize it slightly with company name if possible
        if company_name and len(company_name) < 50:
            # Replace generic references with company name
            compliment = compliment.replace("your company", f"{company_name}")
            compliment = compliment.replace("Your company", f"{company_name}")
        
        return compliment
    
    def generate_compliments_batch(self, leads_data: list) -> list:
        """
        Generate compliments for multiple leads in batch.
        
        Args:
            leads_data: List of lead data dictionaries
            
        Returns:
            List of leads with compliments added
        """
        results = []
        
        for lead in leads_data:
            company_name = lead.get('company_name', '')
            website_content = lead.get('website_content', '')
            
            compliment = self.generate_compliment(company_name, website_content)
            lead['compliment'] = compliment
            
            results.append(lead)
        
        return results
    
    def test_openai_connection(self) -> bool:
        """
        Test the OpenAI API connection.
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            # Simple test call
            response = openai.ChatCompletion.create(
                model=self.config.openai_model,
                messages=[
                    {"role": "user", "content": "Hello"}
                ],
                max_tokens=10
            )
            
            if response.choices:
                logger.info("OpenAI API connection successful")
                return True
            else:
                logger.error("OpenAI API connection failed")
                return False
                
        except Exception as e:
            logger.error(f"Error testing OpenAI API connection: {str(e)}")
            return False
    
    def get_compliment_statistics(self, compliments: list) -> Dict[str, Any]:
        """
        Generate statistics about compliments.
        
        Args:
            compliments: List of generated compliments
            
        Returns:
            Dictionary with statistics
        """
        if not compliments:
            return {}
        
        stats = {
            'total_compliments': len(compliments),
            'ai_generated': 0,
            'fallback_used': 0,
            'average_length': 0,
            'short_compliments': 0,
            'long_compliments': 0
        }
        
        total_length = 0
        
        for compliment in compliments:
            if compliment:
                length = len(compliment)
                total_length += length
                
                if length < 50:
                    stats['short_compliments'] += 1
                elif length > 200:
                    stats['long_compliments'] += 1
        
        if stats['total_compliments'] > 0:
            stats['average_length'] = total_length / stats['total_compliments']
        
        return stats 