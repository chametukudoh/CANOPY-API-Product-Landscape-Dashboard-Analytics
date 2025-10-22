import requests
import time
from typing import Dict, List, Optional
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CanopyAPI:
    """Wrapper for Canopy API endpoints"""
    
    def __init__(self, api_key: str, base_url: str = "https://api.canopyapi.co"):
        self.api_key = api_key
        self.base_url = base_url
        self.headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }
        self.rate_limit_delay = 1  # seconds between requests
        
    def _make_request(self, endpoint: str, params: Dict = None) -> Dict:
        """Make API request with error handling and rate limiting"""
        url = f"{self.base_url}/{endpoint}"
        
        try:
            time.sleep(self.rate_limit_delay)
            response = requests.get(url, headers=self.headers, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP error occurred: {e}")
            raise
        except requests.exceptions.RequestException as e:
            logger.error(f"Request error occurred: {e}")
            raise
            
    def search_products(self, keyword: str, marketplace: str = "US", 
                       page: int = 1) -> Dict:
        """
        Search for products by keyword
        
        Args:
            keyword: Search term
            marketplace: Amazon marketplace (US, UK, etc.)
            page: Page number for pagination
            
        Returns:
            Dict containing search results with ASINs, prices, ratings, etc.
        """
        endpoint = "search"
        params = {
            "keyword": keyword,
            "marketplace": marketplace,
            "page": page
        }
        
        logger.info(f"Searching for keyword: {keyword}")
        return self._make_request(endpoint, params)
    
    def get_product_details(self, asin: str, marketplace: str = "US") -> Dict:
        """
        Get detailed product information
        
        Args:
            asin: Amazon Standard Identification Number
            marketplace: Amazon marketplace
            
        Returns:
            Dict containing product details including brand, category, etc.
        """
        endpoint = f"product/{asin}"
        params = {"marketplace": marketplace}
        
        logger.info(f"Fetching product details for ASIN: {asin}")
        return self._make_request(endpoint, params)
    
    def get_product_reviews(self, asin: str, marketplace: str = "US",
                           page: int = 1) -> Dict:
        """
        Get product reviews
        
        Args:
            asin: Amazon Standard Identification Number
            marketplace: Amazon marketplace
            page: Page number for pagination
            
        Returns:
            Dict containing reviews with ratings, dates, text
        """
        endpoint = f"reviews/{asin}"
        params = {
            "marketplace": marketplace,
            "page": page
        }
        
        logger.info(f"Fetching reviews for ASIN: {asin}")
        return self._make_request(endpoint, params)
    
    def capture_serp_snapshot(self, keywords: List[str], 
                             marketplace: str = "US") -> List[Dict]:
        """
        Capture complete SERP snapshot for multiple keywords
        
        Args:
            keywords: List of keywords to search
            marketplace: Amazon marketplace
            
        Returns:
            List of dicts containing snapshot data for each keyword
        """
        snapshots = []
        timestamp = datetime.utcnow()
        
        for keyword in keywords:
            try:
                search_results = self.search_products(keyword, marketplace)
                
                snapshot = {
                    "keyword": keyword,
                    "marketplace": marketplace,
                    "timestamp": timestamp,
                    "results": []
                }
                
                # Extract key metrics from search results
                if "results" in search_results:
                    for idx, result in enumerate(search_results["results"], 1):
                        snapshot["results"].append({
                            "asin": result.get("asin"),
                            "title": result.get("title"),
                            "price": result.get("price"),
                            "currency": result.get("currency"),
                            "rating": result.get("rating"),
                            "review_count": result.get("review_count"),
                            "position": idx,
                            "is_sponsored": result.get("is_sponsored", False),
                            "image_url": result.get("image_url")
                        })
                
                snapshots.append(snapshot)
                logger.info(f"Captured snapshot for '{keyword}': {len(snapshot['results'])} results")
                
            except Exception as e:
                logger.error(f"Failed to capture snapshot for '{keyword}': {e}")
                continue
                
        return snapshots
    
    def enrich_asin_data(self, asin: str, marketplace: str = "US") -> Dict:
        """
        Enrich ASIN with product details and review summary
        
        Args:
            asin: Amazon Standard Identification Number
            marketplace: Amazon marketplace
            
        Returns:
            Dict with enriched product data
        """
        enriched_data = {
            "asin": asin,
            "marketplace": marketplace,
            "timestamp": datetime.utcnow()
        }
        
        try:
            # Get product details
            product = self.get_product_details(asin, marketplace)
            enriched_data.update({
                "brand": product.get("brand"),
                "category": product.get("category"),
                "subcategory": product.get("subcategory"),
                "title": product.get("title"),
                "price": product.get("price"),
                "rating": product.get("rating"),
                "review_count": product.get("review_count")
            })
            
            # Get review sample
            reviews = self.get_product_reviews(asin, marketplace)
            if "reviews" in reviews:
                enriched_data["recent_reviews"] = reviews["reviews"][:5]  # Top 5
                
        except Exception as e:
            logger.error(f"Failed to enrich ASIN {asin}: {e}")
            
        return enriched_data