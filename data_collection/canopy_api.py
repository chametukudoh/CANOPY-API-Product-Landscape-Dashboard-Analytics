import logging
import re
import time
from datetime import datetime
from typing import Dict, List, Optional

import requests

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CanopyAPI:
    """Wrapper for Canopy API endpoints"""
    
    def __init__(self, api_key: str, base_url: str = "https://rest.canopyapi.co/api/amazon"):
        self.api_key = api_key
        self.base_url = base_url
        self.headers = {
            "Authorization": f"Bearer {api_key}",
            "API-KEY": api_key,
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
            status = getattr(e.response, "status_code", "unknown")
            body = getattr(e.response, "text", "")
            logger.error(f"HTTP error occurred ({status}): {e}")
            if body:
                logger.debug(f"Response body: {body}")
            raise
        except requests.exceptions.RequestException as e:
            logger.error(f"Request error occurred: {e}")
            raise

    @staticmethod
    def _normalize_price(price_info: Optional[Dict]) -> tuple[Optional[float], Optional[str], Optional[str]]:
        """Convert Canopy price payload into (value, currency, display)."""
        if not price_info:
            return None, None, None
        
        value = price_info.get("value")
        currency = price_info.get("currency")
        display = price_info.get("display")
        
        if value is None and display:
            match = re.search(r"[-+]?[0-9]*[\\.,]?[0-9]+", display)
            if match:
                try:
                    value = float(match.group(0).replace(",", ""))
                except ValueError:
                    value = None
        elif value is not None:
            try:
                value = float(value)
            except (TypeError, ValueError):
                value = None
        
        return value, currency, display
            
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
            "searchTerm": keyword,
            "marketplace": marketplace,
            "page": page
        }
        
        logger.info(f"Searching for keyword: {keyword}")
        payload = self._make_request(endpoint, params)
        search_data = (payload or {}).get("data", {}).get("amazonProductSearchResults", {})
        product_results = search_data.get("productResults") or {}
        
        return {
            "results": product_results.get("results") or [],
            "page_info": product_results.get("pageInfo"),
            "refinements": search_data.get("availableRefinements") or []
        }
    
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
        endpoint = f"product/{asin}/reviews"
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
                results = search_results.get("results", []) if isinstance(search_results, dict) else []
                if results:
                    for idx, result in enumerate(results, 1):
                        price_info = result.get("price") or {}
                        price_value, price_currency, price_display = self._normalize_price(price_info)
                        snapshot["results"].append({
                            "asin": result.get("asin"),
                            "title": result.get("title"),
                            "price": price_value,
                            "price_display": price_display,
                            "currency": price_currency or 'USD',
                            "rating": result.get("rating"),
                            "review_count": result.get("ratingsTotal"),
                            "position": idx,
                            "is_sponsored": result.get("sponsored", False),
                            "image_url": result.get("mainImageUrl"),
                            "product_url": result.get("url")
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
