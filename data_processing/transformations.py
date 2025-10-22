"""Data transformation and processing logic"""
import logging
from datetime import datetime, timedelta
from sqlalchemy import func
from database.models import (
    SerpSnapshot, SerpResult, Product, PriceHistory, 
    DailyMetric, Keyword
)

logger = logging.getLogger(__name__)

class DataTransformer:
    """Handles data transformations and aggregations"""
    
    def __init__(self, db_session):
        self.session = db_session
    
    def update_product_from_serp(self, result: dict) -> Product:
        """Update or create product record from SERP result"""
        product = self.session.query(Product).filter(
            Product.asin == result['asin']
        ).first()
        
        if not product:
            product = Product(
                asin=result['asin'],
                title=result['title'],
                current_price=result['price'],
                current_rating=result['rating'],
                current_review_count=result['review_count']
            )
            self.session.add(product)
            logger.info(f"Created new product: {result['asin']}")
        else:
            # Update current metrics
            product.current_price = result['price']
            product.current_rating = result['rating']
            product.current_review_count = result['review_count']
            product.last_updated = datetime.utcnow()
            logger.debug(f"Updated product: {result['asin']}")
        
        # Add price history entry
        price_entry = PriceHistory(
            asin=result['asin'],
            date=datetime.utcnow(),
            price=result['price'],
            currency=result.get('currency', 'USD')
        )
        self.session.add(price_entry)
        
        return product
    
    def compute_daily_metrics(self, date: datetime = None) -> list:
        """Compute aggregated daily metrics for all keywords"""
        if date is None:
            date = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
        
        metrics_created = []
        
        # Get all active keywords
        keywords = self.session.query(Keyword).filter(
            Keyword.is_active == True
        ).all()
        
        for keyword in keywords:
            metric = self._compute_keyword_metrics(keyword, date)
            if metric:
                metrics_created.append(metric)
        
        self.session.commit()
        logger.info(f"Computed metrics for {len(metrics_created)} keywords on {date.date()}")
        return metrics_created
    
    def _compute_keyword_metrics(self, keyword: Keyword, date: datetime) -> DailyMetric:
        """Compute metrics for a single keyword on a specific date"""
        # Get today's snapshots
        snapshots = self.session.query(SerpSnapshot).filter(
            SerpSnapshot.keyword_id == keyword.id,
            func.date(SerpSnapshot.capture_date) == date.date()
        ).all()
        
        if not snapshots:
            return None
        
        # Collect all results
        all_results = []
        for snap in snapshots:
            all_results.extend(snap.results)
        
        if not all_results:
            return None
        
        # Calculate metrics
        prices = [r.price for r in all_results if r.price]
        ratings = [r.rating for r in all_results if r.rating]
        sponsored = sum(1 for r in all_results if r.is_sponsored)
        organic = sum(1 for r in all_results if not r.is_sponsored)
        
        # Detect new entrants
        prev_date = date - timedelta(days=1)
        prev_asins = self._get_asins_for_date(keyword.id, prev_date)
        current_asins = set(r.asin for r in all_results)
        new_entrants = len(current_asins - prev_asins)
        
        # Create metric record
        metric = DailyMetric(
            date=date,
            keyword_id=keyword.id,
            median_price=sorted(prices)[len(prices)//2] if prices else None,
            avg_rating=sum(ratings)/len(ratings) if ratings else None,
            total_products=len(all_results),
            sponsored_count=sponsored,
            organic_count=organic,
            new_entrants=new_entrants
        )
        
        self.session.add(metric)
        return metric
    
    def _get_asins_for_date(self, keyword_id: int, date: datetime) -> set:
        """Get unique ASINs for a keyword on a specific date"""
        snapshots = self.session.query(SerpSnapshot).filter(
            SerpSnapshot.keyword_id == keyword_id,
            func.date(SerpSnapshot.capture_date) == date.date()
        ).all()
        
        asins = set()
        for snap in snapshots:
            asins.update(r.asin for r in snap.results)
        
        return asins