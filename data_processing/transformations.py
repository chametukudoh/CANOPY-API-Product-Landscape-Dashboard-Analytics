"""Data transformation and processing logic"""
import logging
from datetime import datetime, timedelta
from typing import List, Optional, Tuple

from sqlalchemy import func

from database.models import (
    SerpSnapshot,
    SerpResult,
    Product,
    PriceHistory,
    DailyMetric,
    Keyword,
    Review,
    Seller,
)

logger = logging.getLogger(__name__)

class DataTransformer:
    """Handles data transformations and aggregations"""
    
    def __init__(self, db_session):
        self.session = db_session
    
    def update_product_from_serp(self, result: dict) -> Tuple[Product, bool]:
        """Update or create product record from SERP result.

        Returns:
            Tuple[Product, bool]: product instance and a flag indicating if it was newly created.
        """
        product = (
            self.session.query(Product)
            .filter(Product.asin == result["asin"])
            .first()
        )
        price_value = result.get("price")
        price_currency = result.get("currency", "USD")
        rating = result.get("rating")
        review_count = result.get("review_count")

        created = False
        if not product:
            product = Product(
                asin=result["asin"],
                title=result.get("title"),
                current_price=price_value,
                current_rating=rating,
                current_review_count=review_count,
            )
            self.session.add(product)
            created = True
            logger.info(f"Created new product: {result['asin']}")
        else:
            if price_value is not None:
                product.current_price = price_value
            if rating is not None:
                product.current_rating = rating
            if review_count is not None:
                product.current_review_count = review_count
            if not product.title and result.get("title"):
                product.title = result["title"]
            product.last_updated = datetime.utcnow()
            logger.debug(f"Updated product: {result['asin']}")

        if price_value is not None:
            price_entry = PriceHistory(
                asin=result["asin"],
                date=datetime.utcnow(),
                price=price_value,
                currency=price_currency or "USD",
            )
            self.session.add(price_entry)
        else:
            logger.debug(
                "Skipping price history for %s (no price provided)", result["asin"]
            )

        return product, created

    def enrich_product_details(
        self,
        product: Product,
        enrichment: Optional[dict],
        marketplace: Optional[str],
    ) -> None:
        """Apply enrichment payload to product, seller, and review tables."""
        if not enrichment:
            return

        brand = enrichment.get("brand")
        category = enrichment.get("category")
        subcategory = enrichment.get("subcategory")
        enrichment_rating = enrichment.get("rating")
        enrichment_review_count = enrichment.get("review_count")
        price_info = enrichment.get("price")

        if brand:
            product.brand = brand
        if category:
            product.category = category
        if subcategory:
            product.subcategory = subcategory

        if isinstance(price_info, dict):
            price_value = price_info.get("value")
            if price_value is None and price_info.get("display"):
                display = price_info["display"].replace(",", "")
                digits = "".join(ch for ch in display if ch.isdigit() or ch == ".")
                try:
                    price_value = float(digits)
                except (TypeError, ValueError):
                    price_value = None
            if price_value is not None:
                product.current_price = price_value

        if enrichment_rating is not None:
            try:
                product.current_rating = float(enrichment_rating)
            except (TypeError, ValueError):
                logger.debug(
                    "Unable to coerce rating for %s from enrichment payload",
                    product.asin,
                )

        if enrichment_review_count is not None:
            try:
                product.current_review_count = int(enrichment_review_count)
            except (TypeError, ValueError):
                logger.debug(
                    "Unable to coerce review count for %s from enrichment payload",
                    product.asin,
                )

        if marketplace and not product.marketplace:
            product.marketplace = marketplace

        product.last_updated = datetime.utcnow()

        if brand:
            self._upsert_seller_metrics(brand, marketplace or product.marketplace)

        self._upsert_reviews(product.asin, enrichment.get("recent_reviews", []))

    def _upsert_seller_metrics(self, brand: str, marketplace: Optional[str]) -> None:
        seller = (
            self.session.query(Seller)
            .filter(Seller.brand_name == brand)
            .first()
        )

        if not seller:
            seller = Seller(
                brand_name=brand,
                marketplace=marketplace or "US",
                first_seen=datetime.utcnow(),
            )
            self.session.add(seller)
        else:
            if marketplace and not seller.marketplace:
                seller.marketplace = marketplace

        product_q = self.session.query(Product).filter(Product.brand == brand)
        seller.product_count = product_q.count()

        avg_rating = (
            product_q.with_entities(func.avg(Product.current_rating))
            .filter(Product.current_rating.isnot(None))
            .scalar()
        )
        if avg_rating is not None:
            seller.avg_rating = float(avg_rating)

        total_reviews = (
            product_q.with_entities(func.sum(Product.current_review_count))
            .filter(Product.current_review_count.isnot(None))
            .scalar()
        )
        if total_reviews is not None:
            seller.total_reviews = int(total_reviews)

    def _upsert_reviews(self, asin: str, recent_reviews: List[dict]) -> None:
        for review in recent_reviews or []:
            review_id = review.get("review_id") or review.get("id")
            rating = review.get("rating")

            if not review_id or rating is None:
                continue

            exists = (
                self.session.query(Review)
                .filter(Review.review_id == review_id)
                .first()
            )
            if exists:
                continue

            try:
                rating_value = int(float(rating))
            except (TypeError, ValueError):
                continue

            review_date_str = review.get("review_date") or review.get("date")
            review_date = None
            if review_date_str:
                review_date = self._parse_datetime(review_date_str)

            new_review = Review(
                asin=asin,
                review_id=review_id,
                rating=rating_value,
                title=review.get("title"),
                text=review.get("text") or review.get("body"),
                verified_purchase=bool(review.get("verified_purchase")),
                review_date=review_date,
                helpful_votes=review.get("helpful_votes") or 0,
            )
            self.session.add(new_review)

    @staticmethod
    def _parse_datetime(value: str) -> Optional[datetime]:
        try:
            if value.endswith("Z"):
                value = value[:-1] + "+00:00"
            return datetime.fromisoformat(value)
        except ValueError:
            try:
                return datetime.strptime(value, "%Y-%m-%d")
            except ValueError:
                logger.debug("Unable to parse review date: %s", value)
                return None
    
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
        prices = [r.price for r in all_results if r.price is not None]
        ratings = [r.rating for r in all_results if r.rating is not None]
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
