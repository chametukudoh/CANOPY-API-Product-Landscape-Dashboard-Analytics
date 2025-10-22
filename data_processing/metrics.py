"""Metrics calculation and opportunity detection"""
import logging
from datetime import datetime, timedelta
from sqlalchemy import func
from database.models import DailyMetric, Keyword

logger = logging.getLogger(__name__)

class MetricsAnalyzer:
    """Analyzes metrics and detects opportunities"""
    
    def __init__(self, db_session):
        self.session = db_session
    
    def detect_opportunities(self, days_back: int = 7) -> list:
        """
        Detect market opportunities based on recent metrics
        
        Args:
            days_back: Number of days to analyze
        
        Returns:
            List of opportunity dictionaries
        """
        cutoff = datetime.utcnow() - timedelta(days=days_back)
        
        # Get recent metrics
        metrics = self.session.query(DailyMetric).filter(
            DailyMetric.date >= cutoff
        ).all()
        
        # Group by keyword
        keyword_metrics = {}
        for m in metrics:
            if m.keyword_id not in keyword_metrics:
                keyword_metrics[m.keyword_id] = []
            keyword_metrics[m.keyword_id].append(m)
        
        opportunities = []
        
        # Analyze each keyword
        for kw_id, kw_metrics in keyword_metrics.items():
            keyword = self.session.query(Keyword).get(kw_id)
            
            # Calculate averages
            avg_products = sum(m.total_products for m in kw_metrics) / len(kw_metrics)
            avg_sponsored = sum(m.sponsored_count for m in kw_metrics) / len(kw_metrics)
            avg_price = sum(m.median_price for m in kw_metrics if m.median_price) / \
                       len([m for m in kw_metrics if m.median_price])
            
            # Low saturation opportunity
            if avg_products < 20:
                opportunities.append({
                    'type': 'low_saturation',
                    'keyword': keyword.keyword,
                    'avg_products': round(avg_products, 1),
                    'avg_price': round(avg_price, 2) if avg_price else None,
                    'priority': 'high',
                    'reason': f'Only {avg_products:.0f} products on average - low competition'
                })
            
            # Low ad competition
            if avg_sponsored < 3:
                opportunities.append({
                    'type': 'low_ad_competition',
                    'keyword': keyword.keyword,
                    'avg_sponsored_ads': round(avg_sponsored, 1),
                    'priority': 'medium',
                    'reason': f'Only {avg_sponsored:.0f} sponsored ads on average'
                })
            
            # High growth (many new entrants)
            total_new = sum(m.new_entrants for m in kw_metrics)
            if total_new > 5:
                opportunities.append({
                    'type': 'growing_market',
                    'keyword': keyword.keyword,
                    'new_entrants_count': total_new,
                    'priority': 'medium',
                    'reason': f'{total_new} new products entered in last {days_back} days'
                })
        
        logger.info(f"Detected {len(opportunities)} opportunities")
        return opportunities
    
    def get_keyword_summary(self, keyword_id: int, days_back: int = 30) -> dict:
        """Get summary statistics for a keyword"""
        cutoff = datetime.utcnow() - timedelta(days=days_back)
        
        metrics = self.session.query(DailyMetric).filter(
            DailyMetric.keyword_id == keyword_id,
            DailyMetric.date >= cutoff
        ).all()
        
        if not metrics:
            return None
        
        prices = [m.median_price for m in metrics if m.median_price]
        
        return {
            'keyword_id': keyword_id,
            'days_analyzed': len(metrics),
            'avg_products': sum(m.total_products for m in metrics) / len(metrics),
            'avg_sponsored': sum(m.sponsored_count for m in metrics) / len(metrics),
            'avg_price': sum(prices) / len(prices) if prices else None,
            'price_trend': 'increasing' if prices and prices[-1] > prices[0] else 'decreasing',
            'total_new_entrants': sum(m.new_entrants for m in metrics)
        }