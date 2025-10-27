import os
from datetime import datetime, timedelta

import pandas as pd
from sqlalchemy import Integer, cast, func

from database.models import (
    DailyMetric,
    DatabaseManager,
    Keyword,
    PriceHistory,
    Product,
    SerpResult,
    SerpSnapshot,
)

class PowerBIExporter:
    """Export data for Power BI consumption"""
    
    def __init__(self, db_manager: DatabaseManager, export_path: str = "./powerbi_data"):
        self.db = db_manager
        self.export_path = export_path
        os.makedirs(export_path, exist_ok=True)
    
    def export_all(self, days_back: int = 90):
        """Export all datasets for Power BI"""
        print("Starting Power BI data export...")
        
        self.export_keywords()
        self.export_serp_history(days_back)
        self.export_product_master()
        self.export_price_trends(days_back)
        self.export_competitive_metrics(days_back)
        self.export_daily_aggregates(days_back)
        self.export_share_of_visibility(days_back)
        
        print(f"Export completed. Files saved to: {self.export_path}")
    
    def export_keywords(self):
        """Export keyword master table"""
        session = self.db.get_session()
        
        try:
            keywords = session.query(Keyword).all()
            
            df = pd.DataFrame([{
                'keyword_id': k.id,
                'keyword': k.keyword,
                'marketplace': k.marketplace,
                'is_active': k.is_active,
                'created_at': k.created_at
            } for k in keywords])
            
            filepath = os.path.join(self.export_path, 'keywords.csv')
            df.to_csv(filepath, index=False)
            print(f"Exported {len(df)} keywords")
            
        finally:
            session.close()
    
    def export_serp_history(self, days_back: int):
        """Export complete SERP history"""
        session = self.db.get_session()
        
        try:
            cutoff = datetime.utcnow() - timedelta(days=days_back)
            
            # Query with joins
            query = session.query(
                SerpResult.id,
                SerpResult.asin,
                SerpResult.position,
                SerpResult.is_sponsored,
                SerpResult.title,
                SerpResult.price,
                SerpResult.currency,
                SerpResult.rating,
                SerpResult.review_count,
                SerpSnapshot.capture_date,
                SerpSnapshot.keyword_id,
                Keyword.keyword,
                Product.brand,
                Product.category
            ).join(
                SerpSnapshot, SerpResult.snapshot_id == SerpSnapshot.id
            ).join(
                Keyword, SerpSnapshot.keyword_id == Keyword.id
            ).outerjoin(
                Product, SerpResult.asin == Product.asin
            ).filter(
                SerpSnapshot.capture_date >= cutoff
            ).order_by(
                SerpSnapshot.capture_date.desc()
            )
            
            results = query.all()
            
            df = pd.DataFrame([{
                'result_id': r[0],
                'asin': r[1],
                'position': r[2],
                'is_sponsored': r[3],
                'title': r[4],
                'price': r[5],
                'currency': r[6],
                'rating': r[7],
                'review_count': r[8],
                'capture_date': r[9],
                'keyword_id': r[10],
                'keyword': r[11],
                'brand': r[12],
                'category': r[13]
            } for r in results])
            
            filepath = os.path.join(self.export_path, 'serp_history.csv')
            df.to_csv(filepath, index=False)
            print(f"Exported {len(df)} SERP records")
            
        finally:
            session.close()
    
    def export_product_master(self):
        """Export product master data"""
        session = self.db.get_session()
        
        try:
            products = session.query(Product).all()
            
            df = pd.DataFrame([{
                'asin': p.asin,
                'title': p.title,
                'brand': p.brand,
                'category': p.category,
                'subcategory': p.subcategory,
                'marketplace': p.marketplace,
                'current_price': p.current_price,
                'current_rating': p.current_rating,
                'current_review_count': p.current_review_count,
                'first_seen': p.first_seen,
                'last_updated': p.last_updated
            } for p in products])
            
            filepath = os.path.join(self.export_path, 'products.csv')
            df.to_csv(filepath, index=False)
            print(f"Exported {len(df)} products")
            
        finally:
            session.close()
    
    def export_price_trends(self, days_back: int):
        """Export price history for trending"""
        session = self.db.get_session()
        
        try:
            cutoff = datetime.utcnow() - timedelta(days=days_back)
            
            price_history = session.query(
                PriceHistory.asin,
                PriceHistory.date,
                PriceHistory.price,
                PriceHistory.currency,
                Product.brand,
                Product.category
            ).join(
                Product, PriceHistory.asin == Product.asin
            ).filter(
                PriceHistory.date >= cutoff
            ).order_by(
                PriceHistory.date.desc()
            ).all()
            
            df = pd.DataFrame([{
                'asin': ph[0],
                'date': ph[1],
                'price': ph[2],
                'currency': ph[3],
                'brand': ph[4],
                'category': ph[5]
            } for ph in price_history])
            
            filepath = os.path.join(self.export_path, 'price_trends.csv')
            df.to_csv(filepath, index=False)
            print(f"Exported {len(df)} price records")
            
        finally:
            session.close()
    
    def export_competitive_metrics(self, days_back: int):
        """Export competitive positioning metrics"""
        session = self.db.get_session()
        
        try:
            cutoff = datetime.utcnow() - timedelta(days=days_back)
            
            # Calculate metrics by brand and keyword
            query = session.query(
                Keyword.keyword,
                Product.brand,
                func.date(SerpSnapshot.capture_date).label('date'),
                func.avg(SerpResult.position).label('avg_position'),
                func.min(SerpResult.position).label('best_position'),
                func.count(SerpResult.id).label('appearances'),
                func.avg(SerpResult.price).label('avg_price'),
                func.avg(SerpResult.rating).label('avg_rating'),
                func.sum(cast(SerpResult.is_sponsored, Integer)).label('sponsored_count')
            ).join(
                SerpSnapshot, SerpResult.snapshot_id == SerpSnapshot.id
            ).join(
                Keyword, SerpSnapshot.keyword_id == Keyword.id
            ).join(
                Product, SerpResult.asin == Product.asin
            ).filter(
                SerpSnapshot.capture_date >= cutoff,
                Product.brand != None
            ).group_by(
                Keyword.keyword,
                Product.brand,
                func.date(SerpSnapshot.capture_date)
            ).order_by(
                func.date(SerpSnapshot.capture_date).desc()
            )
            
            results = query.all()
            
            df = pd.DataFrame([{
                'keyword': r[0],
                'brand': r[1],
                'date': r[2],
                'avg_position': float(r[3]) if r[3] else None,
                'best_position': r[4],
                'appearances': r[5],
                'avg_price': float(r[6]) if r[6] else None,
                'avg_rating': float(r[7]) if r[7] else None,
                'sponsored_count': r[8]
            } for r in results])
            
            filepath = os.path.join(self.export_path, 'competitive_metrics.csv')
            df.to_csv(filepath, index=False)
            print(f"Exported {len(df)} competitive metric records")
            
        finally:
            session.close()
    
    def export_daily_aggregates(self, days_back: int):
        """Export daily aggregated metrics"""
        session = self.db.get_session()
        
        try:
            cutoff = datetime.utcnow() - timedelta(days=days_back)
            
            metrics = session.query(
                DailyMetric,
                Keyword.keyword
            ).join(
                Keyword, DailyMetric.keyword_id == Keyword.id
            ).filter(
                DailyMetric.date >= cutoff
            ).all()
            
            df = pd.DataFrame([{
                'date': m[0].date,
                'keyword': m[1],
                'category': m[0].category,
                'median_price': m[0].median_price,
                'avg_rating': m[0].avg_rating,
                'total_products': m[0].total_products,
                'sponsored_count': m[0].sponsored_count,
                'organic_count': m[0].organic_count,
                'new_entrants': m[0].new_entrants
            } for m in metrics])
            
            filepath = os.path.join(self.export_path, 'daily_aggregates.csv')
            df.to_csv(filepath, index=False)
            print(f"Exported {len(df)} daily aggregate records")
            
        finally:
            session.close()
    
    def export_share_of_visibility(self, days_back: int):
        """Export share of visibility by ASIN/Brand"""
        session = self.db.get_session()
        
        try:
            cutoff = datetime.utcnow() - timedelta(days=days_back)
            
            # Calculate total appearances per keyword per day
            total_query = session.query(
                Keyword.keyword,
                func.date(SerpSnapshot.capture_date).label('date'),
                func.count(SerpResult.id).label('total_appearances')
            ).join(
                SerpSnapshot, SerpResult.snapshot_id == SerpSnapshot.id
            ).join(
                Keyword, SerpSnapshot.keyword_id == Keyword.id
            ).filter(
                SerpSnapshot.capture_date >= cutoff
            ).group_by(
                Keyword.keyword,
                func.date(SerpSnapshot.capture_date)
            ).subquery()
            
            # Calculate per-ASIN/brand appearances
            visibility_query = session.query(
                Keyword.keyword,
                func.date(SerpSnapshot.capture_date).label('date'),
                SerpResult.asin,
                Product.brand,
                func.count(SerpResult.id).label('asin_appearances'),
                func.avg(SerpResult.position).label('avg_position'),
                total_query.c.total_appearances
            ).join(
                SerpSnapshot, SerpResult.snapshot_id == SerpSnapshot.id
            ).join(
                Keyword, SerpSnapshot.keyword_id == Keyword.id
            ).join(
                Product, SerpResult.asin == Product.asin
            ).join(
                total_query,
                (Keyword.keyword == total_query.c.keyword) &
                (func.date(SerpSnapshot.capture_date) == total_query.c.date)
            ).filter(
                SerpSnapshot.capture_date >= cutoff
            ).group_by(
                Keyword.keyword,
                func.date(SerpSnapshot.capture_date),
                SerpResult.asin,
                Product.brand,
                total_query.c.total_appearances
            )
            
            results = visibility_query.all()
            
            df = pd.DataFrame([{
                'keyword': r[0],
                'date': r[1],
                'asin': r[2],
                'brand': r[3],
                'appearances': r[4],
                'avg_position': float(r[5]) if r[5] else None,
                'total_keyword_appearances': r[6],
                'share_of_visibility': (r[4] / r[6] * 100) if r[6] else 0
            } for r in results])
            
            filepath = os.path.join(self.export_path, 'share_of_visibility.csv')
            df.to_csv(filepath, index=False)
            print(f"Exported {len(df)} visibility records")
            
        finally:
            session.close()
    
    def create_data_model_documentation(self):
        """Create documentation for Power BI data model"""
        doc = """
# Canopy Dashboard - Power BI Data Model Documentation

## Tables and Relationships

### 1. keywords.csv (Dimension)
- Primary Key: keyword_id
- Contains all tracked keywords

### 2. products.csv (Dimension)
- Primary Key: asin
- Master product information

### 3. serp_history.csv (Fact)
- Granularity: Individual SERP result per snapshot
- Foreign Keys: keyword_id, asin
- Use for: Position tracking, sponsored analysis, search result composition

### 4. price_trends.csv (Fact)
- Granularity: Daily price per ASIN
- Foreign Key: asin
- Use for: Price trend analysis, competitive pricing

### 5. competitive_metrics.csv (Aggregated Fact)
- Granularity: Brand x Keyword x Date
- Use for: Brand-level competitive analysis

### 6. daily_aggregates.csv (Aggregated Fact)
- Granularity: Keyword x Date
- Foreign Key: keyword (text)
- Use for: Market-level trends, opportunity identification

### 7. share_of_visibility.csv (Calculated Fact)
- Granularity: ASIN x Keyword x Date
- Foreign Keys: keyword (text), asin
- Use for: Share of voice analysis, competitive positioning

## Suggested Power BI Measures

### Price Metrics
- Median Price = MEDIAN(price_trends[price])
- Price Range = MAX(price_trends[price]) - MIN(price_trends[price])
- Price Percentile 25 = PERCENTILE.INC(price_trends[price], 0.25)
- Price Percentile 75 = PERCENTILE.INC(price_trends[price], 0.75)

### Position Metrics
- Avg Position = AVERAGE(serp_history[position])
- Top 10 Rate = DIVIDE(COUNTROWS(FILTER(serp_history, [position] <= 10)), COUNTROWS(serp_history))
- Position Improvement = [Avg Position Current Period] - [Avg Position Previous Period]

### Visibility Metrics
- Total Share of Visibility = SUM(share_of_visibility[share_of_visibility])
- Visibility Rank = RANKX(ALL(products[brand]), [Total Share of Visibility], , DESC)

### Sponsored Metrics
- Sponsored Rate = DIVIDE(COUNTROWS(FILTER(serp_history, [is_sponsored] = TRUE)), COUNTROWS(serp_history))
- Avg Sponsored Position = CALCULATE(AVERAGE(serp_history[position]), serp_history[is_sponsored] = TRUE)

### Opportunity Metrics
- Low Competition Keywords = COUNTROWS(FILTER(daily_aggregates, [total_products] < 20))
- New Entrants This Week = SUM(daily_aggregates[new_entrants])

## Recommended Visualizations

1. **Competitive Landscape Matrix**: Scatter plot of Avg Price vs Avg Rating by brand
2. **Share of Visibility Trends**: Area chart over time by top brands
3. **Position Heatmap**: Matrix of keywords (rows) x brands (columns) showing avg position
4. **Price Corridors**: Line chart with percentile bands
5. **Sponsored vs Organic**: Stacked bar chart by keyword
6. **New Entrant Tracker**: Table with sparklines showing new ASINs per keyword
7. **Opportunity Dashboard**: Cards showing low-competition keywords with filters

## Data Refresh Schedule
- Full refresh recommended: Daily at 8 PM (after metrics computation)
- Incremental refresh: Supported for fact tables (90 days rolling window)
"""
        
        filepath = os.path.join(self.export_path, 'DATA_MODEL_README.md')
        with open(filepath, 'w') as f:
            f.write(doc)
        print("Created data model documentation")

# Example usage
if __name__ == "__main__":
    DB_CONNECTION = "postgresql://user:password@localhost:5432/canopy_dashboard"
    
    db = DatabaseManager(DB_CONNECTION)
    exporter = PowerBIExporter(db, export_path="./powerbi_exports")
    
    # Export all data for the last 90 days
    exporter.export_all(days_back=90)
    
    # Create documentation
    exporter.create_data_model_documentation()
    
    print("\nData export complete! Import CSVs into Power BI to build your dashboard.")
