from sqlalchemy import (
    create_engine, Column, Integer, String, Float, 
    Boolean, DateTime, ForeignKey, Text, Index
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker
from datetime import datetime

Base = declarative_base()

class Keyword(Base):
    """Target keywords for tracking"""
    __tablename__ = 'keywords'
    
    id = Column(Integer, primary_key=True)
    keyword = Column(String(255), unique=True, nullable=False, index=True)
    marketplace = Column(String(10), default='US')
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationships
    snapshots = relationship('SerpSnapshot', back_populates='keyword_ref')

class SerpSnapshot(Base):
    """Daily SERP snapshots for each keyword"""
    __tablename__ = 'serp_snapshots'
    
    id = Column(Integer, primary_key=True)
    keyword_id = Column(Integer, ForeignKey('keywords.id'), nullable=False)
    capture_date = Column(DateTime, nullable=False, index=True)
    marketplace = Column(String(10), default='US')
    total_results = Column(Integer)
    
    # Relationships
    keyword_ref = relationship('Keyword', back_populates='snapshots')
    results = relationship('SerpResult', back_populates='snapshot')
    
    # Composite index for efficient queries
    __table_args__ = (
        Index('idx_keyword_date', 'keyword_id', 'capture_date'),
    )

class SerpResult(Base):
    """Individual product results within a SERP snapshot"""
    __tablename__ = 'serp_results'
    
    id = Column(Integer, primary_key=True)
    snapshot_id = Column(Integer, ForeignKey('serp_snapshots.id'), nullable=False)
    asin = Column(String(20), nullable=False, index=True)
    position = Column(Integer, nullable=False)
    is_sponsored = Column(Boolean, default=False)
    title = Column(Text)
    price = Column(Float)
    currency = Column(String(3), default='USD')
    rating = Column(Float)
    review_count = Column(Integer)
    image_url = Column(Text)
    
    # Relationships
    snapshot = relationship('SerpSnapshot', back_populates='results')
    product = relationship('Product', foreign_keys=[asin], 
                          primaryjoin='SerpResult.asin==Product.asin')
    
    __table_args__ = (
        Index('idx_snapshot_position', 'snapshot_id', 'position'),
    )

class Product(Base):
    """Enriched product master data"""
    __tablename__ = 'products'
    
    id = Column(Integer, primary_key=True)
    asin = Column(String(20), unique=True, nullable=False, index=True)
    title = Column(Text)
    brand = Column(String(255), index=True)
    category = Column(String(255), index=True)
    subcategory = Column(String(255))
    marketplace = Column(String(10), default='US')
    first_seen = Column(DateTime, default=datetime.utcnow)
    last_updated = Column(DateTime, default=datetime.utcnow, 
                         onupdate=datetime.utcnow)
    
    # Current metrics
    current_price = Column(Float)
    current_rating = Column(Float)
    current_review_count = Column(Integer)
    
    # Relationships
    price_history = relationship('PriceHistory', back_populates='product')
    reviews = relationship('Review', back_populates='product')

class PriceHistory(Base):
    """Historical price tracking"""
    __tablename__ = 'price_history'
    
    id = Column(Integer, primary_key=True)
    asin = Column(String(20), ForeignKey('products.asin'), nullable=False)
    date = Column(DateTime, nullable=False, index=True)
    price = Column(Float, nullable=False)
    currency = Column(String(3), default='USD')
    
    # Relationships
    product = relationship('Product', back_populates='price_history')
    
    __table_args__ = (
        Index('idx_pricehistory_asin_date', 'asin', 'date'),
    )

class Review(Base):
    """Product reviews"""
    __tablename__ = 'reviews'
    
    id = Column(Integer, primary_key=True)
    asin = Column(String(20), ForeignKey('products.asin'), nullable=False)
    review_id = Column(String(50), unique=True)
    rating = Column(Integer, nullable=False)
    title = Column(Text)
    text = Column(Text)
    verified_purchase = Column(Boolean, default=False)
    review_date = Column(DateTime)
    helpful_votes = Column(Integer, default=0)
    captured_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationships
    product = relationship('Product', back_populates='reviews')
    
    __table_args__ = (
        Index('idx_asin_date', 'asin', 'review_date'),
    )

class Seller(Base):
    """Seller/Brand tracking"""
    __tablename__ = 'sellers'
    
    id = Column(Integer, primary_key=True)
    brand_name = Column(String(255), unique=True, nullable=False, index=True)
    marketplace = Column(String(10), default='US')
    first_seen = Column(DateTime, default=datetime.utcnow)
    product_count = Column(Integer, default=0)
    avg_rating = Column(Float)
    total_reviews = Column(Integer, default=0)

class DailyMetric(Base):
    """Aggregated daily metrics for reporting"""
    __tablename__ = 'daily_metrics'
    
    id = Column(Integer, primary_key=True)
    date = Column(DateTime, nullable=False, index=True)
    keyword_id = Column(Integer, ForeignKey('keywords.id'))
    category = Column(String(255))
    
    # Aggregated metrics
    median_price = Column(Float)
    avg_rating = Column(Float)
    total_products = Column(Integer)
    sponsored_count = Column(Integer)
    organic_count = Column(Integer)
    new_entrants = Column(Integer)
    
    __table_args__ = (
        Index('idx_date_keyword', 'date', 'keyword_id'),
    )

# Database setup utility
class DatabaseManager:
    """Manage database connections and operations"""
    
    def __init__(self, connection_string: str):
        self.engine = create_engine(connection_string)
        self.Session = sessionmaker(bind=self.engine)
    
    def create_tables(self):
        """Create all tables"""
        Base.metadata.create_all(self.engine)
    
    def get_session(self):
        """Get a new database session"""
        return self.Session()
    
    def drop_tables(self):
        """Drop all tables (use with caution!)"""
        Base.metadata.drop_all(self.engine)

# Example usage
if __name__ == "__main__":
    # Import config
    import sys
    import os
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    
    from data_collection.config import Config
    
    # Validate configuration
    Config.validate()
    
    # Create database manager with config
    db = DatabaseManager(Config.DB_CONNECTION)
    db.create_tables()
    print("✓ Database tables created successfully!")
    print(f"✓ Connected to: {Config.DB_NAME}")
    print(f"✓ Tables: keywords, serp_snapshots, serp_results, products, price_history, reviews, sellers, daily_metrics")