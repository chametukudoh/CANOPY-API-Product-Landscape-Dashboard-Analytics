import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    # API Settings
    CANOPY_API_KEY = os.getenv('CANOPY_API_KEY')
    CANOPY_BASE_URL = os.getenv('CANOPY_BASE_URL', 'https://rest.canopyapi.co/api/amazon/product')
    
    # Database Settings
    DB_CONNECTION = None
    
    # Application Settings
    MARKETPLACE = os.getenv('MARKETPLACE', 'US')
    LOOKBACK_DAYS = int(os.getenv('LOOKBACK_DAYS', 90))
    EXPORT_PATH = os.getenv('EXPORT_PATH', './powerbi_exports')
    
    @classmethod
    def _build_db_connection(cls):
        """Construct the PostgreSQL connection string once all pieces are present."""
        user = os.getenv('DB_USER')
        password = os.getenv('DB_PASSWORD')
        host = os.getenv('DB_HOST')
        port = os.getenv('DB_PORT')
        name = os.getenv('DB_NAME')
        return f"postgresql://{user}:{password}@{host}:{port}/{name}"
    
    # Validation
    @classmethod
    def validate(cls):
        if not cls.CANOPY_API_KEY:
            raise ValueError("CANOPY_API_KEY is required")
        
        required_db_vars = ['DB_USER', 'DB_PASSWORD', 'DB_HOST', 'DB_PORT', 'DB_NAME']
        missing_db_vars = [var for var in required_db_vars if not os.getenv(var)]
        if missing_db_vars:
            missing = ', '.join(missing_db_vars)
            raise ValueError(f"Database configuration is incomplete. Missing: {missing}")
        
        cls.DB_CONNECTION = cls._build_db_connection()
        return True
