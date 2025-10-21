import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    # API Settings
    CANOPY_API_KEY = os.getenv('CANOPY_API_KEY')
    CANOPY_BASE_URL = os.getenv('CANOPY_BASE_URL', 'https://api.canopyapi.co')
    
    # Database Settings
    DB_CONNECTION = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
    
    # Application Settings
    MARKETPLACE = os.getenv('MARKETPLACE', 'US')
    LOOKBACK_DAYS = int(os.getenv('LOOKBACK_DAYS', 90))
    EXPORT_PATH = os.getenv('EXPORT_PATH', './powerbi_exports')
    
    # Validation
    @classmethod
    def validate(cls):
        if not cls.CANOPY_API_KEY:
            raise ValueError("CANOPY_API_KEY is required")
        if not all([os.getenv('DB_USER'), os.getenv('DB_PASSWORD')]):
            raise ValueError("Database credentials are required")
        return True