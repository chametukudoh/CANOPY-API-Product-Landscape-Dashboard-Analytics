from data_collection.config import Config
from dashboard.export_powerbi import PowerBIExporter
from database.models import DatabaseManager


def export_for_powerbi() -> None:
    """Export dashboard datasets and documentation for Power BI."""
    Config.validate()

    db = DatabaseManager(Config.DB_CONNECTION)
    exporter = PowerBIExporter(db, Config.EXPORT_PATH)

    print(f"Exporting data for last {Config.LOOKBACK_DAYS} days...")
    exporter.export_all(days_back=Config.LOOKBACK_DAYS)
    exporter.create_data_model_documentation()

    print(f"\nExport complete! Files are in: {Config.EXPORT_PATH}")
    print("\nNext steps:")
    print("1. Open Power BI Desktop")
    print("2. Get Data > Text/CSV")
    print("3. Load all CSV files from the export folder")
    print("4. Create relationships between tables")
    print("5. Build visualizations using the recommended measures")


if __name__ == "__main__":
    export_for_powerbi()
