# Product Landscape Dashboard (Analytics)

End-to-end pipeline for monitoring Amazon keyword landscapes and feeding a Power BI dashboard.

## S.T.A.R. Summary
- **Situation:** A brand preparing to launch on Amazon needs clarity on competitors, pricing, reviews, and whitespace.
- **Task:** Track target keywords over time, capturing top ASINs, pricing trends, ratings, sponsorship, and new entrants.
- **Action:** Use the Canopy API to collect SERP results, enrich ASINs, store normalized snapshots in PostgreSQL, compute metrics, and export dashboard-ready datasets.
- **Result:** A reusable analytics stack that surfaces market dynamics, identifies opportunities, and powers interactive reporting tooling.

## Architecture at a Glance
1. **Data Collection (`data_collection/`)**  
   Scheduled jobs call the Canopy REST API (`search`, `product`, `product/{asin}/reviews`) to capture SERP snapshots and product data.
2. **Storage (`database/`)**  
   SQLAlchemy models define tables for keywords, snapshots, SERP results, products, price history, reviews, sellers, and daily metrics.
3. **Processing (`data_processing/`)**  
   Transformation and metrics modules compute aggregates such as median price, share of visibility, sponsored vs organic mix, and opportunity flags.
4. **Delivery (`dashboard/`)**  
   Export scripts produce CSV outputs and documentation for Power BI or similar BI tools.

## Prerequisites
- Python 3.10+
- PostgreSQL 13+ with a database you can create tables in
- Canopy API key with access to the REST endpoints
- Power BI Desktop (or another BI tool) for visualization

## Setup
1. **Clone and create a virtual environment**
   ```powershell
   git clone <repo-url>
   cd canopy-dashboard
   python -m venv venv
   venv\Scripts\activate
   ```
2. **Install dependencies**
   ```powershell
   pip install -r requirements.txt
   ```
3. **Configure environment variables**
   Create `.env` (or copy from `.env.example`) with:
   ```
   CANOPY_API_KEY=your_api_key
   CANOPY_BASE_URL=https://rest.canopyapi.co/api/amazon
   DB_HOST=localhost
   DB_PORT=5432
DB_NAME=canopy_dashboard
DB_USER=canopy_user
   DB_PASSWORD=your_password
   MARKETPLACE=US
   DATA_COLLECTION_TIMES=06:00,12:00,18:00
   LOOKBACK_DAYS=90
   EXPORT_PATH=./powerbi_exports
   ```
4. **Prepare PostgreSQL**
   ```sql
   CREATE DATABASE canopy_dashboard;
   CREATE ROLE canopy_user LOGIN PASSWORD 'strongpassword';
   GRANT ALL PRIVILEGES ON DATABASE canopy_dashboard TO canopy_user;
   GRANT USAGE, CREATE ON SCHEMA public TO canopy_user;
   ```
5. **Initialize tables and seed starter keywords**
   ```powershell
   python init_db.py
   ```

## Running the Pipeline
- **Continuous run:** `python main.py`  
  Performs one immediate collection/metrics/opportunity cycle, then starts the scheduler. Scheduled runs default to the times in `DATA_COLLECTION_TIMES`, with metrics at 19:00.
- **Manual single run:** Inside an interactive shell:
  ```python
  from main import CanopyDashboard
  app = CanopyDashboard()
  app.collect_serp_data()
  app.compute_metrics()
  ```
  Useful for ad-hoc refreshes or testing without keeping the scheduler alive.

Leave the process running (do not close the terminal) to preserve scheduled jobs. Use Ctrl+C to stop and rerun `python main.py` when ready.

## Exporting Data for BI
```powershell
python export_data.py
```
Exports CSV files (keywords, SERP history, product master, price trends, competitive metrics, daily aggregates, share of visibility) and a `DATA_MODEL_README.md` into `EXPORT_PATH`.

## Customization Tips
- **Keywords:** Add or deactivate keywords in the `keywords` table (via SQL or an admin script) before the next collection.
- **Collection window:** Update `DATA_COLLECTION_TIMES` in `.env` and restart `main.py`.
- **Pagination:** Extend `capture_serp_snapshot` to iterate `page=2...N` if you need deeper SERP coverage.
- **BI destination:** Point `EXPORT_PATH` at a shared location (e.g., network share or blob storage) for downstream refresh jobs.

## Troubleshooting
- **401/400 from Canopy API:** Confirm the API key is valid, present in `.env`, and mapped to both `Authorization` and `API-KEY` headers. Some environments require explicitly exporting the variable before running `python main.py`.
- **Permission denied for schema public:** Grant `USAGE` and `CREATE` on the schema to the database user, then re-run `python init_db.py`.
- **Duplicate index errors during `init_db.py`:** Ensure models use unique index names (e.g., `idx_pricehistory_asin_date` vs `idx_reviews_asin_date`).
- **Scheduler stops running:** The scheduler lives inside the `main.py` process. Keep the terminal open or host the script with a process manager to avoid losing future runs.

## Project Structure
```
.
├── data_collection/     # API client, configuration, scheduler
├── data_processing/     # Transformations and metrics computation
├── database/            # SQLAlchemy models and migrations placeholder
├── dashboard/           # Power BI export utilities
├── tests/               # (future) automated tests
├── export_data.py       # Convenience wrapper for BI exports
├── init_db.py           # Database bootstrapper and keyword seeding
├── main.py              # Orchestrates collection, metrics, and scheduling
└── README.md
```

## Next Steps
- Schedule `python main.py` via Windows Task Scheduler or a service wrapper for resilience.
- Layer alerting on top of opportunity detection output (e.g., email or webhook).
- Expand data exports with additional aggregations (branding, share trend deltas, etc.).
