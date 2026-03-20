# pipeline.py
# This is the main entry point for the whole project.
# It runs all three steps in order: scrape → store → alert.
#
# You can run it manually or set it up to run automatically every 2 hours.
#
# To run once:
#     python pipeline.py
#
# To run on a schedule (every 2 hours):
#     python pipeline.py --schedule
#
# To automate with cron (Linux/Mac), add this to your crontab:
#     0 */2 * * * cd /path/to/amanzi-soweto && python pipeline.py

import sys
import time
import logging
import argparse
from datetime import datetime
from pathlib import Path

# ---------------------------------------------------------------------------
# Set up logging so we can see what's happening and save a record of each run.
# Logs go to both the terminal and a file in the logs/ folder.
# ---------------------------------------------------------------------------
BASE_DIR = Path(__file__).parent
LOG_DIR  = BASE_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.FileHandler(LOG_DIR / "pipeline.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
log = logging.getLogger("amanzi")

# ---------------------------------------------------------------------------
# Add our subfolders to the Python path so we can import from them
# ---------------------------------------------------------------------------
sys.path.insert(0, str(BASE_DIR / "scraper"))
sys.path.insert(0, str(BASE_DIR / "database"))
sys.path.insert(0, str(BASE_DIR / "notifier"))

from scraper  import JHBWaterScraper, get_soweto_alerts
from database import setup_sqlite, insert_notices
from notifier import AlertDispatcher


# ---------------------------------------------------------------------------
# Step 1: Scrape
# Visit JHB Water's website and pull back all the notices.
# ---------------------------------------------------------------------------
def step_scrape():
    log.info("Step 1 — Scraping JHB Water website...")
    scraper = JHBWaterScraper()
    df = scraper.run()
    log.info(f"  Found {len(df)} notices, {df['is_soweto'].sum()} affect Soweto")
    return df


# ---------------------------------------------------------------------------
# Step 2: Store
# Save the notices to our database so we have a history.
# ---------------------------------------------------------------------------
def step_store(df, db_path):
    log.info("Step 2 — Saving notices to database...")
    if df.empty:
        log.info("  Nothing to save.")
        return
    insert_notices(df, db_path)


# ---------------------------------------------------------------------------
# Step 3: Alert
# Check who is subscribed and send them WhatsApp/SMS alerts.
# ---------------------------------------------------------------------------
def step_alert(db_path):
    log.info("Step 3 — Sending alerts to subscribers...")
    dispatcher = AlertDispatcher(db_path=db_path)
    dispatcher.dispatch_all_active()


# ---------------------------------------------------------------------------
# Step 4: Report
# Print a summary of what happened this run — useful for checking logs.
# ---------------------------------------------------------------------------
def step_report(df):
    log.info("Step 4 — Pipeline summary")

    if df.empty:
        log.info("  No data this run.")
        return

    alerts = get_soweto_alerts(df)
    log.info(f"  Total notices scraped: {len(df)}")
    log.info(f"  Active Soweto alerts:  {len(alerts)}")

    if not alerts.empty:
        log.info("  Active alerts breakdown:")
        for _, row in alerts.iterrows():
            suburbs = row.get("affected_suburbs", [])
            if isinstance(suburbs, list):
                suburbs_str = ", ".join(suburbs)
            else:
                suburbs_str = str(suburbs)
            log.info(f"    [{row['severity']}] {row['type']} | {suburbs_str} | {row['estimated_duration']}")


# ---------------------------------------------------------------------------
# run_pipeline(db_path)
# Runs all four steps in order and logs how long it took.
# ---------------------------------------------------------------------------
def run_pipeline(db_path):
    start = datetime.now()
    log.info("")
    log.info("=" * 45)
    log.info(f"  AMANZI SOWETO — {start.strftime('%Y-%m-%d %H:%M')}")
    log.info("=" * 45)

    try:
        df = step_scrape()
        step_store(df, db_path)
        step_alert(db_path)
        step_report(df)

        seconds = (datetime.now() - start).seconds
        log.info(f"\n  Done in {seconds}s")

    except Exception as e:
        log.error(f"  Pipeline crashed: {e}", exc_info=True)
        raise  # re-raise so the scheduler knows something went wrong


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Amanzi Soweto Pipeline")
    parser.add_argument(
        "--schedule",
        action="store_true",
        help="Keep running every 2 hours instead of just once"
    )
    parser.add_argument(
        "--db",
        default=str(BASE_DIR / "amanzi_soweto.db"),
        help="Path to the SQLite database file"
    )
    args = parser.parse_args()

    # Make sure the database exists before we try to write to it
    setup_sqlite(args.db)

    if args.schedule:
        log.info("Running in scheduled mode — will repeat every 2 hours.")
        while True:
            run_pipeline(args.db)
            log.info("Sleeping for 2 hours...")
            time.sleep(2 * 60 * 60)  # 2 hours in seconds
    else:
        # Just run once
        run_pipeline(args.db)