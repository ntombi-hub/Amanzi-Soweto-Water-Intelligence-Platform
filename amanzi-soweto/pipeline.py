# pipeline.py — main orchestrator: scrape → store → alert
#
# Run once:          python pipeline.py
# Run on schedule:   python pipeline.py --schedule
# Custom DB path:    python pipeline.py --db /path/to/db.sqlite

import sys
import time
import logging
import argparse
from datetime import datetime
from pathlib import Path

BASE_DIR = Path(__file__).parent
LOG_DIR  = BASE_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.FileHandler(LOG_DIR / "pipeline.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("amanzi")

sys.path.insert(0, str(BASE_DIR / "scraper"))
sys.path.insert(0, str(BASE_DIR / "database"))
sys.path.insert(0, str(BASE_DIR / "notifier"))

from scraper              import JHBWaterScraper, get_soweto_alerts
from rand_water_scraper   import RandWaterScraper, get_soweto_rand_water_alerts
from dam_scraper          import scrape_vaal_dam_level
from database             import setup_sqlite, insert_notices, insert_dam_level
from notifier             import AlertDispatcher

import pandas as pd


def step_scrape():
    log.info("Step 1 — Scraping JHB Water...")
    jhb_df = JHBWaterScraper().run()

    log.info("Step 1b — Scraping Rand Water...")
    rw_df = RandWaterScraper().run()

    frames = [f for f in [jhb_df, rw_df] if not f.empty]
    df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

    soweto_count = df["is_soweto"].sum() if not df.empty and "is_soweto" in df.columns else 0
    log.info(f"  Combined: {len(df)} notices, {soweto_count} affect Soweto")
    return df


def step_dam(db_path):
    log.info("Step 1c — Checking Vaal Dam level...")
    record = scrape_vaal_dam_level()
    if record:
        insert_dam_level(record, db_path)
        log.info(f"  Vaal Dam: {record['level_pct']}% — {record['status']} ({record['severity']})")
    else:
        log.info("  Dam level unavailable this run.")


def step_store(df, db_path):
    log.info("Step 2 — Saving to database...")
    if df.empty:
        log.info("  Nothing to save.")
        return
    insert_notices(df, db_path)


def step_alert(db_path):
    log.info("Step 3 — Sending alerts...")
    AlertDispatcher(db_path=db_path).dispatch_all_active()


def step_report(df):
    log.info("Step 4 — Summary")
    if df.empty:
        log.info("  No data this run.")
        return

    alerts = get_soweto_alerts(df)
    log.info(f"  Total notices: {len(df)}")
    log.info(f"  Soweto alerts: {len(alerts)}")

    for _, row in alerts.iterrows():
        suburbs = row.get("affected_suburbs", [])
        if isinstance(suburbs, list):
            suburbs = ", ".join(suburbs)
        log.info(f"    [{row['severity']}] {row['type']} | {suburbs} | {row['estimated_duration']}")


def run_pipeline(db_path):
    start = datetime.now()
    log.info("")
    log.info("=" * 45)
    log.info(f"  AMANZI SOWETO — {start.strftime('%Y-%m-%d %H:%M')}")
    log.info("=" * 45)

    try:
        df = step_scrape()
        step_dam(db_path)
        step_store(df, db_path)
        step_alert(db_path)
        step_report(df)
        log.info(f"\n  Done in {(datetime.now() - start).seconds}s")
    except Exception as e:
        log.error(f"  Pipeline crashed: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Amanzi Soweto Pipeline")
    parser.add_argument("--schedule", action="store_true",
                        help="Run every 2 hours instead of once")
    parser.add_argument("--db", default=str(BASE_DIR / "amanzi_soweto.db"),
                        help="Path to SQLite database")
    args = parser.parse_args()

    setup_sqlite(args.db)

    if args.schedule:
        log.info("Scheduled mode — repeating every 2 hours.")
        while True:
            run_pipeline(args.db)
            log.info("Sleeping 2 hours...")
            time.sleep(2 * 60 * 60)
    else:
        run_pipeline(args.db)
