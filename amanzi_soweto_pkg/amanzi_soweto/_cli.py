import argparse
import sys
from .scraper            import JHBWaterScraper, get_soweto_alerts
from .rand_water_scraper import RandWaterScraper
from .dam_scraper        import scrape_vaal_dam_level
from .database           import setup_sqlite, insert_notices, insert_dam_level
from .notifier           import AlertDispatcher
import pandas as pd


def main():
    parser = argparse.ArgumentParser(description="Amanzi Soweto — water intelligence pipeline")
    parser.add_argument("--db", default="amanzi_soweto.db", help="Path to SQLite database")
    parser.add_argument("--schedule", action="store_true", help="Run every 2 hours")
    args = parser.parse_args()

    setup_sqlite(args.db)

    if args.schedule:
        import time
        print("Running in scheduled mode — every 2 hours.")
        while True:
            _run(args.db)
            time.sleep(2 * 60 * 60)
    else:
        _run(args.db)


def _run(db_path):
    frames = []
    for ScraperClass in [JHBWaterScraper, RandWaterScraper]:
        df = ScraperClass().run()
        if not df.empty:
            frames.append(df)

    dam = scrape_vaal_dam_level()
    if dam:
        insert_dam_level(dam, db_path)
        print(f"Vaal Dam: {dam['level_pct']}% — {dam['status']}")

    if frames:
        combined = pd.concat(frames, ignore_index=True)
        insert_notices(combined, db_path)
        alerts = get_soweto_alerts(combined)
        print(f"Scraped {len(combined)} notices, {len(alerts)} Soweto alerts.")

    AlertDispatcher(db_path=db_path).dispatch_all_active()
