"""
amanzi-soweto — Water Intelligence Platform for Soweto, Johannesburg.

Usage:
    from amanzi_soweto import JHBWaterScraper, RandWaterScraper, scrape_vaal_dam_level
    from amanzi_soweto import setup_sqlite, insert_notices, get_active_alerts
    from amanzi_soweto import AlertDispatcher, build_message
"""

from .scraper            import JHBWaterScraper, get_soweto_alerts, classify_notice, find_soweto_suburbs
from .rand_water_scraper import RandWaterScraper, get_soweto_rand_water_alerts
from .dam_scraper        import scrape_vaal_dam_level, get_dam_status
from .database           import (
    setup_sqlite, insert_notices, insert_dam_level,
    get_active_alerts, get_latest_dam_level, get_dam_history,
    subscribe, unsubscribe,
)
from .notifier           import build_message, NotificationSender, AlertDispatcher

__version__ = "1.0.0"
__author__  = "Ntombikayise Faith Sibisi"
__email__   = "Sibisintombikayise7@gmail.com"

__all__ = [
    "JHBWaterScraper",
    "RandWaterScraper",
    "scrape_vaal_dam_level",
    "get_dam_status",
    "get_soweto_alerts",
    "get_soweto_rand_water_alerts",
    "classify_notice",
    "find_soweto_suburbs",
    "setup_sqlite",
    "insert_notices",
    "insert_dam_level",
    "get_active_alerts",
    "get_latest_dam_level",
    "get_dam_history",
    "subscribe",
    "unsubscribe",
    "build_message",
    "NotificationSender",
    "AlertDispatcher",
]
