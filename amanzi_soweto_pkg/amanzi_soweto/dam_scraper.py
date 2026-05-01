import requests
from bs4 import BeautifulSoup
from datetime import datetime
import re

DWS_URL    = "https://www.dws.gov.za/Hydrology/Weekly/Weekly.aspx"
BACKUP_URL = "https://www.dws.gov.za/hydrology/weekly/percentages.aspx"
HEADERS    = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}


def get_dam_status(level_pct):
    if level_pct >= 80:   return "Full",      "LOW"
    if level_pct >= 50:   return "Normal",    "LOW"
    if level_pct >= 30:   return "Low",       "MEDIUM"
    if level_pct >= 15:   return "Very Low",  "HIGH"
    return "Critical", "HIGH"


def scrape_vaal_dam_level():
    now = datetime.now().isoformat()
    for url in [DWS_URL, BACKUP_URL]:
        try:
            res = requests.get(url, headers=HEADERS, timeout=15)
            res.raise_for_status()
            text = BeautifulSoup(res.text, "html.parser").get_text(separator=" ")
            match = re.search(r"Vaal.{0,200}?(\d{1,3}(?:\.\d)?)\s*%", text, re.IGNORECASE | re.DOTALL)
            if match:
                level_pct = float(match.group(1))
                status, severity = get_dam_status(level_pct)
                return {
                    "dam_name": "Vaal Dam", "level_pct": level_pct,
                    "status": status, "severity": severity,
                    "source_url": url, "scraped_at": now,
                }
        except requests.RequestException:
            continue
    return None
