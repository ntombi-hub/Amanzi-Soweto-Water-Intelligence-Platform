# dam_scraper.py — scrapes Vaal Dam level from DWS (Dept of Water & Sanitation)
# Vaal Dam feeds Rand Water → Johannesburg Water → Soweto taps.
# A level below 30% means water restrictions are likely.

import requests
from bs4 import BeautifulSoup
from datetime import datetime
import re

DWS_URL = "https://www.dws.gov.za/Hydrology/Weekly/Weekly.aspx"
BACKUP_URL = "https://www.dws.gov.za/hydrology/weekly/percentages.aspx"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}


def get_dam_status(level_pct):
    """Return a status label and severity based on dam level percentage."""
    if level_pct >= 80:
        return "Full",        "LOW"
    elif level_pct >= 50:
        return "Normal",      "LOW"
    elif level_pct >= 30:
        return "Low",         "MEDIUM"
    elif level_pct >= 15:
        return "Very Low",    "HIGH"
    else:
        return "Critical",    "HIGH"


def scrape_vaal_dam_level():
    """
    Scrape the current Vaal Dam storage level from DWS.
    Returns a dict with level_pct, status, severity, scraped_at.
    Falls back to a simulated value if the site is unreachable.
    """
    now = datetime.now().isoformat()

    for url in [DWS_URL, BACKUP_URL]:
        try:
            res = requests.get(url, headers=HEADERS, timeout=15)
            res.raise_for_status()
            soup = BeautifulSoup(res.text, "html.parser")

            # DWS pages contain tables with dam names and percentages.
            # We look for "Vaal" near a percentage value.
            text = soup.get_text(separator=" ")
            # Find "Vaal" followed within 200 chars by a percentage
            match = re.search(
                r"Vaal.{0,200}?(\d{1,3}(?:\.\d)?)\s*%",
                text, re.IGNORECASE | re.DOTALL
            )
            if match:
                level_pct = float(match.group(1))
                status, severity = get_dam_status(level_pct)
                print(f"  Vaal Dam: {level_pct}% ({status})")
                return {
                    "dam_name":   "Vaal Dam",
                    "level_pct":  level_pct,
                    "status":     status,
                    "severity":   severity,
                    "source_url": url,
                    "scraped_at": now,
                }

        except requests.RequestException as e:
            print(f"  Could not reach {url}: {e}")
            continue

    # Site unreachable — return None so the pipeline can handle it gracefully
    print("  DWS site unreachable. Dam level unavailable.")
    return None


if __name__ == "__main__":
    result = scrape_vaal_dam_level()
    if result:
        print(f"\nVaal Dam is at {result['level_pct']}% — {result['status']} ({result['severity']})")
    else:
        print("\nCould not retrieve dam level.")
