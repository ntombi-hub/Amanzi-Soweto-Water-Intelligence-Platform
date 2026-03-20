# scraper.py
# This file grabs water notices from the Johannesburg Water website
# and checks if any of them affect Soweto suburbs.
#
# Think of it like a person visiting the JHB Water site every 2 hours,
# reading the notices, and writing down anything that mentions Soweto.

import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import re
import time


# ---------------------------------------------------------------------------
# These are all the Soweto suburbs and reservoirs we watch for.
# If a notice mentions any of these names, we flag it as Soweto-related.
# ---------------------------------------------------------------------------
SOWETO_SUBURBS = [
    "Soweto", "Orlando", "Diepkloof", "Meadowlands", "Chiawelo",
    "Dlamini", "Protea", "Naledi", "Dobsonville", "Jabulani",
    "Mofolo", "Pimville", "Rockville", "Senaoane", "Moletsane",
    "Tladi", "Mapetla", "Zola", "Emdeni", "Moroka",
    "Klipspruit", "Braamfischerville", "Doornkop", "Freedom Park"
]

# A reservoir problem upstream means the suburb downstream loses water too
SOWETO_RESERVOIRS = [
    "Chiawelo Reservoir",
    "Power Park Reservoir",
    "Braamfischerville Reservoir",
    "Orlando Reservoir"
]


# ---------------------------------------------------------------------------
# classify_notice(text)
#
# Reads the text of a notice and figures out three things:
#   1. What type is it? (emergency, planned work, low pressure, etc.)
#   2. How severe is it? HIGH = no water, MEDIUM = low pressure, LOW = FYI
#   3. How long will it last? (tries to find "4 hours" or "2 days" in the text)
# ---------------------------------------------------------------------------
def classify_notice(text):
    t = text.lower()

    # Check what kind of notice this is
    if any(word in t for word in ["planned", "maintenance", "scheduled", "upgrade"]):
        notice_type = "planned_maintenance"
    elif any(word in t for word in ["burst", "emergency", "unplanned"]):
        notice_type = "emergency_outage"
    elif any(word in t for word in ["low pressure", "reduced pressure"]):
        notice_type = "low_pressure"
    elif any(word in t for word in ["restored", "resolved", "complete"]):
        notice_type = "restoration"
    elif "leak" in t:
        notice_type = "leak"
    else:
        notice_type = "general_notice"

    # Rate how bad it is
    if any(word in t for word in ["no water", "no supply", "burst", "complete outage"]):
        severity = "HIGH"
    elif any(word in t for word in ["low pressure", "intermittent", "reduced"]):
        severity = "MEDIUM"
    else:
        severity = "LOW"

    # Try to grab a duration from the text e.g. "4 hours", "2 days"
    match = re.search(r"(\d+)\s*(hour|day|hr|minute)", t)
    duration = match.group(0) if match else "unknown"

    return {
        "type": notice_type,
        "severity": severity,
        "estimated_duration": duration
    }


# ---------------------------------------------------------------------------
# find_soweto_suburbs(text)
#
# Scans a piece of text and returns a list of any Soweto suburb names it finds.
# Example: "Chiawelo and Dlamini will have no water" → ["Chiawelo", "Dlamini"]
# ---------------------------------------------------------------------------
def find_soweto_suburbs(text):
    found = []
    for place in SOWETO_SUBURBS + SOWETO_RESERVOIRS:
        if place.lower() in text.lower():
            found.append(place)
    return found


# ---------------------------------------------------------------------------
# JHBWaterScraper
#
# The main class that does the scraping. It visits two pages on the JHB Water
# website, reads the content, and returns everything as a clean DataFrame.
# ---------------------------------------------------------------------------
class JHBWaterScraper:

    def __init__(self):
        # The two pages we check
        self.daily_url    = "https://www.johannesburgwater.co.za/daily-water-notices/"
        self.customer_url = "https://www.johannesburgwater.co.za/media/media-statement/customer-notices/"

        # We set a User-Agent so the website thinks we're a normal browser.
        # Without this, some sites block automated requests.
        self.headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            )
        }

    def get_page(self, url):
        # Fetch a URL and return a BeautifulSoup object we can search through.
        # Returns None if the request fails for any reason.
        try:
            print(f"  Fetching: {url}")
            res = requests.get(url, headers=self.headers, timeout=15)
            res.raise_for_status()  # raises an error if status is 4xx or 5xx
            return BeautifulSoup(res.text, "html.parser")
        except requests.RequestException as e:
            print(f"  Could not load {url}: {e}")
            return None

    def extract_notices(self, soup, source_url):
        # Pull readable text blocks from the page and turn each one into a notice.
        notices = []
        now = datetime.now().isoformat()

        # Try a few selectors — the site layout might change over time
        raw_blocks = []
        for selector in ["article", ".entry-content", ".post-content", "main p"]:
            elements = soup.select(selector)
            if elements:
                raw_blocks = [el.get_text(separator=" ", strip=True) for el in elements]
                break

        # If nothing matched, just grab all <p> tags as a fallback
        if not raw_blocks:
            raw_blocks = [p.get_text(strip=True) for p in soup.find_all("p")]

        for block in raw_blocks:
            # Skip anything too short — it's probably a menu item or footer text
            if len(block) < 50:
                continue

            affected_suburbs = find_soweto_suburbs(block)
            classification   = classify_notice(block)

            notices.append({
                "scraped_at":       now,
                "source_url":       source_url,
                "raw_text":         block[:1000],       # cap at 1000 chars
                "affected_suburbs": affected_suburbs,
                "is_soweto":        len(affected_suburbs) > 0,
                **classification                        # unpacks type, severity, duration
            })

        return notices

    def scrape_daily_notices(self):
        # Step 1: scrape the main daily notices page
        print("\nChecking JHB Water daily notices page...")
        soup = self.get_page(self.daily_url)
        if not soup:
            return []
        return self.extract_notices(soup, self.daily_url)

    def scrape_customer_notices(self):
        # Step 2: scrape the customer notices index, then follow each link
        print("\nChecking customer notices index...")
        soup = self.get_page(self.customer_url)
        if not soup:
            return []

        notices = []

        # Collect links to individual notice pages
        links = list({
            a["href"] for a in soup.select("a[href]")
            if "johannesburgwater" in a.get("href", "")
            or a.get("href", "").startswith("/")
        })

        # Only visit the 10 most recent notices to keep things fast
        for link in links[:10]:
            full_url = link if link.startswith("http") else \
                       "https://www.johannesburgwater.co.za" + link

            time.sleep(1)  # wait 1 second between requests — don't hammer the server

            page = self.get_page(full_url)
            if page:
                notices.extend(self.extract_notices(page, full_url))

        return notices

    def run(self):
        # Run both scrapers, combine their results, and return a clean DataFrame.
        all_notices = self.scrape_daily_notices() + self.scrape_customer_notices()

        df = pd.DataFrame(all_notices)

        if df.empty:
            print("\nNo notices found. The site might be down or its layout has changed.")
            return df

        # Drop any duplicate notices (same text appearing on multiple pages)
        df = df.drop_duplicates(subset=["raw_text"])

        print(f"\nDone! Found {len(df)} notices total, {df['is_soweto'].sum()} affect Soweto.")
        return df


# ---------------------------------------------------------------------------
# get_soweto_alerts(df)
#
# From the full list of notices, return only the ones that:
#   - mention a Soweto suburb, AND
#   - are HIGH or MEDIUM severity (things worth alerting residents about)
# ---------------------------------------------------------------------------
def get_soweto_alerts(df):
    if df.empty:
        return df
    return df[
        (df["is_soweto"] == True) &
        (df["severity"].isin(["HIGH", "MEDIUM"]))
    ].sort_values("severity")


# ---------------------------------------------------------------------------
# Run this file directly to do a quick test scrape
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    scraper = JHBWaterScraper()
    df = scraper.run()

    if not df.empty:
        df.to_csv("all_notices.csv", index=False)
        print(f"\nSaved {len(df)} notices to all_notices.csv")

        alerts = get_soweto_alerts(df)
        if not alerts.empty:
            print(f"\n--- {len(alerts)} ACTIVE SOWETO ALERTS ---")
            for _, row in alerts.iterrows():
                suburbs = ", ".join(row["affected_suburbs"]) if row["affected_suburbs"] else "unknown"
                print(f"  [{row['severity']}] {row['type']} | {suburbs} | {row['estimated_duration']}")
        else:
            print("\nNo active Soweto alerts right now. All clear.")