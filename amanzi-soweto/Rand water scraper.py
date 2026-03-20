# rand_water_scraper.py
# This file scrapes maintenance and outage notices from Rand Water.
#
# Why Rand Water matters for Soweto:
# Rand Water is the BULK supplier — they pump water from the Vaal Dam
# and send it to Johannesburg Water, who then distributes it to suburbs.
# So a Rand Water maintenance = Soweto residents lose water at the tap.
#
# The systems that feed Soweto specifically are:
#   - Palmiet system    → Soweto, South Hills, Lenasia, Orange Farm
#   - Eikenhof system   → Soweto South, Lenasia South
#   - Zwartkopjes system→ parts of Johannesburg including Soweto
#   - Daleside system   → Soweto and surrounding areas
#
# Rand Water publishes notices at: randwater.co.za/mediastatements.php
# They also release PDF documents for major maintenance events.

import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import re
import time


# ---------------------------------------------------------------------------
# These are the Rand Water supply systems that affect Soweto.
# If a notice mentions any of these, it could mean Soweto loses water.
# ---------------------------------------------------------------------------
SOWETO_SYSTEMS = [
    "Palmiet",
    "Eikenhof",
    "Zwartkopjes",
    "Daleside",
    "Zuikerbosch",   # treatment plant that feeds Johannesburg
    "Mapleton",      # booster station connected to Soweto supply
]

# Suburbs and areas we watch for in Rand Water notices
SOWETO_KEYWORDS = [
    "Soweto", "Lenasia", "Orange Farm", "South Hills",
    "Johannesburg Water", "Johannesburg south", "Nasrec",
    "Eldorado Park", "Ennerdale", "Turffontein",
    # Also watch for the JHB Water zones that Rand Water supplies
    "Palmiet system", "Eikenhof system", "Zwartkopjes system"
]


# ---------------------------------------------------------------------------
# classify_rand_water_notice(text)
#
# Same idea as the JHB Water classifier but tuned for Rand Water language.
# Rand Water notices tend to use words like "planned maintenance",
# "shutdown", "pumping capacity reduced", "bulk supply".
# ---------------------------------------------------------------------------
def classify_rand_water_notice(text):
    t = text.lower()

    # Rand Water always does planned maintenance — rarely emergency
    if any(word in t for word in ["planned maintenance", "scheduled", "shutdown"]):
        notice_type = "planned_maintenance"
    elif any(word in t for word in ["emergency", "burst", "unplanned", "failure"]):
        notice_type = "emergency_outage"
    elif any(word in t for word in ["restored", "completed", "resume"]):
        notice_type = "restoration"
    else:
        notice_type = "general_notice"

    # Severity — Rand Water maintenance is almost always HIGH impact
    if any(word in t for word in ["no water", "no supply", "complete shutdown", "no pumping"]):
        severity = "HIGH"
    elif any(word in t for word in ["reduced", "low pressure", "intermittent", "capacity reduced"]):
        severity = "MEDIUM"
    else:
        severity = "LOW"

    # Try to find a duration
    match = re.search(r"(\d+)\s*(hour|day|hr|week)", t)
    duration = match.group(0) if match else "unknown"

    # Also try to find specific dates e.g. "29 May to 2 June"
    date_match = re.search(
        r"(\d{1,2}\s+\w+)\s+(?:to|until|–|-)\s+(\d{1,2}\s+\w+)", text
    )
    if date_match:
        duration = f"{date_match.group(1)} to {date_match.group(2)}"

    return {
        "type":               notice_type,
        "severity":           severity,
        "estimated_duration": duration,
        "source":             "Rand Water"   # so we know where it came from
    }


# ---------------------------------------------------------------------------
# affects_soweto(text)
#
# Returns True if the notice could affect Soweto water supply.
# We check both suburb names AND supply system names.
# ---------------------------------------------------------------------------
def affects_soweto(text):
    t = text.lower()
    return any(keyword.lower() in t for keyword in SOWETO_SYSTEMS + SOWETO_KEYWORDS)


# ---------------------------------------------------------------------------
# find_affected_areas(text)
#
# Pulls out any Soweto-related keywords mentioned in the notice.
# e.g. "Palmiet system affecting Soweto and Lenasia" → ["Palmiet", "Soweto", "Lenasia"]
# ---------------------------------------------------------------------------
def find_affected_areas(text):
    found = []
    for keyword in SOWETO_SYSTEMS + SOWETO_KEYWORDS:
        if keyword.lower() in text.lower():
            found.append(keyword)
    # Remove duplicates while keeping order
    return list(dict.fromkeys(found))


# ---------------------------------------------------------------------------
# RandWaterScraper
#
# Scrapes Rand Water's media statements page for maintenance notices.
# Rand Water also releases PDFs for major events — we handle those too.
# ---------------------------------------------------------------------------
class RandWaterScraper:

    def __init__(self):
        # Main pages to check
        self.media_url      = "https://www.randwater.co.za/mediastatements.php"
        self.corporate_url  = "https://www.randwater.co.za/Corporate/MediaStatements"

        # Pretend to be a browser
        self.headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            )
        }

    def get_page(self, url):
        # Fetch a page and return parsed HTML.
        # Returns None if it fails.
        try:
            print(f"  Fetching: {url}")
            res = requests.get(url, headers=self.headers, timeout=15)
            res.raise_for_status()
            return BeautifulSoup(res.text, "html.parser")
        except requests.RequestException as e:
            print(f"  Could not load {url}: {e}")
            return None

    def extract_notices(self, soup, source_url):
        # Pull text blocks from the page and turn each into a notice dict.
        notices = []
        now = datetime.now().isoformat()

        # Try common content containers
        raw_blocks = []
        for selector in [".media-statement", "article", ".news-item", ".entry-content", "main p"]:
            elements = soup.select(selector)
            if elements:
                raw_blocks = [el.get_text(separator=" ", strip=True) for el in elements]
                break

        # Fall back to all paragraphs
        if not raw_blocks:
            raw_blocks = [p.get_text(strip=True) for p in soup.find_all("p")]

        for block in raw_blocks:
            if len(block) < 50:
                continue

            soweto_affected = affects_soweto(block)
            affected_areas  = find_affected_areas(block) if soweto_affected else []
            classification  = classify_rand_water_notice(block)

            notices.append({
                "scraped_at":       now,
                "source_url":       source_url,
                "raw_text":         block[:1500],
                "affected_suburbs": affected_areas,
                "is_soweto":        soweto_affected,
                **classification
            })

        return notices

    def scrape_media_statements(self):
        # Scrape the main media statements listing page
        print("\nChecking Rand Water media statements...")
        soup = self.get_page(self.media_url)

        if not soup:
            # Try the alternate URL if the first one fails
            print("  Trying alternate URL...")
            soup = self.get_page(self.corporate_url)

        if not soup:
            print("  Rand Water site unreachable. Skipping.")
            return []

        notices = self.extract_notices(soup, self.media_url)

        # Also follow links to individual statements
        # Rand Water often has "Read more" links to full notice pages
        links = [
            a["href"] for a in soup.select("a[href]")
            if "randwater" in a.get("href", "").lower()
            and any(word in a.get("href", "").lower()
                    for word in ["maintenance", "notice", "statement", "media"])
        ]

        for link in list(set(links))[:8]:
            full_url = link if link.startswith("http") else "https://www.randwater.co.za" + link
            time.sleep(1)
            page = self.get_page(full_url)
            if page:
                notices.extend(self.extract_notices(page, full_url))

        return notices

    def run(self):
        # Run the scraper and return a clean DataFrame
        all_notices = self.scrape_media_statements()

        df = pd.DataFrame(all_notices)

        if df.empty:
            print("\nNo Rand Water notices found.")
            return df

        # Remove duplicates
        df = df.drop_duplicates(subset=["raw_text"])

        soweto_count = df["is_soweto"].sum()
        print(f"\nRand Water: found {len(df)} notices, {soweto_count} could affect Soweto.")

        return df


# ---------------------------------------------------------------------------
# get_soweto_rand_water_alerts(df)
#
# Same as the JHB Water version — filter to Soweto HIGH/MEDIUM only.
# ---------------------------------------------------------------------------
def get_soweto_rand_water_alerts(df):
    if df.empty:
        return df
    return df[
        (df["is_soweto"] == True) &
        (df["severity"].isin(["HIGH", "MEDIUM"]))
    ].sort_values("severity")


# ---------------------------------------------------------------------------
# Run this file directly to test the Rand Water scraper on its own
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    scraper = RandWaterScraper()
    df = scraper.run()

    if not df.empty:
        df.to_csv("rand_water_notices.csv", index=False)
        print(f"\nSaved {len(df)} notices to rand_water_notices.csv")

        alerts = get_soweto_rand_water_alerts(df)
        if not alerts.empty:
            print(f"\n--- {len(alerts)} RAND WATER ALERTS AFFECTING SOWETO ---")
            for _, row in alerts.iterrows():
                areas = ", ".join(row["affected_suburbs"]) if row["affected_suburbs"] else "Johannesburg area"
                print(f"  [{row['severity']}] {row['type']} | {areas} | {row['estimated_duration']}")
        else:
            print("\nNo Rand Water alerts affecting Soweto right now.")
    else:
        print("\nNo data returned. Check your internet connection or the Rand Water site.")