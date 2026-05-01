import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import re
import time

SOWETO_SUBURBS = [
    "Soweto", "Orlando", "Diepkloof", "Meadowlands", "Chiawelo",
    "Dlamini", "Protea", "Naledi", "Dobsonville", "Jabulani",
    "Mofolo", "Pimville", "Rockville", "Senaoane", "Moletsane",
    "Tladi", "Mapetla", "Zola", "Emdeni", "Moroka",
    "Klipspruit", "Braamfischerville", "Doornkop", "Freedom Park",
]

SOWETO_RESERVOIRS = [
    "Chiawelo Reservoir", "Power Park Reservoir",
    "Braamfischerville Reservoir", "Orlando Reservoir",
]


def classify_notice(text):
    t = text.lower()
    if any(w in t for w in ["planned", "maintenance", "scheduled", "upgrade"]):
        notice_type = "planned_maintenance"
    elif any(w in t for w in ["burst", "emergency", "unplanned"]):
        notice_type = "emergency_outage"
    elif any(w in t for w in ["low pressure", "reduced pressure"]):
        notice_type = "low_pressure"
    elif any(w in t for w in ["restored", "resolved", "complete"]):
        notice_type = "restoration"
    elif "leak" in t:
        notice_type = "leak"
    else:
        notice_type = "general_notice"

    if any(w in t for w in ["no water", "no supply", "burst", "complete outage"]):
        severity = "HIGH"
    elif any(w in t for w in ["low pressure", "intermittent", "reduced"]):
        severity = "MEDIUM"
    else:
        severity = "LOW"

    match = re.search(r"(\d+)\s*(hour|day|hr|minute)", t)
    duration = match.group(0) if match else "unknown"
    return {"type": notice_type, "severity": severity, "estimated_duration": duration}


def find_soweto_suburbs(text):
    return [p for p in SOWETO_SUBURBS + SOWETO_RESERVOIRS if p.lower() in text.lower()]


class JHBWaterScraper:

    def __init__(self):
        self.daily_url    = "https://www.johannesburgwater.co.za/daily-water-notices/"
        self.customer_url = "https://www.johannesburgwater.co.za/media/media-statement/customer-notices/"
        self.headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

    def get_page(self, url):
        try:
            res = requests.get(url, headers=self.headers, timeout=15)
            res.raise_for_status()
            return BeautifulSoup(res.text, "html.parser")
        except requests.RequestException:
            return None

    def extract_notices(self, soup, source_url):
        notices = []
        now = datetime.now().isoformat()
        raw_blocks = []
        for selector in ["article", ".entry-content", ".post-content", "main p"]:
            elements = soup.select(selector)
            if elements:
                raw_blocks = [el.get_text(separator=" ", strip=True) for el in elements]
                break
        if not raw_blocks:
            raw_blocks = [p.get_text(strip=True) for p in soup.find_all("p")]
        for block in raw_blocks:
            if len(block) < 50:
                continue
            affected = find_soweto_suburbs(block)
            notices.append({
                "scraped_at": now, "source_url": source_url,
                "raw_text": block[:1000], "affected_suburbs": affected,
                "is_soweto": len(affected) > 0, **classify_notice(block),
            })
        return notices

    def run(self):
        all_notices = []
        for url in [self.daily_url, self.customer_url]:
            soup = self.get_page(url)
            if soup:
                all_notices.extend(self.extract_notices(soup, url))
        df = pd.DataFrame(all_notices)
        if df.empty:
            return df
        return df.drop_duplicates(subset=["raw_text"])


def get_soweto_alerts(df):
    if df.empty:
        return df
    return df[(df["is_soweto"] == True) & (df["severity"].isin(["HIGH", "MEDIUM"]))].sort_values("severity")
