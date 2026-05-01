import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import re
import time

SOWETO_SYSTEMS = ["Palmiet", "Eikenhof", "Zwartkopjes", "Daleside", "Zuikerbosch", "Mapleton"]
SOWETO_KEYWORDS = [
    "Soweto", "Lenasia", "Orange Farm", "South Hills", "Johannesburg Water",
    "Johannesburg south", "Nasrec", "Eldorado Park", "Ennerdale", "Turffontein",
    "Palmiet system", "Eikenhof system", "Zwartkopjes system",
]


def classify_rand_water_notice(text):
    t = text.lower()
    if any(w in t for w in ["planned maintenance", "scheduled", "shutdown"]):
        notice_type = "planned_maintenance"
    elif any(w in t for w in ["emergency", "burst", "unplanned", "failure"]):
        notice_type = "emergency_outage"
    elif any(w in t for w in ["restored", "completed", "resume"]):
        notice_type = "restoration"
    else:
        notice_type = "general_notice"

    if any(w in t for w in ["no water", "no supply", "complete shutdown", "no pumping"]):
        severity = "HIGH"
    elif any(w in t for w in ["reduced", "low pressure", "intermittent", "capacity reduced"]):
        severity = "MEDIUM"
    else:
        severity = "LOW"

    match = re.search(r"(\d+)\s*(hour|day|hr|week)", t)
    duration = match.group(0) if match else "unknown"
    date_match = re.search(r"(\d{1,2}\s+\w+)\s+(?:to|until|–|-)\s+(\d{1,2}\s+\w+)", text)
    if date_match:
        duration = f"{date_match.group(1)} to {date_match.group(2)}"

    return {"type": notice_type, "severity": severity, "estimated_duration": duration, "source": "Rand Water"}


def affects_soweto(text):
    t = text.lower()
    return any(k.lower() in t for k in SOWETO_SYSTEMS + SOWETO_KEYWORDS)


def find_affected_areas(text):
    found = [k for k in SOWETO_SYSTEMS + SOWETO_KEYWORDS if k.lower() in text.lower()]
    return list(dict.fromkeys(found))


class RandWaterScraper:

    def __init__(self):
        self.media_url     = "https://www.randwater.co.za/mediastatements.php"
        self.corporate_url = "https://www.randwater.co.za/Corporate/MediaStatements"
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
        for selector in [".media-statement", "article", ".news-item", ".entry-content", "main p"]:
            elements = soup.select(selector)
            if elements:
                raw_blocks = [el.get_text(separator=" ", strip=True) for el in elements]
                break
        if not raw_blocks:
            raw_blocks = [p.get_text(strip=True) for p in soup.find_all("p")]
        for block in raw_blocks:
            if len(block) < 50:
                continue
            soweto_affected = affects_soweto(block)
            notices.append({
                "scraped_at": now, "source_url": source_url,
                "raw_text": block[:1500],
                "affected_suburbs": find_affected_areas(block) if soweto_affected else [],
                "is_soweto": soweto_affected,
                **classify_rand_water_notice(block),
            })
        return notices

    def run(self):
        soup = self.get_page(self.media_url) or self.get_page(self.corporate_url)
        if not soup:
            return pd.DataFrame()
        notices = self.extract_notices(soup, self.media_url)
        df = pd.DataFrame(notices)
        if df.empty:
            return df
        return df.drop_duplicates(subset=["raw_text"])


def get_soweto_rand_water_alerts(df):
    if df.empty:
        return df
    return df[(df["is_soweto"] == True) & (df["severity"].isin(["HIGH", "MEDIUM"]))].sort_values("severity")
