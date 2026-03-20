
# database.py
# This file handles everything to do with storing notices.
#
# We use SQLite for development (it's just a file on your computer, no setup needed).
# When you're ready to go live, swap it out for PostgreSQL — the schema is the same.

import sqlite3
import pandas as pd


# ---------------------------------------------------------------------------
# setup_sqlite(db_path)
#
# Creates the database file and all the tables we need.
# Safe to run multiple times — it won't overwrite anything that already exists.
# ---------------------------------------------------------------------------
def setup_sqlite(db_path="amanzi_soweto.db"):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # The main table — every scraped notice goes here
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS water_notices (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            scraped_at          TEXT,           -- when we scraped it
            source_url          TEXT,           -- which page it came from
            raw_text            TEXT,           -- the original text of the notice
            notice_type         TEXT,           -- emergency_outage, planned_maintenance, etc.
            severity            TEXT,           -- HIGH, MEDIUM, or LOW
            estimated_duration  TEXT,           -- e.g. "4 hours" or "unknown"
            affected_suburbs    TEXT,           -- comma-separated list of suburb names
            is_soweto           INTEGER DEFAULT 0,  -- 1 if it mentions a Soweto suburb
            is_active           INTEGER DEFAULT 1,  -- 1 = still ongoing, 0 = resolved
            created_at          TEXT DEFAULT (datetime('now'))
        )
    """)

    # Registry of all Soweto suburbs and which reservoir feeds them.
    # Useful for the map and for filtering notices by zone.
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS soweto_zones (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            suburb_name TEXT UNIQUE,
            zone        TEXT,       -- North, South, East, West, Central
            reservoir   TEXT        -- which reservoir serves this suburb
        )
    """)

    # Who wants to receive alerts and how
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS subscriptions (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            phone_number    TEXT NOT NULL,
            suburb_name     TEXT,
            channel         TEXT DEFAULT 'whatsapp',  -- whatsapp or sms
            is_active       INTEGER DEFAULT 1,
            created_at      TEXT DEFAULT (datetime('now')),
            UNIQUE(phone_number, suburb_name)  -- no duplicate subscriptions
        )
    """)

    # Seed the zones table with Soweto suburbs.
    # We use INSERT OR IGNORE so re-running this won't cause errors.
    suburbs = [
        ("Orlando",             "Central",  "Orlando Reservoir"),
        ("Orlando East",        "Central",  "Orlando Reservoir"),
        ("Diepkloof",           "North",    "Power Park Reservoir"),
        ("Meadowlands",         "North",    "Power Park Reservoir"),
        ("Chiawelo",            "South",    "Chiawelo Reservoir"),
        ("Dlamini",             "South",    "Chiawelo Reservoir"),
        ("Protea North",        "South",    "Chiawelo Reservoir"),
        ("Protea South",        "South",    "Chiawelo Reservoir"),
        ("Naledi",              "South",    "Chiawelo Reservoir"),
        ("Dobsonville",         "West",     "Braamfischerville Reservoir"),
        ("Jabulani",            "Central",  "Orlando Reservoir"),
        ("Mofolo",              "Central",  "Orlando Reservoir"),
        ("Pimville",            "South",    "Chiawelo Reservoir"),
        ("Rockville",           "Central",  "Orlando Reservoir"),
        ("Senaoane",            "South",    "Chiawelo Reservoir"),
        ("Moletsane",           "West",     "Braamfischerville Reservoir"),
        ("Tladi",               "West",     "Braamfischerville Reservoir"),
        ("Mapetla",             "West",     "Braamfischerville Reservoir"),
        ("Zola",                "West",     "Braamfischerville Reservoir"),
        ("Emdeni",              "West",     "Braamfischerville Reservoir"),
        ("Moroka",              "South",    "Chiawelo Reservoir"),
        ("Klipspruit",          "South",    "Chiawelo Reservoir"),
        ("Braamfischerville",   "West",     "Braamfischerville Reservoir"),
        ("Doornkop",            "West",     "Braamfischerville Reservoir"),
        ("Freedom Park",        "South",    "Chiawelo Reservoir"),
    ]

    cursor.executemany(
        "INSERT OR IGNORE INTO soweto_zones (suburb_name, zone, reservoir) VALUES (?, ?, ?)",
        suburbs
    )

    conn.commit()
    conn.close()
    print(f"Database ready at: {db_path}")
    return db_path


# ---------------------------------------------------------------------------
# insert_notices(df, db_path)
#
# Takes the DataFrame from the scraper and saves each row into the database.
# ---------------------------------------------------------------------------
def insert_notices(df, db_path="amanzi_soweto.db"):
    if df.empty:
        print("Nothing to insert — DataFrame is empty.")
        return

    conn = sqlite3.connect(db_path)

    for _, row in df.iterrows():
        # The suburbs come back as a Python list — join them into a string for storage
        suburbs_str = ", ".join(row.get("affected_suburbs", []))

        conn.execute("""
            INSERT INTO water_notices
                (scraped_at, source_url, raw_text, notice_type,
                 severity, estimated_duration, affected_suburbs, is_soweto)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            row.get("scraped_at"),
            row.get("source_url"),
            row.get("raw_text"),
            row.get("type"),
            row.get("severity"),
            row.get("estimated_duration"),
            suburbs_str,
            int(row.get("is_soweto", False))  # convert True/False to 1/0 for SQLite
        ))

    conn.commit()
    conn.close()
    print(f"Saved {len(df)} notices to the database.")


# ---------------------------------------------------------------------------
# get_active_alerts(db_path)
#
# Returns all the HIGH and MEDIUM alerts that are still active.
# This is what the dashboard and notifier read from.
# ---------------------------------------------------------------------------
def get_active_alerts(db_path="amanzi_soweto.db"):
    conn = sqlite3.connect(db_path)
    df = pd.read_sql_query("""
        SELECT *
        FROM water_notices
        WHERE is_soweto = 1
          AND is_active  = 1
          AND severity  IN ('HIGH', 'MEDIUM')
        ORDER BY
            CASE severity WHEN 'HIGH' THEN 1 ELSE 2 END,  -- HIGH comes first
            scraped_at DESC
    """, conn)
    conn.close()
    return df


# ---------------------------------------------------------------------------
# subscribe(phone, suburb, channel, db_path)
#
# Register a resident to receive alerts for their suburb.
# channel can be 'whatsapp' or 'sms'
# ---------------------------------------------------------------------------
def subscribe(phone, suburb, channel="whatsapp", db_path="amanzi_soweto.db"):
    conn = sqlite3.connect(db_path)
    try:
        conn.execute("""
            INSERT OR IGNORE INTO subscriptions (phone_number, suburb_name, channel)
            VALUES (?, ?, ?)
        """, (phone, suburb, channel))
        conn.commit()
        print(f"Subscribed {phone} to {suburb} alerts via {channel}.")
    except Exception as e:
        print(f"Could not subscribe {phone}: {e}")
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# unsubscribe(phone, suburb, db_path)
#
# Deactivate a subscription — we keep the record but set is_active = 0.
# This way we have a history of who subscribed and unsubscribed.
# ---------------------------------------------------------------------------
def unsubscribe(phone, suburb, db_path="amanzi_soweto.db"):
    conn = sqlite3.connect(db_path)
    conn.execute("""
        UPDATE subscriptions
        SET is_active = 0
        WHERE phone_number = ? AND suburb_name = ?
    """, (phone, suburb))
    conn.commit()
    conn.close()
    print(f"Unsubscribed {phone} from {suburb} alerts.")


# ---------------------------------------------------------------------------
# Run this file directly to set up the database and check it's working
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    db = setup_sqlite()

    # Quick check — see what suburbs were seeded
    conn = sqlite3.connect(db)
    zones = pd.read_sql_query("SELECT * FROM soweto_zones ORDER BY zone", conn)
    conn.close()

    print(f"\n{len(zones)} Soweto suburbs registered:")
    print(zones[["suburb_name", "zone", "reservoir"]].to_string(index=False))