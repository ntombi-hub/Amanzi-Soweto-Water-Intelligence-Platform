import sqlite3
import pandas as pd


def setup_sqlite(db_path="amanzi_soweto.db"):
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS water_notices (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            scraped_at TEXT, source_url TEXT, raw_text TEXT,
            notice_type TEXT, severity TEXT, estimated_duration TEXT,
            affected_suburbs TEXT, is_soweto INTEGER DEFAULT 0,
            is_active INTEGER DEFAULT 1, created_at TEXT DEFAULT (datetime('now'))
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS soweto_zones (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            suburb_name TEXT UNIQUE, zone TEXT, reservoir TEXT
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS subscriptions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            phone_number TEXT NOT NULL, suburb_name TEXT,
            channel TEXT DEFAULT 'whatsapp', is_active INTEGER DEFAULT 1,
            created_at TEXT DEFAULT (datetime('now')),
            UNIQUE(phone_number, suburb_name)
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS dam_levels (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            dam_name TEXT, level_pct REAL, status TEXT,
            severity TEXT, source_url TEXT, scraped_at TEXT
        )
    """)
    suburbs = [
        ("Orlando", "Central", "Orlando Reservoir"),
        ("Orlando East", "Central", "Orlando Reservoir"),
        ("Diepkloof", "North", "Power Park Reservoir"),
        ("Meadowlands", "North", "Power Park Reservoir"),
        ("Chiawelo", "South", "Chiawelo Reservoir"),
        ("Dlamini", "South", "Chiawelo Reservoir"),
        ("Protea North", "South", "Chiawelo Reservoir"),
        ("Protea South", "South", "Chiawelo Reservoir"),
        ("Naledi", "South", "Chiawelo Reservoir"),
        ("Dobsonville", "West", "Braamfischerville Reservoir"),
        ("Jabulani", "Central", "Orlando Reservoir"),
        ("Mofolo", "Central", "Orlando Reservoir"),
        ("Pimville", "South", "Chiawelo Reservoir"),
        ("Rockville", "Central", "Orlando Reservoir"),
        ("Senaoane", "South", "Chiawelo Reservoir"),
        ("Moletsane", "West", "Braamfischerville Reservoir"),
        ("Tladi", "West", "Braamfischerville Reservoir"),
        ("Mapetla", "West", "Braamfischerville Reservoir"),
        ("Zola", "West", "Braamfischerville Reservoir"),
        ("Emdeni", "West", "Braamfischerville Reservoir"),
        ("Moroka", "South", "Chiawelo Reservoir"),
        ("Klipspruit", "South", "Chiawelo Reservoir"),
        ("Braamfischerville", "West", "Braamfischerville Reservoir"),
        ("Doornkop", "West", "Braamfischerville Reservoir"),
        ("Freedom Park", "South", "Chiawelo Reservoir"),
    ]
    c.executemany("INSERT OR IGNORE INTO soweto_zones (suburb_name, zone, reservoir) VALUES (?,?,?)", suburbs)
    conn.commit()
    conn.close()
    return db_path


def insert_notices(df, db_path="amanzi_soweto.db"):
    if df.empty:
        return
    conn = sqlite3.connect(db_path)
    for _, row in df.iterrows():
        suburbs_str = ", ".join(row.get("affected_suburbs", []))
        conn.execute("""
            INSERT INTO water_notices
                (scraped_at, source_url, raw_text, notice_type,
                 severity, estimated_duration, affected_suburbs, is_soweto)
            VALUES (?,?,?,?,?,?,?,?)
        """, (
            row.get("scraped_at"), row.get("source_url"), row.get("raw_text"),
            row.get("type"), row.get("severity"), row.get("estimated_duration"),
            suburbs_str, int(row.get("is_soweto", False)),
        ))
    conn.commit()
    conn.close()


def insert_dam_level(record, db_path="amanzi_soweto.db"):
    if not record:
        return
    conn = sqlite3.connect(db_path)
    conn.execute("""
        INSERT INTO dam_levels (dam_name, level_pct, status, severity, source_url, scraped_at)
        VALUES (?,?,?,?,?,?)
    """, (record["dam_name"], record["level_pct"], record["status"],
          record["severity"], record["source_url"], record["scraped_at"]))
    conn.commit()
    conn.close()


def get_active_alerts(db_path="amanzi_soweto.db"):
    conn = sqlite3.connect(db_path)
    df = pd.read_sql_query("""
        SELECT * FROM water_notices
        WHERE is_soweto=1 AND is_active=1 AND severity IN ('HIGH','MEDIUM')
        ORDER BY CASE severity WHEN 'HIGH' THEN 1 ELSE 2 END, scraped_at DESC
    """, conn)
    conn.close()
    return df


def get_latest_dam_level(db_path="amanzi_soweto.db"):
    conn = sqlite3.connect(db_path)
    row = conn.execute("SELECT * FROM dam_levels ORDER BY scraped_at DESC LIMIT 1").fetchone()
    conn.close()
    if not row:
        return None
    return dict(zip(["id", "dam_name", "level_pct", "status", "severity", "source_url", "scraped_at"], row))


def get_dam_history(db_path="amanzi_soweto.db", limit=30):
    conn = sqlite3.connect(db_path)
    df = pd.read_sql_query("SELECT * FROM dam_levels ORDER BY scraped_at DESC LIMIT ?", conn, params=(limit,))
    conn.close()
    return df


def subscribe(phone, suburb, channel="whatsapp", db_path="amanzi_soweto.db"):
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "INSERT OR IGNORE INTO subscriptions (phone_number, suburb_name, channel) VALUES (?,?,?)",
            (phone, suburb, channel)
        )
        conn.commit()
    finally:
        conn.close()


def unsubscribe(phone, suburb, db_path="amanzi_soweto.db"):
    conn = sqlite3.connect(db_path)
    conn.execute(
        "UPDATE subscriptions SET is_active=0 WHERE phone_number=? AND suburb_name=?",
        (phone, suburb)
    )
    conn.commit()
    conn.close()
