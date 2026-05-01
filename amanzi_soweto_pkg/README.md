# amanzi-soweto

Real-time water outage intelligence for Soweto, Johannesburg.

Scrapes Johannesburg Water and Rand Water for outage notices, tracks Vaal Dam levels,
classifies alerts by severity, stores them in SQLite, and sends WhatsApp/SMS alerts via Twilio.

```bash
pip install amanzi-soweto
```

## Usage

```python
from amanzi_soweto import JHBWaterScraper, RandWaterScraper, scrape_vaal_dam_level
from amanzi_soweto import setup_sqlite, insert_notices, get_active_alerts
from amanzi_soweto import AlertDispatcher

# Set up database
setup_sqlite("my.db")

# Scrape notices
df = JHBWaterScraper().run()
insert_notices(df, "my.db")

# Get active alerts
alerts = get_active_alerts("my.db")
print(alerts[["severity", "notice_type", "affected_suburbs"]])

# Check Vaal Dam
dam = scrape_vaal_dam_level()
print(f"Vaal Dam: {dam['level_pct']}% — {dam['status']}")

# Send alerts (requires Twilio credentials in .env)
AlertDispatcher(db_path="my.db").dispatch_all_active()
```

## CLI

```bash
amanzi-pipeline              # run once
amanzi-pipeline --schedule   # run every 2 hours
amanzi-pipeline --db my.db   # custom database path
```

## Optional: Twilio notifications

```bash
pip install amanzi-soweto[notifications]
```

Set environment variables:
```
TWILIO_ACCOUNT_SID=your_sid
TWILIO_AUTH_TOKEN=your_token
TWILIO_WHATSAPP_FROM=whatsapp:+14155238886
```

Without credentials the notifier runs in dry-run mode (prints messages, doesn't send).

## Links

- GitHub: https://github.com/ntombi-hub/amanzi-soweto
- Full platform (dashboard + API): see the main repo README
