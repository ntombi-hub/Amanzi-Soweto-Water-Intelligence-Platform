# 💧 Amanzi Soweto — Water Intelligence Platform

Real-time water outage intelligence for Soweto, Johannesburg.
Scrapes Johannesburg Water and Rand Water, tracks Vaal Dam levels, sends WhatsApp/SMS alerts,
exposes a REST API, and displays a live Streamlit dashboard.

Built by **Ntombikayise Faith Sibisi**

---

## Project Structure

```
amanzi-soweto/
├── scraper/
│   ├── scraper.py              ← JHB Water scraper + classifier
│   ├── rand_water_scraper.py   ← Rand Water scraper
│   └── dam_scraper.py          ← Vaal Dam level scraper (DWS)
├── database/
│   └── database.py             ← SQLite setup, insert, query
├── notifier/
│   └── notifier.py             ← WhatsApp/SMS alerts via Twilio
├── dashboard/
│   └── dashboard.py            ← Streamlit live dashboard
├── api/
│   ├── api.py                  ← FastAPI REST API
│   └── templates/
│       └── subscribe.html      ← Resident subscription form
├── pipeline.py                 ← Orchestrator: scrape → dam → store → alert
├── requirements.txt
├── Procfile                    ← Railway deploy config
├── railway.toml
├── runtime.txt
├── .env.example
└── .gitignore
```

---

## Quick Start

```bash
git clone https://github.com/ntombi-hub/amanzi-soweto
cd amanzi-soweto
python -m venv venv
venv\Scripts\activate        # Windows
pip install -r requirements.txt
copy .env.example .env       # fill in Twilio credentials
python pipeline.py           # run once
```

---

## Running Each Service

You need **3 separate terminal windows** — each command stays running.

**Terminal 1 — Install dependencies (first time only)**
```bash
cd c:\Users\Sibis\Amanzi-Soweto-Water-Intelligence-Platform\amanzi-soweto
pip install -r requirements.txt
```

**Terminal 1 — Pipeline (scrape + store + alert)**
```bash
python pipeline.py
```
To keep it running every 2 hours automatically:
```bash
python pipeline.py --schedule
```

**Terminal 2 — Dashboard**
```bash
streamlit run dashboard/dashboard.py
```
Open in browser: `http://localhost:8501`

**Terminal 3 — API**
```bash
uvicorn api.api:app --reload --port 8000
```
Open in browser: `http://localhost:8000/docs`

| Service       | URL                          |
|---------------|------------------------------|
| Dashboard     | http://localhost:8501        |
| API           | http://localhost:8000        |
| API docs      | http://localhost:8000/docs   |
| Subscribe form| http://localhost:8000/subscribe |

---

## How It Works

```
pipeline.py (every 2 hrs)
    ↓
JHB Water scraper  +  Rand Water scraper  +  Vaal Dam scraper
    ↓
Classify: HIGH / MEDIUM / LOW
    ↓
SQLite (amanzi_soweto.db)
    ↓
WhatsApp/SMS alerts via Twilio  →  subscribed residents
    ↓
Streamlit dashboard  +  FastAPI REST API
```

---

## REST API Endpoints

Base URL (local): `http://localhost:8000`

| Method | Endpoint           | Description                          |
|--------|--------------------|--------------------------------------|
| GET    | `/`                | Health check                         |
| GET    | `/alerts`          | Active HIGH/MEDIUM Soweto alerts     |
| GET    | `/notices`         | All notices (paginated)              |
| GET    | `/dam`             | Latest Vaal Dam level                |
| GET    | `/subscribe`       | Subscription web form (HTML)         |
| POST   | `/subscribe`       | Register phone + suburb (JSON)       |
| POST   | `/subscribe/form`  | Handle HTML form submission          |
| POST   | `/unsubscribe`     | Deactivate a subscription            |

Interactive docs: `http://localhost:8000/docs`

### Subscribe via API (JSON)
```bash
curl -X POST http://localhost:8000/subscribe \
  -H "Content-Type: application/json" \
  -d '{"phone_number": "+27821234567", "suburb_name": "Chiawelo", "channel": "whatsapp"}'
```

---

## Vaal Dam Tracker

The pipeline scrapes the DWS (Dept of Water & Sanitation) weekly hydrology page for Vaal Dam storage levels.

| Level    | Status    | Severity |
|----------|-----------|----------|
| ≥ 80%    | Full      | LOW      |
| 50–79%   | Normal    | LOW      |
| 30–49%   | Low       | MEDIUM   |
| 15–29%   | Very Low  | HIGH     |
| < 15%    | Critical  | HIGH     |

The dashboard shows the current level + a trend chart of the last 30 readings.

---

## Deploy to Railway

1. Push your code to GitHub
2. Go to [railway.app](https://railway.app) → New Project → Deploy from GitHub
3. Select your repo
4. Add environment variables in Railway dashboard (same as your `.env` file)
5. Railway reads `railway.toml` and deploys automatically

For the pipeline worker (scheduled scraping), add a second Railway service pointing to the same repo with start command:
```
python pipeline.py --schedule
```

For the API, add a third service with:
```
uvicorn api.api:app --host 0.0.0.0 --port $PORT
```

---

## Environment Variables

```
TWILIO_ACCOUNT_SID=your_account_sid_here
TWILIO_AUTH_TOKEN=your_auth_token_here
TWILIO_WHATSAPP_FROM=whatsapp:+14155238886
TWILIO_SMS_FROM=+1XXXXXXXXXX
```

> Without Twilio credentials the notifier runs in dry-run mode — messages print to terminal, everything else still works.

---

## Tech Stack

| Tool          | Purpose                    |
|---------------|----------------------------|
| Python        | Core language              |
| BeautifulSoup | Web scraping               |
| Pandas        | Data processing            |
| SQLite        | Database                   |
| Twilio        | WhatsApp & SMS             |
| Streamlit     | Live dashboard             |
| Plotly        | Charts                     |
| FastAPI       | REST API                   |
| Uvicorn       | ASGI server                |
| Railway       | Cloud deployment           |

---

## Roadmap

- [x] Phase 1 — JHB Water scraper + SQLite
- [x] Phase 2 — Soweto filtering + severity classification
- [x] Phase 3 — WhatsApp/SMS via Twilio
- [x] Phase 4 — Streamlit dashboard
- [x] Phase 5 — Rand Water scraper
- [x] Phase 6 — Vaal Dam level tracker
- [x] Phase 7 — Railway deploy config
- [x] Phase 8 — FastAPI REST API
- [x] Phase 9 — Resident subscription web form
- [x] Phase 10 — PyPI package

---

## PyPI Package

The core scraper, database, and notifier are also published as a standalone Python package.

```bash
pip install amanzi-soweto
```

With Twilio notifications:
```bash
pip install amanzi-soweto[notifications]
```

Quick usage:
```python
from amanzi_soweto import JHBWaterScraper, get_active_alerts, setup_sqlite

setup_sqlite("my.db")
df = JHBWaterScraper().run()
print(get_active_alerts("my.db"))
```

CLI:
```bash
amanzi-pipeline              # run once
amanzi-pipeline --schedule   # run every 2 hours
```

Package source is in `amanzi_soweto_pkg/`. To publish:
```bash
cd amanzi_soweto_pkg
pip install build twine
python -m build
twine upload dist/*
```

---

## Contact

**Ntombikayise Faith Sibisi**
Email: Sibisintombikayise7@gmail.com
GitHub: [github.com/ntombi-hub](https://github.com/ntombi-hub)
LinkedIn: [linkedin.com/in/ntombikayisesibisi](https://linkedin.com/in/ntombikayisesibisi)
Location: Soweto, Johannesburg, South Africa
