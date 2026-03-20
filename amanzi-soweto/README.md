# 💧 Amanzi Soweto — Water Intelligence Platform

A real-time water outage intelligence system for Soweto, Johannesburg.
Scrapes Johannesburg Water and Rand Water websites, classifies outage notices by severity,
stores them in a database, sends WhatsApp/SMS alerts to residents, and displays a live dashboard.

Built by **Ntombikayise  Sibisi** — github.com/ntombi-hub

---

## Project Structure

```
amanzi-soweto/
├── scraper/
│   ├── scraper.py              ← JHB Water scraper
│   └── rand_water_scraper.py   ← Rand Water scraper
├── database/
│   └── database.py             ← SQLite setup, insert, query
├── notifier/
│   └── notifier.py             ← WhatsApp/SMS alerts via Twilio
├── dashboard/
│   └── dashboard.py            ← Streamlit dashboard
├── pipeline.py                 ← Main orchestrator
├── requirements.txt
├── .env.example                ← Copy to .env and fill in credentials
└── .gitignore
```

---

## Quick Start

### 1. Clone the repo
```bash
git clone https://github.com/ntombi-hub/amanzi-soweto
cd amanzi-soweto
```

### 2. Create a virtual environment
```bash
python3 -m venv venv
source venv/bin/activate        # Mac/Linux
# venv\Scripts\activate         # Windows
```

### 3. Install dependencies
```bash
pip install -r requirements.txt
```

### 4. Set up your credentials
```bash
cp .env.example .env
# Open .env and fill in your Twilio credentials
```

### 5. Run the pipeline once
```bash
python pipeline.py
```

### 6. Run on a schedule (every 2 hours)
```bash
python pipeline.py --schedule
```

### 7. Launch the dashboard
```bash
streamlit run dashboard/dashboard.py
```

---

## How It Works

```
Scraper (every 2hrs)
    ↓
JHB Water + Rand Water websites
    ↓
Classify by severity (HIGH / MEDIUM / LOW)
    ↓
Store in SQLite / PostgreSQL
    ↓
Send WhatsApp/SMS alerts via Twilio
    ↓
Streamlit dashboard shows live status
```

---

## Soweto Coverage

25+ suburbs across 4 reservoir zones:

| Zone  | Reservoir                    | Suburbs                                              |
|-------|------------------------------|------------------------------------------------------|
| North | Power Park Reservoir         | Diepkloof, Meadowlands                               |
| Central | Orlando Reservoir          | Orlando, Jabulani, Mofolo, Rockville                 |
| South | Chiawelo Reservoir           | Chiawelo, Dlamini, Protea N/S, Naledi, Pimville, Senaoane, Moroka, Freedom Park |
| West  | Braamfischerville Reservoir  | Dobsonville, Moletsane, Tladi, Mapetla, Zola, Emdeni, Braamfischerville, Doornkop |

---

## Environment Variables

Copy `.env.example` to `.env` and fill in:

```
TWILIO_ACCOUNT_SID=your_account_sid_here
TWILIO_AUTH_TOKEN=your_auth_token_here
TWILIO_WHATSAPP_FROM=whatsapp:+14155238886
TWILIO_SMS_FROM=+1XXXXXXXXXX
```

Get Twilio credentials at: https://twilio.com (free account)

---

## Tech Stack

| Tool              | Purpose                        |
|-------------------|--------------------------------|
| Python            | Core language                  |
| BeautifulSoup     | Web scraping                   |
| Pandas            | Data processing                |
| SQLite/PostgreSQL | Database storage               |
| Apache Airflow    | Pipeline scheduling            |
| dbt               | Data transformation            |
| Twilio            | WhatsApp & SMS alerts          |
| Streamlit         | Dashboard                      |
| Plotly            | Charts                         |

---

## Roadmap

- [x] Phase 1 — JHB Water scraper + SQLite storage
- [x] Phase 2 — Soweto suburb filtering + severity classification
- [x] Phase 3 — WhatsApp/SMS notifications via Twilio
- [x] Phase 4 — Streamlit dashboard with severity charts
- [x] Phase 5 — Rand Water scraper added
- [ ] Phase 6 — Vaal Dam level tracker
- [ ] Phase 7 — Deploy to Railway or Render
- [ ] Phase 8 — REST API (FastAPI)
- [ ] Phase 9 — PyPI package

---

## Contact

**Ntombikayise Faith Sibisi**
Email: Sibisintombikayise7@gmail.com
GitHub: github.com/ntombi-hub
LinkedIn: linkedin.com/in/ntombikayisesibisi
Location: Soweto, Johannesburg, South Africa