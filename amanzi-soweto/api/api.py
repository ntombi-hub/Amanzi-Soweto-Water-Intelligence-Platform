# api.py — REST API for Amanzi Soweto
#
# Run with:
#   uvicorn api.api:app --reload --port 8000
#
# Endpoints:
#   GET  /                  → health check
#   GET  /alerts            → active HIGH/MEDIUM Soweto alerts
#   GET  /notices           → all notices (paginated)
#   GET  /dam               → latest Vaal Dam level
#   GET  /subscribe         → subscription web form (HTML)
#   POST /subscribe         → register phone + suburb
#   POST /unsubscribe       → deactivate subscription

import sys
from pathlib import Path
from datetime import datetime
from typing import Optional

from fastapi import FastAPI, HTTPException, Form, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel

BASE_DIR = Path(__file__).parent.parent
sys.path.insert(0, str(BASE_DIR / "database"))
sys.path.insert(0, str(BASE_DIR / "scraper"))

from database import (
    get_active_alerts, get_latest_dam_level,
    subscribe, unsubscribe, setup_sqlite,
)

import sqlite3
import pandas as pd

DB_PATH    = str(BASE_DIR / "amanzi_soweto.db")
templates  = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))

app = FastAPI(
    title="Amanzi Soweto API",
    description="Real-time water outage intelligence for Soweto",
    version="1.0.0",
)

SOWETO_SUBURBS = [
    "Orlando", "Orlando East", "Diepkloof", "Meadowlands", "Chiawelo",
    "Dlamini", "Protea North", "Protea South", "Naledi", "Dobsonville",
    "Jabulani", "Mofolo", "Pimville", "Rockville", "Senaoane", "Moletsane",
    "Tladi", "Mapetla", "Zola", "Emdeni", "Moroka", "Klipspruit",
    "Braamfischerville", "Doornkop", "Freedom Park",
]


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------
class SubscribeRequest(BaseModel):
    phone_number: str
    suburb_name:  str
    channel:      Optional[str] = "whatsapp"

class UnsubscribeRequest(BaseModel):
    phone_number: str
    suburb_name:  str


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _df_to_records(df: pd.DataFrame) -> list:
    if df.empty:
        return []
    return df.where(pd.notnull(df), None).to_dict(orient="records")


def _ensure_db():
    setup_sqlite(DB_PATH)


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------
@app.get("/", tags=["Health"])
def health():
    return {
        "service": "Amanzi Soweto API",
        "status":  "ok",
        "time":    datetime.now().isoformat(),
    }


@app.get("/alerts", tags=["Notices"])
def active_alerts():
    """All active HIGH and MEDIUM Soweto water alerts."""
    _ensure_db()
    df = get_active_alerts(DB_PATH)
    return {"count": len(df), "alerts": _df_to_records(df)}


@app.get("/notices", tags=["Notices"])
def all_notices(page: int = 1, per_page: int = 20, soweto_only: bool = False):
    """Paginated list of all scraped notices."""
    _ensure_db()
    conn = sqlite3.connect(DB_PATH)
    where = "WHERE is_soweto = 1" if soweto_only else ""
    offset = (page - 1) * per_page
    df = pd.read_sql_query(
        f"SELECT * FROM water_notices {where} ORDER BY scraped_at DESC LIMIT ? OFFSET ?",
        conn, params=(per_page, offset)
    )
    total = conn.execute(
        f"SELECT COUNT(*) FROM water_notices {where}"
    ).fetchone()[0]
    conn.close()
    return {
        "page": page, "per_page": per_page,
        "total": total, "notices": _df_to_records(df),
    }


@app.get("/dam", tags=["Dam"])
def dam_level():
    """Latest Vaal Dam storage level."""
    _ensure_db()
    record = get_latest_dam_level(DB_PATH)
    if not record:
        return {"message": "No dam data yet. Run the pipeline first.", "data": None}
    return {"data": record}


@app.get("/subscribe", response_class=HTMLResponse, tags=["Subscriptions"])
def subscribe_form(request: Request):
    """Serve the subscription web form."""
    return templates.TemplateResponse(
        "subscribe.html",
        {"request": request, "suburbs": SOWETO_SUBURBS, "success": None, "error": None},
    )


@app.post("/subscribe", tags=["Subscriptions"])
def subscribe_json(body: SubscribeRequest):
    """Register a phone number for suburb alerts (JSON)."""
    _ensure_db()
    if body.suburb_name not in SOWETO_SUBURBS:
        raise HTTPException(status_code=400, detail=f"Unknown suburb: {body.suburb_name}")
    if body.channel not in ("whatsapp", "sms"):
        raise HTTPException(status_code=400, detail="channel must be 'whatsapp' or 'sms'")
    subscribe(body.phone_number, body.suburb_name, body.channel, DB_PATH)
    return {"status": "subscribed", "phone": body.phone_number, "suburb": body.suburb_name}


@app.post("/subscribe/form", response_class=HTMLResponse, tags=["Subscriptions"])
async def subscribe_form_post(
    request: Request,
    phone_number: str = Form(...),
    suburb_name:  str = Form(...),
    channel:      str = Form("whatsapp"),
):
    """Handle the HTML form submission."""
    _ensure_db()
    error = None
    success = None

    if suburb_name not in SOWETO_SUBURBS:
        error = f"Unknown suburb: {suburb_name}"
    elif channel not in ("whatsapp", "sms"):
        error = "Channel must be whatsapp or sms"
    else:
        try:
            subscribe(phone_number, suburb_name, channel, DB_PATH)
            success = f"✅ Subscribed! You'll receive {channel} alerts for {suburb_name}."
        except Exception as e:
            error = str(e)

    return templates.TemplateResponse(
        "subscribe.html",
        {"request": request, "suburbs": SOWETO_SUBURBS, "success": success, "error": error},
    )


@app.post("/unsubscribe", tags=["Subscriptions"])
def unsubscribe_endpoint(body: UnsubscribeRequest):
    """Deactivate a subscription."""
    _ensure_db()
    unsubscribe(body.phone_number, body.suburb_name, DB_PATH)
    return {"status": "unsubscribed", "phone": body.phone_number, "suburb": body.suburb_name}
