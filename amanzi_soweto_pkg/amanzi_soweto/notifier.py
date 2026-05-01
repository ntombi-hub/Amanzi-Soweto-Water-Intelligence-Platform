import os
import sqlite3
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()


def build_message(notice):
    suburbs  = notice.get("affected_suburbs", "your area")
    severity = notice.get("severity", "LOW")
    n_type   = notice.get("notice_type", "general_notice")
    duration = notice.get("estimated_duration", "unknown")
    emoji    = {"HIGH": "🔴", "MEDIUM": "🟡", "LOW": "🟢"}.get(severity, "ℹ️")
    label    = {
        "emergency_outage":    "Emergency — No Water Supply",
        "planned_maintenance": "Planned Maintenance",
        "low_pressure":        "Low Water Pressure",
        "restoration":         "Water Supply Restored",
        "leak":                "Water Leak Reported",
        "general_notice":      "Water Notice",
    }.get(n_type, "Water Notice")
    if isinstance(suburbs, list):
        suburbs = ", ".join(suburbs) if suburbs else "your area"
    return f"""💧 *AMANZI SOWETO*
{emoji} {label}

📍 *Areas affected:* {suburbs}
⏱️ *Duration:* {duration}
🕐 *Reported:* {datetime.now().strftime('%d %b %Y at %H:%M')}

Questions? Call JHB Water: *011 688 1699*
Email: fault@jwater.co.za

_Reply STOP to unsubscribe_""".strip()


class NotificationSender:

    def __init__(self):
        self.account_sid   = os.getenv("TWILIO_ACCOUNT_SID")
        self.auth_token    = os.getenv("TWILIO_AUTH_TOKEN")
        self.whatsapp_from = os.getenv("TWILIO_WHATSAPP_FROM", "whatsapp:+14155238886")
        self.sms_from      = os.getenv("TWILIO_SMS_FROM")
        self.client        = None
        if self.account_sid and self.auth_token:
            try:
                from twilio.rest import Client
                self.client = Client(self.account_sid, self.auth_token)
            except ImportError:
                pass

    def send_whatsapp(self, to_number, message):
        if not self.client:
            print(f"[DRY RUN] WhatsApp → {to_number}\n{message}")
            return {"status": "dry_run", "to": to_number}
        try:
            msg = self.client.messages.create(
                from_=self.whatsapp_from, to=f"whatsapp:{to_number}", body=message
            )
            return {"status": "sent", "sid": msg.sid, "to": to_number}
        except Exception as e:
            return {"status": "failed", "error": str(e), "to": to_number}

    def send_sms(self, to_number, message):
        plain = message.replace("*", "").replace("_", "")[:160]
        if not self.client:
            print(f"[DRY RUN] SMS → {to_number}\n{plain}")
            return {"status": "dry_run", "to": to_number}
        try:
            msg = self.client.messages.create(from_=self.sms_from, to=to_number, body=plain)
            return {"status": "sent", "sid": msg.sid, "to": to_number}
        except Exception as e:
            return {"status": "failed", "error": str(e), "to": to_number}


class AlertDispatcher:

    def __init__(self, db_path="amanzi_soweto.db"):
        self.db_path = db_path
        self.sender  = NotificationSender()

    def get_subscribers(self, suburbs):
        if not suburbs:
            return []
        conn = sqlite3.connect(self.db_path)
        placeholders = ",".join("?" * len(suburbs))
        rows = conn.execute(
            f"SELECT phone_number, suburb_name, channel FROM subscriptions "
            f"WHERE suburb_name IN ({placeholders}) AND is_active=1",
            suburbs
        ).fetchall()
        conn.close()
        return [{"phone": r[0], "suburb": r[1], "channel": r[2]} for r in rows]

    def send_for_notice(self, notice):
        suburbs = notice.get("affected_suburbs", [])
        if isinstance(suburbs, str):
            suburbs = [s.strip() for s in suburbs.split(",") if s.strip()]
        subscribers = self.get_subscribers(suburbs)
        if not subscribers:
            return []
        message = build_message(notice)
        return [
            self.sender.send_whatsapp(s["phone"], message)
            if s["channel"] == "whatsapp"
            else self.sender.send_sms(s["phone"], message)
            for s in subscribers
        ]

    def dispatch_all_active(self):
        conn = sqlite3.connect(self.db_path)
        rows = conn.execute("""
            SELECT id, notice_type, severity, affected_suburbs, estimated_duration
            FROM water_notices
            WHERE is_soweto=1 AND is_active=1 AND severity IN ('HIGH','MEDIUM')
        """).fetchall()
        conn.close()
        for row in rows:
            self.send_for_notice({
                "id": row[0], "notice_type": row[1], "severity": row[2],
                "affected_suburbs": row[3], "estimated_duration": row[4],
            })
