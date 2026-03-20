# notifier.py
# This file sends WhatsApp and SMS alerts to Soweto residents.
#
# We use Twilio for this. Twilio is a service that lets you send messages
# via code. You'll need a free Twilio account to make this work.
#
# Setup steps:
#   1. Sign up at twilio.com (free)
#   2. Enable the WhatsApp Sandbox in your Twilio dashboard
#   3. Copy your credentials into a .env file (see .env.example)
#   4. pip install twilio python-dotenv

import os
import sqlite3
from datetime import datetime
from dotenv import load_dotenv

# Load credentials from the .env file so we don't hardcode secrets in the code
load_dotenv()


# ---------------------------------------------------------------------------
# build_message(notice)
#
# Takes a notice dict and turns it into a friendly WhatsApp message.
# We keep it short and simple — most residents just want to know:
#   - Are they affected?
#   - How bad is it?
#   - How long will it last?
# ---------------------------------------------------------------------------
def build_message(notice):
    # Pull out what we need from the notice
    suburbs  = notice.get("affected_suburbs", "your area")
    severity = notice.get("severity", "LOW")
    n_type   = notice.get("notice_type", "general_notice")
    duration = notice.get("estimated_duration", "unknown")

    # Pick an emoji and label based on severity
    emoji = {"HIGH": "🔴", "MEDIUM": "🟡", "LOW": "🟢"}.get(severity, "ℹ️")

    # Turn the internal type code into plain English
    labels = {
        "emergency_outage":    "Emergency — No Water Supply",
        "planned_maintenance": "Planned Maintenance",
        "low_pressure":        "Low Water Pressure",
        "restoration":         "Water Supply Restored",
        "leak":                "Water Leak Reported",
        "general_notice":      "Water Notice",
    }
    label = labels.get(n_type, "Water Notice")

    # Handle suburbs being either a list or a string
    if isinstance(suburbs, list):
        suburbs = ", ".join(suburbs) if suburbs else "your area"

    # Build the final message
    message = f"""💧 *AMANZI SOWETO*
{emoji} {label}

📍 *Areas affected:* {suburbs}
⏱️ *Duration:* {duration}
🕐 *Reported:* {datetime.now().strftime('%d %b %Y at %H:%M')}

Questions? Call JHB Water: *011 688 1699*
Email: fault@jwater.co.za

_Reply STOP to unsubscribe_"""

    return message.strip()


# ---------------------------------------------------------------------------
# NotificationSender
#
# Handles the actual sending of messages via Twilio.
# If no Twilio credentials are set, it runs in "dry run" mode —
# it prints the message instead of sending it. Great for testing.
# ---------------------------------------------------------------------------
class NotificationSender:

    def __init__(self):
        # Read credentials from environment variables (set in .env file)
        self.account_sid    = os.getenv("TWILIO_ACCOUNT_SID")
        self.auth_token     = os.getenv("TWILIO_AUTH_TOKEN")
        self.whatsapp_from  = os.getenv("TWILIO_WHATSAPP_FROM", "whatsapp:+14155238886")
        self.sms_from       = os.getenv("TWILIO_SMS_FROM")
        self.client         = None

        # Try to connect to Twilio — if credentials aren't set, we'll just print messages
        self._connect()

    def _connect(self):
        if self.account_sid and self.auth_token:
            try:
                from twilio.rest import Client
                self.client = Client(self.account_sid, self.auth_token)
                print("Twilio connected.")
            except ImportError:
                print("Twilio not installed. Run: pip install twilio")
        else:
            print("No Twilio credentials found. Running in dry-run mode (messages will print, not send).")

    def send_whatsapp(self, to_number, message):
        # Send a WhatsApp message to a phone number.
        # to_number should be in format: +27821234567

        if not self.client:
            # Dry run — just print what would be sent
            print(f"\n[DRY RUN] WhatsApp → {to_number}")
            print(message)
            print("---")
            return {"status": "dry_run", "to": to_number}

        try:
            msg = self.client.messages.create(
                from_=self.whatsapp_from,
                to=f"whatsapp:{to_number}",
                body=message
            )
            print(f"WhatsApp sent to {to_number} (SID: {msg.sid})")
            return {"status": "sent", "sid": msg.sid, "to": to_number}

        except Exception as e:
            print(f"WhatsApp failed for {to_number}: {e}")
            return {"status": "failed", "error": str(e), "to": to_number}

    def send_sms(self, to_number, message):
        # Send a plain SMS — we strip the markdown formatting first
        # since WhatsApp bold (*text*) looks weird as an SMS
        plain_text = message.replace("*", "").replace("_", "")[:160]

        if not self.client:
            print(f"\n[DRY RUN] SMS → {to_number}")
            print(plain_text)
            return {"status": "dry_run", "to": to_number}

        try:
            msg = self.client.messages.create(
                from_=self.sms_from,
                to=to_number,
                body=plain_text
            )
            return {"status": "sent", "sid": msg.sid, "to": to_number}

        except Exception as e:
            return {"status": "failed", "error": str(e), "to": to_number}


# ---------------------------------------------------------------------------
# AlertDispatcher
#
# Coordinates between the database and the sender.
# It looks up which residents are subscribed to affected suburbs,
# then fires off the right message to each one.
# ---------------------------------------------------------------------------
class AlertDispatcher:

    def __init__(self, db_path="amanzi_soweto.db"):
        self.db_path = db_path
        self.sender  = NotificationSender()

    def get_subscribers(self, suburbs):
        # Look up all active subscribers for a given list of suburbs.
        # Returns a list of dicts with phone, suburb, and channel.
        if not suburbs:
            return []

        conn = sqlite3.connect(self.db_path)

        # Build the right number of ? placeholders for the IN clause
        placeholders = ",".join("?" * len(suburbs))
        rows = conn.execute(f"""
            SELECT phone_number, suburb_name, channel
            FROM subscriptions
            WHERE suburb_name IN ({placeholders})
              AND is_active = 1
        """, suburbs).fetchall()
        conn.close()

        return [{"phone": r[0], "suburb": r[1], "channel": r[2]} for r in rows]

    def send_for_notice(self, notice):
        # Send alerts to everyone subscribed to suburbs in this notice.
        suburbs = notice.get("affected_suburbs", [])

        # The DB stores suburbs as a comma-separated string — split it back into a list
        if isinstance(suburbs, str):
            suburbs = [s.strip() for s in suburbs.split(",") if s.strip()]

        subscribers = self.get_subscribers(suburbs)

        if not subscribers:
            print(f"  No subscribers for: {suburbs}")
            return []

        message = build_message(notice)
        results = []

        for sub in subscribers:
            if sub["channel"] == "whatsapp":
                result = self.sender.send_whatsapp(sub["phone"], message)
            else:
                result = self.sender.send_sms(sub["phone"], message)
            results.append(result)

        print(f"Sent {len(results)} alerts for notice affecting {suburbs}")
        return results

    def dispatch_all_active(self):
        # Find all active HIGH/MEDIUM Soweto alerts and send them out.
        conn = sqlite3.connect(self.db_path)
        rows = conn.execute("""
            SELECT id, notice_type, severity, affected_suburbs, estimated_duration
            FROM water_notices
            WHERE is_soweto = 1
              AND is_active  = 1
              AND severity  IN ('HIGH', 'MEDIUM')
        """).fetchall()
        conn.close()

        if not rows:
            print("No active alerts to send right now.")
            return

        print(f"\nDispatching {len(rows)} alert(s)...")
        for row in rows:
            notice = {
                "id":                 row[0],
                "notice_type":        row[1],
                "severity":           row[2],
                "affected_suburbs":   row[3],
                "estimated_duration": row[4],
            }
            self.send_for_notice(notice)


# ---------------------------------------------------------------------------
# Run this file directly to preview what an alert message looks like
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    # Sample notice — replace with real data once the scraper is running
    sample = {
        "notice_type":        "emergency_outage",
        "severity":           "HIGH",
        "affected_suburbs":   ["Chiawelo", "Dlamini", "Protea South"],
        "estimated_duration": "24 hours",
    }

    print("Preview of what a WhatsApp alert looks like:\n")
    print(build_message(sample))

    print("\n" + "=" * 50)
    print("To send real alerts:")
    print("  1. pip install twilio python-dotenv")
    print("  2. Copy .env.example to .env and fill in your Twilio credentials")
    print("  3. Run: python pipeline.py")