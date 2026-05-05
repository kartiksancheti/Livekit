"""tools.py — Tool contract reference for the OutboundAI agent.

The actual tool execution lives in ExotelCallHandler._exec_tool() in agent.py.
This module exists as a clean, importable reference and can be wired to
LiveKit or other frameworks if needed in the future.
"""

import asyncio
import logging
import time
from typing import Optional

from config import cfg
from db import (
    check_slot, get_next_available, insert_appointment, log_call, log_error,
    get_calls_by_phone, get_appointments_by_phone,
    add_contact_memory, get_contact_memory, compress_contact_memory,
)

logger = logging.getLogger("appointment-tools")


async def _log(msg: str, detail: str = "", level: str = "info") -> None:
    try:
        await log_error("agent", msg, detail, level)
    except Exception:
        pass


class AppointmentTools:
    """All function tools available to the appointment-booking agent."""

    def __init__(self, phone_number: Optional[str] = None, lead_name: Optional[str] = None):
        self.phone_number    = phone_number
        self.lead_name       = lead_name
        self._call_start_time = time.time()
        self.recording_url:  Optional[str] = None

    async def check_availability(self, date: str, time: str) -> str:
        """Check if date/time slot is available. Call before confirming any slot."""
        try:
            if await check_slot(date, time):
                return "available"
            next_slot = await get_next_available(date, time)
            return f"unavailable: next available slot is {next_slot}"
        except Exception:
            return "Unable to check availability right now — please suggest a date and I will confirm."

    async def book_appointment(self, name: str, phone: str, date: str, time: str, service: str) -> str:
        """Book after verbal confirmation. Call ONLY after lead confirms all details."""
        try:
            booking_id = await insert_appointment(name, phone, date, time, service)
            return f"Confirmed! Booking ID: {booking_id}. See you on {date} at {time} for {service}."
        except Exception:
            return "Technical issue saving the booking. Our team will confirm shortly."

    async def end_call(self, outcome: str, reason: str = "") -> str:
        """End call and log outcome. ALWAYS call before hanging up."""
        duration = int(time.time() - self._call_start_time)
        try:
            await log_call(
                phone_number=self.phone_number or "unknown",
                lead_name=self.lead_name, outcome=outcome, reason=reason,
                duration_seconds=duration, recording_url=self.recording_url,
            )
        except Exception as exc:
            logger.error("Failed to log call: %s", exc)
        return "Call ended."

    async def transfer_to_human(self, reason: str, call_sid: str = "") -> str:
        """Transfer to human agent via Exotel redirect API."""
        exotel_key   = cfg("EXOTEL_API_KEY")
        exotel_token = cfg("EXOTEL_API_TOKEN")
        exotel_sid   = cfg("EXOTEL_SID")
        transfer_app = cfg("EXOTEL_TRANSFER_APP_ID")

        if not (exotel_key and exotel_token and exotel_sid and transfer_app):
            return "Transfer unavailable: EXOTEL_API_KEY, EXOTEL_API_TOKEN, EXOTEL_SID, EXOTEL_TRANSFER_APP_ID must be set."
        if not call_sid:
            return "Transfer failed: call SID not available."
        try:
            import httpx
            redirect_url = f"https://my.exotel.com/{exotel_sid}/exoml/start_voice/{transfer_app}"
            async with httpx.AsyncClient(timeout=10) as client:
                await client.post(
                    f"https://api.exotel.com/v1/Accounts/{exotel_sid}/Calls/{call_sid}/redirect",
                    auth=(exotel_key, exotel_token),
                    data={"Url": redirect_url, "Method": "POST"},
                )
            return "Transferring you to a human agent now. Please hold."
        except Exception:
            return "Transfer failed. Please call us back directly."

    async def send_sms_confirmation(self, phone: str, message: str) -> str:
        """Send SMS confirmation. Skips silently if Twilio not configured."""
        sid   = cfg("TWILIO_ACCOUNT_SID")
        token = cfg("TWILIO_AUTH_TOKEN")
        from_num = cfg("TWILIO_FROM_NUMBER")
        if not (sid and token and from_num):
            return "SMS skipped: Twilio not configured."
        try:
            from twilio.rest import Client
            client = Client(sid, token)
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, lambda: client.messages.create(body=message, from_=from_num, to=phone))
            return f"SMS sent to {phone}."
        except Exception:
            return "SMS delivery failed, but booking is confirmed."

    async def lookup_contact(self, phone: str) -> str:
        """Look up contact history. Call at the START of every call."""
        try:
            calls        = await get_calls_by_phone(phone)
            appointments = await get_appointments_by_phone(phone)
            memories     = await get_contact_memory(phone)
            if not calls and not appointments and not memories:
                return f"No history for {phone}. First-time contact."
            lines = [f"Contact history for {phone}:"]
            if memories:
                lines.append(f"\nREMEMBERED ({len(memories)} notes):")
                for m in memories[:10]:
                    lines.append(f"  - {m['insight']}")
            if calls:
                lines.append(f"\nCALL HISTORY ({len(calls)} calls):")
                for c in calls[:5]:
                    ts = (c.get("timestamp") or "")[:16]
                    lines.append(f"  - {ts} — {c.get('outcome','?')}: {c.get('reason','')}")
            if appointments:
                lines.append(f"\nAPPOINTMENTS ({len(appointments)}):")
                for a in appointments[:3]:
                    lines.append(f"  - {a.get('date')} {a.get('time')} — {a.get('service')} [{a.get('status')}]")
            return "\n".join(lines)
        except Exception:
            return "Unable to retrieve contact history."

    async def remember_details(self, insight: str) -> str:
        """Store a key insight about this lead for future calls."""
        if not self.phone_number:
            return "Cannot remember — no phone number for this call."
        try:
            await add_contact_memory(self.phone_number, insight)
            memories = await get_contact_memory(self.phone_number)
            if len(memories) >= 5:
                asyncio.create_task(self._compress_memories())
            return f"Remembered: {insight}"
        except Exception:
            return "Could not save detail."

    async def _compress_memories(self) -> None:
        try:
            memories = await get_contact_memory(self.phone_number)
            if len(memories) < 5:
                return
            import google.generativeai as genai
            api_key = cfg("GOOGLE_API_KEY")
            if not api_key:
                return
            genai.configure(api_key=api_key)
            model = genai.GenerativeModel("gemini-2.0-flash")
            bullets = "\n".join(f"- {m['insight']}" for m in memories)
            prompt  = f"Compress these notes about a sales contact into 3-5 concise bullets. Keep all key facts.\n\n{bullets}"
            loop    = asyncio.get_event_loop()
            resp    = await loop.run_in_executor(None, lambda: model.generate_content(prompt))
            if resp.text.strip():
                await compress_contact_memory(self.phone_number, resp.text.strip())
        except Exception as exc:
            logger.warning("Memory compression failed: %s", exc)

    async def book_calcom(self, name: str, email: str, date: str, start_time: str, notes: str = "") -> str:
        """Book in Cal.com after book_appointment succeeds."""
        api_key  = cfg("CALCOM_API_KEY")
        event_id = cfg("CALCOM_EVENT_TYPE_ID")
        tz       = cfg("CALCOM_TIMEZONE")
        if not (api_key and event_id):
            return "Cal.com not configured — add CALCOM_API_KEY and CALCOM_EVENT_TYPE_ID."
        try:
            from datetime import datetime as _dt
            start_iso = _dt.strptime(f"{date} {start_time}", "%Y-%m-%d %H:%M").strftime("%Y-%m-%dT%H:%M:%S.000Z")
            import httpx
            async with httpx.AsyncClient(timeout=15) as client:
                resp = await client.post(
                    "https://api.cal.com/v1/bookings",
                    headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
                    json={"eventTypeId": int(event_id), "start": start_iso, "timeZone": tz,
                          "responses": {"name": name, "email": email, "notes": notes},
                          "metadata": {"source": "OutboundAI"}, "language": "en"},
                )
            data = resp.json()
            if resp.status_code not in (200, 201):
                raise ValueError(data.get("message") or str(data))
            return f"Cal.com booked. UID: {data.get('uid', '')}"
        except Exception as exc:
            return f"Cal.com booking failed: {exc}"

    async def cancel_calcom(self, booking_uid: str, reason: str = "") -> str:
        """Cancel a Cal.com booking by UID."""
        api_key = cfg("CALCOM_API_KEY")
        if not api_key:
            return "Cal.com not configured."
        try:
            import httpx
            async with httpx.AsyncClient(timeout=15) as client:
                resp = await client.delete(
                    f"https://api.cal.com/v1/bookings/{booking_uid}",
                    headers={"Authorization": f"Bearer {api_key}"},
                    params={"reason": reason} if reason else {},
                )
            if resp.status_code not in (200, 204):
                raise ValueError(f"HTTP {resp.status_code}")
            return f"Cancelled Cal.com booking {booking_uid}."
        except Exception as exc:
            return f"Cancellation failed: {exc}"
