"""agent.py — Exotel WebSocket ↔ Gemini Live audio bridge.

Exotel dials the lead, streams mulaw-8kHz audio over WebSocket to our
/ws/exotel endpoint. This module decodes that audio, forwards it to Gemini
Live, plays back Gemini's PCM response, and handles all 9 function tools
(booking, CRM, SMS, Cal.com, transfer, etc.).

Audio pipeline:
  Exotel → mulaw 8 kHz → PCM16 16 kHz → Gemini Live
  Gemini Live → PCM16 24 kHz → PCM16 8 kHz → mulaw 8 kHz → Exotel
"""

import asyncio
import base64
import json
import logging
import time
from typing import Optional

try:
    import audioop  # stdlib — present in Python ≤ 3.12
except ModuleNotFoundError:
    import audioop_lts as audioop  # type: ignore[no-redef]  # Python 3.13+ shim

from config import cfg
from db import (
    check_slot, get_next_available, insert_appointment,
    log_call, log_error, get_setting,
    get_calls_by_phone, get_appointments_by_phone,
    get_contact_memory, add_contact_memory, compress_contact_memory,
    get_agent_profile,
)
from prompts import build_prompt

logger = logging.getLogger("exotel-agent")

# ── Audio sample rates ────────────────────────────────────────────────────────
EXOTEL_RATE      = 8000   # Exotel sends / expects mulaw at 8 kHz
GEMINI_IN_RATE   = 16000  # Gemini Live audio input
GEMINI_OUT_RATE  = 24000  # Gemini Live audio output (default)


# ── Gemini Live tool declarations ─────────────────────────────────────────────

def _build_gemini_tools():
    """Build google.genai Tool declarations for all 9 agent tools."""
    from google.genai import types

    def _s(desc, required=False):
        return types.Schema(type=types.Type.STRING, description=desc)

    return [types.Tool(function_declarations=[
        types.FunctionDeclaration(
            name="check_availability",
            description=(
                "Check whether a date/time slot is available for booking. "
                "Always call BEFORE confirming any slot to the lead."
            ),
            parameters=types.Schema(
                type=types.Type.OBJECT,
                required=["date", "time"],
                properties={
                    "date": _s("Date in YYYY-MM-DD format"),
                    "time": _s("Time in HH:MM 24-hour format"),
                },
            ),
        ),
        types.FunctionDeclaration(
            name="book_appointment",
            description=(
                "Book an appointment after the lead verbally confirms date, time, and service. "
                "Call ONLY after full verbal confirmation."
            ),
            parameters=types.Schema(
                type=types.Type.OBJECT,
                required=["name", "phone", "date", "time", "service"],
                properties={
                    "name":    _s("Lead's full name"),
                    "phone":   _s("Phone number with country code"),
                    "date":    _s("YYYY-MM-DD"),
                    "time":    _s("HH:MM 24-hour"),
                    "service": _s("Type of service being booked"),
                },
            ),
        ),
        types.FunctionDeclaration(
            name="end_call",
            description=(
                "End the call and log the outcome. "
                "ALWAYS call this before hanging up — never go silent."
            ),
            parameters=types.Schema(
                type=types.Type.OBJECT,
                required=["outcome"],
                properties={
                    "outcome": _s(
                        "One of: booked | not_interested | wrong_number | "
                        "voicemail | no_answer | callback_requested"
                    ),
                    "reason": _s("Brief reason for the outcome"),
                },
            ),
        ),
        types.FunctionDeclaration(
            name="transfer_to_human",
            description=(
                "Transfer the call to a human agent. "
                "Use when lead requests human, is angry, or the issue is too complex."
            ),
            parameters=types.Schema(
                type=types.Type.OBJECT,
                required=["reason"],
                properties={"reason": _s("Why you are transferring")},
            ),
        ),
        types.FunctionDeclaration(
            name="send_sms_confirmation",
            description="Send an SMS confirmation to the lead after a successful booking.",
            parameters=types.Schema(
                type=types.Type.OBJECT,
                required=["phone", "message"],
                properties={
                    "phone":   _s("Lead's phone number with country code"),
                    "message": _s("Full SMS message text"),
                },
            ),
        ),
        types.FunctionDeclaration(
            name="lookup_contact",
            description=(
                "Look up a contact's full call history, appointments, and remembered notes. "
                "Call this at the START of every call before speaking."
            ),
            parameters=types.Schema(
                type=types.Type.OBJECT,
                required=["phone"],
                properties={"phone": _s("Phone number with country code")},
            ),
        ),
        types.FunctionDeclaration(
            name="remember_details",
            description=(
                "Store a key insight about this lead for future calls. "
                "Use whenever you learn something useful: preferences, objections, timing."
            ),
            parameters=types.Schema(
                type=types.Type.OBJECT,
                required=["insight"],
                properties={"insight": _s("The detail to remember (max 200 chars)")},
            ),
        ),
        types.FunctionDeclaration(
            name="book_calcom",
            description="Book in Cal.com calendar after book_appointment succeeds.",
            parameters=types.Schema(
                type=types.Type.OBJECT,
                required=["name", "email", "date", "start_time"],
                properties={
                    "name":       _s("Lead's full name"),
                    "email":      _s("Lead's email address"),
                    "date":       _s("YYYY-MM-DD"),
                    "start_time": _s("HH:MM 24-hour"),
                    "notes":      _s("Optional booking notes"),
                },
            ),
        ),
        types.FunctionDeclaration(
            name="cancel_calcom",
            description="Cancel a Cal.com booking by its UID.",
            parameters=types.Schema(
                type=types.Type.OBJECT,
                required=["booking_uid"],
                properties={
                    "booking_uid": _s("The Cal.com booking UID from book_calcom"),
                    "reason":      _s("Optional cancellation reason"),
                },
            ),
        ),
    ])]


# ── Main handler class ────────────────────────────────────────────────────────

class ExotelCallHandler:
    """
    Manages one Exotel WebSocket call session end-to-end:
    audio bridging, Gemini Live session, tool execution, and call logging.
    """

    def __init__(
        self,
        websocket,
        phone_number: Optional[str] = None,
        lead_name: Optional[str] = None,
        agent_profile_id: Optional[str] = None,
        custom_prompt: Optional[str] = None,
    ):
        self.ws               = websocket
        self.phone_number     = phone_number
        self.lead_name        = lead_name
        self.agent_profile_id = agent_profile_id
        self.custom_prompt    = custom_prompt

        # Populated from Exotel start event
        self.stream_sid: Optional[str] = None
        self.call_sid:   Optional[str] = None

        self._call_start     = time.time()
        self.recording_url:  Optional[str] = None
        self._closed         = False

        # audioop rate-conversion states (persist across chunks for smooth audio)
        self._in_state:  object = None   # Exotel 8k → Gemini 16k
        self._out_state: object = None   # Gemini 24k → Exotel 8k

    # ── Helpers ───────────────────────────────────────────────────────────────

    async def _get(self, key: str, default: str = "") -> str:
        try:
            return await get_setting(key, default)
        except Exception:
            return cfg(key, default)

    async def _log(self, msg: str, detail: str = "", level: str = "info") -> None:
        (logger.info if level == "info" else logger.error)(msg)
        try:
            await log_error("agent", msg, detail, level)
        except Exception:
            pass

    # ── Audio codec helpers ───────────────────────────────────────────────────

    def _to_gemini(self, b64_payload: str) -> bytes:
        """raw PCM 8kHz base64 → PCM-16bit-16kHz bytes for Gemini input."""
        pcm_8k = base64.b64decode(b64_payload)   # already raw PCM, no mulaw decode
        pcm_16k, self._in_state = audioop.ratecv(
            pcm_8k, 2, 1, EXOTEL_RATE, GEMINI_IN_RATE, self._in_state
        )
        return pcm_16k

    def _to_exotel(self, pcm_bytes: bytes) -> str:
        """PCM-16bit-24kHz from Gemini → raw PCM 8kHz base64 for Exotel."""
        pcm_8k, self._out_state = audioop.ratecv(
            pcm_bytes, 2, 1, GEMINI_OUT_RATE, EXOTEL_RATE, self._out_state
        )
        return base64.b64encode(pcm_8k).decode()  # raw PCM, no mulaw encoding

    async def _send_media(self, pcm_bytes: bytes) -> None:
        """Encode Gemini audio and push to Exotel WebSocket."""
        if not self.stream_sid or self._closed:
            return
        if not hasattr(self.ws, 'client_state') or self.ws.client_state.value >= 2:
            return
        try:
            payload = self._to_exotel(pcm_bytes)
            await self.ws.send_text(json.dumps({
                "event": "media",
                "stream_sid": self.stream_sid,
                "media": {
                    "payload": payload,
                    "chunk": 1,
                    "timestamp": int(time.time() * 1000),
                },
            }))
        except Exception as exc:
            print(f"DEBUG: send_media error: {exc}", flush=True)
            logger.debug("Exotel send error: %s", exc)

    async def _send_keepalive_silence(self) -> None:
        """Send silence audio to Exotel every 100ms to keep connection alive
        while Gemini is generating its first response."""
        silence_chunk = b'\x00' * 3200
        silence_b64 = base64.b64encode(silence_chunk).decode()
        print("DEBUG: keepalive silence started", flush=True)
        chunks_sent = 0
        try:
            while not self._closed and chunks_sent < 100:
                if self.stream_sid and not self._closed:
                    try:
                        await self.ws.send_text(json.dumps({
                            "event": "media",
                            "stream_sid": self.stream_sid,
                            "media": {
                                "payload": silence_b64,
                                "chunk": chunks_sent + 1,
                                "timestamp": str(chunks_sent * 100),
                            },
                        }))
                        chunks_sent += 1
                    except Exception:
                        break
                await asyncio.sleep(0.1)
        except Exception as e:
            print(f"DEBUG: keepalive silence ended: {e}", flush=True)
        print(f"DEBUG: keepalive silence stopped after {chunks_sent} chunks", flush=True)

    # ── Tool implementations ──────────────────────────────────────────────────

    async def _exec_tool(self, name: str, args: dict) -> str:
        try:
            return await {
                "check_availability":   self._t_check_avail,
                "book_appointment":     self._t_book,
                "end_call":             self._t_end_call,
                "transfer_to_human":    self._t_transfer,
                "send_sms_confirmation":self._t_sms,
                "lookup_contact":       self._t_lookup,
                "remember_details":     self._t_remember,
                "book_calcom":          self._t_book_calcom,
                "cancel_calcom":        self._t_cancel_calcom,
            }.get(name, lambda **_: f"Unknown tool: {name}")(**args)
        except Exception as exc:
            await self._log(f"Tool {name} error: {exc}", str(exc), "error")
            return f"Tool error: {exc}"

    async def _t_check_avail(self, date: str, time: str) -> str:
        if await check_slot(date, time):
            return "available"
        nxt = await get_next_available(date, time)
        return f"unavailable: next available slot is {nxt}"

    async def _t_book(self, name: str, phone: str, date: str, time: str, service: str) -> str:
        bid = await insert_appointment(name, phone, date, time, service)
        return f"Confirmed! Booking ID: {bid}. See you on {date} at {time} for {service}."

    async def _t_end_call(self, outcome: str, reason: str = "") -> str:
        duration = int(time.time() - self._call_start)
        try:
            await log_call(
                phone_number=self.phone_number or "unknown",
                lead_name=self.lead_name,
                outcome=outcome,
                reason=reason,
                duration_seconds=duration,
                recording_url=self.recording_url,
            )
        except Exception as exc:
            logger.error("log_call failed: %s", exc)
        self._closed = True
        try:
            await self.ws.close()
        except Exception:
            pass
        return "Call ended."

    async def _t_transfer(self, reason: str) -> str:
        exotel_key   = cfg("EXOTEL_API_KEY")
        exotel_token = cfg("EXOTEL_API_TOKEN")
        exotel_sid   = cfg("EXOTEL_SID")
        transfer_app = cfg("EXOTEL_TRANSFER_APP_ID")
        transfer_num = cfg("EXOTEL_TRANSFER_NUMBER")

        if not (exotel_key and exotel_token and exotel_sid and self.call_sid):
            return "Transfer unavailable — missing Exotel credentials or call SID."

        try:
            import httpx
            redirect_url = (
                f"https://my.exotel.com/{exotel_sid}/exoml/start_voice/{transfer_app}"
                if transfer_app else
                f"https://api.exotel.com/v1/Accounts/{exotel_sid}/Calls/{self.call_sid}"
            )
            async with httpx.AsyncClient(timeout=10) as client:
                await client.post(
                    f"https://api.exotel.com/v1/Accounts/{exotel_sid}/Calls/{self.call_sid}/redirect",
                    auth=(exotel_key, exotel_token),
                    data={"Url": redirect_url, "Method": "POST"},
                )
            self._closed = True
            return "Transferring you to a human agent now. Please hold."
        except Exception as exc:
            logger.warning("Transfer failed: %s", exc)
            return "Transfer failed. Please call us back directly."

    async def _t_sms(self, phone: str, message: str) -> str:
        sid   = cfg("TWILIO_ACCOUNT_SID")
        token = cfg("TWILIO_AUTH_TOKEN")
        from_ = cfg("TWILIO_FROM_NUMBER")
        if not (sid and token and from_):
            return "SMS skipped: Twilio not configured."
        try:
            from twilio.rest import Client
            client = Client(sid, token)
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(
                None, lambda: client.messages.create(body=message, from_=from_, to=phone)
            )
            return f"SMS sent to {phone}."
        except Exception as exc:
            return "SMS delivery failed — booking is still confirmed."

    async def _t_lookup(self, phone: str) -> str:
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
                    lines.append(f"  • {m['insight']}")
            if calls:
                lines.append(f"\nCALL HISTORY ({len(calls)} calls):")
                for c in calls[:5]:
                    ts = (c.get("timestamp") or "")[:16]
                    lines.append(f"  • {ts} — {c.get('outcome','?')}: {c.get('reason','')}")
            if appointments:
                lines.append(f"\nAPPOINTMENTS ({len(appointments)}):")
                for a in appointments[:3]:
                    lines.append(f"  • {a.get('date')} {a.get('time')} — {a.get('service')} [{a.get('status')}]")
            return "\n".join(lines)
        except Exception:
            return "Unable to retrieve contact history."

    async def _t_remember(self, insight: str) -> str:
        if not self.phone_number:
            return "Cannot remember — no phone number for this call."
        await add_contact_memory(self.phone_number, insight)
        memories = await get_contact_memory(self.phone_number)
        if len(memories) >= 5:
            asyncio.create_task(self._compress_memories())
        return f"Remembered: {insight}"

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
            prompt = f"Compress these notes about a sales contact into 3-5 concise bullets. Keep all key facts.\n\n{bullets}"
            loop = asyncio.get_event_loop()
            resp = await loop.run_in_executor(None, lambda: model.generate_content(prompt))
            if resp.text.strip():
                await compress_contact_memory(self.phone_number, resp.text.strip())
        except Exception as exc:
            logger.warning("Memory compression failed: %s", exc)

    async def _t_book_calcom(self, name: str, email: str, date: str, start_time: str, notes: str = "") -> str:
        api_key  = cfg("CALCOM_API_KEY")
        event_id = cfg("CALCOM_EVENT_TYPE_ID")
        tz       = cfg("CALCOM_TIMEZONE")
        if not (api_key and event_id):
            return "Cal.com not configured — skipping."
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
            return f"Cal.com booked. UID: {data.get('uid','')}"
        except Exception as exc:
            return f"Cal.com booking failed: {exc}"

    async def _t_cancel_calcom(self, booking_uid: str, reason: str = "") -> str:
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

    # ── Gemini receive loop ───────────────────────────────────────────────────

    async def _recv_gemini(self, session) -> None:
        """Read Gemini Live responses: forward audio to Exotel, execute tool calls."""
        from google.genai import types
        print("DEBUG: _recv_gemini started", flush=True)
        response_count = 0
        try:
            async for response in session.receive():
                if self._closed:
                    break

                response_count += 1
                if response_count <= 5:
                    print(f"DEBUG: gemini response #{response_count} type={type(response).__name__}", flush=True)
                    print(f"DEBUG: response attrs={[a for a in dir(response) if not a.startswith('_')]}", flush=True)
                    print(f"DEBUG: server_content={getattr(response, 'server_content', None)}", flush=True)
                    print(f"DEBUG: setup_complete={getattr(response, 'setup_complete', None)}", flush=True)
                    print(f"DEBUG: data={getattr(response, 'data', None)}", flush=True)

                # ── Audio output ──────────────────────────────────────────────
                audio: Optional[bytes] = None

                if getattr(response, "data", None):
                    audio = response.data
                    print(f"DEBUG: audio from response.data len={len(audio)}", flush=True)
                elif (
                    getattr(response, "server_content", None)
                    and getattr(response.server_content, "model_turn", None)
                ):
                    for part in response.server_content.model_turn.parts or []:
                        if getattr(getattr(part, "inline_data", None), "data", None):
                            audio = part.inline_data.data
                            print(f"DEBUG: audio from inline_data len={len(audio)}", flush=True)
                            break
                if audio:
                    await self._send_media(audio)
                elif response_count <= 5:
                    print(f"DEBUG: no audio in this response", flush=True)

                # ── Tool calls ────────────────────────────────────────────────
                if getattr(response, "tool_call", None):
                    fn_responses = []
                    for fn in response.tool_call.function_calls or []:
                        args = dict(fn.args) if fn.args else {}
                        await self._log(f"Tool: {fn.name}({args})")
                        result = await self._exec_tool(fn.name, args)
                        fn_responses.append(types.FunctionResponse(
                            name=fn.name, id=fn.id, response={"result": result}
                        ))
                    if fn_responses:
                        await session.send_tool_response(
                            function_responses=fn_responses
                        )

        except Exception as exc:
            print(f"DEBUG: _recv_gemini error: {type(exc).__name__}: {exc}", flush=True)
            await self._log(f"Gemini receive error: {exc}", str(exc), "error")
      
        print(f"DEBUG: _recv_gemini ended after {response_count} responses", flush=True)

    # ── Exotel receive loop (drains buffer queue then reads live) ─────────────

    async def _recv_exotel_from_queue(self, session, queue: asyncio.Queue) -> None:
        """Process Exotel events — drains buffer queue first, then reads live from WebSocket."""
        from google.genai import types
        print("DEBUG: _recv_exotel_from_queue started", flush=True)
        try:
            while not self._closed:
                # Try queue first (messages buffered during Gemini connect)
                try:
                    kind, raw = queue.get_nowait()
                    if kind == "closed":
                        print("DEBUG: WS closed signal from queue", flush=True)
                        break
                except asyncio.QueueEmpty:
                    # Queue empty — read live from WebSocket
                    # then read live directly (buffer task must be done first)
                    await asyncio.sleep(0)  # yield to let buffer task finish
                    try:
                        kind, raw = queue.get_nowait()
                        if kind == "closed":
                            print("DEBUG: WS closed signal from queue", flush=True)
                            break
                    except asyncio.QueueEmpty:
                        # Truly empty — read live from WebSocket
                        try:
                            raw = await self.ws.receive_text()
                        except Exception as e:
                            print(f"DEBUG: live receive ended: {type(e).__name__}: {e}", flush=True)
                            break

                try:
                    data  = json.loads(raw)
                    event = data.get("event")

                    if event == "connected":
                        await self._log("Exotel WebSocket connected")

                    elif event == "start":
                        s = data.get("start", {})
                        self.stream_sid = s.get("streamSid") or s.get("stream_sid")
                        self.call_sid   = s.get("callSid") or s.get("call_sid")
                        media_format = s.get("media_format", {})
                        print(f"DEBUG: media_format={media_format}", flush=True)
                        print(f"DEBUG: full start payload={json.dumps(s)[:300]}", flush=True)
                        params = s.get("custom_parameters", {}) or s.get("customParameters", {})
                        # custom_parameters may be a JSON string — parse it
                        if isinstance(params, str):
                            try:
                                import json as _json
                                params = _json.loads(params)
                            except Exception:
                                params = {}
                        if not self.phone_number:
                            self.phone_number = (
                                params.get("phone_number") or
                                params.get("phone") or
                                s.get("from") or
                                s.get("From")
                            )
                        if not self.lead_name:
                            self.lead_name = params.get("lead_name") or params.get("name")
                        print(f"DEBUG: params={params} phone={self.phone_number}", flush=True)
                        print(f"DEBUG: stream_sid={self.stream_sid} call_sid={self.call_sid}", flush=True)
                        await self._log(
                            f"Stream started: sid={self.stream_sid} call={self.call_sid} phone={self.phone_number}"
                        )
                        # Send silence immediately to keep Exotel connection alive
                        # while Gemini is generating the first response
                        asyncio.create_task(self._send_keepalive_silence())

                    elif event == "media":
                        track = data["media"].get("track", "inbound")
                        if track == "inbound":
                            try:
                                pcm = self._to_gemini(data["media"]["payload"])
                                await session.send_realtime_input(
                                    audio=types.Blob(
                                        data=pcm,
                                        mime_type=f"audio/pcm;rate={GEMINI_IN_RATE}",
                                    )
                                )
                                await asyncio.sleep(0)  # yield to _recv_gemini
                            except Exception as exc:
                                logger.debug("Audio forward error: %s", exc)
                        elif track == "outbound":
                            pass  # ignore outbound (our own audio echoed back)

                    elif event == "stop":
                        await self._log("Exotel stream stopped")
                        print("DEBUG: got stop event", flush=True)
                        break

                except Exception as pe:
                    print(f"DEBUG: parse error: {pe} raw={raw[:100]}", flush=True)

        except Exception as exc:
            await self._log(f"Exotel receive error: {exc}", str(exc), "error")

    # ── Entry point ───────────────────────────────────────────────────────────

    async def run(self) -> None:
        """
        Build Gemini Live session then run Exotel↔Gemini loops concurrently.
        Called by the /ws/exotel WebSocket endpoint in server.py.

        KEY FIX: Start buffering Exotel messages immediately into a queue so
        Exotel does not time out while Gemini connects (~1 second delay).
        """
        print("DEBUG: handler.run() started", flush=True)
        try:
            from google import genai
            from google.genai import types
            print("DEBUG: google-genai imported OK", flush=True)
        except Exception as imp_err:
            import traceback
            print(f"DEBUG: IMPORT FAILED: {imp_err}\n{traceback.format_exc()}", flush=True)
            return

        api_key = cfg("GOOGLE_API_KEY")
        print(f"DEBUG: GOOGLE_API_KEY present={bool(api_key)}", flush=True)
        if not api_key:
            await self._log("GOOGLE_API_KEY not set", level="error")
            try:
                await self.ws.close()
            except Exception:
                pass
            return

        model_id = cfg("GEMINI_MODEL")
        voice    = cfg("GEMINI_TTS_VOICE")
        print(f"DEBUG: model={model_id} voice={voice}", flush=True)

        # Load agent profile (overrides voice / model / prompt)
        profile = None
        if self.agent_profile_id:
            try:
                profile = await get_agent_profile(self.agent_profile_id)
                if profile:
                    if profile.get("voice"):  voice    = profile["voice"]
                    if profile.get("model"):  model_id = profile["model"]
            except Exception as exc:
                logger.warning("Could not load agent profile: %s", exc)

        business_name = await self._get("BUSINESS_NAME", "our company")
        service_type  = await self._get("SERVICE_TYPE", "our service")
        prompt_tmpl   = self.custom_prompt or (profile and profile.get("system_prompt"))
        system_prompt = build_prompt(
            lead_name=self.lead_name or "there",
            business_name=business_name,
            service_type=service_type,
            custom_prompt=prompt_tmpl,
        )

        print("DEBUG: creating Gemini client", flush=True)
        try:
            client = genai.Client(
                api_key=api_key,
                http_options={"api_version": "v1beta"},
            )
            print("DEBUG: Gemini client created OK", flush=True)
        except Exception as e:
            import traceback
            print(f"DEBUG: Gemini client FAILED: {e}\n{traceback.format_exc()}", flush=True)
            return

        # Silence-prevention config
        realtime_cfg    = None
        ctx_compression = None

        try:
            realtime_cfg = types.RealtimeInputConfig(
                automatic_activity_detection=types.AutomaticActivityDetection(
                    disabled=True,  # disable VAD — we control turn-taking manually
                ),
            )
            logger.info("VAD disabled — manual turn control")
        except Exception as e:
            logger.warning("VAD config skipped: %s", e)

        try:
            ctx_compression = types.ContextWindowCompressionConfig(
                trigger_tokens=25600,
                sliding_window=types.SlidingWindow(target_tokens=12800),
            )
            logger.info("Context compression config applied")
        except Exception as e:
            logger.warning("Context compression skipped: %s", e)

        config_kwargs: dict = dict(
            response_modalities=["AUDIO"],
            speech_config=types.SpeechConfig(
                voice_config=types.VoiceConfig(
                    prebuilt_voice_config=types.PrebuiltVoiceConfig(voice_name=voice)
                )
            ),
            system_instruction=system_prompt,
            tools=_build_gemini_tools(),
        )
        if realtime_cfg:
            config_kwargs["realtime_input_config"] = realtime_cfg
        if ctx_compression:
            config_kwargs["context_window_compression"] = ctx_compression

        config = types.LiveConnectConfig(**config_kwargs)

        # ── Start buffering Exotel messages IMMEDIATELY ───────────────────────
        # This prevents Exotel from timing out while Gemini connects (~1 second)
        exotel_queue: asyncio.Queue = asyncio.Queue()

        async def _buffer_exotel():
            """Read Exotel WebSocket messages into queue without blocking."""
            print("DEBUG: buffer task started", flush=True)
            try:
                while True:
                    try:
                        raw = await self.ws.receive_text()
                        await exotel_queue.put(("text", raw))
                        print(f"DEBUG: buffered: {raw[:80]}", flush=True)
                    except Exception as e:
                        print(f"DEBUG: buffer ended: {type(e).__name__}: {e}", flush=True)
                        await exotel_queue.put(("closed", None))
                        break
            except Exception as e:
                print(f"DEBUG: buffer task error: {e}", flush=True)
                await exotel_queue.put(("closed", None))

        buffer_task = asyncio.create_task(_buffer_exotel())

        # ── Now connect to Gemini (buffer keeps Exotel messages safe) ─────────
        await self._log(f"Gemini Live starting: model={model_id} voice={voice}")
        print(f"DEBUG: connecting to Gemini Live model={model_id}", flush=True)

        try:
            async with client.aio.live.connect(model=model_id, config=config) as session:
                print("DEBUG: Gemini Live session connected!", flush=True)

                # Trigger Gemini to speak first immediately
                try:
                    await session.send_realtime_input(
                        text=f"The call just connected. Greet the lead immediately. Say: Hi, am I speaking with {self.lead_name or 'there'}?"
                    )
                    print("DEBUG: initial greeting sent to Gemini", flush=True)
                except Exception as e:
                    print(f"DEBUG: initial greeting failed: {e}", flush=True)

                # Stop buffer task before starting live reading
                # (can't have two coroutines reading the same WebSocket)
                buffer_task.cancel()
                try:
                    await buffer_task
                except asyncio.CancelledError:
                    pass
                print("DEBUG: buffer task stopped, switching to live reading", flush=True)

                # Run Exotel (queue drain + live) and Gemini receive loops together
                exotel_task = asyncio.create_task(
                    self._recv_exotel_from_queue(session, exotel_queue)
                )
                gemini_task = asyncio.create_task(self._recv_gemini(session))
                keepalive_task = asyncio.create_task(self._send_keepalive_silence())

                # Stop as soon as either loop finishes
                done, pending = await asyncio.wait(
                    [exotel_task, gemini_task],
                    return_when=asyncio.FIRST_COMPLETED,
                )
                keepalive_task.cancel()
                for task in pending:
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass
                for task in done:
                    if not task.cancelled():
                        exc = task.exception()
                        if exc:
                            print(f"DEBUG: task error: {type(exc).__name__}: {exc}", flush=True)

        except Exception as exc:
            await self._log(f"Gemini session error: {exc}", str(exc), "error")
        finally:
            if not buffer_task.done():
                buffer_task.cancel()
                try:
                    await buffer_task
                except asyncio.CancelledError:
                    pass
            if not self._closed:
                duration = int(time.time() - self._call_start)
                try:
                    await log_call(
                        phone_number=self.phone_number or "unknown",
                        lead_name=self.lead_name,
                        outcome="disconnected",
                        reason="session ended without end_call",
                        duration_seconds=duration,
                        recording_url=self.recording_url,
                    )
                except Exception:
                    pass
