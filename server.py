import traceback
import asyncio
import csv
import io
import json
import os
import uuid
from datetime import datetime
from typing import Optional

import httpx
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from fastapi import FastAPI, File, Form, HTTPException, Request, UploadFile, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse

from config import cfg
from agent import ExotelCallHandler
from db import (
    init_db, get_all_settings, save_settings, get_setting, set_setting,
    get_all_appointments, cancel_appointment, get_appointments_by_phone,
    log_call, get_all_calls, get_calls_by_phone, update_call_notes, get_contacts,
    get_stats, log_error, get_errors, get_logs, clear_errors,
    create_campaign, get_all_campaigns, get_campaign, update_campaign_status,
    update_campaign_run_stats, delete_campaign,
    get_contact_memory, add_contact_memory,
    get_all_agent_profiles, get_agent_profile, create_agent_profile,
    update_agent_profile, delete_agent_profile, set_default_agent_profile,
)
from prompts import DEFAULT_SYSTEM_PROMPT

app = FastAPI(title="OutboundAI", version="1.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)
scheduler = AsyncIOScheduler()


# ── Exotel outbound call dispatch ─────────────────────────────────────────────

async def _initiate_exotel_call(
    phone_number: str,
    lead_name: Optional[str] = None,
    system_prompt: Optional[str] = None,
    agent_profile_id: Optional[str] = None,
) -> dict:
    """
    Initiate an outbound call via the Exotel REST API.

    Exotel dials the number, and when the call connects it routes audio
    through the Exotel app configured with our WebSocket URL
    (wss://your-server/ws/exotel).

    Required env vars:
      EXOTEL_API_KEY, EXOTEL_API_TOKEN, EXOTEL_SID,
      EXOTEL_CALLER_ID, EXOTEL_APP_ID

    Optional:
      EXOTEL_STATUS_CALLBACK  — URL for terminal-event webhooks
    """
    api_key   = cfg("EXOTEL_API_KEY")
    api_token = cfg("EXOTEL_API_TOKEN")
    sid       = cfg("EXOTEL_SID")
    caller_id = cfg("EXOTEL_CALLER_ID")
    app_id    = cfg("EXOTEL_APP_ID")

    if not (api_key and api_token and sid):
        raise HTTPException(400, "Exotel credentials not configured. Set EXOTEL_API_KEY, EXOTEL_API_TOKEN, EXOTEL_SID.")
    if not caller_id:
        raise HTTPException(400, "EXOTEL_CALLER_ID not configured.")
    if not app_id:
        raise HTTPException(400, "EXOTEL_APP_ID not configured. Create an Exotel app with WebSocket streaming and set its ID here.")

    clean = phone_number.strip().replace(" ", "").replace("-", "")
    if not clean.startswith("+"):
        clean = "+" + clean

    # CustomField carries per-call metadata into the WebSocket start event
    custom_field = json.dumps({
        "phone_number":     clean,
        "lead_name":        lead_name or "",
        "agent_profile_id": agent_profile_id or "",
        "system_prompt":    (system_prompt or "")[:500],  # Exotel field has size limits
    })

    status_cb = cfg("EXOTEL_STATUS_CALLBACK")
    payload = {
        "From":     clean,
        "CallerId": caller_id,
        "Url":      f"https://my.exotel.com/{sid}/exoml/start_voice/{app_id}",
    }
    if status_cb:
        payload["StatusCallback"]        = status_cb
        payload["StatusCallbackEvents[0]"] = "terminal"
    if custom_field:
        payload["CustomField"] = custom_field

    url = f"https://api.exotel.com/v1/Accounts/{sid}/Calls/connect"
    print(f"DEBUG EXOTEL PAYLOAD: From={clean} CallerId={caller_id} AppId={app_id} Url={payload['Url']}", flush=True)

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.post(url, auth=(api_key, api_token), data=payload)
        print(f"DEBUG EXOTEL RESPONSE: status={resp.status_code} body={resp.text[:300]}", flush=True)
        data = resp.json() if resp.headers.get("content-type", "").startswith("application/json") else {}
        if resp.status_code not in (200, 201):
            detail = data.get("RestException", {}).get("Message", "") or str(data) or f"HTTP {resp.status_code}"
            raise ValueError(detail)
        call_sid = (data.get("Call") or {}).get("Sid", "")
        await log_error("server", f"Exotel call dispatched to {clean}", f"call_sid={call_sid}", "info")
        return {"status": "dialing", "phone": clean, "lead_name": lead_name, "call_sid": call_sid}
    except HTTPException:
        raise
    except Exception as exc:
        await log_error("server", f"Exotel dispatch error: {exc}", str(exc), "error")
        raise HTTPException(500, f"Exotel call failed: {exc}")


# ── Campaign execution ────────────────────────────────────────────────────────

async def _run_campaign(campaign_id: str) -> None:
    campaign = await get_campaign(campaign_id)
    if not campaign or campaign.get("status") not in ("active", "scheduled"):
        return
    await log_error("scheduler", f"Campaign '{campaign['name']}' started", f"id={campaign_id}", "info")
    try:
        contacts = json.loads(campaign.get("contacts_json") or "[]")
    except Exception:
        contacts = []

    delay      = int(campaign.get("call_delay_seconds") or 3)
    sys_prompt = campaign.get("system_prompt")
    profile_id = campaign.get("agent_profile_id")
    dispatched = failed = 0

    for contact in contacts:
        phone = contact.get("phone") or contact.get("phone_number") or ""
        name  = contact.get("name") or contact.get("lead_name") or ""
        if not phone:
            failed += 1
            continue
        try:
            await _initiate_exotel_call(phone, name, sys_prompt, profile_id)
            dispatched += 1
        except Exception as exc:
            await log_error("scheduler", f"Campaign call failed for {phone}", str(exc), "error")
            failed += 1
        await asyncio.sleep(delay)

    await update_campaign_run_stats(campaign_id, dispatched, failed)
    await log_error(
        "scheduler",
        f"Campaign '{campaign['name']}' done: {dispatched} dispatched, {failed} failed",
        "", "info",
    )


def _schedule_campaign(campaign: dict) -> None:
    cid    = campaign["id"]
    stype  = campaign.get("schedule_type", "once")
    stime  = campaign.get("schedule_time", "09:00")
    job_id = f"campaign_{cid}"
    if scheduler.get_job(job_id):
        scheduler.remove_job(job_id)
    try:
        hour, minute = (stime or "09:00").split(":")
        if stype == "daily":
            scheduler.add_job(
                _run_campaign, CronTrigger(hour=int(hour), minute=int(minute)),
                id=job_id, args=[cid], replace_existing=True,
            )
        elif stype == "weekdays":
            scheduler.add_job(
                _run_campaign, CronTrigger(day_of_week="mon-fri", hour=int(hour), minute=int(minute)),
                id=job_id, args=[cid], replace_existing=True,
            )
    except Exception:
        pass


# ── Startup / Shutdown ────────────────────────────────────────────────────────

@app.on_event("startup")
async def startup_event() -> None:
    init_db()
    scheduler.start()
    try:
        campaigns = await get_all_campaigns()
        for c in campaigns:
            if c.get("status") == "active" and c.get("schedule_type") in ("daily", "weekdays"):
                _schedule_campaign(c)
    except Exception as exc:
        print(f"Campaign scheduling startup error: {exc}")
    await log_error("server", "OutboundAI server started", "", "info")


@app.on_event("shutdown")
async def shutdown_event() -> None:
    scheduler.shutdown(wait=False)


# ── Exotel WebSocket endpoint ─────────────────────────────────────────────────

@app.websocket("/ws/exotel")
async def exotel_ws(
    websocket: WebSocket,
    phone_number: Optional[str] = None,
    lead_name: Optional[str] = None,
    agent_profile_id: Optional[str] = None,
):
    """
    Exotel streams call audio here via WebSocket.
    Configure your Exotel app's Stream URL as:
        wss://your-server.com/ws/exotel

    Optionally pass query params for pre-seeded metadata:
        ?phone_number=+91...&lead_name=Rahul&agent_profile_id=<uuid>

    Per-call metadata can also be embedded in Exotel's CustomField JSON
    (phone_number, lead_name, agent_profile_id, system_prompt) — the
    handler reads them from the WebSocket start event.
    """
    await websocket.accept()
    handler = ExotelCallHandler(
        websocket=websocket,
        phone_number=phone_number,
        lead_name=lead_name,
        agent_profile_id=agent_profile_id,
    )
    try:
        await handler.run()
    except WebSocketDisconnect:
        pass
    except Exception as exc:
        tb = traceback.format_exc()
        # Print directly to stdout so it shows in Coolify logs regardless of Supabase
        print(f"WEBSOCKET ERROR: {type(exc).__name__}: {exc}\n{tb}", flush=True)
        try:
            await log_error("server", f"WebSocket handler error: {type(exc).__name__}: {exc}", tb, "error")
        except Exception:
            pass

# ── Exotel status callback (optional webhook) ─────────────────────────────────

@app.post("/api/exotel/callback")
async def exotel_callback(request: Request) -> dict:
    """
    Receives Exotel terminal-event webhooks.
    Useful for logging missed calls that never hit the WebSocket.
    """
    form = await request.form()
    status   = form.get("Status", "")
    call_sid = form.get("CallSid", "")
    to       = form.get("To", "")
    duration = int(form.get("Duration", 0) or 0)
    await log_error("server", f"Exotel callback: {to} → {status}", f"call_sid={call_sid}", "info")
    return {"received": True}


# ── Root / UI ─────────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def serve_ui() -> HTMLResponse:
    ui_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "ui", "index.html")
    if os.path.exists(ui_path):
        with open(ui_path, "r", encoding="utf-8") as f:
            return HTMLResponse(f.read())
    return HTMLResponse("<h1>UI not found. Place ui/index.html in the app directory.</h1>", status_code=500)


@app.get("/api/health")
async def health() -> dict:
    return {"status": "ok", "timestamp": datetime.now().isoformat()}


# ── Single Call ───────────────────────────────────────────────────────────────

@app.post("/api/call/single")
async def single_call(request: Request) -> dict:
    data  = await request.json()
    phone = data.get("phone") or data.get("phone_number") or ""
    if not phone:
        raise HTTPException(400, "phone required")
    return await _initiate_exotel_call(
        phone_number=phone,
        lead_name=data.get("name") or data.get("lead_name"),
        system_prompt=data.get("system_prompt"),
        agent_profile_id=data.get("agent_profile_id"),
    )


# ── Batch CSV ─────────────────────────────────────────────────────────────────

@app.post("/api/call/batch")
async def batch_call(
    file: UploadFile = File(None),
    contacts_json: str = Form(None),
    system_prompt: str = Form(None),
    agent_profile_id: str = Form(None),
    delay_seconds: int = Form(3),
) -> dict:
    contacts: list = []

    if file and file.filename:
        content = await file.read()
        try:
            text = content.decode("utf-8-sig")
        except Exception:
            text = content.decode("latin-1")
        reader = csv.DictReader(io.StringIO(text))
        for row in reader:
            phone = row.get("phone") or row.get("phone_number") or row.get("Phone") or ""
            name  = row.get("name") or row.get("Name") or row.get("lead_name") or ""
            if phone:
                contacts.append({"phone": phone, "name": name})
    elif contacts_json:
        try:
            contacts = json.loads(contacts_json)
        except Exception:
            raise HTTPException(400, "Invalid contacts_json")

    if not contacts:
        raise HTTPException(400, "No contacts provided")

    async def _run():
        for contact in contacts:
            try:
                await _initiate_exotel_call(
                    contact.get("phone", ""), contact.get("name"),
                    system_prompt, agent_profile_id,
                )
            except Exception as exc:
                await log_error("server", f"Batch call failed: {contact.get('phone')}", str(exc), "error")
            await asyncio.sleep(delay_seconds)

    asyncio.create_task(_run())
    return {"status": "started", "total": len(contacts), "delay_seconds": delay_seconds}


# ── Appointments ──────────────────────────────────────────────────────────────

@app.get("/api/appointments")
async def list_appointments(date: Optional[str] = None) -> list:
    return await get_all_appointments(date)


@app.delete("/api/appointments/{appointment_id}")
async def cancel_appt(appointment_id: str) -> dict:
    ok = await cancel_appointment(appointment_id)
    if not ok:
        raise HTTPException(404, "Appointment not found or already cancelled")
    return {"status": "cancelled"}


# ── Call Logs ─────────────────────────────────────────────────────────────────

@app.get("/api/call-logs")
async def list_call_logs(page: int = 1, limit: int = 20) -> list:
    return await get_all_calls(page, limit)


@app.patch("/api/call-logs/{call_id}/notes")
async def update_notes(call_id: str, request: Request) -> dict:
    data = await request.json()
    ok   = await update_call_notes(call_id, data.get("notes", ""))
    if not ok:
        raise HTTPException(404, "Call log not found")
    return {"status": "updated"}


# ── Contacts / CRM ────────────────────────────────────────────────────────────

@app.get("/api/contacts")
async def list_contacts() -> list:
    return await get_contacts()


@app.get("/api/contacts/{phone}/history")
async def contact_history(phone: str) -> dict:
    calls        = await get_calls_by_phone(phone)
    appointments = await get_appointments_by_phone(phone)
    memories     = await get_contact_memory(phone)
    return {"phone": phone, "calls": calls, "appointments": appointments, "memories": memories}


@app.get("/api/contacts/{phone}/memory")
async def get_memory(phone: str) -> list:
    return await get_contact_memory(phone)


@app.post("/api/contacts/{phone}/memory")
async def add_memory(phone: str, request: Request) -> dict:
    data    = await request.json()
    insight = data.get("insight", "")
    if not insight:
        raise HTTPException(400, "insight required")
    await add_contact_memory(phone, insight)
    return {"status": "saved"}


# ── Stats ─────────────────────────────────────────────────────────────────────

@app.get("/api/stats")
async def stats() -> dict:
    return await get_stats()


# ── Campaigns ─────────────────────────────────────────────────────────────────

@app.get("/api/campaigns")
async def list_campaigns() -> list:
    return await get_all_campaigns()


@app.post("/api/campaigns")
async def create_camp(request: Request) -> dict:
    data = await request.json()
    name = (data.get("name") or "").strip()
    if not name:
        raise HTTPException(400, "name required")
    campaign_id = await create_campaign(
        name=name,
        contacts_json=json.dumps(data.get("contacts", [])),
        schedule_type=data.get("schedule_type", "once"),
        schedule_time=data.get("schedule_time", "09:00"),
        call_delay_seconds=int(data.get("call_delay_seconds", 3)),
        system_prompt=data.get("system_prompt"),
        agent_profile_id=data.get("agent_profile_id"),
    )
    campaign = await get_campaign(campaign_id)
    if campaign and campaign.get("schedule_type") in ("daily", "weekdays"):
        _schedule_campaign(campaign)
    return {"status": "created", "id": campaign_id}


@app.delete("/api/campaigns/{campaign_id}")
async def del_campaign(campaign_id: str) -> dict:
    job_id = f"campaign_{campaign_id}"
    if scheduler.get_job(job_id):
        scheduler.remove_job(job_id)
    ok = await delete_campaign(campaign_id)
    if not ok:
        raise HTTPException(404, "Campaign not found")
    return {"status": "deleted"}


@app.patch("/api/campaigns/{campaign_id}/status")
async def update_camp_status(campaign_id: str, request: Request) -> dict:
    data   = await request.json()
    status = data.get("status", "")
    if status not in ("active", "paused", "completed"):
        raise HTTPException(400, "status must be: active | paused | completed")
    ok = await update_campaign_status(campaign_id, status)
    if not ok:
        raise HTTPException(404, "Campaign not found")
    return {"status": "updated"}


@app.post("/api/campaigns/{campaign_id}/run")
async def run_campaign_now(campaign_id: str) -> dict:
    campaign = await get_campaign(campaign_id)
    if not campaign:
        raise HTTPException(404, "Campaign not found")
    asyncio.create_task(_run_campaign(campaign_id))
    return {"status": "running", "id": campaign_id}


# ── Agent Profiles ────────────────────────────────────────────────────────────

@app.get("/api/agent-profiles")
async def list_profiles() -> list:
    return await get_all_agent_profiles()


@app.post("/api/agent-profiles")
async def create_profile(request: Request) -> dict:
    data = await request.json()
    name = (data.get("name") or "").strip()
    if not name:
        raise HTTPException(400, "name required")
    pid = await create_agent_profile(
        name=name,
        voice=data.get("voice", "Aoede"),
        model=data.get("model", "gemini-3.1-flash-live-preview"),
        system_prompt=data.get("system_prompt"),
        enabled_tools=json.dumps(data.get("enabled_tools", [])),
        is_default=bool(data.get("is_default", False)),
    )
    return {"status": "created", "id": pid}


@app.put("/api/agent-profiles/{profile_id}")
async def update_profile(profile_id: str, request: Request) -> dict:
    data    = await request.json()
    updates = {f: data[f] for f in ("name", "voice", "model", "system_prompt") if f in data}
    if "enabled_tools" in data:
        updates["enabled_tools"] = json.dumps(data["enabled_tools"])
    if not updates:
        raise HTTPException(400, "No fields to update")
    ok = await update_agent_profile(profile_id, updates)
    if not ok:
        raise HTTPException(404, "Profile not found")
    return {"status": "updated"}


@app.delete("/api/agent-profiles/{profile_id}")
async def del_profile(profile_id: str) -> dict:
    ok = await delete_agent_profile(profile_id)
    if not ok:
        raise HTTPException(404, "Profile not found")
    return {"status": "deleted"}


@app.post("/api/agent-profiles/{profile_id}/set-default")
async def set_default_profile(profile_id: str) -> dict:
    await set_default_agent_profile(profile_id)
    return {"status": "updated"}


# ── Settings ──────────────────────────────────────────────────────────────────

@app.get("/api/settings")
async def get_settings_ep() -> dict:
    return await get_all_settings()


@app.post("/api/settings")
async def post_settings(request: Request) -> dict:
    data = await request.json()
    await save_settings(data)
    for k, v in data.items():
        if v:
            os.environ[k] = str(v)
    return {"status": "saved"}


@app.get("/api/settings/prompt")
async def get_prompt() -> dict:
    custom = await get_setting("CUSTOM_PROMPT", "")
    return {"prompt": custom or DEFAULT_SYSTEM_PROMPT, "is_custom": bool(custom)}


@app.post("/api/settings/prompt")
async def save_prompt(request: Request) -> dict:
    data = await request.json()
    await set_setting("CUSTOM_PROMPT", (data.get("prompt") or "").strip())
    return {"status": "saved"}


@app.delete("/api/settings/prompt")
async def reset_prompt() -> dict:
    await set_setting("CUSTOM_PROMPT", "")
    return {"status": "reset", "prompt": DEFAULT_SYSTEM_PROMPT}


# ── Logs ──────────────────────────────────────────────────────────────────────

@app.get("/api/logs")
async def list_logs(
    level: Optional[str] = None,
    source: Optional[str] = None,
    limit: int = 200,
) -> list:
    return await get_logs(level=level, source=source, limit=limit)


@app.delete("/api/logs")
async def delete_logs() -> dict:
    await clear_errors()
    return {"status": "cleared"}
