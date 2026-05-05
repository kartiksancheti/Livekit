import asyncio
import threading
import uuid
from datetime import datetime, timedelta
from typing import Optional
from collections import defaultdict

from config import cfg, SENSITIVE_KEYS, ALL_SETTING_KEYS

# ── Singleton Supabase client ─────────────────────────────────────────────────
# The sync client (httpx.Client) is thread-safe for concurrent requests.
# Creating one client per call (the prior bug) opened a new httpx session on
# every DB operation, exhausting file descriptors and slowing calls.

_db_client = None
_db_lock = threading.Lock()


def _get_db():
    """Return the shared sync Supabase client, creating it on first call."""
    global _db_client
    if _db_client is None:
        with _db_lock:
            if _db_client is None:
                from supabase import create_client
                _db_client = create_client(cfg("SUPABASE_URL"), cfg("SUPABASE_SERVICE_KEY"))
    return _db_client


async def _run(fn):
    """Run a synchronous Supabase call in a thread pool without blocking the event loop."""
    return await asyncio.to_thread(fn)


def init_db() -> None:
    url = cfg("SUPABASE_URL")
    key = cfg("SUPABASE_SERVICE_KEY")
    if not url or not key:
        print("WARNING: SUPABASE_URL or SUPABASE_SERVICE_KEY not set.")
        return
    try:
        db = _get_db()
        db.table("settings").select("key").limit(1).execute()
        print("Supabase connected")
    except Exception as exc:
        print(f"WARNING: Supabase connection failed: {exc}")
        print("   Run supabase_schema.sql in your Supabase Dashboard -> SQL Editor")


# ── Settings ──────────────────────────────────────────────────────────────────

async def get_all_settings() -> dict:
    result = await _run(lambda: _get_db().table("settings").select("key, value").execute())
    out: dict = {}
    for k in ALL_SETTING_KEYS:
        env_val = cfg(k)
        if k in SENSITIVE_KEYS:
            out[k] = {"value": "", "configured": bool(env_val)}
        else:
            out[k] = {"value": env_val, "configured": bool(env_val)}
    for row in (result.data or []):
        k, v = row["key"], row["value"]
        if k == "TEST_KEY":
            continue
        if k in SENSITIVE_KEYS:
            out[k] = {"value": "", "configured": bool(v)}
        else:
            out[k] = {"value": v, "configured": bool(v)}
    return out


async def save_settings(data: dict) -> None:
    updated_at = datetime.now().isoformat()
    rows = [
        {"key": k, "value": str(v), "updated_at": updated_at}
        for k, v in data.items()
        if v is not None and v != ""
    ]
    if rows:
        await _run(lambda: _get_db().table("settings").upsert(rows, on_conflict="key").execute())


async def get_setting(key: str, default: str = "") -> str:
    result = await _run(
        lambda: _get_db().table("settings").select("value").eq("key", key).maybe_single().execute()
    )
    if result and result.data:
        return result.data["value"]
    return cfg(key) or default


async def set_setting(key: str, value: str) -> None:
    row = {"key": key, "value": value, "updated_at": datetime.now().isoformat()}
    await _run(lambda: _get_db().table("settings").upsert(row, on_conflict="key").execute())


async def get_enabled_tools() -> list:
    raw = await get_setting("ENABLED_TOOLS", "")
    if not raw:
        return []
    try:
        import json
        result = json.loads(raw)
        return result if isinstance(result, list) else []
    except Exception:
        return []


# ── Error logs ────────────────────────────────────────────────────────────────

async def log_error(source: str, message: str, detail: str = "", level: str = "error") -> None:
    try:
        row = {
            "id": str(uuid.uuid4()),
            "source": source,
            "level": level,
            "message": message[:500],
            "detail": detail[:2000],
            "timestamp": datetime.now().isoformat(),
        }
        await _run(lambda: _get_db().table("error_logs").insert(row).execute())
    except Exception:
        pass


async def get_errors(limit: int = 100) -> list:
    result = await _run(
        lambda: _get_db().table("error_logs").select("*").order("timestamp", desc=True).limit(limit).execute()
    )
    return result.data or []


async def get_logs(level: Optional[str] = None, source: Optional[str] = None, limit: int = 200) -> list:
    def _q():
        q = _get_db().table("error_logs").select("*").order("timestamp", desc=True).limit(limit)
        if level:
            q = q.eq("level", level)
        if source:
            q = q.eq("source", source)
        return q.execute()
    result = await _run(_q)
    return result.data or []


async def clear_errors() -> None:
    await _run(lambda: _get_db().table("error_logs").delete().neq("id", "").execute())


# ── Appointments ──────────────────────────────────────────────────────────────

async def insert_appointment(name: str, phone: str, date: str, time: str, service: str) -> str:
    full_id    = str(uuid.uuid4())
    booking_id = full_id[:8].upper()
    row = {
        "id": full_id, "name": name, "phone": phone,
        "date": date, "time": time, "service": service,
        "status": "booked", "created_at": datetime.now().isoformat(),
    }
    await _run(lambda: _get_db().table("appointments").insert(row).execute())
    return booking_id


async def check_slot(date: str, time: str) -> bool:
    """Returns True if slot is available (no existing booking)."""
    result = await _run(
        lambda: _get_db().table("appointments").select("id")
        .eq("date", date).eq("time", time).eq("status", "booked")
        .maybe_single().execute()
    )
    return result.data is None


async def get_next_available(date: str, time: str) -> str:
    try:
        dt = datetime.strptime(f"{date} {time}", "%Y-%m-%d %H:%M")
    except ValueError:
        dt = datetime.now().replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
    for _ in range(7 * 24):
        dt += timedelta(hours=1)
        if 9 <= dt.hour < 18:
            if await check_slot(dt.strftime("%Y-%m-%d"), dt.strftime("%H:%M")):
                return f"{dt.strftime('%Y-%m-%d')} at {dt.strftime('%H:%M')}"
    return "no open slots found in the next 7 days"


async def get_all_appointments(date_filter: Optional[str] = None) -> list:
    def _q():
        q = _get_db().table("appointments").select("*").order("date").order("time")
        if date_filter:
            q = q.eq("date", date_filter)
        return q.execute()
    result = await _run(_q)
    return result.data or []


async def cancel_appointment(appointment_id: str) -> bool:
    result = await _run(
        lambda: _get_db().table("appointments")
        .update({"status": "cancelled"})
        .eq("id", appointment_id).eq("status", "booked").execute()
    )
    return len(result.data or []) > 0


async def get_appointments_by_phone(phone: str) -> list:
    result = await _run(
        lambda: _get_db().table("appointments").select("*").eq("phone", phone).order("date", desc=True).execute()
    )
    return result.data or []


# ── Call logs ─────────────────────────────────────────────────────────────────

async def log_call(
    phone_number: str, lead_name: Optional[str], outcome: str, reason: str,
    duration_seconds: int, recording_url: Optional[str] = None, notes: Optional[str] = None,
) -> None:
    row: dict = {
        "id": str(uuid.uuid4()), "phone_number": phone_number, "lead_name": lead_name,
        "outcome": outcome, "reason": reason, "duration_seconds": duration_seconds,
        "timestamp": datetime.now().isoformat(),
    }
    if recording_url:
        row["recording_url"] = recording_url
    if notes:
        row["notes"] = notes
    await _run(lambda: _get_db().table("call_logs").insert(row).execute())


async def get_all_calls(page: int = 1, limit: int = 20) -> list:
    offset = (page - 1) * limit
    result = await _run(
        lambda: _get_db().table("call_logs").select("*")
        .order("timestamp", desc=True).range(offset, offset + limit - 1).execute()
    )
    return result.data or []


async def get_calls_by_phone(phone: str) -> list:
    result = await _run(
        lambda: _get_db().table("call_logs").select("*")
        .eq("phone_number", phone).order("timestamp", desc=True).execute()
    )
    return result.data or []


async def update_call_notes(call_id: str, notes: str) -> bool:
    result = await _run(
        lambda: _get_db().table("call_logs").update({"notes": notes}).eq("id", call_id).execute()
    )
    return len(result.data or []) > 0


async def get_contacts() -> list:
    result = await _run(
        lambda: _get_db().table("call_logs").select("*").order("timestamp", desc=True).execute()
    )
    rows = result.data or []
    contacts: dict = {}
    for row in rows:
        phone = row["phone_number"]
        if phone not in contacts:
            contacts[phone] = {
                "phone_number": phone, "lead_name": row.get("lead_name"),
                "total_calls": 0, "booked": 0,
                "last_call": row["timestamp"], "last_outcome": row.get("outcome"),
            }
        contacts[phone]["total_calls"] += 1
        if row.get("outcome") == "booked":
            contacts[phone]["booked"] += 1
    return sorted(contacts.values(), key=lambda c: c["last_call"], reverse=True)


# ── Stats ─────────────────────────────────────────────────────────────────────

async def get_stats() -> dict:
    result = await _run(
        lambda: _get_db().table("call_logs").select("outcome, duration_seconds, timestamp").execute()
    )
    rows           = result.data or []
    total_calls    = len(rows)
    booked         = sum(1 for r in rows if r.get("outcome") == "booked")
    not_interested = sum(1 for r in rows if r.get("outcome") == "not_interested")
    durations      = [r["duration_seconds"] for r in rows if r.get("duration_seconds")]
    avg_dur        = sum(durations) / len(durations) if durations else 0
    booking_rate   = round((booked / total_calls * 100) if total_calls else 0, 1)
    outcomes: dict = {}
    for r in rows:
        o = r.get("outcome") or "unknown"
        outcomes[o] = outcomes.get(o, 0) + 1
    daily: dict = defaultdict(int)
    for r in rows:
        ts = (r.get("timestamp") or "")[:10]
        if ts:
            daily[ts] += 1
    today = datetime.now().date()
    timeline = [
        {"date": (today - timedelta(days=i)).isoformat(), "count": daily.get((today - timedelta(days=i)).isoformat(), 0)}
        for i in range(13, -1, -1)
    ]
    dur_sum: dict = defaultdict(float)
    dur_cnt: dict = defaultdict(int)
    for r in rows:
        o   = r.get("outcome") or "unknown"
        sec = r.get("duration_seconds")
        if sec:
            dur_sum[o] += sec
            dur_cnt[o] += 1
    duration_by_outcome = {o: dur_sum[o] / dur_cnt[o] for o in dur_sum}
    return {
        "total_calls": total_calls, "booked": booked, "not_interested": not_interested,
        "avg_duration_seconds": round(avg_dur, 1), "booking_rate_percent": booking_rate,
        "outcomes": outcomes, "timeline": timeline, "duration_by_outcome": duration_by_outcome,
    }


# ── Campaigns ─────────────────────────────────────────────────────────────────

async def create_campaign(
    name: str, contacts_json: str, schedule_type: str = "once",
    schedule_time: str = "09:00", call_delay_seconds: int = 3,
    system_prompt: Optional[str] = None, agent_profile_id: Optional[str] = None,
) -> str:
    campaign_id = str(uuid.uuid4())
    row: dict = {
        "id": campaign_id, "name": name, "status": "active",
        "contacts_json": contacts_json, "schedule_type": schedule_type,
        "schedule_time": schedule_time, "call_delay_seconds": call_delay_seconds,
        "created_at": datetime.now().isoformat(), "total_dispatched": 0, "total_failed": 0,
    }
    if system_prompt:
        row["system_prompt"] = system_prompt
    if agent_profile_id:
        row["agent_profile_id"] = agent_profile_id
    await _run(lambda: _get_db().table("campaigns").insert(row).execute())
    return campaign_id


async def get_all_campaigns() -> list:
    result = await _run(
        lambda: _get_db().table("campaigns").select("*").order("created_at", desc=True).execute()
    )
    return result.data or []


async def get_campaign(campaign_id: str) -> Optional[dict]:
    result = await _run(
        lambda: _get_db().table("campaigns").select("*").eq("id", campaign_id).maybe_single().execute()
    )
    return result.data if result else None


async def update_campaign_status(campaign_id: str, status: str) -> bool:
    result = await _run(
        lambda: _get_db().table("campaigns").update({"status": status}).eq("id", campaign_id).execute()
    )
    return len(result.data or []) > 0


async def update_campaign_run_stats(campaign_id: str, dispatched: int, failed: int) -> None:
    updates = {
        "last_run_at": datetime.now().isoformat(),
        "total_dispatched": dispatched, "total_failed": failed, "status": "completed",
    }
    await _run(lambda: _get_db().table("campaigns").update(updates).eq("id", campaign_id).execute())


async def delete_campaign(campaign_id: str) -> bool:
    result = await _run(
        lambda: _get_db().table("campaigns").delete().eq("id", campaign_id).execute()
    )
    return len(result.data or []) > 0


# ── Contact Memory ────────────────────────────────────────────────────────────

async def add_contact_memory(phone: str, insight: str) -> None:
    row = {
        "id": str(uuid.uuid4()), "phone_number": phone,
        "insight": insight[:1000], "created_at": datetime.now().isoformat(),
    }
    await _run(lambda: _get_db().table("contact_memory").insert(row).execute())


async def get_contact_memory(phone: str) -> list:
    result = await _run(
        lambda: _get_db().table("contact_memory").select("insight, created_at")
        .eq("phone_number", phone).order("created_at", desc=True).limit(20).execute()
    )
    return result.data or []


async def compress_contact_memory(phone: str, compressed: str) -> None:
    row = {
        "id": str(uuid.uuid4()), "phone_number": phone,
        "insight": compressed[:2000], "created_at": datetime.now().isoformat(),
    }
    await _run(lambda: _get_db().table("contact_memory").delete().eq("phone_number", phone).execute())
    await _run(lambda: _get_db().table("contact_memory").insert(row).execute())


# ── Agent Profiles ────────────────────────────────────────────────────────────

async def get_all_agent_profiles() -> list:
    result = await _run(
        lambda: _get_db().table("agent_profiles").select("*").order("created_at").execute()
    )
    return result.data or []


async def get_agent_profile(profile_id: str) -> Optional[dict]:
    result = await _run(
        lambda: _get_db().table("agent_profiles").select("*").eq("id", profile_id).maybe_single().execute()
    )
    return result.data if result else None


async def create_agent_profile(
    name: str, voice: str = "Aoede", model: str = "gemini-3.1-flash-live-preview",
    system_prompt: Optional[str] = None, enabled_tools: str = "[]", is_default: bool = False,
) -> str:
    profile_id = str(uuid.uuid4())
    if is_default:
        await _run(
            lambda: _get_db().table("agent_profiles").update({"is_default": 0}).neq("id", "placeholder").execute()
        )
    row = {
        "id": profile_id, "name": name, "voice": voice, "model": model,
        "system_prompt": system_prompt, "enabled_tools": enabled_tools,
        "is_default": 1 if is_default else 0, "created_at": datetime.now().isoformat(),
    }
    await _run(lambda: _get_db().table("agent_profiles").insert(row).execute())
    return profile_id


async def update_agent_profile(profile_id: str, updates: dict) -> bool:
    result = await _run(
        lambda: _get_db().table("agent_profiles").update(updates).eq("id", profile_id).execute()
    )
    return len(result.data or []) > 0


async def delete_agent_profile(profile_id: str) -> bool:
    result = await _run(
        lambda: _get_db().table("agent_profiles").delete().eq("id", profile_id).execute()
    )
    return len(result.data or []) > 0


async def set_default_agent_profile(profile_id: str) -> None:
    await _run(
        lambda: _get_db().table("agent_profiles").update({"is_default": 0}).neq("id", "placeholder").execute()
    )
    await _run(
        lambda: _get_db().table("agent_profiles").update({"is_default": 1}).eq("id", profile_id).execute()
    )
