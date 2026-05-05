"""config.py — Single source of truth for ALL environment variables in OutboundAI.

Import cfg() instead of calling os.getenv() directly anywhere else.

Precedence (highest wins):
  1. Docker / Coolify / VPS environment variables (set at container/process start)
  2. .env file (local dev — only fills keys not already in os.environ)
  3. Default from _REGISTRY (safe fallback for optional settings)
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Load .env with override=False so container env vars always take precedence.
_env_file = Path(__file__).parent / ".env"
if _env_file.exists():
    load_dotenv(_env_file, override=False)

# ── Master registry of every env var used in OutboundAI ──────────────────────
_REGISTRY: dict = {
    # Google / Gemini
    "GOOGLE_API_KEY":           "",
    "GEMINI_MODEL":             "gemini-3.1-flash-live-preview",
    "GEMINI_TTS_VOICE":         "Aoede",
    # Exotel telephony
    "EXOTEL_API_KEY":           "",
    "EXOTEL_API_TOKEN":         "",
    "EXOTEL_SID":               "",
    "EXOTEL_CALLER_ID":         "",
    "EXOTEL_APP_ID":            "",
    "EXOTEL_WEBSOCKET_URL":     "",
    "EXOTEL_STATUS_CALLBACK":   "",
    "EXOTEL_TRANSFER_APP_ID":   "",
    "EXOTEL_TRANSFER_NUMBER":   "",
    # Business defaults (shown to agent via system prompt)
    "BUSINESS_NAME":            "our company",
    "SERVICE_TYPE":             "our service",
    # Supabase
    "SUPABASE_URL":             "",
    "SUPABASE_SERVICE_KEY":     "",
    # S3 / Supabase Storage (call recordings — optional)
    "S3_ACCESS_KEY_ID":         "",
    "S3_SECRET_ACCESS_KEY":     "",
    "S3_ENDPOINT_URL":          "",
    "S3_REGION":                "ap-northeast-1",
    "S3_BUCKET":                "call-recordings",
    # Cal.com (calendar booking — optional)
    "CALCOM_API_KEY":           "",
    "CALCOM_EVENT_TYPE_ID":     "",
    "CALCOM_TIMEZONE":          "Asia/Kolkata",
    # Twilio SMS (confirmation texts — optional)
    "TWILIO_ACCOUNT_SID":       "",
    "TWILIO_AUTH_TOKEN":        "",
    "TWILIO_FROM_NUMBER":       "",
    # Deepgram (optional fallback STT)
    "DEEPGRAM_API_KEY":         "",
    # Agent / prompt overrides (stored in Supabase settings table)
    "ENABLED_TOOLS":            "",
    "CUSTOM_PROMPT":            "",
}

# Keys whose values must never be returned in API responses
SENSITIVE_KEYS: frozenset = frozenset({
    "GOOGLE_API_KEY",
    "EXOTEL_API_TOKEN",
    "SUPABASE_SERVICE_KEY",
    "S3_SECRET_ACCESS_KEY",
    "CALCOM_API_KEY",
    "TWILIO_AUTH_TOKEN",
    "DEEPGRAM_API_KEY",
})

# Ordered list of all setting keys (for the UI settings page)
ALL_SETTING_KEYS: list = list(_REGISTRY.keys())


def cfg(key: str, fallback: str = "") -> str:
    """Return the live value for *key*.

    Reads os.environ at call time so that values written via the
    /api/settings endpoint (which updates os.environ in-process) are
    always reflected without restarting.
    """
    return os.getenv(key, _REGISTRY.get(key, fallback))
