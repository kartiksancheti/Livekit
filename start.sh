#!/bin/bash
set -e
cd "$(dirname "$0")"

echo "Starting OutboundAI (Exotel WebSocket mode)..."

# Load .env if present. Using set -a / set +a handles values that contain
# '=' (e.g. base64-encoded Supabase JWT tokens) correctly, unlike xargs.
if [ -f ".env" ]; then
    set -a
    # shellcheck source=.env
    source .env
    set +a
fi

echo "Configuration:"
echo "  Gemini model : ${GEMINI_MODEL:-gemini-3.1-flash-live-preview}"
echo "  Exotel SID   : ${EXOTEL_SID:-<not set>}"
echo "  WebSocket URL: ${EXOTEL_WEBSOCKET_URL:-<not set>}"
echo "  Supabase     : ${SUPABASE_URL:-<not set>}"

echo "Starting FastAPI + WebSocket server on port 8000..."
exec uvicorn server:app --host 0.0.0.0 --port 8000
