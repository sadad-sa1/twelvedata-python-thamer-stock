"""Render HTTP health endpoint and Twelve Data WebSocket price receiver."""
import json
import os
import threading
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from websocket import WebSocketApp

STATE = {"connection": "not_configured", "last_price_at": None, "last_price": None,
         "message_count": 0, "last_error": None, "symbols": []}
LOCK = threading.Lock()
KEY = os.getenv("TWELVE_DATA_API_KEY", "")
SYMBOLS = list(dict.fromkeys(s.strip().upper() for s in os.getenv("TD_WS_SYMBOLS", "AAPL").split(",") if s.strip()))
STATE["symbols"] = SYMBOLS


def update(**changes):
    with LOCK:
        STATE.update(changes)


def stream_loop():
    if not KEY:
        print("TWELVE_DATA_API_KEY not configured; price stream disabled", flush=True)
        return
    retries = 0
    while True:
        update(connection="connecting")
        def on_open(ws):
            nonlocal retries
            retries = 0
            update(connection="connected", last_error=None)
            ws.send(json.dumps({"action": "subscribe", "params": {"symbols": ",".join(SYMBOLS)}}))
            print("Twelve Data socket connected; subscribing", flush=True)

        def on_message(ws, raw):
            try:
                event = json.loads(raw)
            except (ValueError, TypeError):
                return
            kind = event.get("event")
            if kind == "price" and event.get("symbol") in SYMBOLS:
                try:
                    price = float(event["price"])
                except (TypeError, ValueError, KeyError):
                    return
                with LOCK:
                    STATE["last_price_at"] = datetime.now(timezone.utc).isoformat()
                    STATE["last_price"] = {"symbol": event["symbol"], "price": price,
                                           "timestamp": event.get("timestamp")}
                    STATE["message_count"] += 1
                print("PRICE event received: " + str(event["symbol"]), flush=True)
            elif kind in ("subscribe-status", "error"):
                status = event.get("status")
                print("Twelve Data event: " + str(kind) + ", status=" + str(status), flush=True)
                if status == "error" or kind == "error":
                    update(connection="subscription_error", last_error=str(event.get("message", "subscription error"))[:200])
        def on_error(ws, error):
            # Avoid including full request URL or credential in logs.
            message = str(error).replace(KEY, "[REDACTED]")[:200]
            update(connection="error", last_error=message)
            print("WebSocket error: " + message, flush=True)
        def on_close(ws, code, reason):
            update(connection="disconnected")
            print("WebSocket disconnected code=" + str(code), flush=True)
        ws = WebSocketApp("wss://ws.twelvedata.com/v1/quotes/price?apikey=" + KEY,
                          on_open=on_open, on_message=on_message, on_error=on_error, on_close=on_close)
        try:
            ws.run_forever(ping_interval=20, ping_timeout=10)
        except Exception as exc:
            update(connection="error", last_error=str(exc).replace(KEY, "[REDACTED]")[:200])
        retries += 1
        time.sleep(min(30, 2 ** min(retries, 5)))



# Benzinga live news stream runs independently of the Twelve Data price stream.
BENZINGA_KEY = os.getenv("BENZINGA_API_KEY", "")
NEWS_STATE = {"connection": "not_configured", "message_count": 0, "last_news_at": None,
              "last_news": None, "last_error": None}

def benzinga_stream_loop():
    if not BENZINGA_KEY:
        print("Benzinga key missing; news stream disabled", flush=True)
        return
    from urllib.parse import urlencode
    retries = 0
    while True:
        with LOCK:
            NEWS_STATE["connection"] = "connecting"

        def on_open(ws):
            nonlocal retries
            retries = 0
            with LOCK:
                NEWS_STATE.update(connection="connected", last_error=None)
            print("BENZINGA connected", flush=True)

        def on_message(ws, raw):
            try:
                event = json.loads(raw)
            except (ValueError, TypeError):
                return
            if not isinstance(event, dict):
                return
            data = event.get("data") or {}
            if not isinstance(data, dict):
                data = {}
            content = data.get("content") or {}
            if not isinstance(content, dict):
                content = {}
            stocks = content.get("stocks") or []
            symbols = [s.get("name") for s in stocks if isinstance(s, dict) and s.get("name")] if isinstance(stocks, list) else []
            news = {"action": data.get("action"), "id": content.get("id") or event.get("id"),
                    "title": str(content.get("title") or "")[:250], "symbols": symbols[:25]}
            now = datetime.now(timezone.utc).isoformat()
            with LOCK:
                NEWS_STATE["message_count"] += 1
                NEWS_STATE["last_news_at"] = now
                NEWS_STATE["last_news"] = news
            print("BENZINGA news received: " + json.dumps(news, ensure_ascii=False), flush=True)

        def on_error(ws, error):
            message = str(error).replace(BENZINGA_KEY, "[REDACTED]")[:180]
            with LOCK:
                NEWS_STATE.update(connection="error", last_error=message)
            print("BENZINGA socket error: " + message, flush=True)

        def on_close(ws, code, reason):
            with LOCK:
                NEWS_STATE["connection"] = "disconnected"
            print("BENZINGA disconnected code=" + str(code), flush=True)

        url = "wss://api.benzinga.com/api/v1/news/stream?" + urlencode({"token": BENZINGA_KEY})
        ws = WebSocketApp(url, on_open=on_open, on_message=on_message, on_error=on_error, on_close=on_close)
        try:
            ws.run_forever(ping_interval=25, ping_timeout=10)
        except Exception as exc:
            on_error(ws, exc)
        retries += 1
        time.sleep(min(60, 2 ** min(retries, 6)))

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path not in ("/", "/health", "/status"):
            self.send_error(404)
            return
        with LOCK:
            snapshot = dict(STATE)
        payload = {"ok": True, "service": "twelvedata-thamer-stock", "stream": snapshot, "benzinga": dict(NEWS_STATE)}
        body = json.dumps(payload).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


if __name__ == "__main__":
    threading.Thread(target=stream_loop, daemon=True).start()
    threading.Thread(target=benzinga_stream_loop, daemon=True).start()
    port = int(os.getenv("PORT", "10000"))
    print("HTTP server listening on port " + str(port), flush=True)
    ThreadingHTTPServer(("0.0.0.0", port), Handler).serve_forever()
