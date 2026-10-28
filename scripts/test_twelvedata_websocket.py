#!/usr/bin/env python3
"""Run an end-to-end Twelve Data WebSocket price-stream smoke test.

Never put API credentials in source control. Requires websocket-client.
Exit 0 only after an actual price event is received.
"""
import argparse
import json
import os
import sys
import threading
import time
from websocket import WebSocketApp

URL = "wss://ws.twelvedata.com/v1/quotes/price?apikey="


def run():
    parser = argparse.ArgumentParser(description="Test actual Twelve Data WebSocket price delivery")
    parser.add_argument("--symbols", default=os.getenv("TD_WS_SYMBOLS", "AAPL"))
    parser.add_argument("--timeout", type=float, default=45.0)
    args = parser.parse_args()
    key = os.getenv("TWELVE_DATA_API_KEY") or os.getenv("TD_API_KEY")
    if not key:
        print("FAIL: Missing TWELVE_DATA_API_KEY environment variable", file=sys.stderr)
        return 2

    symbols = ",".join(dict.fromkeys(x.strip().upper() for x in args.symbols.split(",") if x.strip()))
    if not symbols:
        print("FAIL: no symbols", file=sys.stderr)
        return 2

    state = {"opened": False, "subscribed": False, "price": False, "error": None}
    done = threading.Event()

    def on_open(ws):
        state["opened"] = True
        print("OPEN: WebSocket handshake completed", flush=True)
        ws.send(json.dumps({"action": "subscribe", "params": {"symbols": symbols}}))

    def on_message(ws, raw):
        try:
            event = json.loads(raw)
        except json.JSONDecodeError:
            print("WARN: unparseable response")
            return
        kind = event.get("event")
        if kind == "price" and event.get("symbol") and event.get("price") is not None:
            print("PRICE: " + json.dumps({"symbol": event["symbol"], "price": event["price"], "timestamp": event.get("timestamp")}), flush=True)
            state["price"] = True
            done.set()
            ws.close()
        elif kind == "subscribe-status":
            state["subscribed"] = True
            print("SUBSCRIPTION: " + json.dumps(event, ensure_ascii=False), flush=True)
            if event.get("status") == "error":
                state["error"] = event.get("message", "subscription rejected")
                done.set()
                ws.close()
        elif kind == "heartbeat":
            print("HEARTBEAT received", flush=True)
        else:
            # Avoid dumping credentials or entire undocumented payloads.
            print("EVENT: " + str(kind) + " status=" + str(event.get("status")) + " code=" + str(event.get("code")), flush=True)
            if event.get("status") == "error" or kind == "error":
                state["error"] = event.get("message", "server error")
                done.set()
                ws.close()

    def on_error(ws, error):
        state["error"] = str(error)
        done.set()

    def on_close(ws, code, reason):
        print(f"CLOSED: code={code} reason={reason}", flush=True)
        done.set()

    ws = WebSocketApp(URL + key, on_open=on_open, on_message=on_message,
                      on_error=on_error, on_close=on_close)
    worker = threading.Thread(target=lambda: ws.run_forever(ping_interval=15, ping_timeout=10), daemon=True)
    worker.start()
    try:
        done.wait(max(args.timeout, 1.0))
    finally:
        ws.close()
        worker.join(timeout=3)
    if state["price"]:
        print("PASS: received a live price event")
        return 0
    print("FAIL: No price received. WebSocket opened={} subscription_response={} error={}".format(
        state["opened"], state["subscribed"], state["error"] or "none"), file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(run())
