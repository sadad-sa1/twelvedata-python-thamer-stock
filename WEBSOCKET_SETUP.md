# Twelve Data live WebSocket smoke test

This repository already contains the Python Twelve Data WebSocket client at `src/twelvedata/websocket.py`. To verify **actual price messages**, run the independent smoke test:

```bash
python -m pip install websocket-client
export TWELVE_DATA_API_KEY='your-key-here'
python scripts/test_twelvedata_websocket.py --symbols AAPL --timeout 45
```

Store `TWELVE_DATA_API_KEY` as a secret in your hosting platform. Never commit it. Choose symbols included in your subscription. The test exits 0 **only** when an actual `price` event arrives; a successful REST response or open socket is insufficient. If the market is closed, a timeout alone does not indicate that authentication failed. Live stock coverage and concurrent symbol limits depend on your Twelve Data plan.

For hosting on Render, select the appropriate workspace and service before changing any environment variables. Deploying a GitHub repository alone does not establish a persistent market data connection.
