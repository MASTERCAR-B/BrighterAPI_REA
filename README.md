# Brighter Data Real-Time API (`BrighterAPI_REA`)

Real-time Python SDK, CLI, and local HTTP + WebSocket API server for **Brighter Data** (`https://app.brighterdata.com/brighter-lens` and `/pivots/*`), reverse-engineered with **REA (`rea@6.7.0`)**.

Authenticates directly with **your own paid Brighter Data account** via pure-Python AWS Cognito SRP-6a (`USER_SRP_AUTH` + automatic `REFRESH_TOKEN_AUTH`) — **no browser required at runtime** and **zero credentials stored in source**.

## What Data Is Included

- **Brighter Lens / Event Engine (`/brighter-lens`)**:
  - All 17 Event Types (`1H`, `4H`, `Session`, `Daily`, `Weekly`, `Monthly` Candles; `Daily`/`Weekly` High & Low Sweeps; `Asia`/`London`/`New York`/`Post` Sessions; `FOMC`, `CPI`, `NFP`)
  - Historical & active event instances with precomputed `MAE` / `MFE` / `Close` across `1h`, `4h`, `12h`, `1d`, `3d`, `1w`, `2w`
  - Full event buffer (`/events/{asset}/buffer/events`) with `rsi14_1h`, `rsi14_1d`, `atrPct7_1d`, `atrPctRank7_1d`, day/session/context filters
  - Parallel binary Float32 path fetcher & decoder (`/events/{asset}/buffer/paths?pathEncoding=binary` → `closePath`, `highPath`, `lowPath`, `volPath`)
  - Faithful port of Brighter Lens client-side analytics:
    - All 8 historical outcome predicates (`closed_bullish`, `closed_bearish`, `low_first`, `high_first`, `took_out_high_later`, `took_out_low_later`, `had_more_displacement`, `had_more_total_displacement`)
    - MFE / MAE / Close / Range distributions (`p10`, `p25`, `p50`, `p75`, `p90`, `mean`, and live event percentile rank)
    - Step-by-step percentile fan (`p10..p90`)
    - Exact Euclidean **Fractal Similarity Matching** (top-K historical analogs + analog subset outcome rates)
- **Candles & Context Time-Series**:
  - OHLCV candles across `5m`, `15m`, `30m`, `1h`, `4h`, `1D`, `1W`, `1M` for all 91 supported assets
  - Historical RSI context time-series (`rsi_14_1h`, `rsi_14_1d`)
- **Pivots Engine (`/pivots/time`, `/pivots/distance`, `/pivots/summary`, `/pivots/overview`)**:
  - Time Lens: P1/P2 minute buckets, Data Explorer tables (`hour`, `day_of_week`, `three_day`, etc.), Time P1 flip risk, P2s later %, early-P1 warnings
  - Distance Lens: `p1_delta_open`, `p2_delta_open`, `total_bullish_displacement`, `total_bearish_displacement` distributions, Distance P1 flip risk, small-wick warnings
  - Exact **Bullish & Bearish Confidence Targets Ladder** (`90%` down to `20%`) with live hit detection
  - Multi-asset `/pivots/overview`
- **Real-Time WebSocket Stream (`wss://d3s1aak61z0hie.cloudfront.net/ws`)**:
  - Live `snapshot` and `delta` updates across all 6 pivot timeframes (`1h`, `4h`, `S`, `1D`, `1W`, `1M`) + live price ticks and auto-refreshing JWT reconnects

## Quickstart

### 1. Configure your paid account credentials
Copy `.env.example` to `.env` (gitignored) and enter your Brighter Data login:

```env
BRIGHTER_EMAIL=you@example.com
BRIGHTER_PASSWORD=your_password_here
```

### 2. Install optional streaming / server dependencies
`bd.BrighterClient` (REST + Cognito SRP-6a + Analytics) requires **zero third-party packages** (Python 3.10+ stdlib only). For real-time WebSocket streaming and the local FastAPI/WS server:

```bash
uv venv
uv pip install -r requirements.txt
```

### 3. Python SDK — Sync Snapshot
```python
import bd

c = bd.BrighterClient()

# Everything from Brighter Lens + Pivots in one parallel call:
snap = c.full_snapshot("BTCUSD", pivot_tf="1D", event_type="Daily Candle", lens_limit=150)

# Brighter Lens analytics (outcomes, MFE/MAE, percentile fan, top-12 fractal analogs):
lens = c.lens_snapshot("BTCUSD", event_type="Daily Candle", limit=200)

# Pivots Time + Distance + Confidence Targets Ladder (90%..20%):
piv = c.pivot_summary("BTCUSD", timeframe="1D")

# Raw building blocks:
assets = c.assets()
candles = c.candles("BTCUSD", "1h", limit=100)
rsi = c.context_series("BTCUSD", attributes=("rsi_14_1h", "rsi_14_1d"), lookback_days=7)
ov = c.pivot_overview("1D", assets=["BTCUSD", "ETHUSDT", "SOLUSDT"])
```

### 4. Python SDK — Real-Time WebSocket Stream
```python
import asyncio
import bd

async def main():
    async with bd.BrighterLiveFeed(assets=["BTCUSD", "ETHUSDT"], preload_pivots_tf="1D") as feed:
        snap = feed.snapshot("BTCUSD")
        print(snap["timeframes"]["1D"]["candle"], snap["timeframes"]["1D"]["p1"])
        async for ev in feed.stream():
            print(ev["type"], ev.get("asset"), ev.get("timeframe"))

asyncio.run(main())
```

### 5. CLI Usage
```bash
python bd.py --asset BTCUSD --tf 1D                      # formatted summary table
python bd.py --asset BTCUSD --tf 1D --json               # full JSON snapshot
python bd.py --asset BTCUSD --event-type "Daily Candle" --lens-only
python bd.py --asset BTCUSD --tf 1D --pivots-only
python bd.py --asset BTCUSD --tf 1D --stream             # live WebSocket JSONL stream
```

### 6. Local HTTP + WebSocket API Server
```bash
python server.py --host 127.0.0.1 --port 8766 --asset BTCUSD --tf 1D
```

Interactive Swagger docs at `http://127.0.0.1:8766/docs`. Key routes:
- `GET /health`
- `GET /v1/live` — zero-latency live P1/P2 + Time/Distance metrics from hot WS state
- `GET /v1/snapshot` — full parallel snapshot (Brighter Lens + Pivots + Candles + Context)
- `GET /v1/lens/event-types`, `/v1/lens/events`, `/v1/lens/buffer`, `/v1/lens/analyze`
- `GET /v1/candles`, `/v1/context`
- `GET /v1/pivots/overview`, `/v1/pivots/time`, `/v1/pivots/distance`, `/v1/pivots/summary`
- `GET /v1/assets`, `/v1/sessions`, `/v1/account/me`, `/v1/account/watchlist`
- `WS  /ws/live` — real-time push stream

### 7. Live Self-Test
```bash
python selftest_live.py
```

## Documentation
See [`spec.md`](spec.md) for the complete reverse-engineered protocol, REA evidence IDs, Cognito SRP-6a flow, REST endpoints, binary Float32 path layout, WebSocket framing, exact formulas, and entitlement boundaries.
