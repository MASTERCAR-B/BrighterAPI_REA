"""
server.py - Local real-time HTTP + WebSocket API server for Brighter Data
(Brighter Lens / Event Engine + Pivots Time/Distance/Confidence Ladder + Live WS).

Run:
    python server.py --host 127.0.0.1 --port 8766 --asset BTCUSD --tf 1D

Endpoints (interactive Swagger docs at http://127.0.0.1:8766/docs):
    GET  /health                     Server + live WebSocket feed status
    GET  /v1/live                    Zero-latency live P1/P2 + Time/Distance metrics from hot WS state
    GET  /v1/snapshot                Full parallel snapshot (Brighter Lens + Pivots + Candles + Context)
    GET  /v1/assets                  All 91 supported Brighter Data assets & history dates
    GET  /v1/sessions                Trading session schedules (Asia, London, New York, Post)
    GET  /v1/account/me              Authenticated user profile & active entitlements
    GET  /v1/account/watchlist       Saved asset watchlist
    GET  /v1/lens/event-types        All 17 Brighter Lens event types & horizon configs
    GET  /v1/lens/events             Historical/active event instances with precomputed MAE/MFE/close
    GET  /v1/lens/buffer             Event buffer + decoded binary Float32 price paths
    GET  /v1/lens/analyze            Full Brighter Lens analysis (outcomes, bands, MFE/MAE, fractal analogs)
    GET  /v1/candles                 OHLCV candles for any asset & timeframe
    GET  /v1/context                 Historical RSI context series (rsi_14_1h, rsi_14_1d)
    GET  /v1/pivots/overview         Multi-asset live Pivots overview
    GET  /v1/pivots/time             Time Lens buckets, explorer table & live risk metrics
    GET  /v1/pivots/distance         Distance Lens distributions, risk metrics & Confidence Targets ladder
    GET  /v1/pivots/summary          Combined Time + Distance + Confidence Targets summary
    WS   /ws/live                    Real-time WebSocket push stream of live P1/P2 & tick updates
"""

from __future__ import annotations

import asyncio
import os
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect

import bd

bd.load_dotenv()

DEFAULT_ASSETS = os.environ.get("BD_ASSETS", os.environ.get("BD_ASSET", "BTCUSD"))
DEFAULT_TF = os.environ.get("BD_TF", "1D")

client = bd.BrighterClient()
live_feed: bd.BrighterLiveFeed | None = None


@asynccontextmanager
async def lifespan(_app: FastAPI):
    global live_feed
    live_feed = bd.BrighterLiveFeed(
        assets=DEFAULT_ASSETS,
        client=client,
        preload_pivots_tf=DEFAULT_TF,
    )
    await live_feed.start()
    try:
        yield
    finally:
        await live_feed.stop()


app = FastAPI(
    title="Brighter Data Real-Time API (BrighterAPI_REA)",
    version="1.0.0",
    description=(
        "Local Python REST + WebSocket API server for Brighter Data "
        "(Brighter Lens / Event Engine, Pivots Time/Distance/Confidence Targets, Candles, Context Series, and Live WS)."
    ),
    lifespan=lifespan,
)


async def _run(fn, *args, **kwargs):
    loop = asyncio.get_running_loop()
    try:
        return await loop.run_in_executor(None, lambda: fn(*args, **kwargs))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except bd.BrighterAuthError as exc:
        raise HTTPException(status_code=401, detail=str(exc)) from exc
    except bd.BrighterHTTPError as exc:
        status = exc.status if 400 <= exc.status < 500 else 502
        raise HTTPException(status_code=status, detail=str(exc)) from exc
    except bd.BrighterError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@app.get("/health")
async def health() -> dict[str, Any]:
    return {
        "ok": True,
        "live_feed": {
            "assets": live_feed.assets if live_feed else [],
            "connected": live_feed.connected if live_feed else False,
            "message_count": live_feed.message_count if live_feed else 0,
            "last_update_ms": live_feed.last_update_ms if live_feed else 0,
            "timeframes_cached": sorted({tf for _, tf in live_feed._states}) if live_feed else [],
        },
    }


@app.get("/v1/live")
async def get_live(
    asset: str | None = Query(None, pattern=r"^[A-Za-z0-9._-]{2,24}$"),
    tf: str | None = Query(None),
) -> dict[str, Any]:
    """Zero-latency live P1/P2 + computed Time/Distance metrics from the hot WebSocket feed."""
    if live_feed is None:
        raise HTTPException(status_code=503, detail="Live feed not initialized")
    if asset:
        sym = bd.normalize_asset(asset)
        if sym not in live_feed.assets:
            await live_feed.subscribe_asset(sym)
            await asyncio.sleep(0.5)
    try:
        return live_feed.snapshot(asset=asset, timeframe=tf)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/v1/snapshot")
async def get_full_snapshot(
    asset: str = Query("BTCUSD", pattern=r"^[A-Za-z0-9._-]{2,24}$"),
    pivot_tf: str = Query("1D"),
    candle_tf: str = Query("1h"),
    candle_limit: int = Query(100, ge=1, le=1000),
    event_type: str = Query("Daily Candle"),
    lens_limit: int = Query(150, ge=5, le=1000),
    top_k_fractals: int = Query(12, ge=1, le=50),
    include_lens: bool = True,
    include_pivots: bool = True,
    include_candles: bool = True,
    include_context: bool = True,
) -> dict[str, Any]:
    live_state = live_feed.get_state(asset, pivot_tf) if live_feed else None
    return await _run(
        client.full_snapshot,
        asset=asset,
        pivot_tf=pivot_tf,
        candle_tf=candle_tf,
        candle_limit=candle_limit,
        event_type=event_type,
        lens_limit=lens_limit,
        top_k_fractals=top_k_fractals,
        include_lens=include_lens,
        include_pivots=include_pivots,
        include_candles=include_candles,
        include_context=include_context,
        live_state=live_state,
    )


# --- Metadata & Account ---


@app.get("/v1/assets")
async def get_assets() -> dict[str, Any]:
    rows = await _run(client.assets)
    return {"count": len(rows), "assets": rows}


@app.get("/v1/sessions")
async def get_sessions() -> dict[str, Any]:
    rows = await _run(client.sessions)
    return {"count": len(rows), "sessions": rows}


@app.get("/v1/account/me")
async def get_account_me() -> dict[str, Any]:
    return await _run(client.account_me)


@app.get("/v1/account/watchlist")
async def get_watchlist() -> dict[str, Any]:
    rows = await _run(client.watchlist)
    return {"watchlist": rows}


# --- Brighter Lens / Event Engine ---


@app.get("/v1/lens/event-types")
async def get_event_types() -> dict[str, Any]:
    rows = await _run(client.event_types)
    return {"count": len(rows), "event_types": rows}


@app.get("/v1/lens/events")
async def get_lens_events(
    asset: str = Query("BTCUSD", pattern=r"^[A-Za-z0-9._-]{2,24}$"),
    from_ts: int | None = None,
    to_ts: int | None = None,
    types: str | None = None,
    active: bool | None = None,
    limit: int = Query(200, ge=1, le=1000),
) -> dict[str, Any]:
    return await _run(
        client.events,
        asset=asset,
        from_ts=from_ts,
        to_ts=to_ts,
        types=types,
        active=active,
        limit=limit,
    )


@app.get("/v1/lens/buffer")
async def get_lens_buffer(
    asset: str = Query("BTCUSD", pattern=r"^[A-Za-z0-9._-]{2,24}$"),
    event_type: str = Query("Daily Candle"),
    limit: int = Query(100, ge=1, le=500),
    max_steps: int = Query(1000, ge=10, le=2000),
    include_paths: bool = True,
    day_type: str | None = Query(None, pattern=r"^(weekday|weekend)$"),
    day_of_week: str | None = None,
    session: str | None = None,
) -> dict[str, Any]:
    buf = await _run(
        client.buffer_events,
        asset=asset,
        event_type=event_type,
        limit=limit,
        day_type=day_type,
        day_of_week=day_of_week,
        session=session,
    )
    out: dict[str, Any] = {"asset": bd.normalize_asset(asset), "event_type": event_type, **buf}
    if include_paths:
        ids = [int(e["eventId"]) for e in (buf.get("events") or []) if e.get("eventId") is not None]
        path_res = await _run(client.buffer_paths, asset, ids, max_steps=max_steps, binary=True)
        out["pathGranularityMinutes"] = path_res["pathGranularityMinutes"]
        out["paths"] = path_res["paths"]
    return out


@app.get("/v1/lens/analyze")
async def get_lens_analyze(
    asset: str = Query("BTCUSD", pattern=r"^[A-Za-z0-9._-]{2,24}$"),
    event_type: str = Query("Daily Candle"),
    limit: int = Query(200, ge=5, le=1000),
    max_steps: int = Query(1000, ge=10, le=2000),
    horizon_steps: int | None = Query(None, ge=1, le=2000),
    target_event_id: int | None = None,
    top_k_fractals: int = Query(12, ge=1, le=50),
    include_paths: bool = False,
    include_step_bands: bool = True,
    day_type: str | None = Query(None, pattern=r"^(weekday|weekend)$"),
    day_of_week: str | None = None,
    session: str | None = None,
) -> dict[str, Any]:
    return await _run(
        client.lens_snapshot,
        asset=asset,
        event_type=event_type,
        limit=limit,
        max_steps=max_steps,
        horizon_steps=horizon_steps,
        target_event_id=target_event_id,
        top_k_fractals=top_k_fractals,
        include_paths=include_paths,
        include_step_bands=include_step_bands,
        day_type=day_type,
        day_of_week=day_of_week,
        session=session,
    )


@app.get("/v1/candles")
async def get_candles(
    asset: str = Query("BTCUSD", pattern=r"^[A-Za-z0-9._-]{2,24}$"),
    tf: str = Query("1h"),
    from_ts: int | None = None,
    to_ts: int | None = None,
    limit: int = Query(300, ge=1, le=2000),
) -> dict[str, Any]:
    return await _run(client.candles, asset=asset, timeframe=tf, from_ts=from_ts, to_ts=to_ts, limit=limit)


@app.get("/v1/context")
async def get_context_series(
    asset: str = Query("BTCUSD", pattern=r"^[A-Za-z0-9._-]{2,24}$"),
    attributes: str = Query("rsi_14_1h,rsi_14_1d"),
    from_ts: int | None = None,
    to_ts: int | None = None,
    lookback_days: int = Query(30, ge=1, le=365),
) -> dict[str, Any]:
    attrs: list[str] = [a.strip() for a in attributes.split(",") if a.strip()]
    return await _run(
        client.context_series,
        asset=asset,
        attributes=attrs,
        from_ts=from_ts,
        to_ts=to_ts,
        lookback_days=lookback_days,
    )


# --- Pivots Engine ---


@app.get("/v1/pivots/overview")
async def get_pivots_overview(
    tf: str = Query("1D"),
    assets: str | None = None,
    confidence: int | None = Query(None, ge=10, le=95),
    start_date: str | None = None,
    end_date: str | None = None,
    day_of_week: str | None = None,
    session: str | None = None,
    auto_defaults: bool = False,
) -> dict[str, Any]:
    return await _run(
        client.pivot_overview,
        timeframe=tf,
        assets=assets,
        confidence=confidence,
        start_date=start_date,
        end_date=end_date,
        day_of_week=day_of_week,
        session=session,
        auto_defaults=auto_defaults,
    )


@app.get("/v1/pivots/time")
async def get_pivots_time(
    asset: str = Query("BTCUSD", pattern=r"^[A-Za-z0-9._-]{2,24}$"),
    tf: str = Query("1D"),
    group_by: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    day_of_week: str | None = None,
    session: str | None = None,
    hour: str | None = None,
    four_hour_candle: str | None = None,
    auto_defaults: bool = True,
) -> dict[str, Any]:
    fkw = {
        "start_date": start_date,
        "end_date": end_date,
        "day_of_week": day_of_week,
        "session": session,
        "hour": hour,
        "four_hour_candle": four_hour_candle,
        "auto_defaults": auto_defaults,
    }
    buckets = await _run(client.pivot_time_buckets, asset, tf, **fkw)
    explorer = await _run(client.pivot_time_explorer, asset, tf, group_by, **fkw)
    live_state = live_feed.get_state(asset, tf) if live_feed else None
    return {
        "asset": bd.normalize_asset(asset),
        "timeframe": bd.resolve_pivot_tf(tf),
        "live_state": live_state,
        "time_metrics": bd.compute_pivot_time_metrics(buckets, live_state) if live_state else None,
        "buckets": buckets,
        "explorer": explorer,
    }


@app.get("/v1/pivots/distance")
async def get_pivots_distance(
    asset: str = Query("BTCUSD", pattern=r"^[A-Za-z0-9._-]{2,24}$"),
    tf: str = Query("1D"),
    start_date: str | None = None,
    end_date: str | None = None,
    day_of_week: str | None = None,
    session: str | None = None,
    hour: str | None = None,
    four_hour_candle: str | None = None,
    auto_defaults: bool = True,
    include_raw: bool = False,
) -> dict[str, Any]:
    fkw = {
        "start_date": start_date,
        "end_date": end_date,
        "day_of_week": day_of_week,
        "session": session,
        "hour": hour,
        "four_hour_candle": four_hour_candle,
        "auto_defaults": auto_defaults,
    }
    dist = await _run(client.pivot_distance_distributions, asset, tf, **fkw)
    live_state = live_feed.get_state(asset, tf) if live_feed else None
    metrics = (
        bd.compute_pivot_distance_metrics(dist, live_state)
        if live_state
        else {"confidence_targets": bd.compute_confidence_targets(dist.get("distributions") or {})}
    )
    out: dict[str, Any] = {
        "asset": bd.normalize_asset(asset),
        "timeframe": bd.resolve_pivot_tf(tf),
        "period": dist.get("period"),
        "filters_applied": dist.get("filters_applied"),
        "total_candles": dist.get("total_candles"),
        "live_state": live_state,
        "distance_metrics": metrics,
    }
    if include_raw:
        out["distributions"] = dist.get("distributions")
    return out


@app.get("/v1/pivots/summary")
async def get_pivots_summary(
    asset: str = Query("BTCUSD", pattern=r"^[A-Za-z0-9._-]{2,24}$"),
    tf: str = Query("1D"),
    start_date: str | None = None,
    end_date: str | None = None,
    day_of_week: str | None = None,
    session: str | None = None,
    hour: str | None = None,
    four_hour_candle: str | None = None,
    auto_defaults: bool = True,
    include_raw_distributions: bool = False,
) -> dict[str, Any]:
    live_state = live_feed.get_state(asset, tf) if live_feed else None
    return await _run(
        client.pivot_summary,
        asset=asset,
        timeframe=tf,
        live_state=live_state,
        include_raw_distributions=include_raw_distributions,
        start_date=start_date,
        end_date=end_date,
        day_of_week=day_of_week,
        session=session,
        hour=hour,
        four_hour_candle=four_hour_candle,
        auto_defaults=auto_defaults,
    )


# --- Real-Time WebSocket Push Stream ---


@app.websocket("/ws/live")
async def ws_live(
    websocket: WebSocket,
    asset: str | None = Query(None, pattern=r"^[A-Za-z0-9._-]{2,24}$"),
    tf: str | None = Query(None),
) -> None:
    """Real-time WebSocket push stream of live Brighter Data P1/P2 states and ticks."""
    if live_feed is None:
        await websocket.close(code=1013)
        return
    if len(live_feed._subscribers) >= 100:
        await websocket.close(code=1013)
        return

    target_asset = bd.normalize_asset(asset) if asset else (live_feed.assets[0] if live_feed.assets else "BTCUSD")
    target_tf = bd.resolve_pivot_tf(tf) if tf else None
    if target_asset not in live_feed.assets:
        await live_feed.subscribe_asset(target_asset)

    await websocket.accept()
    try:
        await websocket.send_json({"type": "snapshot", "snapshot": live_feed.snapshot(asset=target_asset, timeframe=target_tf)})
        async for ev in live_feed.stream():
            ev_asset = ev.get("asset")
            ev_tf = ev.get("timeframe")
            if ev_asset and ev_asset != target_asset:
                continue
            if target_tf and ev_tf and ev_tf != target_tf:
                continue
            await websocket.send_json(ev)
    except WebSocketDisconnect:
        pass


if __name__ == "__main__":
    import argparse

    import uvicorn

    ap = argparse.ArgumentParser(description="Run local Brighter Data real-time HTTP + WebSocket API server")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=8766)
    ap.add_argument("--asset", default=DEFAULT_ASSETS, help="Comma-separated assets to subscribe on WS (e.g. BTCUSD,ETHUSDT)")
    ap.add_argument("--tf", default=DEFAULT_TF, help="Default pivot timeframe to preload (1h, 4h, S, 1D, 1W, 1M)")
    args = ap.parse_args()

    DEFAULT_ASSETS = args.asset
    DEFAULT_TF = args.tf
    uvicorn.run(app, host=args.host, port=args.port)
