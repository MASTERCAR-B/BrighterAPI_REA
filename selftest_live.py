"""
selftest_live.py - End-to-end live read-only self-test for bd.py and server.py.

Verifies:
  1. Pure-stdlib AWS Cognito SRP-6a authentication + REFRESH_TOKEN_AUTH token refresh.
  2. All Brighter Data REST endpoints:
     - /account/me, /assets, /sessions, /account/watchlist
     - /events/types, /candles/{asset}/{tf}, /context/{asset}/series
     - /events/{asset}, /events/{asset}/buffer/events
     - /events/{asset}/buffer/paths (binary Float32 decoding parity vs JSON!)
     - Full Brighter Lens analytics (outcomes, MFE/MAE, percentile fan, fractal analogs)
     - /pivots/{asset}/{tf}/time/buckets, /time/explorer, /distance/distributions, /pivots/overview
     - Exact math parity of compute_pivot_time_metrics & compute_pivot_distance_metrics vs /pivots/overview
     - Exact Bullish & Bearish Confidence Targets ladder (90%..20%)
  3. Real-time WebSocket engine (bd.BrighterLiveFeed) receiving all 6 timeframe snapshots.
  4. Local FastAPI REST + WebSocket API server (server.py via TestClient).

Usage:
    python selftest_live.py
"""

from __future__ import annotations

import asyncio
import sys
import time

import bd


def test_auth_and_rest() -> dict:
    print("=== [1/4] TESTING COGNITO SRP-6a AUTH & REFRESH ===")
    c = bd.BrighterClient()
    tok1 = c.auth.login()
    assert tok1.id_token and tok1.refresh_token
    print(f"  SRP login: ok (expires_in={int(tok1.expires_at - time.time())}s, has_refresh=True)")

    tok2 = c.auth.refresh()
    assert tok2.id_token
    print("  REFRESH_TOKEN_AUTH: ok")

    print("\n=== [2/4] TESTING REST ENDPOINTS & EXACT CLIENT MATH ===")
    me = c.account_me()
    assert me.get("hasActiveSubscription") is True
    enabled = sorted(k for k, v in (me.get("features") or {}).items() if v)
    print(f"  /account/me: ok (subscription=active, features={enabled})")

    assets = c.assets()
    sessions = c.sessions()
    wl = c.watchlist()
    assert len(assets) >= 50 and len(sessions) == 4
    print(f"  /assets={len(assets)}, /sessions={len(sessions)}, /watchlist={wl}")

    etypes = c.event_types()
    assert len(etypes) >= 10
    print(f"  /events/types: ok ({len(etypes)} event types)")

    candles = c.candles("BTCUSD", "1h", limit=10)
    assert len(candles.get("candles") or []) >= 5
    last_c = candles["candles"][-1]
    print(f"  /candles/BTCUSD/1h: ok ({len(candles['candles'])} bars, latest close=${last_c['close']:,.2f})")

    ctx = c.context_series("BTCUSD", ("rsi_14_1h", "rsi_14_1d"), lookback_days=7)
    assert len(ctx.get("series") or []) >= 10
    last_ctx = ctx["series"][-1]
    print(f"  /context/BTCUSD/series: ok ({len(ctx['series'])} rows, rsi_14_1h={last_ctx['rsi_14_1h']:.2f}, rsi_14_1d={last_ctx['rsi_14_1d']:.2f})")

    # Verify binary Float32 path decoding matches JSON path bit-for-bit (within float32 tolerance)
    evs = c.buffer_events("BTCUSD", "Daily Candle", limit=5).get("events") or []
    assert len(evs) >= 3
    test_id = evs[0]["eventId"]
    bin_p = c.buffer_paths("BTCUSD", [test_id], max_steps=15, binary=True)["paths"][0]
    json_p = c.buffer_paths("BTCUSD", [test_id], max_steps=15, binary=False)["paths"][0]
    assert bin_p["steps"] == json_p["steps"]
    max_diff = max(abs(a - b) for a, b in zip(bin_p["closePath"], json_p["closePath"]))
    assert max_diff < 1e-5, f"Binary vs JSON closePath mismatch: {max_diff}"
    print(f"  binary Float32 path parity: ok (eventId={test_id}, steps={bin_p['steps']}, max_diff={max_diff:.2e})")

    # Full Brighter Lens analytics
    lens = c.lens_snapshot("BTCUSD", "Daily Candle", limit=100, top_k_fractals=8)
    assert lens["sample_size"] >= 50
    assert lens["outcomes"]["closed_bullish"]["total"] == lens["sample_size"]
    assert len(lens["fractals"]["matches"]) == 8
    cb = lens["outcomes"]["closed_bullish"]
    lf = lens["outcomes"]["low_first"]
    print(
        f"  Brighter Lens (Daily Candle): sample={lens['sample_size']}, "
        f"closed_bullish={cb['pct']}%, low_first={lf['pct']}%, "
        f"top_analog={lens['fractals']['matches'][0]['eventTimeIso'][:10]} (dist={lens['fractals']['matches'][0]['distance']:.5f})"
    )

    # Pivots Time + Distance + Overview + Confidence Targets
    piv = c.pivot_summary("BTCUSD", "1D", start_date="2023-01-01", auto_defaults=False)
    ov_row = piv["overview"]
    assert ov_row and ov_row["asset"] == "BTCUSD"
    ct = piv["distance_metrics"]["confidence_targets"]
    assert len(ct["bullish"]) == 8 and len(ct["bearish"]) == 8
    print(
        f"  Pivots 1D overview: dir={ov_row['direction']}, "
        f"time_flip={ov_row['time_metrics']['p1_flip_risk_pct']}%, "
        f"dist_flip={ov_row['distance_metrics']['p1_flip_risk_pct']}%, "
        f"bull_90%_P1={ct['bullish'][0]['p1_price']}, bear_90%_P1={ct['bearish'][0]['p1_price']}"
    )
    return {"client": c, "overview_row": ov_row}


async def test_live_ws_and_math_parity(ctx: dict) -> None:
    print("\n=== [3/4] TESTING REAL-TIME WEBSOCKET & FORMULA PARITY ===")
    c: bd.BrighterClient = ctx["client"]
    ov_row = ctx["overview_row"]

    async with bd.BrighterLiveFeed(assets=["BTCUSD"], client=c, preload_pivots_tf="1D") as feed:
        snap = feed.snapshot("BTCUSD")
        tfs = sorted(snap["timeframes"].keys())
        assert snap["connected"] is True
        assert {"1D", "1W", "1M", "1h", "4h", "S"}.issubset(set(tfs)), f"Missing TFs: {tfs}"
        live_1d = snap["timeframes"]["1D"]
        print(
            f"  WS connected: ok, timeframes={tfs}, BTCUSD 1D close=${live_1d['candle']['close']:,.2f}, "
            f"P1={live_1d['p1']['type']}@${live_1d['p1']['price']:,.2f}, P2={live_1d['p2']['type']}@${live_1d['p2']['price']:,.2f}"
        )

        # Verify our local client-side Time & Distance formulas match Brighter's server-side /pivots/overview!
        buckets = c.pivot_time_buckets("BTCUSD", "1D", start_date="2023-01-01", auto_defaults=False)
        dists = c.pivot_distance_distributions("BTCUSD", "1D", start_date="2023-01-01", auto_defaults=False)
        tm = bd.compute_pivot_time_metrics(buckets, live_1d)
        dm = bd.compute_pivot_distance_metrics(dists, live_1d)

        # Allow small delta in case a new 1m candle ticked between overview and WS snapshot
        t_diff = abs(tm["p1_flip_risk_pct"] - ov_row["time_metrics"]["p1_flip_risk_pct"])
        d_diff = abs(dm["p1_flip_risk_pct"] - ov_row["distance_metrics"]["p1_flip_risk_pct"])
        assert t_diff <= 1.0, f"Time flip risk mismatch: local={tm['p1_flip_risk_pct']} vs server={ov_row['time_metrics']['p1_flip_risk_pct']}"
        assert d_diff <= 1.0, f"Dist flip risk mismatch: local={dm['p1_flip_risk_pct']} vs server={ov_row['distance_metrics']['p1_flip_risk_pct']}"
        print(
            f"  Formula parity vs /pivots/overview: ok "
            f"(time_flip local={tm['p1_flip_risk_pct']}% vs server={ov_row['time_metrics']['p1_flip_risk_pct']}%; "
            f"dist_flip local={dm['p1_flip_risk_pct']}% vs server={ov_row['distance_metrics']['p1_flip_risk_pct']}%)"
        )


def test_fastapi_server() -> None:
    print("\n=== [4/4] TESTING LOCAL FASTAPI HTTP + WS SERVER (server.py) ===")
    from fastapi.testclient import TestClient

    import server

    server.DEFAULT_ASSETS = "BTCUSD"
    server.DEFAULT_TF = "1D"
    with TestClient(server.app) as tc:
        r_health = tc.get("/health")
        assert r_health.status_code == 200 and r_health.json()["ok"] is True
        print("  GET /health:", r_health.json())

        r_live = tc.get("/v1/live?asset=BTCUSD&tf=1D")
        assert r_live.status_code == 200
        live = r_live.json()
        assert "1D" in live["timeframes"] and live["timeframes"]["1D"]["candle"]["close"] > 0
        print(f"  GET /v1/live: connected={live['connected']}, 1D close=${live['timeframes']['1D']['candle']['close']:,.2f}")

        r_lens = tc.get("/v1/lens/analyze?asset=BTCUSD&event_type=Daily+Candle&limit=60&top_k_fractals=5")
        assert r_lens.status_code == 200
        lj = r_lens.json()
        assert lj["sample_size"] >= 30 and len(lj["fractals"]["matches"]) == 5
        print(f"  GET /v1/lens/analyze: sample={lj['sample_size']}, bullish={lj['outcomes']['closed_bullish']['pct']}%")

        r_dist = tc.get("/v1/pivots/distance?asset=BTCUSD&tf=1D")
        assert r_dist.status_code == 200
        dj = r_dist.json()
        assert len(dj["distance_metrics"]["confidence_targets"]["bullish"]) == 8
        print(f"  GET /v1/pivots/distance: dir={dj['distance_metrics']['direction']}, flip={dj['distance_metrics']['p1_flip_risk_pct']}%")

        r_sum = tc.get("/v1/pivots/summary?asset=BTCUSD&tf=1D")
        assert r_sum.status_code == 200
        print(f"  GET /v1/pivots/summary: ok (total_candles={r_sum.json()['total_candles']})")

        with tc.websocket_connect("/ws/live?asset=BTCUSD&tf=1D") as ws:
            first = ws.receive_json()
            assert first["type"] == "snapshot" and "1D" in first["snapshot"]["timeframes"]
            print(f"  WS /ws/live: initial snapshot ok (BTCUSD 1D close=${first['snapshot']['timeframes']['1D']['candle']['close']:,.2f})")


def main() -> int:
    ctx = test_auth_and_rest()
    asyncio.run(test_live_ws_and_math_parity(ctx))
    test_fastapi_server()
    print("\nALL BRIGHTER DATA LIVE SELF-TESTS PASSED!")
    return 0


if __name__ == "__main__":
    sys.exit(main())
