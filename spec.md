# Brighter Data (`app.brighterdata.com`) API & Brighter Lens — Spec

Reverse-engineered via **REA (`rea@6.7.0`)** from `https://app.brighterdata.com/brighter-lens` and `/pivots/*` on `2026-11-24`.

Status legend:
- **VERIFIED** = observed in REA browser capture and/or called directly live with HTTP 200 / live WS frames confirmed.
- **CODE** = read directly from Brighter Data's shipped Next.js chunks (`13624d889ed9b6b2.js`, `7485a0ab93a6906a.js`, `398757d77e106d6a.js`, `448c69f556754396.js`, `b73a391cfa60c7f2.js`, `b99d2032adee978a.js`, `dea93284d8a224cf.js`, `b5d587f47c7a376c.js`).
- **LIMITATION** = observed caveat or server-enforced entitlement boundary.

REA Evidence references from this investigation:
- Static JS application analysis (21 Next.js chunks): `ev_93c9398ea19441cb546ef3e794ec1ea77b6b58e91d56db1b5f86e3949cdb1c2e`
- Authenticated browser scenario capture (`/login` → `/brighter-lens`, 318 retained events, Cognito SRP + REST + WebSocket frames): `ev_37ee679affdc28f87dd038a3c0289ac0a45e65216db8a881e0233f0e22b9ebfe`

---

## 1. Architecture & Authentication (VERIFIED + CODE)

Defined in `13624d889ed9b6b2.js` (module `9849`) and `7485a0ab93a6906a.js` (module `3948` / `2178`):

| Layer | Base URL | Auth / Protocol |
|---|---|---|
| Identity Provider | `https://cognito-idp.eu-west-3.amazonaws.com/` | AWS Cognito User Pool `eu-west-3_WKf8kAGSL`, Client ID `m1jk2eo51rhqsks584ts83k3t`, `USER_SRP_AUTH` (SRP-6a) → `PASSWORD_VERIFIER` → `IdToken` (3600s TTL) + `RefreshToken` (`REFRESH_TOKEN_AUTH`) |
| REST API Backend | `https://api.brighterdata.com` | JSON over HTTPS, `Authorization: Bearer <IdToken>` (`Origin: https://app.brighterdata.com`) |
| Live WebSocket | `wss://d3s1aak61z0hie.cloudfront.net/ws?token=<IdToken>` | JSON WebSocket frames (`subscribe` / `unsubscribe` → `snapshot` / `delta`) |

Note: `USER_PASSWORD_AUTH` is disabled on the Cognito app client (`HTTP 400 InvalidParameterException`). `bd.py` implements standard AWS Cognito SRP-6a (`USER_SRP_AUTH` + `REFRESH_TOKEN_AUTH`) in pure Python stdlib.

---

## 2. REST Endpoint Table (ALL VERIFIED HTTP 200)

### Account & Metadata
| Endpoint | Path | Notes |
|---|---|---|
| Profile & Features | `GET /account/me` | Returns `{id, email, name, features, hasActiveSubscription, discord}` |
| Subscription | `GET /account/subscription` | Active plan status & billing period |
| Watchlist | `GET /account/watchlist` | Saved symbols (e.g. `["BTCUSD", "ETHUSDT", "SOLUSDT"]`) |
| Bookmarks | `GET /account/bookmarks` | Saved filter presets |
| Drawings | `GET /account/drawings/{asset}` | Optional `?since=<cursor>` |
| Settings | `GET /account/settings?prefix=chart.` | Also `prefix=drawing.` |
| Plans | `GET /plans` | Available subscription tiers |
| Assets | `GET /assets` | 91 Crypto assets (ByBit) with `availableFrom` and `availableFromByTimeframe` (`5m, 15m, 30m, 1h, 4h, 1D, 1W, 1M, S`) |
| Sessions | `GET /sessions` | `Asia` (19:00–03:00 NY), `London` (03:00–08:30), `New_York` (08:30–16:30), `Post` (16:30–19:00) |

### Brighter Lens / Event Engine
| Endpoint | Path | Notes |
|---|---|---|
| Event Types | `GET /events/types` | 17 event types with `eventTypeKey`, `eventTypeName`, `eventCategory`, `eventGroup`, `timeframe`, `pathGranularity`, `maxHorizonSteps`, `applicableHorizons` |
| Event Instances | `GET /events/{asset}` | Query: `from`, `to`, `types`, `active=true\|false`, `limit`. Includes precomputed `mae/mfe/close` for `1h, 4h, 12h, 1d, 3d, 1w, 2w` |
| Event Buffer | `GET /events/{asset}/buffer/events` | Query: `eventType`, `limit`, `before`, `since`, `includeEventId`, `contextFilters` (e.g. `rsi_14_1d:-35;rsi_14_1h:35-70`), `dayType`, `dayOfWeek`, `session`, `candleWindowStart`, `candleWindowEnd`, `weekOfMonth`, `weekOfQuarter`, `macroDirection`, `macroSchedule`, `macroStrength`. Includes `rsi14_1h`, `rsi14_1d`, `atrPct7_1d`, `atrPctRank7_1d` |
| Event Paths | `GET /events/{asset}/buffer/paths` | Query: `eventIds` (CSV, batched in 50s), `maxSteps` (default 1000), `pathEncoding=binary` (optional). Returns `pathGranularityMinutes` and `pathsBinary[]` (or JSON `paths[]`) |
| OHLCV Candles | `GET /candles/{asset}/{timeframe}` | Timeframes: `5m, 15m, 30m, 1h, 4h, 1D, 1W, 1M`. Query: `from`, `to`, `limit` |
| Context Series | `GET /context/{asset}/series` | Query: `attributes=rsi_14_1h,rsi_14_1d`, `from`, `to` |
| Swing Failures | `GET /events/{asset}/swing-failures` | Query: `type` (swing pivot event type), `since`, `before`, `dayOfWeek` |

### Pivots Engine
| Endpoint | Path | Notes |
|---|---|---|
| Time Buckets | `GET /pivots/{asset}/{tf}/time/buckets` | `tf` in `1h, 4h, S, 1D, 1W, 1M`. Query: `start_date`, `end_date`, `day_of_week`, `session`, `hour`, `four_hour_candle`. Returns `bucket_size_minutes`, `total_candles`, `p1_buckets`, `p2_buckets` |
| Time Explorer | `GET /pivots/{asset}/{tf}/time/explorer` | Adds `group_by` (`five_minute`, `thirty_minute`, `hour`, `day_of_week`, `three_day`). Returns rows with `label, value, p1_count, p1_pct, p2_count, p2_pct, last_p1, last_p2` |
| Distance Distributions | `GET /pivots/{asset}/{tf}/distance/distributions` | Returns `distributions.{p1_delta_open, p2_delta_open, total_bullish_displacement, total_bearish_displacement}` as `[{value, count}]` |
| Multi-Asset Overview | `GET /pivots/overview` | Query: `timeframe`, `assets` (CSV), `confidence`, plus filter params. Returns `current_state`, `time_metrics`, `distance_metrics`, `advanced_metrics` |

---

## 3. Binary Event Path Format (VERIFIED BIT-FOR-BIT)

When calling `GET /events/{asset}/buffer/paths?eventIds=...&maxSteps=1000&pathEncoding=binary`:
Each element in `pathsBinary` has `{eventId, steps, data}` where `data` is a base64-encoded little-endian `Float32Array` of length `4 * steps`:
- `[0 .. steps)`: `closePath` (fractional return `(close - anchorPrice) / anchorPrice`)
- `[steps .. 2*steps)`: `highPath` (fractional return `(high - anchorPrice) / anchorPrice`)
- `[2*steps .. 3*steps)`: `lowPath` (fractional return `(low - anchorPrice) / anchorPrice`)
- `[3*steps .. 4*steps)`: `volPath` (per-step volume)

Verified bit-for-bit against non-binary JSON output of the same endpoint.

---

## 4. Real-Time WebSocket Protocol (VERIFIED LIVE)

1. Connect to `wss://d3s1aak61z0hie.cloudfront.net/ws?token=<IdToken>`.
2. Send subscribe message:
   ```json
   {"action": "subscribe", "asset": "BTCUSD"}
   ```
3. Server immediately pushes 6 `snapshot` frames — one per timeframe (`1D`, `1W`, `1M`, `1h`, `4h`, `S`):
   ```json
   {
     "type": "snapshot",
     "timeframe": "1D",
     "states": [{
       "asset": "BTCUSD", "periodStart": 1795478400000, "periodEnd": 1795564800000,
       "openPrice": 61189.4, "highPrice": 61572.2, "lowPrice": 59156.8, "closePrice": 59644.5,
       "p1Timestamp": 1795482600000, "p2Timestamp": 1795539780000,
       "p1DeltaOpen": 0.006256, "p2DeltaOpen": -0.033218,
       "bullishDisplacement": 0.04083, "bearishDisplacement": -0.039229,
       "p1MinutesSinceStart": 70, "p2MinutesSinceStart": 1023,
       "isBullish": false, "retracement": 0.202
     }]
   }
   ```
4. Server streams incremental `delta` frames with optional `tick` and partial `updates[]` merged onto the cached raw state keyed by `(asset, timeframe)`.
5. Unsubscribe with `{"action": "unsubscribe", "asset": "BTCUSD"}`.

---

## 5. Exact Client-Side Analytics Formulas (CODE + VERIFIED)

### Pivots Math (`dea93284d8a224cf.js` / `b73a391cfa60c7f2.js` / `448c69f556754396.js`, modules `67906`, `91021`, `52629`)
- **Time P1 Flip Risk**: `bucketPctAtOrAfter(p1_buckets, p2_minutes_since_start)`
- **Time P2s Later**: `bucketPctAfter(p2_buckets, p2_minutes_since_start)`
- **Time P1 Later (Warning ≥ 90%)**: `bucketPctAfter(p1_buckets, p1_minutes_since_start)`
- **Distance P1 Flip Risk**: Let `r = |p1_delta_open|`. If `isBullish`, `P1_opp = sum(p1_delta_open >= r)`, `P2_same = sum(p2_delta_open >= r)`; else `P1_opp = sum(p1_delta_open <= -r)`, `P2_same = sum(p2_delta_open <= -r)`. Returns `100 * P1_opp / (P1_opp + P2_same)`.
- **Distance P1 Wick Risk ("P1 Further", Warning ≥ 90%)**: Share of same-sign `p1_delta_open` entries with `|val| > |p1_delta_open|`.
- **Distance P2s Further**: Share of same-direction displacement (`total_bullish_displacement` if bullish else `total_bearish_displacement`) with `|val| > |current_displacement|`.
- **Confidence Targets (`90, 80, 70, 60, 50, 40, 30, 20`)**:
  - `absPercentile(dist, cft)`: sort by `|value|` ascending; return first `|value|` where cumulative count `>= ceil((1 - cft/100) * total)`.
  - Bullish: `p1Pct = -absPercentile(p1_delta_open < 0, cft)`, `p1Price = open * (1 + p1Pct)`; `p2Pct = +absPercentile(total_bullish_displacement, cft)`, `p2Price = low * (1 + p2Pct)`.
  - Bearish: `p1Pct = +absPercentile(p1_delta_open > 0, cft)`, `p1Price = open * (1 + p1Pct)`; `p2Pct = -absPercentile(total_bearish_displacement, cft)`, `p2Price = high * (1 + p2Pct)`.
- **Thresholds**: `P1_FLIP_LOW=30`, `P1_FLIP_HIGH=70`, `P2S_LATER_LOW=30`, `P2S_LATER_HIGH=75`, `P2S_FURTHER=50`, `WARNING=90`; Reliability `<50` Low, `<70` Moderate, `>=70` High.

### Brighter Lens Analytics (`b5d587f47c7a376c.js`)
- **Outcome Predicates (`m5`)**: `closed_bullish` (`close[n-1] > 0`), `closed_bearish` (`close[n-1] < 0`), `low_first` / `high_first` (earliest argmin vs argmax across `lowPath` / `highPath`), `took_out_high_later`, `took_out_low_later`, `had_more_displacement`, `had_more_total_displacement` (`(hi - lo) / (1 + lo)` comparison).
- **Fractal Similarity Matching**: Euclidean distance `d = sqrt(sum((target_close[i] - hist_close[i])^2))` over `i = 0..min(t_len, h_len, horizon)-1`; `similarity = 1 / (1 + d)`; `alike_pct = round(sim / max_sim * 100)`.

---

## 6. Observed Limitations & Account Entitlements
- Server-enforced feature flags from `/account/me` govern optional add-ons (`event_lens_hl_sweeps`, `event_lens_macro_events`, `event_lens_full_history`, `api_access`, `marcus_agent`). This client strictly accesses endpoints authorized for the authenticated subscription (`p1p2_dashboard`, `event_engine`).
- Cognito `IdToken` expires every 3600s; `CognitoSRPAuth` automatically refreshes via `REFRESH_TOKEN_AUTH` with a 120s safety margin and retries once on HTTP 401.
