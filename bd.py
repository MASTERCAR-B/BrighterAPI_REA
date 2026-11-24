"""
bd.py - Real-time Python SDK, Cognito SRP-6a client, analytics engine & WebSocket feed
for Brighter Data (https://app.brighterdata.com/brighter-lens and /pivots/*).

Reverse-engineered via REA (reverse-engineer-anything) from https://app.brighterdata.com.
Uses your own paid Brighter Data account credentials via AWS Cognito SRP-6a
(pure Python stdlib, zero browser required at runtime) with automatic JWT refresh.

Backends:
  - Auth: AWS Cognito User Pool `eu-west-3_WKf8kAGSL` (SRP-6a + Refresh Token)
      https://cognito-idp.eu-west-3.amazonaws.com/
  - REST API (Brighter Lens / Event Engine + Pivots + Candles + Context + Account):
      https://api.brighterdata.com  (Authorization: Bearer <IdToken>)
  - Real-Time WebSocket (Live P1/P2 states + ticks across 1h, 4h, S, 1D, 1W, 1M):
      wss://d3s1aak61z0hie.cloudfront.net/ws?token=<IdToken>

Core REST + Auth + Analytics use Python 3.10+ stdlib only.
Real-time streaming (`BrighterLiveFeed`) uses `websockets`.
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import concurrent.futures
import contextlib
import datetime
import hashlib
import hmac
import json
import math
import os
import random
import re
import secrets
import struct
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import AsyncIterator, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import (
    Any,
)
from zoneinfo import ZoneInfo

API_URL = "https://api.brighterdata.com"
WS_URL = "wss://d3s1aak61z0hie.cloudfront.net/ws"
COGNITO_URL = "https://cognito-idp.eu-west-3.amazonaws.com/"
COGNITO_USER_POOL_ID = "eu-west-3_WKf8kAGSL"
COGNITO_CLIENT_ID = "m1jk2eo51rhqsks584ts83k3t"
ORIGIN = "https://app.brighterdata.com"
REFERER = "https://app.brighterdata.com/"
UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36"
)

# Exact Pivot timeframes supported by Brighter Data (`/pivots/*` & WebSocket)
PIVOT_TIMEFRAMES: tuple[str, ...] = ("1h", "4h", "S", "1D", "1W", "1M")
CANDLE_TIMEFRAMES: tuple[str, ...] = ("5m", "15m", "30m", "1h", "4h", "1D", "1W", "1M")

_PIVOT_TF_ALIASES: dict[str, str] = {
    "1h": "1h",
    "hourly": "1h",
    "1-hourly": "1h",
    "4h": "4h",
    "4-hourly": "4h",
    "s": "S",
    "session": "S",
    "1d": "1D",
    "d": "1D",
    "daily": "1D",
    "1w": "1W",
    "w": "1W",
    "weekly": "1W",
    "1m": "1M",
    "1mo": "1M",
    "monthly": "1M",
}

_CANDLE_TF_ALIASES: dict[str, str] = {
    "5m": "5m",
    "15m": "15m",
    "30m": "30m",
    "1h": "1h",
    "4h": "4h",
    "1d": "1D",
    "1w": "1W",
    "1m": "1M",
    "1mo": "1M",
}

# Exact Brighter Data frontend constants (`448c69f556754396.js` module 91021)
CONFIDENCE_LEVELS: tuple[int, ...] = (90, 80, 70, 60, 50, 40, 30, 20)
THRESHOLDS: dict[str, float] = {
    "P1_FLIP_LOW": 30.0,
    "P1_FLIP_HIGH": 70.0,
    "P2S_LATER_LOW": 30.0,
    "P2S_LATER_HIGH": 75.0,
    "P2S_FURTHER": 50.0,
    "WARNING": 90.0,
}

TIMEFRAME_LABELS: dict[str, tuple[str, str, str]] = {
    # tf: (adjective, singular_noun, plural_noun)
    "1h": ("hourly", "hour", "hours"),
    "4h": ("4-hour", "4-hour candle", "4h candles"),
    "S": ("session", "session", "sessions"),
    "1D": ("daily", "day", "days"),
    "1W": ("weekly", "week", "weeks"),
    "1M": ("monthly", "month", "months"),
}

DEFAULT_EXPLORER_GROUP_BY: dict[str, str] = {
    "1h": "five_minute",
    "4h": "thirty_minute",
    "S": "hour",
    "1D": "hour",
    "1W": "day_of_week",
    "1M": "three_day",
}

# Context filter bucket definitions (`b5d587f47c7a376c.js` `mb`)
CONTEXT_FILTER_BUCKETS: dict[str, dict[str, tuple[float | None, float | None]]] = {
    "rsi_14_1d": {"oversold": (None, 35.0), "neutral": (35.0, 70.0), "overbought": (70.0, None)},
    "rsi_14_1h": {"oversold": (None, 35.0), "neutral": (35.0, 70.0), "overbought": (70.0, None)},
}

# Brighter Lens outcome definitions (`b5d587f47c7a376c.js` `rW` & `m5`)
LENS_OUTCOMES: tuple[dict[str, Any], ...] = (
    {"id": "closed_bullish", "name": "Closed bullish", "group": "direction"},
    {"id": "closed_bearish", "name": "Closed bearish", "group": "direction"},
    {"id": "low_first", "name": "Put in low first", "group": "price_movement"},
    {"id": "high_first", "name": "Put in high first", "group": "price_movement"},
    {"id": "took_out_high_later", "name": "Took out high later", "group": "price_movement"},
    {"id": "took_out_low_later", "name": "Took out low later", "group": "price_movement"},
    {"id": "had_more_displacement", "name": "Made a new high or low", "group": "price_movement"},
    {"id": "had_more_total_displacement", "name": "Had more total displacement", "group": "price_movement"},
)

# RFC 5054 3072-bit prime used by AWS Cognito SRP-6a
_SRP_N_HEX = (
    "FFFFFFFFFFFFFFFFC90FDAA22168C234C4C6628B80DC1CD1"
    "29024E088A67CC74020BBEA63B139B22514A08798E3404DD"
    "EF9519B3CD3A431B302B0A6DF25F14374FE1356D6D51C245"
    "E485B576625E7EC6F44C42E9A637ED6B0BFF5CB6F406B7ED"
    "EE386BFB5A899FA5AE9F24117C4B1FE649286651ECE45B3D"
    "C2007CB8A163BF0598DA48361C55D39A69163FA8FD24CF5F"
    "83655D23DCA3AD961C62F356208552BB9ED529077096966D"
    "670C354E4ABC9804F1746C08CA18217C32905E462E36CE3B"
    "E39E772C180E86039B2783A2EC07A28FB5C55DF06F4C52C9"
    "DE2BCBF6955817183995497CEA956AE515D2261898FA0510"
    "15728E5A8AAAC42DAD33170D04507A33A85521ABDF1CBA64"
    "ECFB850458DBEF0A8AEA71575D060C7DB3970F85A6E1E4C7"
    "ABF5AE8CDB0933D71E8C94E04A25619DCEE3D2261AD2EE6B"
    "F12FFA06D98A0864D87602733EC86A64521F2B18177B200C"
    "BBE117577A615D6C770988C0BAD946E208E24FA074E5AB31"
    "43DB5BFCE0FD108E4B82D120A93AD2CAFFFFFFFFFFFFFFFF"
)
_SRP_N = int(_SRP_N_HEX, 16)
_SRP_G = 2
_SRP_INFO = b"Caldera Derived Key"


class BrighterError(Exception):
    """Base error for Brighter Data client failures."""


class BrighterAuthError(BrighterError):
    """Authentication or token refresh error."""


class BrighterHTTPError(BrighterError):
    def __init__(self, url: str, status: int, body: str = ""):
        self.url = url
        self.status = status
        self.body = body
        super().__init__(f"HTTP {status} on {url}: {body[:500]}")


# ---------------------------------------------------------------------------
# Helpers & Environment Loader
# ---------------------------------------------------------------------------


def load_dotenv(path: str | Path | None = None) -> dict[str, str]:
    """Load KEY=VALUE pairs from a local .env file without overwriting existing env vars."""
    candidates: list[Path] = []
    if path:
        candidates.append(Path(path))
    else:
        candidates.extend([Path.cwd() / ".env", Path(__file__).resolve().parent / ".env"])

    loaded: dict[str, str] = {}
    for p in candidates:
        if not p.is_file():
            continue
        try:
            for raw_line in p.read_text(encoding="utf-8").splitlines():
                line = raw_line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, v = line.split("=", 1)
                k = k.strip()
                v = v.strip()
                if len(v) >= 2 and v[0] == v[-1] and v[0] in ("'", '"'):
                    v = v[1:-1]
                if k and k not in os.environ:
                    os.environ[k] = v
                if k:
                    loaded[k] = v
            break
        except OSError:
            continue
    return loaded


def resolve_pivot_tf(tf: str) -> str:
    key = tf.strip().lower()
    if key not in _PIVOT_TF_ALIASES:
        raise ValueError(f"Unsupported pivot timeframe {tf!r}; expected one of {PIVOT_TIMEFRAMES}")
    return _PIVOT_TF_ALIASES[key]


def resolve_candle_tf(tf: str) -> str:
    key = tf.strip().lower()
    if key not in _CANDLE_TF_ALIASES:
        raise ValueError(f"Unsupported candle timeframe {tf!r}; expected one of {CANDLE_TIMEFRAMES}")
    return _CANDLE_TF_ALIASES[key]


def normalize_asset(asset: str) -> str:
    sym = asset.strip().upper()
    if not re.match(r"^[A-Z0-9._-]{2,24}$", sym):
        raise ValueError(f"Invalid asset symbol: {asset!r}")
    return sym


def _decode_jwt_exp(token: str) -> float:
    """Read JWT exp claim without verifying signature (for proactive local refresh)."""
    try:
        parts = token.split(".")
        if len(parts) < 2:
            return 0.0
        payload = parts[1] + "=" * (-len(parts[1]) % 4)
        data = json.loads(base64.urlsafe_b64decode(payload.encode("ascii")).decode("utf-8"))
        return float(data.get("exp", 0.0))
    except Exception:
        return 0.0


def _pad_hex(val: int | str) -> str:
    h = format(val, "x") if isinstance(val, int) else val
    if len(h) % 2 == 1:
        h = "0" + h
    if h and h[0] in "89abcdefABCDEF":
        h = "00" + h
    return h


def _hex_sha256(hex_str: str) -> str:
    return hashlib.sha256(bytes.fromhex(hex_str)).hexdigest()


def _bytes_sha256(buf: bytes) -> str:
    return hashlib.sha256(buf).hexdigest().zfill(64)


def _format_utc_cognito_timestamp(now: datetime.datetime | None = None) -> str:
    dt = now or datetime.datetime.now(datetime.timezone.utc)
    return dt.strftime("%a %b ") + str(dt.day) + dt.strftime(" %H:%M:%S UTC %Y")


def _default_day_of_week(now: datetime.datetime | None = None) -> str:
    dt = now or datetime.datetime.now(datetime.timezone.utc)
    # Python weekday(): Mon=0..Sun=6; Brighter API: Mon=1..Fri=5, Sat=6, Sun=7
    return "6,7" if dt.weekday() >= 5 else "1,2,3,4,5"


def _current_ny_session(sessions: Sequence[dict[str, Any]], now: datetime.datetime | None = None) -> str:
    utc_now = now or datetime.datetime.now(datetime.timezone.utc)
    try:
        dt = utc_now.astimezone(ZoneInfo("America/New_York"))
    except Exception:
        # Pure-stdlib fallback when Windows lacks `tzdata` (US Eastern DST approximation)
        y = utc_now.year
        mar_start = datetime.datetime(y, 3, 8, 7, tzinfo=datetime.timezone.utc)
        mar_dst = mar_start + datetime.timedelta(days=(6 - mar_start.weekday()) % 7)
        nov_start = datetime.datetime(y, 11, 1, 6, tzinfo=datetime.timezone.utc)
        nov_std = nov_start + datetime.timedelta(days=(6 - nov_start.weekday()) % 7)
        offset = -4 if mar_dst <= utc_now < nov_std else -5
        dt = utc_now.astimezone(datetime.timezone(datetime.timedelta(hours=offset)))
    ny_min = dt.hour * 60 + dt.minute
    for s in sessions:
        name = str(s.get("sessionName") or "")
        st = str(s.get("startTimeNy") or "")
        en = str(s.get("endTimeNy") or "")
        if len(st) >= 5 and len(en) >= 5:
            sm = int(st[:2]) * 60 + int(st[3:5])
            em = int(en[:2]) * 60 + int(en[3:5])
            in_range = (sm <= ny_min < em) if sm <= em else (ny_min >= sm or ny_min < em)
            if in_range and name:
                return name
    # Fallback using Brighter's static NY clock schedule
    if ny_min >= 1140 or ny_min < 180:
        return "Asia"
    if ny_min < 510:
        return "London"
    if ny_min < 990:
        return "New_York"
    return "Post"


# ---------------------------------------------------------------------------
# AWS Cognito SRP-6a Authenticator (Zero Dependencies)
# ---------------------------------------------------------------------------


@dataclass
class AuthTokens:
    id_token: str
    access_token: str = ""
    refresh_token: str = ""
    expires_at: float = 0.0

    def is_valid(self, leeway_s: float = 120.0) -> bool:
        if not self.id_token:
            return False
        if self.expires_at <= 0:
            return True
        return time.time() + leeway_s < self.expires_at


class CognitoSRPAuth:
    """Thread-safe pure-stdlib AWS Cognito SRP-6a + Refresh Token manager."""

    def __init__(
        self,
        email: str | None = None,
        password: str | None = None,
        id_token: str | None = None,
        user_pool_id: str = COGNITO_USER_POOL_ID,
        client_id: str = COGNITO_CLIENT_ID,
        cognito_url: str = COGNITO_URL,
        timeout: float = 15.0,
    ) -> None:
        load_dotenv()
        self.email = (email or os.environ.get("BRIGHTER_EMAIL", "")).strip().lower()
        self.password = password or os.environ.get("BRIGHTER_PASSWORD", "")
        self.user_pool_id = user_pool_id
        self.client_id = client_id
        self.cognito_url = cognito_url
        self.timeout = timeout
        self._lock = threading.Lock()

        raw_tok = (id_token or os.environ.get("BRIGHTER_ID_TOKEN") or os.environ.get("BRIGHTER_BEARER_TOKEN") or "").strip()
        if raw_tok.lower().startswith("bearer "):
            raw_tok = raw_tok[7:].strip()

        self._tokens: AuthTokens | None = None
        if raw_tok:
            self._tokens = AuthTokens(id_token=raw_tok, expires_at=_decode_jwt_exp(raw_tok))

    def _cognito_post(self, target: str, payload: dict[str, Any]) -> dict[str, Any]:
        req = urllib.request.Request(
            self.cognito_url,
            data=json.dumps(payload).encode("utf-8"),
            headers={
                "Content-Type": "application/x-amz-json-1.1",
                "X-Amz-Target": f"AWSCognitoIdentityProviderService.{target}",
                "User-Agent": UA,
            },
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            try:
                err_json = json.loads(body)
                msg = err_json.get("message") or err_json.get("__type") or body[:300]
            except Exception:
                msg = body[:300]
            raise BrighterAuthError(f"Cognito {target} failed (HTTP {exc.code}): {msg}") from exc
        except urllib.error.URLError as exc:
            raise BrighterAuthError(f"Cognito network error on {target}: {exc}") from exc

    def _srp_login_locked(self) -> AuthTokens:
        if not self.email or not self.password:
            raise BrighterAuthError(
                "Missing Brighter Data credentials. Set BRIGHTER_EMAIL and BRIGHTER_PASSWORD "
                "(or BRIGHTER_ID_TOKEN) in your environment or .env file."
            )

        pool_name = self.user_pool_id.split("_", 1)[1]
        a = int.from_bytes(secrets.token_bytes(128), "big") % _SRP_N
        A = pow(_SRP_G, a, _SRP_N)

        init_resp = self._cognito_post(
            "InitiateAuth",
            {
                "AuthFlow": "USER_SRP_AUTH",
                "ClientId": self.client_id,
                "AuthParameters": {"USERNAME": self.email, "SRP_A": format(A, "x")},
            },
        )
        challenge = init_resp.get("ChallengeName")
        if challenge != "PASSWORD_VERIFIER":
            raise BrighterAuthError(f"Unsupported Cognito challenge: {challenge!r}")

        cp = init_resp["ChallengeParameters"]
        user_id_for_srp = cp["USER_ID_FOR_SRP"]
        salt = int(cp["SALT"], 16)
        B = int(cp["SRP_B"], 16)
        secret_block = base64.b64decode(cp["SECRET_BLOCK"])

        k = int(_hex_sha256("00" + _SRP_N_HEX + "0" + format(_SRP_G, "x")), 16)
        u = int(_hex_sha256(_pad_hex(A) + _pad_hex(B)), 16)
        x = int(
            _hex_sha256(
                _pad_hex(salt) + _bytes_sha256(f"{pool_name}{user_id_for_srp}:{self.password}".encode())
            ),
            16,
        )
        S = pow((B - k * pow(_SRP_G, x, _SRP_N)) % _SRP_N, (a + u * x), _SRP_N)

        prk = hmac.new(bytes.fromhex(_pad_hex(u)), bytes.fromhex(_pad_hex(S)), hashlib.sha256).digest()
        derived_key = hmac.new(prk, _SRP_INFO + b"\x01", hashlib.sha256).digest()[:16]

        ts = _format_utc_cognito_timestamp()
        msg = pool_name.encode("utf-8") + user_id_for_srp.encode("utf-8") + secret_block + ts.encode("utf-8")
        signature = base64.b64encode(hmac.new(derived_key, msg, hashlib.sha256).digest()).decode("ascii")

        resp = self._cognito_post(
            "RespondToAuthChallenge",
            {
                "ChallengeName": "PASSWORD_VERIFIER",
                "ClientId": self.client_id,
                "ChallengeResponses": {
                    "USERNAME": user_id_for_srp,
                    "PASSWORD_CLAIM_SECRET_BLOCK": cp["SECRET_BLOCK"],
                    "TIMESTAMP": ts,
                    "PASSWORD_CLAIM_SIGNATURE": signature,
                },
            },
        )
        ar = resp.get("AuthenticationResult")
        if not ar or not ar.get("IdToken"):
            raise BrighterAuthError(f"Cognito login did not return tokens (challenge={resp.get('ChallengeName')!r})")

        id_tok = ar["IdToken"]
        exp = _decode_jwt_exp(id_tok) or (time.time() + float(ar.get("ExpiresIn", 3600)))
        self._tokens = AuthTokens(
            id_token=id_tok,
            access_token=ar.get("AccessToken", ""),
            refresh_token=ar.get("RefreshToken", ""),
            expires_at=exp,
        )
        return self._tokens

    def _refresh_locked(self) -> AuthTokens:
        if not self._tokens or not self._tokens.refresh_token:
            return self._srp_login_locked()
        try:
            resp = self._cognito_post(
                "InitiateAuth",
                {
                    "AuthFlow": "REFRESH_TOKEN_AUTH",
                    "ClientId": self.client_id,
                    "AuthParameters": {"REFRESH_TOKEN": self._tokens.refresh_token},
                },
            )
            ar = resp.get("AuthenticationResult") or {}
            id_tok = ar.get("IdToken")
            if not id_tok:
                return self._srp_login_locked()
            exp = _decode_jwt_exp(id_tok) or (time.time() + float(ar.get("ExpiresIn", 3600)))
            self._tokens = AuthTokens(
                id_token=id_tok,
                access_token=ar.get("AccessToken", self._tokens.access_token),
                refresh_token=ar.get("RefreshToken", self._tokens.refresh_token),
                expires_at=exp,
            )
            return self._tokens
        except BrighterAuthError:
            if self.email and self.password:
                return self._srp_login_locked()
            raise

    def get_id_token(self, force_refresh: bool = False) -> str:
        with self._lock:
            if not force_refresh and self._tokens and self._tokens.is_valid():
                return self._tokens.id_token
            if self._tokens and self._tokens.refresh_token:
                return self._refresh_locked().id_token
            return self._srp_login_locked().id_token

    def login(self) -> AuthTokens:
        with self._lock:
            return self._srp_login_locked()

    def refresh(self) -> AuthTokens:
        with self._lock:
            return self._refresh_locked()

    @property
    def token_expires_at(self) -> float:
        return self._tokens.expires_at if self._tokens else 0.0


# ---------------------------------------------------------------------------
# Exact Brighter Data Pivot Math (ported from module 67906, 91021, 52629)
# ---------------------------------------------------------------------------


def _dist_total(rows: Sequence[dict[str, Any]]) -> float:
    return float(sum(float(r.get("count", 0)) for r in rows))


def bucket_pct_after(buckets: Sequence[dict[str, Any]], minutes: float) -> float:
    denom = _dist_total(buckets)
    if denom <= 0:
        return 0.0
    num = sum(float(b.get("count", 0)) for b in buckets if float(b.get("minutes", 0)) > minutes)
    return (num / denom) * 100.0


def bucket_pct_at_or_after(buckets: Sequence[dict[str, Any]], minutes: float) -> float:
    denom = _dist_total(buckets)
    if denom <= 0:
        return 0.0
    num = sum(float(b.get("count", 0)) for b in buckets if float(b.get("minutes", 0)) >= minutes)
    return (num / denom) * 100.0


def calculate_time_p1_flip_risk(p1_buckets: Sequence[dict[str, Any]], p2_minutes_since_start: float) -> float:
    return bucket_pct_at_or_after(p1_buckets, p2_minutes_since_start)


def abs_percentile(distribution: Sequence[dict[str, Any]], confidence_pct: float) -> float:
    """Exact Brighter Data `absPercentile` (module 67906): upper-tail confidence level."""
    items = sorted(
        ((abs(float(r["value"])), float(r.get("count", 0))) for r in distribution),
        key=lambda x: x[0],
    )
    if not items:
        return 0.0
    total_cnt = sum(c for _, c in items)
    target = math.ceil((1.0 - confidence_pct / 100.0) * total_cnt)
    cum = 0.0
    for val, cnt in items:
        cum += cnt
        if cum >= target:
            return val
    return items[-1][0]


def calculate_distance_p1_wick_risk(p1_delta_open_dist: Sequence[dict[str, Any]], p1_delta_open: float) -> float:
    """% of same-side historical P1s whose wick exceeded |p1_delta_open|."""
    same_side = [r for r in p1_delta_open_dist if (float(r["value"]) < 0 if p1_delta_open < 0 else float(r["value"]) > 0)]
    denom = _dist_total(same_side)
    if denom <= 0:
        return 0.0
    thresh = abs(p1_delta_open)
    num = sum(float(r.get("count", 0)) for r in same_side if abs(float(r["value"])) > thresh)
    return (num / denom) * 100.0


def calculate_distance_p1_flip_risk(
    p1_delta_open_dist: Sequence[dict[str, Any]],
    p2_delta_open_dist: Sequence[dict[str, Any]],
    p1_delta_open: float,
    is_bullish: bool,
) -> float:
    """Exact Brighter Data Distance P1 Flip Risk (`calculateDistanceP1FlipRisk`, module 67906)."""
    r = abs(p1_delta_open)
    if is_bullish:
        p1_opp = sum(float(x.get("count", 0)) for x in p1_delta_open_dist if float(x["value"]) >= r)
        p2_same = sum(float(x.get("count", 0)) for x in p2_delta_open_dist if float(x["value"]) >= r)
    else:
        p1_opp = sum(float(x.get("count", 0)) for x in p1_delta_open_dist if float(x["value"]) <= -r)
        p2_same = sum(float(x.get("count", 0)) for x in p2_delta_open_dist if float(x["value"]) <= -r)
    denom = p1_opp + p2_same
    return (p1_opp / denom) * 100.0 if denom > 0 else 0.0


def calculate_pct_more_displacement(displacement_dist: Sequence[dict[str, Any]], current_displacement: float) -> float:
    denom = _dist_total(displacement_dist)
    if denom <= 0:
        return 0.0
    thresh = abs(current_displacement)
    num = sum(float(r.get("count", 0)) for r in displacement_dist if abs(float(r["value"])) > thresh)
    return (num / denom) * 100.0


def classify_p1_flip_risk(pct_val: float) -> dict[str, str]:
    if pct_val < THRESHOLDS["P1_FLIP_LOW"]:
        return {"type": "success", "label": "Low"}
    if pct_val < THRESHOLDS["P1_FLIP_HIGH"]:
        return {"type": "warning", "label": "Moderate"}
    return {"type": "danger", "label": "High"}


def classify_p2_later_risk(pct_val: float) -> dict[str, str]:
    if pct_val < THRESHOLDS["P2S_LATER_LOW"]:
        return {"type": "success", "label": "P2 is likely to hold"}
    if pct_val < THRESHOLDS["P2S_LATER_HIGH"]:
        return {"type": "warning", "label": "Unclear if P2 is in"}
    return {"type": "danger", "label": "Unlikely that P2 is in"}


def classify_p2_further_risk(pct_val: float) -> dict[str, str]:
    if pct_val < THRESHOLDS["P2S_FURTHER"]:
        return {"type": "success", "label": "P2 is likely to hold"}
    return {"type": "danger", "label": "Unlikely that P2 is in"}


def classify_data_reliability(count: int) -> dict[str, Any]:
    if count < 50:
        return {"type": "danger", "label": "Low reliability", "count": count}
    if count < 70:
        return {"type": "warning", "label": "Moderate reliability", "count": count}
    return {"type": "success", "label": "High reliability", "count": count}


def compute_confidence_targets(
    distributions: dict[str, Any],
    candle: dict[str, Any] | None = None,
    levels: Sequence[int] = CONFIDENCE_LEVELS,
) -> dict[str, list[dict[str, Any]]]:
    """Exact port of Brighter Data Bullish & Bearish Confidence Targets Ladder (module 52629)."""
    p1_dist = distributions.get("p1_delta_open") or []
    bull_disp = distributions.get("total_bullish_displacement") or []
    bear_disp = distributions.get("total_bearish_displacement") or []

    p1_neg = [r for r in p1_dist if float(r["value"]) < 0]
    p1_pos = [r for r in p1_dist if float(r["value"]) > 0]

    open_px = float(candle["open"]) if candle and candle.get("open") is not None else None
    low_px = float(candle["low"]) if candle and candle.get("low") is not None else None
    high_px = float(candle["high"]) if candle and candle.get("high") is not None else None

    bullish: list[dict[str, Any]] = []
    bearish: list[dict[str, Any]] = []

    for cft in levels:
        b_p1_pct = -abs_percentile(p1_neg, cft)
        b_p2_pct = abs_percentile(bull_disp, cft)
        b_p1_px = open_px * (1.0 + b_p1_pct) if open_px is not None else None
        b_p2_px = low_px * (1.0 + b_p2_pct) if low_px is not None else None
        bullish.append(
            {
                "confidence": int(cft),
                "p1_pct": round(b_p1_pct, 6),
                "p1_pct_display": round(b_p1_pct * 100.0, 2),
                "p1_price": round(b_p1_px, 4) if b_p1_px is not None else None,
                "p1_hit": bool(b_p1_px is not None and low_px is not None and low_px <= b_p1_px),
                "p2_pct": round(b_p2_pct, 6),
                "p2_pct_display": round(b_p2_pct * 100.0, 2),
                "p2_price": round(b_p2_px, 4) if b_p2_px is not None else None,
                "p2_hit": bool(b_p2_px is not None and high_px is not None and high_px >= b_p2_px),
            }
        )

        s_p1_pct = abs_percentile(p1_pos, cft)
        s_p2_pct = -abs_percentile(bear_disp, cft)
        s_p1_px = open_px * (1.0 + s_p1_pct) if open_px is not None else None
        s_p2_px = high_px * (1.0 + s_p2_pct) if high_px is not None else None
        bearish.append(
            {
                "confidence": int(cft),
                "p1_pct": round(s_p1_pct, 6),
                "p1_pct_display": round(s_p1_pct * 100.0, 2),
                "p1_price": round(s_p1_px, 4) if s_p1_px is not None else None,
                "p1_hit": bool(s_p1_px is not None and high_px is not None and high_px >= s_p1_px),
                "p2_pct": round(s_p2_pct, 6),
                "p2_pct_display": round(s_p2_pct * 100.0, 2),
                "p2_price": round(s_p2_px, 4) if s_p2_px is not None else None,
                "p2_hit": bool(s_p2_px is not None and low_px is not None and low_px <= s_p2_px),
            }
        )

    return {"bullish": bullish, "bearish": bearish}


def compute_pivot_time_metrics(buckets_data: dict[str, Any], live_state: dict[str, Any]) -> dict[str, Any]:
    """Compute exact Brighter Data Time Lens Key Insights from buckets + live P1/P2 state."""
    p1_buckets = buckets_data.get("p1_buckets") or []
    p2_buckets = buckets_data.get("p2_buckets") or []
    total_candles = int(buckets_data.get("total_candles") or _dist_total(p1_buckets))

    p1_min = float(live_state["p1"]["minutes_since_start"])
    p2_min = float(live_state["p2"]["minutes_since_start"])
    elapsed = float(live_state["period"]["minutes_elapsed"])

    p1_after_p1 = bucket_pct_after(p1_buckets, p1_min)
    p1_at_or_after_now = bucket_pct_at_or_after(p1_buckets, elapsed)
    p2_after_now = bucket_pct_after(p2_buckets, elapsed)
    p2_after_p2 = bucket_pct_after(p2_buckets, p2_min)
    flip_risk = calculate_time_p1_flip_risk(p1_buckets, p2_min)

    return {
        "p1_flip_risk_pct": round(flip_risk, 1),
        "p1_flip_risk_tier": classify_p1_flip_risk(flip_risk),
        "p2s_later_pct": round(p2_after_p2, 1),
        "p2s_later_tier": classify_p2_later_risk(p2_after_p2),
        "p1_formed_after_p1_pct": round(p1_after_p1, 1),
        "p1_formed_at_or_after_now_pct": round(p1_at_or_after_now, 1),
        "p2_formed_after_now_pct": round(p2_after_now, 1),
        "p1_warning": p1_after_p1 >= THRESHOLDS["WARNING"],
        "data_points": total_candles,
        "reliability": classify_data_reliability(total_candles),
    }


def compute_pivot_distance_metrics(dist_data: dict[str, Any], live_state: dict[str, Any]) -> dict[str, Any]:
    """Compute exact Brighter Data Distance Lens Key Insights + Confidence Targets."""
    dists = dist_data.get("distributions") or {}
    p1_dist = dists.get("p1_delta_open") or []
    p2_dist = dists.get("p2_delta_open") or []
    bull_disp = dists.get("total_bullish_displacement") or []
    bear_disp = dists.get("total_bearish_displacement") or []

    is_bullish = bool(live_state["is_bullish"])
    p1_delta_open = float(live_state["p1"]["delta_open"])
    disp = live_state["displacement"]
    cur_disp = float(disp["total_bullish"] if is_bullish else disp["total_bearish"])
    opp_disp = float(disp["total_bearish"] if is_bullish else disp["total_bullish"])

    same_dir_dist = bull_disp if is_bullish else bear_disp
    opp_dir_dist = bear_disp if is_bullish else bull_disp

    wick_risk = calculate_distance_p1_wick_risk(p1_dist, p1_delta_open)
    flip_risk = calculate_distance_p1_flip_risk(p1_dist, p2_dist, p1_delta_open, is_bullish)
    p2_further = calculate_pct_more_displacement(same_dir_dist, cur_disp)
    opp_further = calculate_pct_more_displacement(opp_dir_dist, opp_disp)
    same_dir_count = int(_dist_total(same_dir_dist))

    return {
        "direction": "Bullish" if is_bullish else "Bearish",
        "p1_flip_risk_pct": round(flip_risk, 1),
        "p1_flip_risk_tier": classify_p1_flip_risk(flip_risk),
        "p2s_further_pct": round(p2_further, 1),
        "p2s_further_tier": classify_p2_further_risk(p2_further),
        "opposite_displacement_further_pct": round(opp_further, 1),
        "p1_further_pct": round(wick_risk, 1),
        "p1_warning": wick_risk >= THRESHOLDS["WARNING"],
        "data_points": same_dir_count,
        "total_candles": int(dist_data.get("total_candles") or _dist_total(p1_dist)),
        "reliability": classify_data_reliability(same_dir_count),
        "confidence_targets": compute_confidence_targets(dists, live_state.get("candle")),
    }


# ---------------------------------------------------------------------------
# Exact Brighter Lens Binary Path Decoder & Analytics Engine (`b5d587f47c7a376c.js`)
# ---------------------------------------------------------------------------


def decode_binary_path_entry(entry: dict[str, Any]) -> dict[str, Any]:
    """Decode one `pathsBinary` record (base64 little-endian Float32Array[4*steps])."""
    steps = int(entry.get("steps", 0))
    raw = base64.b64decode(entry["data"])
    count = len(raw) // 4
    vals = struct.unpack(f"<{count}f", raw) if count else ()
    close_path = list(vals[0:steps])
    high_path = list(vals[steps : 2 * steps]) if count >= 2 * steps else []
    low_path = list(vals[2 * steps : 3 * steps]) if count >= 3 * steps else []
    vol_path = list(vals[3 * steps : 4 * steps]) if count >= 4 * steps else []
    return {
        "eventId": entry["eventId"],
        "steps": steps,
        "closePath": close_path,
        "highPath": high_path,
        "lowPath": low_path,
        "volPath": vol_path,
    }


def _linear_percentile(sorted_vals: Sequence[float], pct: float) -> float:
    if not sorted_vals:
        return 0.0
    pos = max(0.0, min(100.0, pct)) / 100.0 * (len(sorted_vals) - 1)
    lo = math.floor(pos)
    hi = math.ceil(pos)
    if lo == hi:
        return float(sorted_vals[lo])
    return float(sorted_vals[lo] + (sorted_vals[hi] - sorted_vals[lo]) * (pos - lo))


def _percentile_rank(val: float, population: Sequence[float]) -> float:
    n = len(population)
    if n == 0:
        return 0.0
    lt = sum(1 for x in population if x < val)
    le = sum(1 for x in population if x <= val)
    return ((lt + le) / 2.0) / n * 100.0


def _eval_lens_predicate(
    predicate_id: str,
    close_path: Sequence[float],
    high_path: Sequence[float],
    low_path: Sequence[float],
    horizon: int,
    current_step: int,
    live_max_high: float | None,
    live_min_low: float | None,
) -> bool:
    """Exact port of Brighter Lens `m5` outcome predicates (`b5d587f47c7a376c.js`)."""
    n = min(len(close_path), horizon)
    if n <= 0:
        return False

    if predicate_id == "closed_bullish":
        return close_path[n - 1] > 0.0
    if predicate_id == "closed_bearish":
        return close_path[n - 1] < 0.0

    if predicate_id in ("low_first", "high_first"):
        max_h, max_i = -math.inf, 0
        min_l, min_i = math.inf, 0
        for i in range(n):
            if high_path[i] > max_h:
                max_h, max_i = high_path[i], i
            if low_path[i] < min_l:
                min_l, min_i = low_path[i], i
        if predicate_id == "low_first":
            return min_i < max_i
        return max_i < min_i

    if predicate_id == "took_out_high_later":
        if current_step <= 0 or n <= current_step:
            return False
        pre_hi = max(high_path[:current_step])
        return any(high_path[i] > pre_hi for i in range(current_step, n))

    if predicate_id == "took_out_low_later":
        if current_step <= 0 or n <= current_step:
            return False
        pre_lo = min(low_path[:current_step])
        return any(low_path[i] < pre_lo for i in range(current_step, n))

    if predicate_id == "had_more_displacement":
        if current_step <= 0 or n <= current_step:
            return False
        pre_hi = max(high_path[:current_step])
        pre_lo = min(low_path[:current_step])
        return any(high_path[i] > pre_hi or low_path[i] < pre_lo for i in range(current_step, n))

    if predicate_id == "had_more_total_displacement":
        hi = max(high_path[:n])
        lo = min(low_path[:n])
        ref_hi = live_max_high if live_max_high is not None else hi
        ref_lo = live_min_low if live_min_low is not None else lo
        return ((hi - lo) / (1.0 + lo)) > ((ref_hi - ref_lo) / (1.0 + ref_lo))

    raise ValueError(f"Unknown outcome predicate: {predicate_id}")


def analyze_lens_paths(
    events: Sequence[dict[str, Any]],
    paths: Sequence[dict[str, Any]],
    *,
    horizon_steps: int | None = None,
    target_event_id: int | None = None,
    top_k_fractals: int = 12,
    include_step_bands: bool = True,
) -> dict[str, Any]:
    """
    Exact port of Brighter Lens client-side analytics (`b5d587f47c7a376c.js`):
      - Evaluates all 8 historical outcome predicates at the chosen horizon & active elapsed step
      - Computes MFE / MAE / Close return distributions (p10/p25/p50/p75/p90, mean) + target percentile rank
      - Computes step-by-step percentile fan (p10/p25/p50/p75/p90 of closePath)
      - Computes top-K Euclidean fractal analogs (exact `sqrt(sum((a[i]-b[i])^2))`) and their subset outcome rates
    """
    path_by_id = {p["eventId"]: p for p in paths}

    # Select target event (explicit ID, else active event, else latest event)
    target_ev: dict[str, Any] | None = None
    if target_event_id is not None:
        target_ev = next((e for e in events if e.get("eventId") == target_event_id), None)
    if target_ev is None:
        target_ev = next((e for e in events if e.get("isActive")), None)
    if target_ev is None and events:
        target_ev = events[0]

    # Determine horizon
    completed_lengths = [
        len(path_by_id[e["eventId"]]["closePath"])
        for e in events
        if e["eventId"] in path_by_id and not e.get("isActive") and path_by_id[e["eventId"]]["closePath"]
    ]
    default_horizon = max(completed_lengths) if completed_lengths else 0
    horizon = min(horizon_steps, default_horizon) if (horizon_steps and default_horizon) else (horizon_steps or default_horizon)

    target_path = path_by_id.get(target_ev["eventId"]) if target_ev else None
    current_step = len(target_path["closePath"]) if target_path else 0
    live_max_high = max(target_path["highPath"]) if target_path and target_path.get("highPath") else None
    live_min_low = min(target_path["lowPath"]) if target_path and target_path.get("lowPath") else None

    # Build completed historical sample
    sample: list[tuple[dict[str, Any], dict[str, Any]]] = []
    for ev in events:
        p = path_by_id.get(ev["eventId"])
        if not p or not p.get("closePath"):
            continue
        if ev.get("isActive") and len(p["closePath"]) < horizon:
            continue
        sample.append((ev, p))

    # 1. Outcome rates across full sample
    outcomes: dict[str, Any] = {}
    for spec in LENS_OUTCOMES:
        pid = spec["id"]
        hits = 0
        for _, p in sample:
            if _eval_lens_predicate(
                pid,
                p["closePath"],
                p["highPath"],
                p["lowPath"],
                horizon,
                current_step,
                live_max_high,
                live_min_low,
            ):
                hits += 1
        total_n = len(sample)
        rate = hits / total_n if total_n > 0 else 0.0
        outcomes[pid] = {
            "name": spec["name"],
            "group": spec["group"],
            "count": hits,
            "total": total_n,
            "rate": round(rate, 4),
            "pct": round(rate * 100.0, 1),
        }

    # 2. MFE / MAE / Horizon-Close distributions
    mfe_vals: list[float] = []
    mae_vals: list[float] = []
    close_vals: list[float] = []
    range_vals: list[float] = []
    for _, p in sample:
        n = min(len(p["closePath"]), horizon)
        if n <= 0:
            continue
        hi = max(p["highPath"][:n])
        lo = min(p["lowPath"][:n])
        cl = p["closePath"][n - 1]
        mfe_vals.append(hi)
        mae_vals.append(lo)
        close_vals.append(cl)
        range_vals.append(hi - lo)

    def _dist_summary(vals: list[float], cur_val: float | None = None) -> dict[str, Any]:
        if not vals:
            return {"count": 0}
        sv = sorted(vals)
        res: dict[str, Any] = {
            "count": len(sv),
            "mean": round(sum(sv) / len(sv), 6),
            "p10": round(_linear_percentile(sv, 10), 6),
            "p25": round(_linear_percentile(sv, 25), 6),
            "p50": round(_linear_percentile(sv, 50), 6),
            "p75": round(_linear_percentile(sv, 75), 6),
            "p90": round(_linear_percentile(sv, 90), 6),
        }
        if cur_val is not None:
            res["current"] = round(cur_val, 6)
            res["current_percentile_rank"] = round(_percentile_rank(cur_val, sv), 1)
        return res

    cur_close = target_path["closePath"][-1] if target_path and target_path.get("closePath") else None
    cur_range = (live_max_high - live_min_low) if (live_max_high is not None and live_min_low is not None) else None

    distributions = {
        "mfe": _dist_summary(mfe_vals, live_max_high),
        "mae": _dist_summary(mae_vals, live_min_low),
        "close": _dist_summary(close_vals, cur_close),
        "range": _dist_summary(range_vals, cur_range),
    }

    # 3. Step-by-step percentile fan (p10, p25, p50, p75, p90 of closePath)
    step_bands: dict[str, list[float]] | None = None
    if include_step_bands and horizon > 0 and sample:
        p10, p25, p50, p75, p90 = [], [], [], [], []
        for s_idx in range(horizon):
            col = [p["closePath"][s_idx] for _, p in sample if s_idx < len(p["closePath"])]
            if not col:
                break
            col.sort()
            p10.append(round(_linear_percentile(col, 10), 6))
            p25.append(round(_linear_percentile(col, 25), 6))
            p50.append(round(_linear_percentile(col, 50), 6))
            p75.append(round(_linear_percentile(col, 75), 6))
            p90.append(round(_linear_percentile(col, 90), 6))
        step_bands = {"p10": p10, "p25": p25, "p50": p50, "p75": p75, "p90": p90}

    # 4. Exact Brighter Lens Fractal Similarity Matching (Euclidean distance over closePath[0..c])
    fractals: dict[str, Any] = {"matches": [], "subset_outcomes": {}, "compared_steps": 0}
    if target_ev and target_path and len(target_path["closePath"]) >= 2:
        t_close = target_path["closePath"]
        t_len = len(t_close)
        scored: list[tuple[float, dict[str, Any], dict[str, Any]]] = []
        for ev, p in sample:
            if ev["eventId"] == target_ev["eventId"] or ev.get("isActive"):
                continue
            cand = p["closePath"]
            c = min(t_len, len(cand), horizon if horizon > 0 else t_len)
            if c < 2:
                continue
            sq_sum = 0.0
            for i in range(c):
                d = t_close[i] - cand[i]
                sq_sum += d * d
            dist = math.sqrt(sq_sum)
            if math.isfinite(dist):
                scored.append((dist, ev, p))

        scored.sort(key=lambda x: x[0])
        top = scored[: max(0, top_k_fractals)]
        raw_sims = [1.0 / (1.0 + max(0.0, d)) for d, _, _ in top]
        max_sim = max(raw_sims) if raw_sims else 0.0

        matches = []
        for (dist, ev, p), sim in zip(top, raw_sims):
            n = min(len(p["closePath"]), horizon) if horizon > 0 else len(p["closePath"])
            end_close = p["closePath"][n - 1] if n > 0 else 0.0
            matches.append(
                {
                    "eventId": ev["eventId"],
                    "eventTimestamp": ev["eventTimestamp"],
                    "eventTimeIso": datetime.datetime.fromtimestamp(
                        ev["eventTimestamp"], tz=datetime.timezone.utc
                    ).isoformat(),
                    "anchorPrice": ev.get("anchorPrice"),
                    "distance": round(dist, 6),
                    "similarity": round(sim, 6),
                    "alike_pct": round((sim / max_sim) * 100.0) if max_sim > 0 else 0,
                    "closed_bullish": end_close >= 0.0,
                    "horizon_close_return": round(end_close, 6),
                    "horizon_mfe": round(max(p["highPath"][:n]), 6) if n > 0 and p["highPath"] else None,
                    "horizon_mae": round(min(p["lowPath"][:n]), 6) if n > 0 and p["lowPath"] else None,
                }
            )

        subset_outcomes: dict[str, Any] = {}
        if top:
            for spec in LENS_OUTCOMES:
                pid = spec["id"]
                hits = sum(
                    1
                    for _, _, p in top
                    if _eval_lens_predicate(
                        pid,
                        p["closePath"],
                        p["highPath"],
                        p["lowPath"],
                        horizon,
                        current_step,
                        live_max_high,
                        live_min_low,
                    )
                )
                rate = hits / len(top)
                subset_outcomes[pid] = {
                    "name": spec["name"],
                    "count": hits,
                    "total": len(top),
                    "rate": round(rate, 4),
                    "pct": round(rate * 100.0, 1),
                }

        fractals = {
            "compared_steps": min(t_len, horizon) if horizon > 0 else t_len,
            "population_checked": len(scored),
            "bullish_count": sum(1 for m in matches if m["closed_bullish"]),
            "total": len(matches),
            "matches": matches,
            "subset_outcomes": subset_outcomes,
        }

    return {
        "target_event": target_ev,
        "target_current_state": {
            "steps_elapsed": current_step,
            "horizon_steps": horizon,
            "anchor_price": target_ev.get("anchorPrice") if target_ev else None,
            "current_return": round(cur_close, 6) if cur_close is not None else None,
            "current_mfe": round(live_max_high, 6) if live_max_high is not None else None,
            "current_mae": round(live_min_low, 6) if live_min_low is not None else None,
            "current_price": (
                round(float(target_ev["anchorPrice"]) * (1.0 + cur_close), 4)
                if target_ev and target_ev.get("anchorPrice") and cur_close is not None
                else None
            ),
        },
        "sample_size": len(sample),
        "total_events_fetched": len(events),
        "outcomes": outcomes,
        "distributions": distributions,
        "step_bands": step_bands,
        "fractals": fractals,
    }


# ---------------------------------------------------------------------------
# WebSocket State Normalizer (ported from `398757d77e106d6a.js`)
# ---------------------------------------------------------------------------


def normalize_ws_state(asset: str, timeframe: str, raw: dict[str, Any]) -> dict[str, Any] | None:
    if not raw or raw.get("p1Timestamp") is None or raw.get("p2Timestamp") is None:
        return None
    tf = _PIVOT_TF_ALIASES.get(timeframe.lower(), timeframe)
    is_bullish = bool(raw.get("isBullish"))
    p1_type = "low" if is_bullish else "high"
    p2_type = "high" if is_bullish else "low"
    period_start = int(raw["periodStart"])
    period_end = int(raw["periodEnd"])
    now_ms = int(time.time() * 1000)

    open_px = float(raw["openPrice"])
    high_px = float(raw["highPrice"])
    low_px = float(raw["lowPrice"])
    close_px = float(raw.get("closePrice") if raw.get("closePrice") is not None else open_px)

    return {
        "asset": asset.upper(),
        "timeframe": tf,
        "date": raw.get("date"),
        "candle_number": raw.get("candleNumber"),
        "period": {
            "start_ms": period_start,
            "end_ms": period_end,
            "start_utc": datetime.datetime.fromtimestamp(period_start / 1000.0, tz=datetime.timezone.utc).isoformat(),
            "end_utc": datetime.datetime.fromtimestamp(period_end / 1000.0, tz=datetime.timezone.utc).isoformat(),
            "minutes_elapsed": round(max(0.0, (now_ms - period_start) / 60000.0), 2),
        },
        "candle": {
            "open": open_px,
            "high": high_px,
            "low": low_px,
            "close": close_px,
            "volume": float(raw.get("volume", 0.0)),
        },
        "p1": {
            "type": p1_type,
            "price": low_px if p1_type == "low" else high_px,
            "timestamp_ms": int(raw["p1Timestamp"]),
            "timestamp_utc": datetime.datetime.fromtimestamp(
                int(raw["p1Timestamp"]) / 1000.0, tz=datetime.timezone.utc
            ).isoformat(),
            "minutes_since_start": float(raw.get("p1MinutesSinceStart", 0)),
            "delta_open": float(raw.get("p1DeltaOpen", 0.0)),
        },
        "p2": {
            "type": p2_type,
            "price": high_px if p2_type == "high" else low_px,
            "timestamp_ms": int(raw["p2Timestamp"]),
            "timestamp_utc": datetime.datetime.fromtimestamp(
                int(raw["p2Timestamp"]) / 1000.0, tz=datetime.timezone.utc
            ).isoformat(),
            "minutes_since_start": float(raw.get("p2MinutesSinceStart", 0)),
            "delta_open": float(raw.get("p2DeltaOpen", 0.0)),
        },
        "displacement": {
            "total_bullish": float(raw.get("bullishDisplacement", 0.0)),
            "total_bearish": float(raw.get("bearishDisplacement", 0.0)),
        },
        "is_bullish": is_bullish,
        "retracement": raw.get("retracement"),
        "latest_update_ms": raw.get("latestUpdate"),
    }


def _encode_context_filters(context_filters: dict[str, Any] | None) -> str | None:
    if not context_filters:
        return None
    parts: list[str] = []
    for code, spec in context_filters.items():
        if isinstance(spec, str):
            parts.append(f"{code}:{spec}")
            continue
        if isinstance(spec, dict) and "buckets" in spec:
            defs = CONTEXT_FILTER_BUCKETS.get(code, {})
            ranges = []
            for b in spec["buckets"]:
                if b in defs:
                    lo, hi = defs[b]
                    ranges.append(f"{'' if lo is None else lo}-{'' if hi is None else hi}")
            if ranges:
                parts.append(f"{code}:{','.join(ranges)}")
        elif isinstance(spec, dict) and "range" in spec:
            lo, hi = spec["range"]
            parts.append(f"{code}:{'' if lo is None else lo}-{'' if hi is None else hi}")
        elif isinstance(spec, (list, tuple)) and len(spec) == 2:
            lo, hi = spec
            parts.append(f"{code}:{'' if lo is None else lo}-{'' if hi is None else hi}")
    return ";".join(parts) if parts else None


# ---------------------------------------------------------------------------
# Synchronous REST Client (Python 3.10+ stdlib only)
# ---------------------------------------------------------------------------


class BrighterClient:
    """
    Synchronous Python REST client for Brighter Data (`https://api.brighterdata.com`).
    Zero third-party dependencies — uses pure stdlib Cognito SRP-6a authentication.
    """

    def __init__(
        self,
        email: str | None = None,
        password: str | None = None,
        id_token: str | None = None,
        *,
        api_url: str = API_URL,
        timeout: float = 20.0,
        auth: CognitoSRPAuth | None = None,
    ) -> None:
        self.api_url = api_url.rstrip("/")
        self.timeout = timeout
        self.auth = auth or CognitoSRPAuth(email=email, password=password, id_token=id_token, timeout=timeout)
        self._assets_cache: list[dict[str, Any]] | None = None
        self._sessions_cache: list[dict[str, Any]] | None = None
        self._event_types_cache: list[dict[str, Any]] | None = None

    def _request(
        self,
        path: str,
        params: dict[str, Any] | None = None,
        *,
        method: str = "GET",
        body: dict[str, Any] | None = None,
        retry_auth: bool = True,
    ) -> Any:
        url = f"{self.api_url}{path}"
        if params:
            cleaned = {k: str(v) for k, v in params.items() if v is not None and str(v) != ""}
            if cleaned:
                sep = "&" if "?" in url else "?"
                url = f"{url}{sep}{urllib.parse.urlencode(cleaned)}"

        token = self.auth.get_id_token()
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Origin": ORIGIN,
            "Referer": REFERER,
            "User-Agent": UA,
        }
        data = json.dumps(body).encode("utf-8") if body is not None else None
        req = urllib.request.Request(url, data=data, headers=headers, method=method)

        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                raw = resp.read().decode("utf-8")
                return json.loads(raw) if raw else {}
        except urllib.error.HTTPError as exc:
            err_body = exc.read().decode("utf-8", errors="replace")
            if exc.code == 401 and retry_auth:
                self.auth.get_id_token(force_refresh=True)
                return self._request(path, params=params, method=method, body=body, retry_auth=False)
            raise BrighterHTTPError(url, exc.code, err_body) from exc
        except urllib.error.URLError as exc:
            raise BrighterError(f"Network error on {url}: {exc}") from exc

    # --- Metadata & Account (Read-Only) ---

    def account_me(self) -> dict[str, Any]:
        return self._request("/account/me").get("data", {})

    def account_subscription(self) -> dict[str, Any]:
        return self._request("/account/subscription").get("data", {})

    def watchlist(self) -> list[str]:
        return self._request("/account/watchlist").get("data", [])

    def bookmarks(self) -> list[dict[str, Any]]:
        return self._request("/account/bookmarks").get("data", [])

    def drawings(self, asset: str, since: str | None = None) -> dict[str, Any]:
        sym = normalize_asset(asset)
        return self._request(f"/account/drawings/{urllib.parse.quote(sym)}", {"since": since}).get("data", {})

    def settings(self, prefix: str = "chart.") -> dict[str, Any]:
        return self._request("/account/settings", {"prefix": prefix}).get("data", {})

    def plans(self) -> list[dict[str, Any]]:
        return self._request("/plans").get("data", [])

    def assets(self, *, refresh: bool = False) -> list[dict[str, Any]]:
        if self._assets_cache is None or refresh:
            self._assets_cache = self._request("/assets").get("data", [])
        return list(self._assets_cache)

    def sessions(self, *, refresh: bool = False) -> list[dict[str, Any]]:
        if self._sessions_cache is None or refresh:
            self._sessions_cache = self._request("/sessions").get("data", [])
        return list(self._sessions_cache)

    def asset_start_date(self, asset: str, timeframe: str | None = None) -> str:
        sym = normalize_asset(asset)
        for row in self.assets():
            if str(row.get("assetSymbol", "")).upper() == sym:
                if timeframe:
                    by_tf = row.get("availableFromByTimeframe") or {}
                    if timeframe in by_tf:
                        return str(by_tf[timeframe])
                if row.get("availableFrom"):
                    return str(row["availableFrom"])
        return "2023-01-01"

    # --- Brighter Lens / Event Engine ---

    def event_types(self, *, refresh: bool = False) -> list[dict[str, Any]]:
        if self._event_types_cache is None or refresh:
            self._event_types_cache = self._request("/events/types").get("data", [])
        return list(self._event_types_cache)

    def candles(
        self,
        asset: str = "BTCUSD",
        timeframe: str = "1h",
        *,
        from_ts: int | None = None,
        to_ts: int | None = None,
        limit: int = 300,
    ) -> dict[str, Any]:
        sym = normalize_asset(asset)
        tf = resolve_candle_tf(timeframe)
        return self._request(
            f"/candles/{urllib.parse.quote(sym)}/{urllib.parse.quote(tf)}",
            {"from": from_ts, "to": to_ts, "limit": limit},
        ).get("data", {})

    def context_series(
        self,
        asset: str = "BTCUSD",
        attributes: Sequence[str] = ("rsi_14_1h", "rsi_14_1d"),
        *,
        from_ts: int | None = None,
        to_ts: int | None = None,
        lookback_days: int = 30,
    ) -> dict[str, Any]:
        sym = normalize_asset(asset)
        now_s = int(time.time())
        t_to = to_ts if to_ts is not None else now_s
        t_from = from_ts if from_ts is not None else (t_to - lookback_days * 86400)
        return self._request(
            f"/context/{urllib.parse.quote(sym)}/series",
            {"attributes": ",".join(attributes), "from": t_from, "to": t_to},
        ).get("data", {})

    def events(
        self,
        asset: str = "BTCUSD",
        *,
        from_ts: int | None = None,
        to_ts: int | None = None,
        types: Sequence[str] | str | None = None,
        active: bool | None = None,
        limit: int = 200,
    ) -> dict[str, Any]:
        sym = normalize_asset(asset)
        types_str = ",".join(types) if isinstance(types, (list, tuple)) else types
        params: dict[str, Any] = {
            "from": from_ts,
            "to": to_ts,
            "types": types_str,
            "active": "true" if active is True else ("false" if active is False else None),
        }
        if limit != 200:
            params["limit"] = limit
        return self._request(f"/events/{urllib.parse.quote(sym)}", params)

    def buffer_events(
        self,
        asset: str = "BTCUSD",
        event_type: str = "Daily Candle",
        *,
        limit: int = 500,
        before: int | None = None,
        since: int | None = None,
        include_event_id: int | None = None,
        context_filters: dict[str, Any] | None = None,
        day_type: str | None = None,
        day_of_week: Sequence[int] | str | None = None,
        session: str | None = None,
        candle_window_start: int | None = None,
        candle_window_end: int | None = None,
        week_of_month: str | None = None,
        week_of_quarter: str | None = None,
        macro_direction: Sequence[str] | None = None,
        macro_schedule: Sequence[str] | None = None,
        macro_strength: Sequence[str] | None = None,
    ) -> dict[str, Any]:
        sym = normalize_asset(asset)
        dow = ",".join(str(x) for x in day_of_week) if isinstance(day_of_week, (list, tuple)) else day_of_week
        params: dict[str, Any] = {
            "eventType": event_type,
            "limit": limit,
            "before": before,
            "since": since,
            "includeEventId": include_event_id,
            "contextFilters": _encode_context_filters(context_filters),
            "dayType": day_type,
            "dayOfWeek": dow,
            "session": session,
            "candleWindowStart": candle_window_start,
            "candleWindowEnd": candle_window_end,
            "weekOfMonth": week_of_month,
            "weekOfQuarter": week_of_quarter,
            "macroDirection": ",".join(macro_direction) if macro_direction else None,
            "macroSchedule": ",".join(macro_schedule) if macro_schedule else None,
            "macroStrength": ",".join(macro_strength) if macro_strength else None,
        }
        return self._request(f"/events/{urllib.parse.quote(sym)}/buffer/events", params)

    def buffer_paths(
        self,
        asset: str,
        event_ids: Sequence[int],
        *,
        max_steps: int = 1000,
        binary: bool = True,
        batch_size: int = 50,
        max_workers: int = 8,
    ) -> dict[str, Any]:
        """Fetch historical event price paths (`closePath`, `highPath`, `lowPath`, `volPath`) in parallel batches."""
        sym = normalize_asset(asset)
        ids = [int(x) for x in event_ids]
        if not ids:
            return {"paths": [], "pathGranularityMinutes": 5}

        batches = [ids[i : i + batch_size] for i in range(0, len(ids), batch_size)]

        def _fetch_batch(batch: list[int]) -> tuple[list[dict[str, Any]], int]:
            params: dict[str, Any] = {
                "eventIds": ",".join(str(i) for i in batch),
                "maxSteps": max_steps,
            }
            if binary:
                params["pathEncoding"] = "binary"
            resp = self._request(f"/events/{urllib.parse.quote(sym)}/buffer/paths", params)
            gran = int(resp.get("pathGranularityMinutes", 5))
            if resp.get("pathEncoding") == "binary" and "pathsBinary" in resp:
                return [decode_binary_path_entry(item) for item in resp["pathsBinary"]], gran
            return list(resp.get("paths") or []), gran

        # Pre-warm auth token before parallel fan-out so all threads share one JWT
        self.auth.get_id_token()
        all_paths: list[dict[str, Any]] = []
        granularity = 5
        if len(batches) == 1:
            all_paths, granularity = _fetch_batch(batches[0])
        else:
            with concurrent.futures.ThreadPoolExecutor(max_workers=min(max_workers, len(batches))) as pool:
                for batch_paths, gran in pool.map(_fetch_batch, batches):
                    all_paths.extend(batch_paths)
                    granularity = gran
        return {"paths": all_paths, "pathGranularityMinutes": granularity}

    def lens_snapshot(
        self,
        asset: str = "BTCUSD",
        event_type: str = "Daily Candle",
        *,
        limit: int = 200,
        max_steps: int = 1000,
        horizon_steps: int | None = None,
        target_event_id: int | None = None,
        top_k_fractals: int = 12,
        include_paths: bool = False,
        include_step_bands: bool = True,
        context_filters: dict[str, Any] | None = None,
        day_type: str | None = None,
        day_of_week: Sequence[int] | str | None = None,
        session: str | None = None,
    ) -> dict[str, Any]:
        """Full Brighter Lens analysis: fetches buffer events + binary paths and runs exact client analytics."""
        sym = normalize_asset(asset)
        buf = self.buffer_events(
            sym,
            event_type,
            limit=limit,
            context_filters=context_filters,
            day_type=day_type,
            day_of_week=day_of_week,
            session=session,
        )
        evs = list(buf.get("events") or [])
        ids = [int(e["eventId"]) for e in evs if e.get("eventId") is not None]
        path_res = self.buffer_paths(sym, ids, max_steps=max_steps, binary=True)
        analysis = analyze_lens_paths(
            evs,
            path_res["paths"],
            horizon_steps=horizon_steps,
            target_event_id=target_event_id,
            top_k_fractals=top_k_fractals,
            include_step_bands=include_step_bands,
        )
        out: dict[str, Any] = {
            "asset": sym,
            "event_type": event_type,
            "path_granularity_minutes": path_res["pathGranularityMinutes"],
            **analysis,
        }
        if include_paths:
            out["paths"] = path_res["paths"]
        return out

    # --- Pivots Engine ---

    def _build_pivot_filters(
        self,
        asset: str,
        timeframe: str,
        *,
        start_date: str | None = None,
        end_date: str | None = None,
        day_of_week: str | None = None,
        session: str | None = None,
        hour: str | None = None,
        four_hour_candle: str | None = None,
        auto_defaults: bool = True,
    ) -> dict[str, Any]:
        tf = resolve_pivot_tf(timeframe)
        filters: dict[str, Any] = {
            "start_date": start_date or (self.asset_start_date(asset, tf) if auto_defaults else "2023-01-01"),
            "end_date": end_date,
        }
        now = datetime.datetime.now(datetime.timezone.utc)
        if tf in ("1h", "4h", "S", "1D"):
            if day_of_week is not None:
                filters["day_of_week"] = day_of_week
            elif auto_defaults:
                filters["day_of_week"] = _default_day_of_week(now)
        if tf == "1h":
            filters["hour"] = hour or (f"{now.hour:02d}:00:00" if auto_defaults else None)
        elif tf == "4h":
            slot = 4 * (now.hour // 4)
            filters["four_hour_candle"] = four_hour_candle or (f"{slot:02d}:00:00" if auto_defaults else None)
        elif tf == "S":
            filters["session"] = session or (_current_ny_session(self.sessions(), now) if auto_defaults else None)
        return {k: v for k, v in filters.items() if v is not None and str(v) != ""}

    def pivot_time_buckets(
        self,
        asset: str = "BTCUSD",
        timeframe: str = "1D",
        **filter_kwargs: Any,
    ) -> dict[str, Any]:
        sym = normalize_asset(asset)
        tf = resolve_pivot_tf(timeframe)
        filters = self._build_pivot_filters(sym, tf, **filter_kwargs)
        return self._request(f"/pivots/{urllib.parse.quote(sym)}/{tf}/time/buckets", filters).get("data", {})

    def pivot_time_explorer(
        self,
        asset: str = "BTCUSD",
        timeframe: str = "1D",
        group_by: str | None = None,
        **filter_kwargs: Any,
    ) -> dict[str, Any]:
        sym = normalize_asset(asset)
        tf = resolve_pivot_tf(timeframe)
        filters = self._build_pivot_filters(sym, tf, **filter_kwargs)
        filters["group_by"] = group_by or DEFAULT_EXPLORER_GROUP_BY.get(tf, "hour")
        return self._request(f"/pivots/{urllib.parse.quote(sym)}/{tf}/time/explorer", filters).get("data", {})

    def pivot_distance_distributions(
        self,
        asset: str = "BTCUSD",
        timeframe: str = "1D",
        **filter_kwargs: Any,
    ) -> dict[str, Any]:
        sym = normalize_asset(asset)
        tf = resolve_pivot_tf(timeframe)
        filters = self._build_pivot_filters(sym, tf, **filter_kwargs)
        return self._request(
            f"/pivots/{urllib.parse.quote(sym)}/{tf}/distance/distributions", filters
        ).get("data", {})

    def pivot_overview(
        self,
        timeframe: str = "1D",
        assets: Sequence[str] | str | None = None,
        *,
        confidence: int | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        day_of_week: str | None = None,
        session: str | None = None,
        hour: str | None = None,
        four_hour_candle: str | None = None,
        auto_defaults: bool = False,
    ) -> dict[str, Any]:
        tf = resolve_pivot_tf(timeframe)
        asset_csv = (
            ",".join(normalize_asset(a) for a in assets)
            if isinstance(assets, (list, tuple))
            else (",".join(normalize_asset(a) for a in assets.split(",") if a.strip()) if isinstance(assets, str) else None)
        )
        ref_asset = asset_csv.split(",")[0] if asset_csv else "BTCUSD"
        params = self._build_pivot_filters(
            ref_asset,
            tf,
            start_date=start_date,
            end_date=end_date,
            day_of_week=day_of_week,
            session=session,
            hour=hour,
            four_hour_candle=four_hour_candle,
            auto_defaults=auto_defaults,
        )
        params["timeframe"] = tf
        if confidence is not None:
            params["confidence"] = confidence
        if asset_csv:
            params["assets"] = asset_csv
        return self._request("/pivots/overview", params)

    def pivot_summary(
        self,
        asset: str = "BTCUSD",
        timeframe: str = "1D",
        *,
        live_state: dict[str, Any] | None = None,
        include_raw_distributions: bool = False,
        **filter_kwargs: Any,
    ) -> dict[str, Any]:
        """Full Pivot Summary (Time + Distance + Confidence Targets Ladder + Overview)."""
        sym = normalize_asset(asset)
        tf = resolve_pivot_tf(timeframe)
        # Pre-warm auth + caches before parallel execution
        self.auth.get_id_token()
        if filter_kwargs.get("auto_defaults", True):
            self.assets()
            if tf == "S":
                self.sessions()

        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
            f_buckets = pool.submit(self.pivot_time_buckets, sym, tf, **filter_kwargs)
            f_explorer = pool.submit(self.pivot_time_explorer, sym, tf, None, **filter_kwargs)
            f_dist = pool.submit(self.pivot_distance_distributions, sym, tf, **filter_kwargs)
            f_ov = pool.submit(
                self.pivot_overview,
                tf,
                [sym],
                start_date=filter_kwargs.get("start_date"),
                end_date=filter_kwargs.get("end_date"),
                day_of_week=filter_kwargs.get("day_of_week"),
                session=filter_kwargs.get("session"),
                hour=filter_kwargs.get("hour"),
                four_hour_candle=filter_kwargs.get("four_hour_candle"),
                auto_defaults=filter_kwargs.get("auto_defaults", True),
            )
            buckets = f_buckets.result()
            explorer = f_explorer.result()
            dist = f_dist.result()
            overview_resp = f_ov.result()

        ov_rows = (overview_resp.get("data") or {}).get("data") or []
        ov_row = ov_rows[0] if ov_rows else None

        # Derive live_state fallback from latest candles if WS state not provided
        candle_for_targets = live_state["candle"] if live_state else None
        if candle_for_targets is None and tf in CANDLE_TIMEFRAMES:
            try:
                c_data = self.candles(sym, tf, limit=2)
                c_list = c_data.get("candles") or []
                if c_list:
                    candle_for_targets = c_list[-1]
            except BrighterError:
                pass

        result: dict[str, Any] = {
            "asset": sym,
            "timeframe": tf,
            "period": buckets.get("period") or dist.get("period"),
            "filters_applied": buckets.get("filters_applied") or dist.get("filters_applied") or {},
            "overview": ov_row,
            "live_state": live_state,
            "time_metrics": compute_pivot_time_metrics(buckets, live_state) if live_state else (ov_row or {}).get("time_metrics"),
            "distance_metrics": (
                compute_pivot_distance_metrics(dist, live_state)
                if live_state
                else {
                    **((ov_row or {}).get("distance_metrics") or {}),
                    "confidence_targets": compute_confidence_targets(dist.get("distributions") or {}, candle_for_targets),
                }
            ),
            "time_explorer": explorer.get("data") or [],
            "bucket_size_minutes": buckets.get("bucket_size_minutes"),
            "total_candles": buckets.get("total_candles") or dist.get("total_candles"),
        }
        if include_raw_distributions:
            result["time_buckets"] = buckets
            result["distance_distributions"] = dist
        return result

    def full_snapshot(
        self,
        asset: str = "BTCUSD",
        *,
        pivot_tf: str = "1D",
        candle_tf: str = "1h",
        candle_limit: int = 100,
        event_type: str = "Daily Candle",
        lens_limit: int = 150,
        top_k_fractals: int = 12,
        include_lens: bool = True,
        include_pivots: bool = True,
        include_candles: bool = True,
        include_context: bool = True,
        live_state: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Everything from Brighter Lens + Pivots for an asset in one parallel call."""
        sym = normalize_asset(asset)
        self.auth.get_id_token()
        self.assets()

        tasks: dict[str, concurrent.futures.Future[Any]] = {}
        with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:
            if include_lens:
                tasks["lens"] = pool.submit(
                    self.lens_snapshot,
                    sym,
                    event_type,
                    limit=lens_limit,
                    top_k_fractals=top_k_fractals,
                    include_step_bands=True,
                )
            if include_pivots:
                tasks["pivots"] = pool.submit(
                    self.pivot_summary,
                    sym,
                    pivot_tf,
                    live_state=live_state,
                    auto_defaults=True,
                )
            if include_candles:
                tasks["candles"] = pool.submit(self.candles, sym, candle_tf, limit=candle_limit)
            if include_context:
                tasks["context"] = pool.submit(self.context_series, sym, ("rsi_14_1h", "rsi_14_1d"), lookback_days=7)

        out: dict[str, Any] = {
            "asset": sym,
            "timestamp_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "pivot_timeframe": resolve_pivot_tf(pivot_tf),
            "event_type": event_type,
        }
        for key, fut in tasks.items():
            out[key] = fut.result()
        return out


# ---------------------------------------------------------------------------
# Real-Time WebSocket Engine (`wss://d3s1aak61z0hie.cloudfront.net/ws`)
# ---------------------------------------------------------------------------


class BrighterLiveFeed:
    """
    Async real-time WebSocket streaming engine for Brighter Data.

    Maintains hot in-memory P1/P2 state across all 6 timeframes (`1h`, `4h`, `S`, `1D`, `1W`, `1M`)
    for subscribed assets, auto-refreshes Cognito JWTs on reconnect, and broadcasts live events.
    """

    def __init__(
        self,
        assets: Sequence[str] | str = ("BTCUSD",),
        *,
        client: BrighterClient | None = None,
        ws_url: str = WS_URL,
        preload_pivots_tf: str | None = "1D",
    ) -> None:
        if isinstance(assets, str):
            self.assets: list[str] = [normalize_asset(a) for a in assets.split(",") if a.strip()]
        else:
            self.assets = [normalize_asset(a) for a in assets]
        self.client = client or BrighterClient()
        self.ws_url = ws_url
        self.preload_pivots_tf = resolve_pivot_tf(preload_pivots_tf) if preload_pivots_tf else None

        self._raw_states: dict[tuple[str, str], dict[str, Any]] = {}
        self._states: dict[tuple[str, str], dict[str, Any]] = {}
        self._last_tick: dict[str, dict[str, Any]] = {}
        self._cached_buckets: dict[tuple[str, str], dict[str, Any]] = {}
        self._cached_dists: dict[tuple[str, str], dict[str, Any]] = {}

        self._subscribers: list[asyncio.Queue[dict[str, Any]]] = []
        self._ws: Any = None
        self._task: asyncio.Task[Any] | None = None
        self._stop_event = asyncio.Event()
        self._first_snapshot_event = asyncio.Event()
        self._ready_event = asyncio.Event()

        self.connected: bool = False
        self.message_count: int = 0
        self.last_update_ms: int = 0

    async def __aenter__(self) -> BrighterLiveFeed:
        await self.start()
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.stop()

    async def start(self) -> None:
        if self._task is not None:
            return
        self._stop_event.clear()
        self._first_snapshot_event.clear()
        self._ready_event.clear()

        if self.preload_pivots_tf and self.assets:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, self._preload_distributions)

        self._task = asyncio.create_task(self._run_loop(), name="brighter-ws-feed")
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(self._first_snapshot_event.wait(), timeout=12.0)
        if self._first_snapshot_event.is_set() and not self._ready_event.is_set():
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(self._ready_event.wait(), timeout=2.0)

    def _preload_distributions(self) -> None:
        tf = self.preload_pivots_tf
        if not tf:
            return
        for sym in self.assets:
            try:
                self._cached_buckets[(sym, tf)] = self.client.pivot_time_buckets(sym, tf, auto_defaults=True)
                self._cached_dists[(sym, tf)] = self.client.pivot_distance_distributions(sym, tf, auto_defaults=True)
            except Exception:
                pass

    async def stop(self) -> None:
        self._stop_event.set()
        if self._ws is not None:
            with contextlib.suppress(Exception):
                await self._ws.close()
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None
        self.connected = False

    async def subscribe_asset(self, asset: str) -> None:
        sym = normalize_asset(asset)
        if sym not in self.assets:
            self.assets.append(sym)
        if self._ws is not None and self.connected:
            await self._ws.send(json.dumps({"action": "subscribe", "asset": sym}))

    def _emit(self, event: dict[str, Any]) -> None:
        for q in list(self._subscribers):
            if q.full():
                with contextlib.suppress(asyncio.QueueEmpty):
                    q.get_nowait()
            with contextlib.suppress(asyncio.QueueFull):
                q.put_nowait(event)

    def _handle_message(self, msg: dict[str, Any]) -> None:
        self.message_count += 1
        self.last_update_ms = int(time.time() * 1000)
        mtype = msg.get("type")

        if mtype == "snapshot":
            raw_tf = str(msg.get("timeframe", ""))
            tf = _PIVOT_TF_ALIASES.get(raw_tf.lower(), raw_tf)
            for st in msg.get("states") or []:
                sym = str(st.get("asset", "")).upper()
                if not sym:
                    continue
                self._raw_states[(sym, raw_tf)] = dict(st)
                norm = normalize_ws_state(sym, tf, st)
                if norm:
                    self._states[(sym, tf)] = norm
                    self._emit({"type": "snapshot", "asset": sym, "timeframe": tf, "state": norm})
            if self._states:
                self._first_snapshot_event.set()
                expected = {(a, tf) for a in self.assets for tf in PIVOT_TIMEFRAMES}
                if expected.issubset(self._states.keys()):
                    self._ready_event.set()

        elif mtype == "delta":
            tick = msg.get("tick")
            if tick:
                t_sym = str(tick.get("asset") or tick.get("symbol") or "").upper()
                if t_sym:
                    self._last_tick[t_sym] = tick
                self._emit({"type": "tick", "tick": tick})

            for upd in msg.get("updates") or []:
                sym = str(upd.get("asset", "")).upper()
                raw_tf = str(upd.get("timeframe") or msg.get("timeframe") or "")
                if not sym or not raw_tf:
                    continue
                base = self._raw_states.get((sym, raw_tf))
                if base is None:
                    continue
                merged = {**base, **upd}
                self._raw_states[(sym, raw_tf)] = merged
                tf = _PIVOT_TF_ALIASES.get(raw_tf.lower(), raw_tf)
                norm = normalize_ws_state(sym, tf, merged)
                if norm:
                    self._states[(sym, tf)] = norm
                    self._emit({"type": "delta", "asset": sym, "timeframe": tf, "state": norm})

    async def _run_loop(self) -> None:
        try:
            import websockets
        except ImportError as exc:
            raise BrighterError("Install `websockets>=12.0` to use BrighterLiveFeed.") from exc

        loop = asyncio.get_running_loop()
        attempt = 0
        while not self._stop_event.is_set():
            try:
                force_refresh = attempt > 0
                token = await loop.run_in_executor(None, self.client.auth.get_id_token, force_refresh)
                url = f"{self.ws_url}?token={urllib.parse.quote(token, safe='')}"
                async with websockets.connect(
                    url,
                    origin=ORIGIN,
                    additional_headers={"User-Agent": UA},
                    ping_interval=25,
                    ping_timeout=20,
                    max_size=16 * 1024 * 1024,
                ) as ws:
                    self._ws = ws
                    self.connected = True
                    attempt = 0
                    for sym in list(self.assets):
                        await ws.send(json.dumps({"action": "subscribe", "asset": sym}))
                    async for raw in ws:
                        if self._stop_event.is_set():
                            break
                        try:
                            self._handle_message(json.loads(raw))
                        except json.JSONDecodeError:
                            continue
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.connected = False
                self._ws = None
                if self._stop_event.is_set():
                    break
                attempt += 1
                delay = min(30.0, (1.0 * (2 ** min(attempt - 1, 5))) + random.uniform(0.0, 0.5))
                self._emit({"type": "reconnect", "attempt": attempt, "delay_s": round(delay, 2), "error": str(exc)})
                try:
                    await asyncio.wait_for(self._stop_event.wait(), timeout=delay)
                except asyncio.TimeoutError:
                    continue
            finally:
                self.connected = False
                self._ws = None

    async def stream(self, max_queue: int = 256) -> AsyncIterator[dict[str, Any]]:
        q: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=max_queue)
        self._subscribers.append(q)
        try:
            while not self._stop_event.is_set():
                ev = await q.get()
                yield ev
        finally:
            if q in self._subscribers:
                self._subscribers.remove(q)

    def get_state(self, asset: str, timeframe: str) -> dict[str, Any] | None:
        return self._states.get((normalize_asset(asset), resolve_pivot_tf(timeframe)))

    def snapshot(self, asset: str | None = None, timeframe: str | None = None) -> dict[str, Any]:
        target_asset = normalize_asset(asset) if asset else (self.assets[0] if self.assets else "BTCUSD")
        states_by_tf: dict[str, Any] = {}
        for (sym, tf), st in sorted(self._states.items()):
            if sym == target_asset and (timeframe is None or tf == resolve_pivot_tf(timeframe)):
                entry: dict[str, Any] = dict(st)
                buckets = self._cached_buckets.get((sym, tf))
                dists = self._cached_dists.get((sym, tf))
                if buckets:
                    entry["time_metrics"] = compute_pivot_time_metrics(buckets, st)
                if dists:
                    entry["distance_metrics"] = compute_pivot_distance_metrics(dists, st)
                states_by_tf[tf] = entry

        return {
            "asset": target_asset,
            "connected": self.connected,
            "subscribed_assets": list(self.assets),
            "message_count": self.message_count,
            "last_update_ms": self.last_update_ms,
            "last_tick": self._last_tick.get(target_asset),
            "timeframes": states_by_tf,
        }


# ---------------------------------------------------------------------------
# CLI Formatter & Entrypoint
# ---------------------------------------------------------------------------


def _fmt_price(v: float | None) -> str:
    if v is None:
        return "—"
    if abs(v) >= 1000:
        return f"${v:,.2f}"
    if abs(v) >= 1:
        return f"${v:,.4f}"
    return f"${v:,.6f}"


def _print_table(snap: dict[str, Any]) -> None:
    sym = snap["asset"]
    print(f"=== Brighter Data Snapshot: {sym} ({snap['timestamp_utc']}) ===")

    piv = snap.get("pivots")
    if piv:
        ov = piv.get("overview") or {}
        tm = piv.get("time_metrics") or {}
        dm = piv.get("distance_metrics") or {}
        print(f"\n[Pivots {piv['timeframe']}] direction={ov.get('direction', '?')}  total_candles={piv.get('total_candles')}")
        if tm:
            print(
                f"  Time Lens:     P1 flip risk={tm.get('p1_flip_risk_pct')}%  "
                f"P2s later={tm.get('p2s_later_pct')}%  warning={tm.get('p1_warning')}  n={tm.get('data_points')}"
            )
        if dm:
            print(
                f"  Distance Lens: P1 flip risk={dm.get('p1_flip_risk_pct')}%  "
                f"P2s further={dm.get('p2s_further_pct')}%  warning={dm.get('p1_warning')}  n={dm.get('data_points')}"
            )
            ct = dm.get("confidence_targets") or {}
            bull = ct.get("bullish") or []
            bear = ct.get("bearish") or []
            if bull and bear:
                print("  Confidence Ladder (90% / 70% / 50% / 20%):")
                for cft in (90, 70, 50, 20):
                    b = next((r for r in bull if r["confidence"] == cft), None)
                    s = next((r for r in bear if r["confidence"] == cft), None)
                    if b and s:
                        print(
                            f"    {cft:2d}%  Bull P1={_fmt_price(b['p1_price'])} ({b['p1_pct_display']:+.2f}%) "
                            f"P2={_fmt_price(b['p2_price'])} ({b['p2_pct_display']:+.2f}%) | "
                            f"Bear P1={_fmt_price(s['p1_price'])} ({s['p1_pct_display']:+.2f}%) "
                            f"P2={_fmt_price(s['p2_price'])} ({s['p2_pct_display']:+.2f}%)"
                        )

    lens = snap.get("lens")
    if lens:
        cs = lens.get("target_current_state") or {}
        print(
            f"\n[Brighter Lens: {lens['event_type']}] sample={lens['sample_size']}  "
            f"anchor={_fmt_price(cs.get('anchor_price'))}  current={_fmt_price(cs.get('current_price'))} "
            f"({(cs.get('current_return') or 0) * 100:+.2f}%)  step={cs.get('steps_elapsed')}/{cs.get('horizon_steps')}"
        )
        for oid in ("closed_bullish", "closed_bearish", "low_first", "high_first", "took_out_high_later", "took_out_low_later"):
            o = (lens.get("outcomes") or {}).get(oid)
            if o:
                print(f"  {o['name']:28s}: {o['pct']:5.1f}% ({o['count']}/{o['total']})")
        fr = lens.get("fractals") or {}
        if fr.get("matches"):
            print(
                f"  Top {fr['total']} Fractal Analogs ({fr['bullish_count']}/{fr['total']} bullish, "
                f"checked {fr['population_checked']} over {fr['compared_steps']} steps):"
            )
            for m in fr["matches"][:5]:
                side = "BULL" if m["closed_bullish"] else "BEAR"
                print(
                    f"    {m['eventTimeIso'][:10]}  alike={m['alike_pct']:3d}%  {side}  "
                    f"close={m['horizon_close_return']*100:+.2f}%  dist={m['distance']:.5f}"
                )


async def _run_stream_cli(asset: str, tf: str) -> None:
    async with BrighterLiveFeed(assets=[asset], preload_pivots_tf=tf) as feed:
        snap = feed.snapshot(asset=asset)
        print(f"Connected to Brighter Data WS for {asset}. Timeframes loaded: {list(snap['timeframes'].keys())}")
        async for ev in feed.stream():
            print(json.dumps(ev))


def main(argv: Sequence[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Brighter Data SDK & CLI (Brighter Lens + Pivots + Live WS)")
    ap.add_argument("--asset", default=os.environ.get("BD_ASSET", "BTCUSD"))
    ap.add_argument("--tf", default=os.environ.get("BD_TF", "1D"), help="Pivot timeframe: 1h, 4h, S, 1D, 1W, 1M")
    ap.add_argument("--event-type", default="Daily Candle", help="Brighter Lens event type name")
    ap.add_argument("--limit", type=int, default=150, help="Historical events to analyze in Brighter Lens")
    ap.add_argument("--lens-only", action="store_true", help="Fetch Brighter Lens analysis only")
    ap.add_argument("--pivots-only", action="store_true", help="Fetch Pivots summary only")
    ap.add_argument("--stream", action="store_true", help="Stream live WebSocket updates as JSON lines")
    ap.add_argument("--json", action="store_true", help="Print full JSON output")
    args = ap.parse_args(argv)

    if args.stream:
        asyncio.run(_run_stream_cli(args.asset, args.tf))
        return 0

    client = BrighterClient()
    if args.lens_only:
        data = client.lens_snapshot(args.asset, args.event_type, limit=args.limit)
        print(json.dumps(data, indent=2))
        return 0
    if args.pivots_only:
        data = client.pivot_summary(args.asset, args.tf)
        print(json.dumps(data, indent=2))
        return 0

    snap = client.full_snapshot(
        args.asset,
        pivot_tf=args.tf,
        event_type=args.event_type,
        lens_limit=args.limit,
    )
    if args.json:
        print(json.dumps(snap, indent=2))
    else:
        _print_table(snap)
    return 0


if __name__ == "__main__":
    sys.exit(main())
