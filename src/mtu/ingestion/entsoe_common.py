"""Shared helpers for ENTSO-E Transparency Platform ingestion.

Covers token loading, URL/parameter building, chunked HTTP fetch with
retries, and common constants. Kept deliberately minimal: one function
per concern, stdlib XML, no new third-party dependencies (we already
use `requests` in the OMIE scripts).

References:
    - Detailed Data Descriptions v3r4 (docs/entsoe/entsoe_v3r4.pdf), §3.13.
    - RESTful API parameter shapes cross-checked against the EnergieID
      entsoe-py wrapper (https://github.com/EnergieID/entsoe-py).
"""

from __future__ import annotations

import io
import os
import time
import zipfile
from pathlib import Path
from typing import Iterable

import requests

# ---- Constants ---------------------------------------------------------

API_URL = "https://web-api.tp.entsoe.eu/api"

# Spain — single REE control area / scheduling area / bidding zone.
SPAIN_EIC = "10YES-REE------0"

# Verified documentType codes (entsoe-py + API guide).
DOC_TYPE = {
    "imbalance_prices":       "A85",   # §3.13.3 Imbalance prices (TR 17.1.g)
    "imbalance_volumes":      "A86",   # §3.13.3 Total imbalance volume (TR 17.1.h)
    "activated_prices":       "A84",   # §3.13.2 Activated balancing energy prices (TR 17.1.f)
    "aggregated_bids":        "A24",   # §3.13.2 Aggregated balancing energy bids (GL EB 12.3.e)
    "current_balancing_state": "A86",  # §3.13.3 (with businessType=B33)
    "wind_solar_forecast":    "A69",   # §3.3.1 Day-ahead forecast of wind & solar
    "installed_capacity":     "A68",   # §14.1.A Installed generation capacity aggregated
    "wind_solar_actual":      "A75",   # §16.1.B Actual generation per production type
}

# processType A16 = Realised. Default for activation/imbalance documents.
PROCESS_TYPE_REALISED = "A16"

# processType A01 = Day-ahead. Used for forecast documents (A69, A71).
PROCESS_TYPE_DAY_AHEAD = "A01"

# processType A18 = Intraday total. Used for the current (rolling) wind &
# solar forecast: TSO republishes every MTU, API returns the most recent
# forecast issued before each MTU in the queried window.
PROCESS_TYPE_INTRADAY = "A18"

# processType A33 = Year ahead. Used for installed capacity (A68), which
# TSOs publish annually with the reference year's 01-01T00:00 timestamp.
PROCESS_TYPE_YEAR_AHEAD = "A33"

# businessType B33 = the discriminator for current balancing state vs
# total imbalance volume (both carry documentType A86).
BUSINESS_TYPE_CURRENT_STATE = "B33"

USER_AGENT = "mtu15-thesis-entsoe-sync/1.0"


# ---- Token loading -----------------------------------------------------

def load_token(env_path: Path | None = None) -> str:
    """Return ENTSOE_TOKEN from environment or project-root `.env`.

    Precedence: `os.environ` -> `.env` file. The `.env` file is a plain
    `KEY=value` text file (gitignored). Inline comments and surrounding
    quotes are tolerated.
    """
    token = os.environ.get("ENTSOE_TOKEN")
    if token:
        return token.strip()

    if env_path is None:
        env_path = Path(__file__).resolve().parents[3] / ".env"

    if env_path.exists():
        for raw in env_path.read_text(encoding="utf-8").splitlines():
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            if key.strip() == "ENTSOE_TOKEN":
                return value.split("#", 1)[0].strip().strip('"').strip("'")

    raise RuntimeError(
        "ENTSOE_TOKEN not found. Set it in the environment or in the "
        "project-root .env file."
    )


# ---- Time helpers ------------------------------------------------------

def format_period(ts: str) -> str:
    """Convert an ISO timestamp to ENTSO-E period format `YYYYMMDDHHMM`.

    Accepts `YYYY-MM-DD` (assumed 00:00 UTC) or `YYYY-MM-DD HH:MM`.
    ENTSO-E interprets all period timestamps in UTC.
    """
    s = ts.strip().replace("T", " ")
    if len(s) == 10:
        return s.replace("-", "") + "0000"
    if len(s) == 16:
        return s.replace("-", "").replace(":", "").replace(" ", "")
    raise ValueError(f"Unrecognised timestamp: {ts!r}")


def month_chunks(start_ym: str, end_ym: str) -> Iterable[tuple[str, str, str]]:
    """Yield `(yyyymm, period_start, period_end)` for an inclusive monthly range.

    `period_end` is the first instant of the *next* month, matching
    ENTSO-E's half-open interval convention.
    """
    import pandas as pd

    start = pd.Period(start_ym, freq="M")
    end = pd.Period(end_ym, freq="M")
    if end < start:
        raise ValueError("end-month cannot be before start-month")

    cur = start
    while cur <= end:
        ps = cur.to_timestamp()
        pe = (cur + 1).to_timestamp()
        yield (
            cur.strftime("%Y%m"),
            ps.strftime("%Y-%m-%d"),
            pe.strftime("%Y-%m-%d"),
        )
        cur += 1


def year_chunks(start_year: int, end_year: int) -> Iterable[tuple[str, str, str]]:
    """Yield `(yyyy, period_start, period_end)` for an inclusive yearly range.

    `period_end` is the first instant of the *next* year (half-open).
    Used by A68 (installed capacity), which is published annually.
    """
    if end_year < start_year:
        raise ValueError("end-year cannot be before start-year")
    for y in range(start_year, end_year + 1):
        yield (
            f"{y:04d}",
            f"{y:04d}-01-01",
            f"{y + 1:04d}-01-01",
        )


# ---- HTTP fetch --------------------------------------------------------

def fetch_document(
    *,
    session: requests.Session,
    token: str,
    params: dict,
    timeout: int = 300,
    max_retries: int = 4,
) -> tuple[bytes, str]:
    """GET a document from the ENTSO-E REST API; return XML bytes.

    ENTSO-E returns plain XML for short windows and a ZIP containing one
    or more XMLs for longer windows — we transparently unwrap the ZIP
    and concatenate multiple XMLs into a single synthetic document.

    Returns `(xml_bytes, status)` where status is:
        - "ok"       : HTTP 200, document body
        - "empty"    : HTTP 400 "No matching data found" (verbatim body)

    Raises on any other non-2xx after retries.
    """
    q = {"securityToken": token, **params}
    last_err: str | None = None

    for attempt in range(1, max_retries + 1):
        try:
            r = session.get(API_URL, params=q, timeout=timeout)
        except requests.RequestException as e:
            last_err = f"request error: {e}"
            _backoff(attempt, max_retries, last_err)
            continue

        if r.status_code == 200:
            return _unwrap_xml(r.content), "ok"

        if r.status_code == 400 and b"No matching data found" in r.content:
            return r.content, "empty"

        if 500 <= r.status_code < 600 and attempt < max_retries:
            last_err = f"HTTP {r.status_code}"
            _backoff(attempt, max_retries, last_err)
            continue

        r.raise_for_status()
        raise RuntimeError(
            f"Unexpected HTTP {r.status_code}: {r.text[:300]}"
        )

    raise RuntimeError(f"Exceeded retries ({max_retries}): {last_err}")


def _unwrap_xml(body: bytes) -> bytes:
    """Return XML bytes whether the body is raw XML or a ZIP of XMLs.

    When the ZIP contains multiple XMLs (ENTSO-E can split long windows
    into chunks), concatenate them inside a synthetic wrapper root so
    downstream parsers can iterate `TimeSeries` uniformly.
    """
    if not body.startswith(b"PK\x03\x04"):
        return body

    parts: list[bytes] = []
    with zipfile.ZipFile(io.BytesIO(body), "r") as zf:
        for info in zf.infolist():
            if info.is_dir() or not info.filename.lower().endswith(".xml"):
                continue
            parts.append(zf.read(info.filename))

    if not parts:
        raise RuntimeError("ENTSO-E ZIP did not contain any XML member")
    if len(parts) == 1:
        return parts[0]

    # Strip each part's XML declaration and wrap in a synthetic root so
    # the concatenated payload stays well-formed.
    stripped = []
    for p in parts:
        s = p.lstrip()
        if s.startswith(b"<?xml"):
            s = s.split(b"?>", 1)[1].lstrip()
        stripped.append(s)
    merged = (
        b"<?xml version='1.0' encoding='UTF-8'?>\n"
        b"<entsoe_merged>\n" + b"\n".join(stripped) + b"\n</entsoe_merged>\n"
    )
    return merged


def _backoff(attempt: int, max_retries: int, reason: str) -> None:
    delay = min(30 * attempt, 300)
    print(
        f"[WARN ENTSOE] {reason} — attempt {attempt}/{max_retries}, "
        f"sleeping {delay}s"
    )
    time.sleep(delay)
