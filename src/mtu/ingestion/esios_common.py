"""Shared helpers for ESIOS (REE) public-archive ingestion.

ESIOS — Sistema de Información del Operador del Sistema — is REE's
data platform. The public *archives* endpoint serves bulk historical
files without authentication; per-subject (BRP-private) archives
require BRP-role registration which we do not have.

URL pattern:
    https://api.esios.ree.es/archives/{archive_id}/download
        ?date_type=datos
        &start_date={ISO}
        &end_date={ISO}
        &locale=es

Each archive_id corresponds to a document family. For monthly archives
the request is typically for one calendar month at a time; the response
is a ZIP containing many inner files (one per inner concept, e.g. for
liquicomun: impdsvqh, endrozrqh, etc.).

This module mirrors the structure of `entsoe_common.py`. There is no
auth token — public archives are open. If REE introduces auth in the
future, add an env-var loader here.

References:
    - Public list of archives:
      https://www.esios.ree.es/es/descargas
    - Archive 3 = A2_liquicomun (monthly settlement bundle)

Naming convention for downloaded files (chosen for this project):

    data/raw/esios/{family}/{yyyymm}/<source-filename>

This preserves the source archive grouping per month, allowing
incremental syncs without conflicts.
"""

from __future__ import annotations

import io
import time
import zipfile
from pathlib import Path
from typing import Iterable

import requests

# ---- Constants ---------------------------------------------------------

API_URL = "https://api.esios.ree.es/archives/{archive_id}/download"

# Verified archive IDs (extend as new families are added).
ARCHIVES = {
    "liquicomun":            3,    # A2_liquicomun monthly settlement ZIP
    # Tier 2 — scaffolded but not synced yet:
    # "totalasigsec":         (lookup),
    # "curvas_ofertas_afrr":  (lookup),
    # "totalrp48prec":        (lookup),
    # "indisponibilidades":   (lookup),
}

USER_AGENT = "mtu15-thesis-esios-sync/1.0"


# ---- URL helpers -------------------------------------------------------

def archive_url(archive_id: int) -> str:
    """Format the ESIOS archive download URL for a given archive ID."""
    return API_URL.format(archive_id=archive_id)


def month_chunks(start_ym: str, end_ym: str) -> Iterable[tuple[str, str, str]]:
    """Yield (yyyymm, period_start_iso, period_end_iso) per month inclusive.

    period_start = first day of month at 00:00 UTC.
    period_end   = last day of month at 23:59 UTC.

    ESIOS archives use ISO-8601 timestamps with timezone offset.
    """
    import pandas as pd

    start = pd.Period(start_ym, freq="M")
    end = pd.Period(end_ym, freq="M")
    if end < start:
        raise ValueError("end-month cannot be before start-month")

    cur = start
    while cur <= end:
        ps = cur.to_timestamp().strftime("%Y-%m-%dT00:00:00+00:00")
        pe = (cur.to_timestamp() + pd.offsets.MonthEnd(0)).strftime(
            "%Y-%m-%dT23:59:59+00:00"
        )
        yield cur.strftime("%Y%m"), ps, pe
        cur += 1


# ---- HTTP fetch --------------------------------------------------------

def fetch_archive(
    *,
    session: requests.Session,
    archive_id: int,
    start_iso: str,
    end_iso: str,
    timeout: int = 300,
    max_retries: int = 4,
) -> tuple[bytes, str]:
    """Download an archive payload (ZIP or single file) from ESIOS.

    Returns `(body, status)` where status is "ok" or "empty".
    """
    url = archive_url(archive_id)
    params = {
        "date_type": "datos",
        "start_date": start_iso,
        "end_date": end_iso,
        "locale": "es",
    }
    last_err: str | None = None

    for attempt in range(1, max_retries + 1):
        try:
            r = session.get(url, params=params, timeout=timeout)
        except requests.RequestException as e:
            last_err = f"request error: {e}"
            _backoff(attempt, max_retries, last_err)
            continue

        if r.status_code == 200:
            # ESIOS returns 200 with very small body when empty
            if len(r.content) < 200:
                return r.content, "empty"
            return r.content, "ok"

        if r.status_code == 404:
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


def extract_zip(payload: bytes, out_dir: Path) -> list[Path]:
    """If payload is a ZIP, extract members into `out_dir` and return
    list of extracted file paths. If not a ZIP, write the raw payload
    as a single file and return its path.

    `out_dir` must already exist.
    """
    if not payload.startswith(b"PK\x03\x04"):
        raw_path = out_dir / "payload.bin"
        raw_path.write_bytes(payload)
        return [raw_path]

    paths: list[Path] = []
    with zipfile.ZipFile(io.BytesIO(payload), "r") as zf:
        for info in zf.infolist():
            if info.is_dir():
                continue
            target = out_dir / info.filename
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(zf.read(info.filename))
            paths.append(target)
    return paths


def _backoff(attempt: int, max_retries: int, reason: str) -> None:
    delay = min(30 * attempt, 300)
    print(
        f"[WARN ESIOS] {reason} — attempt {attempt}/{max_retries}, "
        f"sleeping {delay}s"
    )
    time.sleep(delay)
