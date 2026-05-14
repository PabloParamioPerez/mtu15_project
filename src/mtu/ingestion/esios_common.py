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
#
# Settlement vintages — REE publishes liquicomun in two stages:
#   A2 (provisional) — released M+1, restated until C2 is final
#   C2 (definitive) — released M+3, canonical for historical analysis
#
# For thesis use we take C2 by default and only fall back to A2 for
# the most recent month(s) where C2 is not yet published.
ARCHIVES = {
    "liquicomun_a2":         3,    # A2 provisional settlement (latest month)
    "liquicomun_c2":         8,    # C2 definitive settlement (historical)
    # Tier 2 — secondary/tertiary regulation + technical-restriction prices.
    # Verified active 2026-04-27 against the public archive endpoint.
    # Empty-archive note: id=27 (totalrp48prec) returns an empty ZIP for any
    # date range; deprecated. Use 28 (totalrp48preccierre, closure version)
    # which is what we want for stable historical analysis anyway.
    "liquicierre":           17,   # aFRR settlement detail (2015 → 2024-12-03)
    "totalrp48preccierre":   28,   # technical-restriction (RZ) closure prices (2015 → now)
    "ree_balancing_bids":   181,   # mFRR bid-level (2022-05-24 → 2024-12-10), DAILY chunks
    "liquicierresrs":       203,   # aFRR settlement (post-ISP15 format, 2024-11-22 → now)
    "curvas_ofertas_afrr":  234,   # aFRR offer curves (2024-11-20 → now), DAILY chunks, .xls payload
    "indisponibilidades":   105,   # generation/consumption-unit outages, snapshot per day, .xls payload
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


def day_chunks(start_date: str, end_date: str) -> Iterable[tuple[str, str, str]]:
    """Yield (yyyymmdd, period_start_iso, period_end_iso) per day inclusive.

    Used for archives that exceed the CDN per-request size budget at monthly
    granularity (e.g. ree_balancing_bids id=181, curvas_ofertas_afrr id=234).
    """
    import pandas as pd

    start = pd.Timestamp(start_date)
    end = pd.Timestamp(end_date)
    if end < start:
        raise ValueError("end-date cannot be before start-date")

    cur = start
    while cur <= end:
        ps = cur.strftime("%Y-%m-%dT00:00:00+00:00")
        pe = cur.strftime("%Y-%m-%dT23:59:59+00:00")
        yield cur.strftime("%Y%m%d"), ps, pe
        cur += pd.Timedelta(days=1)


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

    Handles the ESIOS S3-redirect quirk: large archives respond with HTTP 307
    pointing to a pre-signed S3 URL with AWS4-HMAC-SHA256 signature.
    Re-sending the API key on that redirect leg invalidates the AWS signature
    (returns 403 SignatureDoesNotMatch). We follow the redirect manually with
    a plain `requests.get` (no auth headers).
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
            # allow_redirects=False so we can manually re-fetch without auth headers
            r = session.get(url, params=params, timeout=timeout, allow_redirects=False)
        except requests.RequestException as e:
            last_err = f"request error: {e}"
            _backoff(attempt, max_retries, last_err)
            continue

        # 307 redirect to S3 — fetch Location *without* re-sending the API key
        if r.status_code in (301, 302, 303, 307, 308):
            location = r.headers.get("Location") or r.headers.get("location")
            if not location:
                raise RuntimeError(f"redirect without Location header (HTTP {r.status_code})")
            try:
                r2 = requests.get(location, timeout=timeout)  # no session, no auth headers
            except requests.RequestException as e:
                last_err = f"S3 follow-up error: {e}"
                _backoff(attempt, max_retries, last_err)
                continue
            if r2.status_code == 200:
                if len(r2.content) < 200:
                    return r2.content, "empty"
                return r2.content, "ok"
            if r2.status_code == 404:
                return r2.content, "empty"
            r2.raise_for_status()
            raise RuntimeError(f"S3 unexpected HTTP {r2.status_code}: {r2.text[:300]}")

        if r.status_code == 200:
            # ESIOS sometimes returns 200 directly (no redirect) for small payloads
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


# ---- Generic single-archive sync loop ---------------------------------

def sync_archive_loop(
    *,
    archive_id: int,
    chunks: Iterable[tuple[str, str, str]],
    raw_root: Path,
    file_stem: str,
    market: str,
    category: str,
    file_family: str,
    manifest_csv: Path,
    session: requests.Session,
    timeout: int = 600,
    max_retries: int = 4,
    overwrite: bool = False,
    extract: bool = True,
    is_zip_hint: bool | None = None,
    refresh_recent: int = 0,
    payload_suffix: str = ".zip",
) -> dict[str, int]:
    """Generic per-chunk sync loop for a single ESIOS archive.

    Layout: raw_root/<chunk_key>/<file_stem>_<chunk_key><payload_suffix>
    plus optional extracted/ subdir if extract=True.

    `is_zip_hint`: if False, the payload is written verbatim without ZIP
    extraction (used for archives that return non-ZIP payloads, e.g. .xls).
    If True or None (default), call extract_zip which auto-detects.

    Returns a dict with keys downloaded / skipped / empty.
    """
    from mtu.parsing.omie_common import (
        append_csv_row,
        ensure_dir,
        sha256_file,
        utc_now_iso,
    )

    ensure_dir(raw_root)
    chunks_list = list(chunks)
    total = len(chunks_list)
    totals = {"downloaded": 0, "skipped": 0, "empty": 0}

    for i, (key, ps_iso, pe_iso) in enumerate(chunks_list):
        chunk_dir = raw_root / key
        ensure_dir(chunk_dir)

        # Existing-file pattern check
        pattern = f"{file_stem}_{key}*"
        existing = list(chunk_dir.glob(pattern))
        is_refresh = (refresh_recent > 0) and (i >= total - refresh_recent)
        if existing and not overwrite and not is_refresh:
            print(f"[SKIP]    {existing[0].name}")
            totals["skipped"] += 1
            continue
        if existing and is_refresh and not overwrite:
            print(f"[REFRESH] {existing[0].name}")

        body, status = fetch_archive(
            session=session,
            archive_id=archive_id,
            start_iso=ps_iso,
            end_iso=pe_iso,
            timeout=timeout,
            max_retries=max_retries,
        )

        if status == "empty":
            print(f"[EMPTY]   {key} (no payload)")
            totals["empty"] += 1
            try:
                chunk_dir.rmdir()
            except OSError:
                pass
            continue

        # Auto-detect ZIP magic if no explicit hint
        is_zip = body.startswith(b"PK\x03\x04") if is_zip_hint is None else is_zip_hint
        suffix = payload_suffix if is_zip else _infer_suffix(body, payload_suffix)
        out_name = f"{file_stem}_{key}{suffix}"
        out_path = chunk_dir / out_name

        tmp = out_path.with_suffix(out_path.suffix + ".part")
        tmp.write_bytes(body)
        tmp.replace(out_path)

        n_inner = 0
        if extract and is_zip:
            extracted_dir = chunk_dir / "extracted"
            ensure_dir(extracted_dir)
            extracted_paths = extract_zip(body, extracted_dir)
            n_inner = len(extracted_paths)

        append_csv_row(
            manifest_csv,
            {
                "downloaded_at": utc_now_iso(),
                "source_url": (
                    f"esios:archive_{archive_id}"
                    f"?start_date={ps_iso}&end_date={pe_iso}"
                ),
                "market": market,
                "category": category,
                "file_family": file_family,
                "filename": out_name,
                "size_bytes": out_path.stat().st_size,
                "sha256": sha256_file(out_path),
                "is_zip": is_zip,
                "file_date": _key_to_iso_date(key),
                "version_suffix": "",
                "notes": (
                    f"esios_public_archive;status={status};"
                    f"n_inner={n_inner}"
                ),
            },
        )

        totals["downloaded"] += 1
        print(
            f"[OK]      {out_name} "
            f"({out_path.stat().st_size/1e6:.2f} MB, {n_inner} inner files)"
        )

    return totals


def _infer_suffix(body: bytes, default: str) -> str:
    """Infer a sensible file extension from payload magic bytes."""
    if body.startswith(b"PK\x03\x04"):
        return ".zip"
    if body.startswith(b"\xd0\xcf\x11\xe0"):
        return ".xls"      # OLE2 compound document (Excel 97-2003)
    if body.startswith(b"PK\x03\x04") and b"xl/" in body[:1024]:
        return ".xlsx"
    if body[:4] == b"<?xm" or body[:1] == b"<":
        return ".xml"
    if body[:1] == b"{" or body[:1] == b"[":
        return ".json"
    return default


def _key_to_iso_date(key: str) -> str:
    """Convert chunk key to ISO date for manifest. Accepts YYYYMM or YYYYMMDD."""
    if len(key) == 6:
        return f"{key[:4]}-{key[4:]}-01"
    if len(key) == 8:
        return f"{key[:4]}-{key[4:6]}-{key[6:]}"
    return key
