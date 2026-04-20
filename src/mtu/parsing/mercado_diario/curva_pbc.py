"""
Parser for OMIE "Curvas agregadas de oferta y demanda del mercado diario" (curva_pbc).

--- What one row means ---
Each row is one step of the published aggregated daily market curve. It represents
a block of MW offered or cleared at a given price, for a given period, country, and
side (buy C / sell V). Multiple rows per (period, country, offer_type, curve_type)
are expected — they form the staircase curve, ordered by price.

--- Directly known from spec and file inspection ---
- Two format variants distinguished by the first column header:
    MTU60 (pre-Oct 2025): first column "Hora"  (integer 1–24/25), 8 data fields
    MTU15 (post-Oct 2025): first column "Periodo" (HhQq string),   9 data fields
- Period encoding (MTU15): H{h}Q{q} → period_num = (h-1)*4 + q  (H1Q1=1, H24Q4=96)
- Energy/power column renamed between formats: "Energía" (MTU60) → "Potencia" (MTU15)
- MTU60 files have no Tipología de Oferta column; parser outputs None for that field
- Encoding: latin-1 (OMIE standard); file may contain corrupted chars if read as UTF-8
- Final separator line (all semicolons) is skipped
- Unidad is typically blank for this file family

--- Uncertain / TODO ---
- The download spec mentions a session number "ss" in the filename, but actual files
  observed are curva_pbc_YYYYMMDD.v with no session component. The FILENAME_RE below
  captures only date + version. If session-numbered variants appear (e.g. curva_pbc_2018010101.1),
  the regex and parse_filename_metadata will need updating.
- Pais domain: "MI", "ES", "PT" observed; others may exist (international interconnections).
  Unknown values are warned but preserved.
- Tipología de Oferta: known values documented in module docstring; others may exist.
  Unknown values are warned but preserved raw.
"""

from __future__ import annotations

import re
from datetime import datetime
from pathlib import Path

import pandas as pd

from mtu.parsing.omie_common import (
    append_csv_row,
    ensure_dir,
    parse_decimal,
    sha256_file,
    utc_now_iso,
    visible_files,
)

FILENAME_RE = re.compile(r"^curva_pbc_(\d{8})\.(\d+)$")
PERIOD_RE = re.compile(r"^H(\d{1,2})Q([1-4])$")

KNOWN_OFFER_TYPES = {"C", "V"}
KNOWN_CURVE_TYPES = {"O", "C"}
KNOWN_COUNTRIES = {"MI", "ES", "PT"}

# Tipología de Oferta values documented in spec (MTU15 only).
# Others may exist; they are preserved raw with a warning.
KNOWN_TIPOLOGIAS = {"S", "C01", "C02", "C04", "Imp PT", "Imp FR", "Imp ES", "Exp PT", "Exp FR", "Exp ES"}


def parse_filename_metadata(path: Path) -> dict:
    m = FILENAME_RE.match(path.name)
    if not m:
        raise ValueError(f"Unexpected filename format for curva_pbc: {path.name}")
    yyyymmdd, version_suffix = m.groups()
    file_date = pd.to_datetime(yyyymmdd, format="%Y%m%d").date()
    return {
        "file_date": file_date.isoformat(),
        "version_suffix": version_suffix,
    }


def _parse_period(raw: str, fmt: str, filename: str, lineno: int) -> int | None:
    """Return an integer period number (1-based).

    MTU60: raw is a plain integer string ("1".."25").
    MTU15: raw is H{h}Q{q}; period_num = (h-1)*4 + q.
    Returns None and prints a warning if the value cannot be parsed.
    """
    if fmt == "mtu60":
        try:
            return int(raw)
        except ValueError:
            print(f"WARNING: {filename} line {lineno}: cannot parse Hora {raw!r}")
            return None
    else:
        m = PERIOD_RE.match(raw)
        if not m:
            print(f"WARNING: {filename} line {lineno}: cannot parse Periodo {raw!r}")
            return None
        h, q = int(m.group(1)), int(m.group(2))
        return (h - 1) * 4 + q


def parse_curva_pbc_file(path: Path) -> pd.DataFrame:
    """
    Parse one OMIE curva_pbc raw file into a tidy DataFrame.

    Expected formats:
      MTU60:  OMIE header; blank line; Hora;Fecha;Pais;Unidad;Tipo Oferta;Energía C/V;Precio C/V;O/C;
      MTU15:  OMIE header; blank line; Periodo;Fecha;Pais;Unidad;Tipo Oferta;Potencia C/V;Precio C/V;O/C;Tipología;
    """
    meta = parse_filename_metadata(path)

    with path.open("r", encoding="latin-1", errors="replace") as f:
        lines = [ln.rstrip("\n\r") for ln in f]

    if not lines:
        raise ValueError(f"Empty file: {path.name}")

    # --- Line 0: report/session metadata ---
    header_parts = lines[0].split(";")
    report_datetime_raw = header_parts[1].strip() if len(header_parts) > 1 else ""
    session_date_raw = header_parts[3].strip() if len(header_parts) > 3 else ""

    try:
        session_date_iso = datetime.strptime(session_date_raw, "%d/%m/%Y").date().isoformat()
    except ValueError:
        session_date_iso = ""

    if session_date_iso and session_date_iso != meta["file_date"]:
        print(
            f"WARNING: {path.name}: filename date {meta['file_date']} "
            f"!= session date in header {session_date_iso}"
        )

    # --- Line 2: column names (line 1 is blank) ---
    if len(lines) < 3:
        raise ValueError(f"File too short (missing column header): {path.name}")

    col_parts = [c.strip() for c in lines[2].split(";")]
    if col_parts and col_parts[-1] == "":
        col_parts = col_parts[:-1]

    first_col = col_parts[0].lower() if col_parts else ""

    if first_col == "hora":
        fmt = "mtu60"
        mtu_minutes = 60
        n_expected = 8
    elif first_col == "periodo":
        fmt = "mtu15"
        mtu_minutes = 15
        n_expected = 9
    else:
        raise ValueError(
            f"{path.name}: unrecognized first column {col_parts[0]!r} "
            f"(expected 'Hora' or 'Periodo')"
        )

    # --- Data rows (start at line index 3) ---
    rows = []
    row_order = 0

    for lineno, line in enumerate(lines[3:], start=4):
        raw_line = line.strip()

        if not raw_line:
            continue
        if set(raw_line) <= {";"}:
            continue

        parts = raw_line.split(";")
        if parts and parts[-1] == "":
            parts = parts[:-1]

        if len(parts) != n_expected:
            print(
                f"WARNING: {path.name} line {lineno}: "
                f"expected {n_expected} fields, got {len(parts)} -> {parts!r}, skipping"
            )
            continue

        if fmt == "mtu60":
            period_raw, date_raw, country, unit, offer_type, power_raw, price_raw, curve_type = parts
            offer_typology = None
        else:
            period_raw, date_raw, country, unit, offer_type, power_raw, price_raw, curve_type, offer_typology = parts
            offer_typology = offer_typology.strip() if offer_typology else None

        period_raw = period_raw.strip()
        offer_type = offer_type.strip()
        curve_type = curve_type.strip()
        country = country.strip()
        unit = unit.strip()

        # Categorical validation (warn, preserve)
        if offer_type not in KNOWN_OFFER_TYPES:
            print(f"WARNING: {path.name} line {lineno}: unexpected offer_type {offer_type!r}")
        if curve_type not in KNOWN_CURVE_TYPES:
            print(f"WARNING: {path.name} line {lineno}: unexpected curve_type {curve_type!r}")
        if country not in KNOWN_COUNTRIES:
            print(f"WARNING: {path.name} line {lineno}: unexpected country {country!r}")
        if offer_typology is not None and offer_typology not in KNOWN_TIPOLOGIAS:
            print(f"WARNING: {path.name} line {lineno}: unknown tipologia {offer_typology!r}")

        period_num = _parse_period(period_raw, fmt, path.name, lineno)

        try:
            row_date = datetime.strptime(date_raw.strip(), "%d/%m/%Y").date().isoformat()
        except ValueError:
            print(f"WARNING: {path.name} line {lineno}: cannot parse date {date_raw!r}")
            row_date = ""

        try:
            power_mw = parse_decimal(power_raw)
        except Exception as e:
            print(f"WARNING: {path.name} line {lineno}: cannot parse power {power_raw!r}: {e}")
            power_mw = None

        try:
            price_eur_mwh = parse_decimal(price_raw)
        except Exception as e:
            print(f"WARNING: {path.name} line {lineno}: cannot parse price {price_raw!r}: {e}")
            price_eur_mwh = None

        rows.append(
            {
                "date": row_date,
                "period_raw": period_raw,
                "period_num": period_num,
                "country": country,
                "unit": unit,
                "offer_type": offer_type,
                "power_mw": power_mw,
                "price_eur_mwh": price_eur_mwh,
                "curve_type": curve_type,
                "offer_typology": offer_typology,
                "mtu_minutes": mtu_minutes,
                "row_order": row_order,
            }
        )
        row_order += 1

    if not rows:
        raise ValueError(f"No data rows found in {path.name}")

    df = pd.DataFrame(rows)

    df["market"] = "mercado_diario"
    df["category"] = "curvas"
    df["file_family"] = "curva_pbc"
    df["version_suffix"] = meta["version_suffix"]
    df["report_datetime_raw"] = report_datetime_raw
    df["source_file"] = path.name
    df["source_path"] = str(path)

    df = df[
        [
            "date",
            "period_raw",
            "period_num",
            "country",
            "unit",
            "offer_type",
            "power_mw",
            "price_eur_mwh",
            "curve_type",
            "offer_typology",
            "mtu_minutes",
            "row_order",
            "market",
            "category",
            "file_family",
            "version_suffix",
            "report_datetime_raw",
            "source_file",
            "source_path",
        ]
    ]

    return df


def write_parquet_for_file(df: pd.DataFrame, output_dir: Path, source_file_name: str) -> Path:
    ensure_dir(output_dir)
    out_path = output_dir / f"{source_file_name}.parquet"
    df.to_parquet(out_path, index=False)
    return out_path


def parse_folder_and_write(
    raw_dir: Path,
    processed_dir: Path,
    ingestion_log_csv: Path,
) -> pd.DataFrame:
    """
    Parse all visible curva_pbc files in raw_dir, write one parquet per file to
    processed_dir, append rows to ingestion_log.csv, and return a summary DataFrame.

    Incremental: skips files whose output parquet already exists.
    Skipped files appear in the summary but are NOT appended to ingestion_log.csv.
    """
    ensure_dir(processed_dir)

    files = visible_files(raw_dir)
    summary_rows = []

    for path in files:
        if not FILENAME_RE.match(path.name):
            summary_rows.append(
                {
                    "filename": path.name,
                    "status": "skipped",
                    "rows_output": 0,
                    "output_path": "",
                    "error_message": "Filename does not match curva_pbc pattern",
                }
            )
            continue

        out_path = processed_dir / f"{path.name}.parquet"
        if out_path.exists():
            summary_rows.append(
                {
                    "filename": path.name,
                    "status": "skipped",
                    "rows_output": 0,
                    "output_path": str(out_path),
                    "error_message": "Output parquet already exists",
                }
            )
            continue

        try:
            df = parse_curva_pbc_file(path)
            out_path = write_parquet_for_file(df, processed_dir, path.name)

            row = {
                "ingested_at": utc_now_iso(),
                "market": "mercado_diario",
                "category": "curvas",
                "file_family": "curva_pbc",
                "filename": path.name,
                "parser_name": "mtu.parsing.curva_pbc.parse_curva_pbc_file:v1",
                "raw_file_kind": "omie_text",
                "rows_read": len(df),
                "rows_output": len(df),
                "status": "success",
                "output_path": str(out_path),
                "error_message": "",
            }
            append_csv_row(ingestion_log_csv, row)

            summary_rows.append(
                {
                    "filename": path.name,
                    "status": "success",
                    "rows_output": len(df),
                    "output_path": str(out_path),
                    "error_message": "",
                }
            )

        except Exception as e:
            row = {
                "ingested_at": utc_now_iso(),
                "market": "mercado_diario",
                "category": "curvas",
                "file_family": "curva_pbc",
                "filename": path.name,
                "parser_name": "mtu.parsing.curva_pbc.parse_curva_pbc_file:v1",
                "raw_file_kind": "omie_text",
                "rows_read": "",
                "rows_output": 0,
                "status": "failed",
                "output_path": "",
                "error_message": str(e),
            }
            append_csv_row(ingestion_log_csv, row)

            summary_rows.append(
                {
                    "filename": path.name,
                    "status": "failed",
                    "rows_output": 0,
                    "output_path": "",
                    "error_message": str(e),
                }
            )

    return pd.DataFrame(summary_rows)


def build_download_manifest_row_for_existing_file(path: Path) -> dict:
    """
    Helper to backfill download_manifest.csv for files already on disk.
    """
    meta = parse_filename_metadata(path)
    return {
        "downloaded_at": utc_now_iso(),
        "source_url": "",
        "market": "mercado_diario",
        "category": "curvas",
        "file_family": "curva_pbc",
        "filename": path.name,
        "size_bytes": path.stat().st_size,
        "sha256": sha256_file(path),
        "is_zip": False,
        "file_date": meta["file_date"],
        "version_suffix": meta["version_suffix"],
        "notes": "manual_download_backfill",
    }
