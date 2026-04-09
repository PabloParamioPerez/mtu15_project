"""
Parser for OMIE "Curvas agregadas de oferta y demanda del mercado intradiario de subastas" (curva_pibc).

--- What one row means ---
Each row is one step of the published aggregated intraday auction curve. It represents
a block of MW offered or cleared at a given price, for a given period, country, and
side (buy C / sell V). Multiple rows per (period, country, offer_type, curve_type)
are expected — they form the staircase curve, ordered by price.

--- Directly known from spec and file inspection ---
- Filename pattern changed on 2025-06-07:
    Before: curva_pibc_YYYYMMDD{SS}.{version}  — zero-padded 2-digit session (01–06)
      e.g. curva_pibc_2024010101.1 → date=2024-01-01, session=1, version=1
    From 2025-06-07: curva_pibc_YYYYMMDD{S}.{version}  — single-digit session (1–6)
      e.g. curva_pibc_202604092.1 → date=2026-04-09, session=2, version=1
- Sessions: intraday auction sessions 1–6 (IDA 1 through IDA 6)
- Three format variants distinguished by first column header and field count:
    MTU60       (pre ~2024-06-14):  "Hora",    8 fields — no extra columns
    MTU60_SBO   (~2024-06-14 to ~2025): "Hora", 9 fields — extra "Simple Block Orders" (N/S)
    MTU15       (post-Oct 2025):    "Periodo", 9 fields — extra "Tipología de Oferta"
- Period encoding (MTU15): H{h}Q{q} → period_num = (h-1)*4 + q  (H1Q1=1, H24Q4=96)
- Energy/power column name: "Energía" (MTU60/MTU60_SBO) → "Potencia" (MTU15) — not used by parser
- simple_block_orders column: populated only for MTU60_SBO files (None otherwise)
- offer_typology column: populated only for MTU15 files (None otherwise)
- Genuinely empty files (header + separator only, no data rows) are returned as empty DataFrames
- Header line 0: "OMIE - Mercado de electricidad;Fecha Emisión :...;;Mercado IDA {n} - DD/MM/YYYY;..."
  Session number and session date are extracted from header_parts[3]
- Encoding: latin-1 (OMIE standard)
- Final separator line (all semicolons) is skipped
- Unidad is typically blank for this file family

--- Uncertain / TODO ---
- MTU60 column header not directly inspected; assumed analogous to curva_pbc ("Hora" → 8 fields).
  If different, the format detection will raise with an informative error.
- Pais domain: "MI", "ES", "PT" observed in curva_pbc; same expected here.
- Tipología de Oferta: same known set as curva_pbc. Unknown values are warned but preserved.
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

FILENAME_RE = re.compile(r"^curva_pibc_(\d{8})(\d{1,2})\.(\d+)$")
PERIOD_RE = re.compile(r"^H(\d{1,2})Q([1-4])$")
SESSION_RE = re.compile(r"IDA\s+(\d+)", re.IGNORECASE)
HEADER_DATE_RE = re.compile(r"(\d{2}/\d{2}/\d{4})")

KNOWN_OFFER_TYPES = {"C", "V"}
KNOWN_CURVE_TYPES = {"O", "C"}
KNOWN_COUNTRIES = {"MI", "ES", "PT"}

KNOWN_TIPOLOGIAS = {"S", "C01", "C02", "C04", "Imp PT", "Imp FR", "Imp ES", "Exp PT", "Exp FR", "Exp ES"}


def parse_filename_metadata(path: Path) -> dict:
    m = FILENAME_RE.match(path.name)
    if not m:
        raise ValueError(f"Unexpected filename format for curva_pibc: {path.name}")
    yyyymmdd, session_str, version_suffix = m.groups()
    file_date = pd.to_datetime(yyyymmdd, format="%Y%m%d").date()
    return {
        "file_date": file_date.isoformat(),
        "session_number": int(session_str),
        "version_suffix": version_suffix,
    }


def _parse_period(raw: str, fmt: str, filename: str, lineno: int) -> int | None:
    """Return an integer period number (1-based).

    MTU60: raw is a plain integer string ("1".."25").
    MTU15: raw is H{h}Q{q}; period_num = (h-1)*4 + q.
    Returns None and prints a warning if the value cannot be parsed.
    """
    if fmt in ("mtu60", "mtu60_sbo"):
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


def parse_curva_pibc_file(path: Path) -> pd.DataFrame:
    """
    Parse one OMIE curva_pibc raw file into a tidy DataFrame.

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

    # Extract session date from "Mercado IDA {n} - DD/MM/YYYY" in header_parts[3]
    session_date_raw = ""
    if len(header_parts) > 3:
        date_m = HEADER_DATE_RE.search(header_parts[3])
        if date_m:
            session_date_raw = date_m.group(1)

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
        mtu_minutes = 60
        # Distinguish plain MTU60 (8 fields) from MTU60+SimpleBlockOrders (9 fields)
        # by checking whether the last named column contains "block"
        last_col = col_parts[-1].lower() if col_parts else ""
        if "block" in last_col:
            fmt = "mtu60_sbo"
            n_expected = 9
        else:
            fmt = "mtu60"
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
            simple_block_orders = None
        elif fmt == "mtu60_sbo":
            period_raw, date_raw, country, unit, offer_type, power_raw, price_raw, curve_type, sbo_raw = parts
            offer_typology = None
            simple_block_orders = sbo_raw.strip() if sbo_raw else None
        else:
            period_raw, date_raw, country, unit, offer_type, power_raw, price_raw, curve_type, offer_typology = parts
            offer_typology = offer_typology.strip() if offer_typology else None
            simple_block_orders = None

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
                "session_number": meta["session_number"],
                "period_raw": period_raw,
                "period_num": period_num,
                "country": country,
                "unit": unit,
                "offer_type": offer_type,
                "power_mw": power_mw,
                "price_eur_mwh": price_eur_mwh,
                "curve_type": curve_type,
                "offer_typology": offer_typology,
                "simple_block_orders": simple_block_orders,
                "mtu_minutes": mtu_minutes,
                "row_order": row_order,
            }
        )
        row_order += 1

    if not rows:
        # Genuinely empty file (header + separator only) — return empty DataFrame
        return pd.DataFrame(columns=[
            "date", "session_number", "period_raw", "period_num", "country", "unit",
            "offer_type", "power_mw", "price_eur_mwh", "curve_type", "offer_typology",
            "simple_block_orders", "mtu_minutes", "row_order", "market", "category",
            "file_family", "version_suffix", "report_datetime_raw", "source_file", "source_path",
        ])

    df = pd.DataFrame(rows)

    df["market"] = "mercado_intradiario_subastas"
    df["category"] = "curvas"
    df["file_family"] = "curva_pibc"
    df["version_suffix"] = meta["version_suffix"]
    df["report_datetime_raw"] = report_datetime_raw
    df["source_file"] = path.name
    df["source_path"] = str(path)

    df = df[
        [
            "date",
            "session_number",
            "period_raw",
            "period_num",
            "country",
            "unit",
            "offer_type",
            "power_mw",
            "price_eur_mwh",
            "curve_type",
            "offer_typology",
            "simple_block_orders",
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
    Parse all visible curva_pibc files in raw_dir, write one parquet per file to
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
                    "error_message": "Filename does not match curva_pibc pattern",
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
            df = parse_curva_pibc_file(path)
            out_path = write_parquet_for_file(df, processed_dir, path.name)
            status_str = "success_empty" if len(df) == 0 else "success"

            row = {
                "ingested_at": utc_now_iso(),
                "market": "mercado_intradiario_subastas",
                "category": "curvas",
                "file_family": "curva_pibc",
                "filename": path.name,
                "parser_name": "mtu.parsing.curva_pibc.parse_curva_pibc_file:v1",
                "raw_file_kind": "omie_text",
                "rows_read": len(df),
                "rows_output": len(df),
                "status": status_str,
                "output_path": str(out_path),
                "error_message": "",
            }
            append_csv_row(ingestion_log_csv, row)

            summary_rows.append(
                {
                    "filename": path.name,
                    "status": status_str,
                    "rows_output": len(df),
                    "output_path": str(out_path),
                    "error_message": "",
                }
            )

        except Exception as e:
            row = {
                "ingested_at": utc_now_iso(),
                "market": "mercado_intradiario_subastas",
                "category": "curvas",
                "file_family": "curva_pibc",
                "filename": path.name,
                "parser_name": "mtu.parsing.curva_pibc.parse_curva_pibc_file:v1",
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
        "market": "mercado_intradiario_subastas",
        "category": "curvas",
        "file_family": "curva_pibc",
        "filename": path.name,
        "size_bytes": path.stat().st_size,
        "sha256": sha256_file(path),
        "is_zip": False,
        "file_date": meta["file_date"],
        "session_number": meta["session_number"],
        "version_suffix": meta["version_suffix"],
        "notes": "manual_download_backfill",
    }
