from __future__ import annotations

import re
from pathlib import Path

import pandas as pd

from mtu.parsing.omie_common import (
    append_csv_row,
    ensure_dir,
    parse_decimal,
    utc_now_iso,
    visible_files,
)

FILENAME_RE = re.compile(r"^orders_(\d{8})\.(\d+)$")
# Optional A/B suffix on each time component handles DST fall-back days (25-hour day).
# Spring-forward days (23-hour day) produce apparent 120-min or 75-min windows with no suffix.
CONTRACT_RE = re.compile(r"^(\d{8}) (\d{2}):(\d{2})([AB]?)-(\d{8}) (\d{2}):(\d{2})([AB]?)$")

MARKET = "mercado_intradiario_continuo"
CATEGORY = "ofertas"
FILE_FAMILY = "orders"


def parse_filename_metadata(path: Path) -> dict:
    m = FILENAME_RE.match(path.name)
    if not m:
        raise ValueError(f"Unexpected filename format for orders: {path.name}")

    yyyymmdd, version_suffix = m.groups()
    file_date = pd.to_datetime(yyyymmdd, format="%Y%m%d").date()

    return {
        "file_date": file_date.isoformat(),
        "version_suffix": version_suffix,
    }


def parse_contract(contract_str: str) -> dict:
    """Parse OMIE contract string, including DST transition variants.

    Normal:        '20180613 23:00-20180614 00:00'  (duration 15 or 60)
    Fall-back (A/B suffix, 25-hour day):
        '20181028 01:00-20181028 02:00A'  → 60-min, A marks first occurrence of 02:00
        '20181028 02:00A-20181028 02:00B' → 60-min, the extra "summer" hour
        '20181028 02:00B-20181028 03:00'  → 60-min, B marks second occurrence
    Spring-forward (23-hour day):
        '20190331 01:00-20190331 03:00'   → apparent 120 min → mtu=60
        '20250330 01:45-20250330 03:00'   → apparent  75 min → mtu=15

    Returns delivery_date (ISO), delivery_start (ISO datetime str),
    period (1-indexed), mtu_minutes (15 or 60), and dst_suffix (raw suffix string).
    """
    m = CONTRACT_RE.match(contract_str.strip())
    if not m:
        raise ValueError(f"Unexpected contract format: {contract_str!r}")

    s_date, s_hh, s_mm, s_sfx, e_date, e_hh, e_mm, e_sfx = m.groups()
    s_hh, s_mm, e_hh, e_mm = int(s_hh), int(s_mm), int(e_hh), int(e_mm)

    start_minutes = s_hh * 60 + s_mm
    end_minutes = e_hh * 60 + e_mm

    # Last period of day crosses midnight: end_date > start_date
    if e_date != s_date:
        end_minutes += 24 * 60

    apparent_duration = end_minutes - start_minutes

    # --- Determine mtu_minutes ---
    # For fall-back A→B contracts the clock goes back 60 min, so actual = apparent + 60.
    actual_duration = apparent_duration + 60 if (s_sfx == "A" and e_sfx == "B") else apparent_duration

    if actual_duration in (15, 60):
        mtu_minutes = actual_duration
    elif apparent_duration == 120:
        # Spring-forward: 01:00-03:00 spans the skipped hour → one 60-min MTU
        mtu_minutes = 60
    elif apparent_duration == 75:
        # Spring-forward at 15-min: 01:45-03:00 → one 15-min MTU
        mtu_minutes = 15
    else:
        raise ValueError(
            f"Unexpected contract duration {apparent_duration} min: {contract_str!r}"
        )

    # --- Determine period (1-indexed, using start time, adjusted for B suffix) ---
    if mtu_minutes == 60:
        period = s_hh + 1  # 00:00 → 1, 23:00 → 24
        if s_sfx == "B":
            # Second occurrence of the hour on fall-back day: shift by one period
            period += 1
    else:
        period = s_hh * 4 + s_mm // 15 + 1  # 00:00 → 1, 23:45 → 96
        if s_sfx == "B":
            # Second occurrence: shift by four 15-min periods (one hour)
            period += 4

    delivery_date = f"{s_date[:4]}-{s_date[4:6]}-{s_date[6:8]}"
    delivery_start = f"{delivery_date} {s_hh:02d}:{s_mm:02d}"
    dst_suffix = f"{s_sfx}/{e_sfx}" if (s_sfx or e_sfx) else ""

    return {
        "delivery_date": delivery_date,
        "delivery_start": delivery_start,
        "period": period,
        "mtu_minutes": mtu_minutes,
        "dst_suffix": dst_suffix,
    }


def _parse_optional_decimal(raw: str) -> float | None:
    raw = raw.strip()
    if not raw:
        return None
    return parse_decimal(raw)


def parse_orders_file(path: Path) -> pd.DataFrame:
    meta = parse_filename_metadata(path)
    rows = []
    data_row_counter = 0

    with path.open("r", encoding="latin-1", errors="replace") as f:
        for i, raw_line in enumerate(f):
            line = raw_line.strip()

            if not line:
                continue

            # Skip title row and column header row (first non-empty line is title,
            # second non-empty is headers starting with "Fecha")
            if i == 0:
                continue  # title: "OMIE - Mercado de electricidad;..."
            if i == 2:
                continue  # column headers: "Fecha;Contrato;Zona;..."

            # Skip separator lines (all semicolons or just whitespace)
            if set(line) <= {";"}:
                continue

            parts = line.split(";")
            if parts and parts[-1] == "":
                parts = parts[:-1]

            if len(parts) != 13:
                raise ValueError(
                    f"{path.name} line {i + 1}: expected 13 fields, got {len(parts)} -> {parts!r}"
                )

            data_row_counter += 1
            (
                fecha,
                contrato,
                zona,
                agente,
                unidad,
                precio,
                cantidad,
                tipo_oferta,
                cond_ejec,
                cond_vald,
                cant_red,
                ppd,
                momento_envio,
            ) = parts

            try:
                contract_meta = parse_contract(contrato)
            except ValueError as exc:
                raise ValueError(f"{path.name} line {i + 1}: {exc}") from exc

            rows.append(
                {
                    "trade_date": pd.to_datetime(fecha.strip(), format="%d/%m/%Y").date().isoformat(),
                    "contract": contrato.strip(),
                    "delivery_date": contract_meta["delivery_date"],
                    "delivery_start": contract_meta["delivery_start"],
                    "period": contract_meta["period"],
                    "mtu_minutes": contract_meta["mtu_minutes"],
                    "dst_suffix": contract_meta["dst_suffix"],
                    "zone": zona.strip(),
                    "agent": agente.strip(),
                    "unit_code": unidad.strip(),
                    "price_eur_mwh": parse_decimal(precio),
                    "quantity_mw": parse_decimal(cantidad),
                    "offer_type": tipo_oferta.strip(),
                    "exec_condition": cond_ejec.strip(),
                    "validity_condition": cond_vald.strip(),
                    "reduced_qty_mw": _parse_optional_decimal(cant_red),
                    "ppd": _parse_optional_decimal(ppd),
                    "submitted_at": momento_envio.strip(),
                    "row_number_in_file": data_row_counter,
                }
            )

    df = pd.DataFrame(rows)

    if df.empty:
        return df

    df["file_date"] = meta["file_date"]
    df["version_suffix"] = meta["version_suffix"]
    df["market"] = MARKET
    df["category"] = CATEGORY
    df["file_family"] = FILE_FAMILY
    df["source_file"] = path.name
    df["source_path"] = str(path)

    df = df[
        [
            "trade_date",
            "contract",
            "delivery_date",
            "delivery_start",
            "period",
            "mtu_minutes",
            "dst_suffix",
            "zone",
            "agent",
            "unit_code",
            "price_eur_mwh",
            "quantity_mw",
            "offer_type",
            "exec_condition",
            "validity_condition",
            "reduced_qty_mw",
            "ppd",
            "submitted_at",
            "row_number_in_file",
            "file_date",
            "version_suffix",
            "market",
            "category",
            "file_family",
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
                    "error_message": "Filename does not match orders pattern",
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
            df = parse_orders_file(path)

            if df.empty:
                summary_rows.append(
                    {
                        "filename": path.name,
                        "status": "skipped",
                        "rows_output": 0,
                        "output_path": "",
                        "error_message": "Empty file (no data rows)",
                    }
                )
                continue

            out_path = write_parquet_for_file(df, processed_dir, path.name)

            row = {
                "ingested_at": utc_now_iso(),
                "market": MARKET,
                "category": CATEGORY,
                "file_family": FILE_FAMILY,
                "filename": path.name,
                "parser_name": "mtu.parsing.orders.parse_orders_file:v1",
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
                "market": MARKET,
                "category": CATEGORY,
                "file_family": FILE_FAMILY,
                "filename": path.name,
                "parser_name": "mtu.parsing.orders.parse_orders_file:v1",
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
