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

# Matches both capacidad_inter_pbc_YYYYMMDD.v and capacidad_inter_pvp_YYYYMMDD.v
_FILENAME_RE: dict[str, re.Pattern] = {
    "capacidad_inter_pbc": re.compile(r"^capacidad_inter_pbc_(\d{8})\.(\d+)$"),
    "capacidad_inter_pvp": re.compile(r"^capacidad_inter_pvp_(\d{8})\.(\d+)$"),
}

# Period notation: H{hour}Q{quarter} (post-reform, 15-min), H{hour} (pre-reform hourly),
# or bare integer (pre-reform hourly in older files where the column header was "Hora")
_PERIOD_RE = re.compile(r"^H(\d{1,2})(?:Q(\d))?$")
_PERIOD_BARE_RE = re.compile(r"^(\d{1,2})$")

MARKET = "mercado_diario"
CATEGORY = "capacidades"

_BORDER_NAMES = {2: "PT", 3: "FR", 4: "MA", 5: "AND"}


def _parse_hxqn(s: str) -> tuple[int, int]:
    """Parse OMIE period string.

    H{hour}        → pre-reform hourly  (period = hour,           mtu_minutes = 60)
    H{hour}Q{q}    → post-reform 15-min (period = (hour-1)*4 + q, mtu_minutes = 15)
    {integer}      → pre-reform hourly bare format (period = integer, mtu_minutes = 60)

    Returns (period_1indexed, mtu_minutes).
    """
    s = s.strip()
    m = _PERIOD_RE.match(s)
    if not m:
        m2 = _PERIOD_BARE_RE.match(s)
        if m2:
            return int(m2.group(1)), 60
        raise ValueError(f"Unrecognised period string: {s!r}")
    hour = int(m.group(1))
    quarter_str = m.group(2)
    if quarter_str is None:
        return hour, 60
    return (hour - 1) * 4 + int(quarter_str), 15


def parse_filename_metadata(path: Path, file_family: str) -> dict:
    pattern = _FILENAME_RE[file_family]
    m = pattern.match(path.name)
    if not m:
        raise ValueError(f"Unexpected filename for {file_family}: {path.name}")
    yyyymmdd, version_suffix = m.groups()
    file_date = pd.to_datetime(yyyymmdd, format="%Y%m%d").date()
    return {"file_date": file_date.isoformat(), "version_suffix": version_suffix}


def parse_capacidad_inter_file(path: Path, file_family: str) -> pd.DataFrame:
    meta = parse_filename_metadata(path, file_family)
    rows = []
    data_row_counter = 0

    with path.open("r", encoding="latin-1", errors="replace") as f:
        for i, raw_line in enumerate(f):
            line = raw_line.strip()

            if not line:
                continue
            if i == 0:
                continue  # title line
            if i == 2:
                continue  # column header line

            # End marker: all semicolons (e.g. ";;;;;;;;")
            if set(line) <= {";"}:
                continue

            parts = line.split(";")
            if parts and parts[-1] == "":
                parts = parts[:-1]

            if len(parts) != 9:
                raise ValueError(
                    f"{path.name} line {i + 1}: expected 9 fields, got {len(parts)} -> {parts!r}"
                )

            data_row_counter += 1
            (
                periodo, fecha, frontera,
                cap_import, occ_import, free_import,
                cap_export, occ_export, free_export,
            ) = parts

            try:
                period, mtu_minutes = _parse_hxqn(periodo)
            except ValueError as exc:
                raise ValueError(f"{path.name} line {i + 1}: {exc}") from exc

            try:
                delivery_date = pd.to_datetime(
                    fecha.strip(), format="%d/%m/%Y"
                ).date().isoformat()
            except (ValueError, TypeError) as exc:
                raise ValueError(f"{path.name} line {i + 1}: bad Fecha {fecha!r}: {exc}") from exc

            border_code = int(frontera.strip())

            rows.append(
                {
                    "delivery_date": delivery_date,
                    "period_str": periodo.strip(),
                    "period": period,
                    "mtu_minutes": mtu_minutes,
                    "border_code": border_code,
                    "border_name": _BORDER_NAMES.get(border_code, str(border_code)),
                    "cap_import_mw": parse_decimal(cap_import),
                    "occ_import_mw": parse_decimal(occ_import),
                    "free_import_mw": parse_decimal(free_import),
                    "cap_export_mw": parse_decimal(cap_export),
                    "occ_export_mw": parse_decimal(occ_export),
                    "free_export_mw": parse_decimal(free_export),
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
    df["file_family"] = file_family
    df["source_file"] = path.name
    df["source_path"] = str(path)

    df = df[
        [
            "delivery_date",
            "period_str",
            "period",
            "mtu_minutes",
            "border_code",
            "border_name",
            "cap_import_mw",
            "occ_import_mw",
            "free_import_mw",
            "cap_export_mw",
            "occ_export_mw",
            "free_export_mw",
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
    file_family: str,
) -> pd.DataFrame:
    ensure_dir(processed_dir)

    pattern = _FILENAME_RE[file_family]
    files = visible_files(raw_dir)
    summary_rows = []

    for path in files:
        if not pattern.match(path.name):
            summary_rows.append(
                {
                    "filename": path.name,
                    "status": "skipped",
                    "rows_output": 0,
                    "output_path": "",
                    "error_message": f"Filename does not match {file_family} pattern",
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
            df = parse_capacidad_inter_file(path, file_family)

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
                "file_family": file_family,
                "filename": path.name,
                "parser_name": "mtu.parsing.capacidad_inter.parse_capacidad_inter_file:v1",
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
                "file_family": file_family,
                "filename": path.name,
                "parser_name": "mtu.parsing.capacidad_inter.parse_capacidad_inter_file:v1",
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
