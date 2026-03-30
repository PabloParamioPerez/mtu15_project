from __future__ import annotations

import re
from pathlib import Path

import pandas as pd

from mtu.parsing.omie_common import (
    append_csv_row,
    ensure_dir,
    parse_decimal,
    read_text_lines,
    sha256_file,
    utc_now_iso,
    visible_files,
)

FILENAME_RE = re.compile(r"^marginalpdbc_(\d{8})\.(\d+)$")

# Allowed counts including DST days
MTU60_ALLOWED_COUNTS = {23, 24, 25}
MTU15_ALLOWED_COUNTS = {92, 96, 100}


def parse_filename_metadata(path: Path) -> dict:
    m = FILENAME_RE.match(path.name)
    if not m:
        raise ValueError(f"Unexpected filename format for marginalpdbc: {path.name}")

    yyyymmdd, version_suffix = m.groups()
    file_date = pd.to_datetime(yyyymmdd, format="%Y%m%d").date()

    return {
        "file_date": file_date.isoformat(),
        "version_suffix": version_suffix,
    }


def infer_mtu_minutes_from_periods(periods: pd.Series) -> int:
    """
    Infer market time unit from period numbering.
    - MTU60 usually has periods up to 24/25
    - MTU15 usually has periods up to 96/100
    """
    max_period = int(periods.max())

    if max_period <= 25:
        return 60
    if max_period <= 100:
        return 15

    raise ValueError(f"Cannot infer MTU from max period={max_period}")


def validate_period_count_for_mtu(path: Path, n_rows: int, mtu_minutes: int) -> None:
    if mtu_minutes == 60 and n_rows not in MTU60_ALLOWED_COUNTS:
        raise ValueError(
            f"{path.name}: inferred MTU60 but row count={n_rows} "
            f"(expected one of {sorted(MTU60_ALLOWED_COUNTS)})"
        )
    if mtu_minutes == 15 and n_rows not in MTU15_ALLOWED_COUNTS:
        raise ValueError(
            f"{path.name}: inferred MTU15 but row count={n_rows} "
            f"(expected one of {sorted(MTU15_ALLOWED_COUNTS)})"
        )


def parse_marginalpdbc_file(path: Path) -> pd.DataFrame:
    """
    Parse one OMIE MARGINALPDBC raw file into a tidy DataFrame.

    Expected format:
      MARGINALPDBC;
      YYYY;MM;DD;PERIOD;PT_PRICE;ES_PRICE;
      ...
    """
    meta = parse_filename_metadata(path)
    lines = [ln.strip() for ln in read_text_lines(path) if ln.strip()]

    if not lines:
        raise ValueError(f"Empty file: {path}")

    header = lines[0].rstrip(";").upper()
    if header != "MARGINALPDBC":
        raise ValueError(f"Unexpected header in {path.name}: {lines[0]!r}")

    rows = []
    for i, line in enumerate(lines[1:], start=2):
        raw_line = line.strip()

        # Skip OMIE footer markers / separators
        if raw_line == "*" or set(raw_line) <= {";"}:
            continue

        parts = raw_line.split(";")

        # OMIE rows often end with trailing ';' -> final empty token
        if parts and parts[-1] == "":
            parts = parts[:-1]

        if len(parts) != 6:
            raise ValueError(
                f"{path.name} line {i}: expected 6 fields, got {len(parts)} -> {parts!r}"
            )

        yyyy, mm, dd, period, price_pt, price_es = parts
        rows.append(
            {
                "year": int(yyyy),
                "month": int(mm),
                "day": int(dd),
                "period": int(period),
                "price_pt_eur_mwh": parse_decimal(price_pt),
                "price_es_eur_mwh": parse_decimal(price_es),
            }
        )

    df = pd.DataFrame(rows)

    if df.empty:
        raise ValueError(f"No data rows found in {path.name}")

    df["date"] = pd.to_datetime(df[["year", "month", "day"]]).dt.date.astype(str)

    # Validation checks
    unique_dates = df["date"].drop_duplicates().tolist()
    if len(unique_dates) != 1:
        raise ValueError(f"{path.name} contains multiple dates: {unique_dates}")

    if unique_dates[0] != meta["file_date"]:
        raise ValueError(
            f"Filename date {meta['file_date']} != content date {unique_dates[0]} in {path.name}"
        )

    if df["period"].duplicated().any():
        dups = df.loc[df["period"].duplicated(), "period"].tolist()
        raise ValueError(f"{path.name} has duplicated periods: {dups}")

    # Infer MTU and validate row count (works for MTU60 + MTU15, incl DST)
    mtu_minutes = infer_mtu_minutes_from_periods(df["period"])
    n_periods_in_file = len(df)
    validate_period_count_for_mtu(path, n_periods_in_file, mtu_minutes)

    # Sort just in case
    df = df.sort_values(["date", "period"]).reset_index(drop=True)

    # Add metadata columns
    df["source_file"] = path.name
    df["source_path"] = str(path)
    df["file_family"] = "marginalpdbc"
    df["market"] = "mercado_diario"
    df["category"] = "precios"
    df["version_suffix"] = meta["version_suffix"]
    df["mtu_minutes"] = mtu_minutes
    df["n_periods_in_file"] = n_periods_in_file

    # Reorder columns (analysis-friendly first)
    df = df[
        [
            "date",
            "period",
            "price_es_eur_mwh",
            "price_pt_eur_mwh",
            "mtu_minutes",
            "n_periods_in_file",
            "market",
            "category",
            "file_family",
            "version_suffix",
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
    Parse all visible files in raw_dir, write one parquet per file to processed_dir,
    append rows to ingestion_log.csv for success/failed parses, and return a summary
    DataFrame.

    Incremental behavior:
    - if output parquet already exists, skip parsing;
    - skipped files are returned in the summary but are NOT appended to ingestion_log.csv.
    """
    ensure_dir(processed_dir)

    files = visible_files(raw_dir)
    summary_rows = []

    for path in files:
        # Skip accidental non-data files that are visible for any reason
        if not FILENAME_RE.match(path.name):
            summary_rows.append(
                {
                    "filename": path.name,
                    "status": "skipped",
                    "rows_output": 0,
                    "output_path": "",
                    "error_message": "Filename does not match marginalpdbc pattern",
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
            df = parse_marginalpdbc_file(path)
            out_path = write_parquet_for_file(df, processed_dir, path.name)

            row = {
                "ingested_at": utc_now_iso(),
                "market": "mercado_diario",
                "category": "precios",
                "file_family": "marginalpdbc",
                "filename": path.name,
                "parser_name": "mtu.parsing.marginalpdbc.parse_marginalpdbc_file:v3",
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
                "category": "precios",
                "file_family": "marginalpdbc",
                "filename": path.name,
                "parser_name": "mtu.parsing.marginalpdbc.parse_marginalpdbc_file:v3",
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
    Optional helper if you want to backfill download_manifest.csv for files you already
    downloaded manually.
    """
    meta = parse_filename_metadata(path)
    return {
        "downloaded_at": utc_now_iso(),
        "source_url": "",
        "market": "mercado_diario",
        "category": "precios",
        "file_family": "marginalpdbc",
        "filename": path.name,
        "size_bytes": path.stat().st_size,
        "sha256": sha256_file(path),
        "is_zip": False,
        "file_date": meta["file_date"],
        "version_suffix": meta["version_suffix"],
        "notes": "manual_download_backfill",
    }
