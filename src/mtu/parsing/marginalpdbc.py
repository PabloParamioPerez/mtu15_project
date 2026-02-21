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


def parse_marginalpdbc_file(path: Path) -> pd.DataFrame:
    """
    Parse one OMIE MARGINALPDBC raw file into a tidy DataFrame.

    Expected format:
      MARGINALPDBC;
      YYYY;MM;DD;PERIOD;PT_PRICE;ES_PRICE;
      ...
      *
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

        # OMIE rows often end with a trailing ';' -> final empty token
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

    # Sort just in case
    df = df.sort_values(["date", "period"]).reset_index(drop=True)

    # Add metadata columns
    df["source_file"] = path.name
    df["source_path"] = str(path)
    df["file_family"] = "marginalpdbc"
    df["market"] = "mercado_diario"
    df["category"] = "precios"
    df["version_suffix"] = meta["version_suffix"]

    # Reorder columns (analysis-friendly first)
    df = df[
        [
            "date",
            "period",
            "price_es_eur_mwh",
            "price_pt_eur_mwh",
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
    append rows to ingestion_log.csv, and return a summary DataFrame.
    """
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

        try:
            df = parse_marginalpdbc_file(path)
            out_path = write_parquet_for_file(df, processed_dir, path.name)

            row = {
                "ingested_at": utc_now_iso(),
                "market": "mercado_diario",
                "category": "precios",
                "file_family": "marginalpdbc",
                "filename": path.name,
                "parser_name": "mtu.parsing.marginalpdbc.parse_marginalpdbc_file:v1",
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
                "parser_name": "mtu.parsing.marginalpdbc.parse_marginalpdbc_file:v1",
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