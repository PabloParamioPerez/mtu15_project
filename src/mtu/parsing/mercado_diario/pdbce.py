from __future__ import annotations

import re
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

FILENAME_RE = re.compile(r"^pdbce_(\d{8})\.(\d+)$")


def parse_filename_metadata(path: Path) -> dict:
    m = FILENAME_RE.match(path.name)
    if not m:
        raise ValueError(f"Unexpected filename format for pdbce: {path.name}")

    yyyymmdd, version_suffix = m.groups()
    file_date = pd.to_datetime(yyyymmdd, format="%Y%m%d").date()

    return {
        "file_date": file_date.isoformat(),
        "version_suffix": version_suffix,
    }


def infer_mtu_minutes_from_periods(periods: pd.Series) -> int:
    unique_periods = sorted(set(int(x) for x in periods.tolist()))
    hi = max(unique_periods)

    if hi <= 25:
        return 60
    if hi <= 100:
        return 15

    raise ValueError(f"Unexpected maximum period value {hi}")


def validate_period_values(path: Path, periods: pd.Series, mtu_minutes: int) -> None:
    p = [int(x) for x in periods.tolist()]
    if not p:
        raise ValueError(f"{path.name}: no periods found")

    lo, hi = min(p), max(p)

    if mtu_minutes == 60:
        valid_lo, valid_hi = 1, 25
    elif mtu_minutes == 15:
        valid_lo, valid_hi = 1, 100
    else:
        raise ValueError(f"{path.name}: unsupported mtu_minutes={mtu_minutes}")

    if lo < valid_lo or hi > valid_hi:
        raise ValueError(
            f"{path.name}: period range {lo}..{hi} outside valid bounds "
            f"{valid_lo}..{valid_hi} for MTU{mtu_minutes}"
        )


def parse_pdbce_file(path: Path) -> pd.DataFrame:
    meta = parse_filename_metadata(path)
    rows = []

    with path.open("r", encoding="latin-1", errors="replace") as f:
        for i, raw_line in enumerate(f):
            line = raw_line.strip()

            if not line:
                continue

            if i == 0:
                if line != "PDBCE;":
                    raise ValueError(f"{path.name}: expected first line 'PDBCE;', got {line!r}")
                continue

            if line == "*" or set(line) <= {";"}:
                continue

            parts = line.split(";")
            if parts and parts[-1] == "":
                parts = parts[:-1]

            if len(parts) != 9:
                raise ValueError(
                    f"{path.name}: expected 9 fields, got {len(parts)} -> {parts!r}"
                )

            yyyy, mm, dd, period, unit_code, assigned_power, grupo, offer_type, offer_number = parts
            grupo_stripped = grupo.strip()

            rows.append(
                {
                    "year": int(yyyy),
                    "month": int(mm),
                    "day": int(dd),
                    "period": int(period),
                    "unit_code": unit_code.strip(),
                    "assigned_power_mw": parse_decimal(assigned_power),
                    "grupo_empresarial": grupo_stripped if grupo_stripped else None,
                    "offer_type": int(offer_type),
                    "offer_number": int(offer_number),
                }
            )

    df = pd.DataFrame(rows)

    if df.empty:
        raise ValueError(f"No data rows found in {path.name}")

    df["date"] = pd.to_datetime(df[["year", "month", "day"]]).dt.date.astype(str)

    unique_dates = sorted(df["date"].drop_duplicates().tolist())
    if len(unique_dates) != 1:
        raise ValueError(f"{path.name}: contains multiple dates {unique_dates}")

    if unique_dates[0] != meta["file_date"]:
        raise ValueError(
            f"Filename date {meta['file_date']} != content date {unique_dates[0]} in {path.name}"
        )

    dup_key = ["date", "period", "unit_code", "grupo_empresarial", "offer_type", "offer_number"]
    dup_mask = df.duplicated(subset=dup_key)
    if dup_mask.any():
        dups = (
            df.loc[dup_mask, dup_key]
            .drop_duplicates()
            .sort_values(dup_key)
        )
        dups_preview = [tuple(x) for x in dups.head(10).to_numpy().tolist()]
        more = "..." if len(dups) > 10 else ""
        raise ValueError(
            f"{path.name}: duplicated (date, period, unit_code, grupo_empresarial, offer_type, offer_number) rows: "
            f"{dups_preview}{more}"
        )

    mtu_minutes = infer_mtu_minutes_from_periods(df["period"])
    validate_period_values(path, df["period"], mtu_minutes)

    df = df.sort_values(["date", "period", "unit_code", "grupo_empresarial", "offer_type", "offer_number"]).reset_index(drop=True)

    df["mtu_minutes"] = mtu_minutes
    df["market"] = "mercado_diario"
    df["category"] = "programas"
    df["file_family"] = "pdbce"
    df["version_suffix"] = meta["version_suffix"]
    df["source_file"] = path.name
    df["source_path"] = str(path)

    df = df[
        [
            "date",
            "period",
            "unit_code",
            "assigned_power_mw",
            "grupo_empresarial",
            "offer_type",
            "offer_number",
            "mtu_minutes",
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
                    "error_message": "Filename does not match pdbce pattern",
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
            df = parse_pdbce_file(path)
            out_path = write_parquet_for_file(df, processed_dir, path.name)

            row = {
                "ingested_at": utc_now_iso(),
                "market": "mercado_diario",
                "category": "programas",
                "file_family": "pdbce",
                "filename": path.name,
                "parser_name": "mtu.parsing.pdbce.parse_pdbce_file:v1",
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
                "category": "programas",
                "file_family": "pdbce",
                "filename": path.name,
                "parser_name": "mtu.parsing.pdbce.parse_pdbce_file:v1",
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
    meta = parse_filename_metadata(path)
    return {
        "downloaded_at": utc_now_iso(),
        "source_url": "",
        "market": "mercado_diario",
        "category": "programas",
        "file_family": "pdbce",
        "filename": path.name,
        "size_bytes": path.stat().st_size,
        "sha256": sha256_file(path),
        "is_zip": False,
        "file_date": meta["file_date"],
        "version_suffix": meta["version_suffix"],
        "notes": "manual_download_backfill",
    }
