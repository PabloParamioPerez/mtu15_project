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

FILENAME_RE = re.compile(r"^pibcac_(\d{8})(\d{2})\.(\d+)$")


def parse_filename_metadata(path: Path) -> dict:
    m = FILENAME_RE.match(path.name)
    if not m:
        raise ValueError(f"Unexpected filename format for pibcac: {path.name}")

    yyyymmdd, round_number, version_suffix = m.groups()
    file_date = pd.to_datetime(yyyymmdd, format="%Y%m%d").date()

    return {
        "file_date": file_date.isoformat(),
        "round_number_from_filename": int(round_number),
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


def validate_content_dates(path: Path, df: pd.DataFrame) -> None:
    unique_dates = sorted(pd.to_datetime(df["date"]).dt.date.unique().tolist())

    if len(unique_dates) == 0:
        raise ValueError(f"{path.name}: no dates found in content")

    if len(unique_dates) > 2:
        raise ValueError(f"{path.name}: contains more than two content dates: {unique_dates}")

    if len(unique_dates) == 2:
        delta_days = (unique_dates[1] - unique_dates[0]).days
        if delta_days != 1:
            raise ValueError(
                f"{path.name}: content dates are not adjacent: "
                f"{unique_dates[0]} and {unique_dates[1]}"
            )


def parse_pibcac_file(path: Path) -> pd.DataFrame:
    meta = parse_filename_metadata(path)
    rows = []
    data_row_counter = 0

    with path.open("r", encoding="latin-1", errors="replace") as f:
        for i, raw_line in enumerate(f):
            line = raw_line.strip()

            if not line:
                continue

            if i == 0:
                if line != "PIBCAC;":
                    raise ValueError(f"{path.name}: expected first line 'PIBCAC;', got {line!r}")
                continue

            if line == "*" or set(line) <= {";"}:
                continue

            parts = line.split(";")
            if parts and parts[-1] == "":
                parts = parts[:-1]

            if len(parts) != 7:
                raise ValueError(
                    f"{path.name}: expected 7 fields, got {len(parts)} -> {parts!r}"
                )

            data_row_counter += 1
            yyyy, mm, dd, period, round_number, unit_code, assigned_power = parts

            rows.append(
                {
                    "year": int(yyyy),
                    "month": int(mm),
                    "day": int(dd),
                    "period": int(period),
                    "round_number": int(round_number),
                    "unit_code": unit_code.strip(),
                    "assigned_power_mw": parse_decimal(assigned_power),
                    "raw_row_number_in_file": data_row_counter,
                }
            )

    df = pd.DataFrame(rows)

    if df.empty:
        return df

    if df["round_number"].nunique() != 1:
        raise ValueError(
            f"{path.name}: multiple round_number values found in content: "
            f"{sorted(df['round_number'].unique().tolist())}"
        )

    round_number_content = int(df["round_number"].iloc[0])
    if round_number_content != meta["round_number_from_filename"]:
        raise ValueError(
            f"{path.name}: filename round_number={meta['round_number_from_filename']} "
            f"!= content round_number={round_number_content}"
        )

    df["date"] = pd.to_datetime(df[["year", "month", "day"]]).dt.date.astype(str)

    validate_content_dates(path, df)

    mtu_minutes = infer_mtu_minutes_from_periods(df["period"])
    validate_period_values(path, df["period"], mtu_minutes)

    df["row_number_in_file"] = df["raw_row_number_in_file"]

    df["mtu_minutes"] = mtu_minutes
    df["market"] = "mercado_intradiario_continuo"
    df["category"] = "programas"
    df["file_family"] = "pibcac"
    df["version_suffix"] = meta["version_suffix"]
    df["source_file"] = path.name
    df["source_path"] = str(path)

    df = df[
        [
            "date",
            "round_number",
            "period",
            "unit_code",
            "assigned_power_mw",
            "raw_row_number_in_file",
            "row_number_in_file",
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
                    "error_message": "Filename does not match pibcac pattern",
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
            df = parse_pibcac_file(path)

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
                "market": "mercado_intradiario_continuo",
                "category": "programas",
                "file_family": "pibcac",
                "filename": path.name,
                "parser_name": "mtu.parsing.pibcac.parse_pibcac_file:v1",
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
                "market": "mercado_intradiario_continuo",
                "category": "programas",
                "file_family": "pibcac",
                "filename": path.name,
                "parser_name": "mtu.parsing.pibcac.parse_pibcac_file:v1",
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
