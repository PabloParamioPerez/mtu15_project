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

FILENAME_RE = re.compile(r"^pibca_(\d{8})(\d{2})\.(\d+)$")


def parse_filename_metadata(path: Path) -> dict:
    m = FILENAME_RE.match(path.name)
    if not m:
        raise ValueError(f"Unexpected filename format for pibca: {path.name}")

    yyyymmdd, session_number, version_suffix = m.groups()
    file_date = pd.to_datetime(yyyymmdd, format="%Y%m%d").date()

    return {
        "file_date": file_date.isoformat(),
        "session_number_from_filename": int(session_number),
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


def parse_pibca_file(path: Path) -> pd.DataFrame:
    meta = parse_filename_metadata(path)
    rows = []

    with path.open("r", encoding="latin-1", errors="replace") as f:
        for i, raw_line in enumerate(f):
            line = raw_line.strip()

            if not line:
                continue

            if i == 0:
                if line != "PIBCA;":
                    raise ValueError(f"{path.name}: expected first line 'PIBCA;', got {line!r}")
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

            yyyy, mm, dd, period, session_number, unit_code, assigned_power, unused_zero, offer_type = parts

            rows.append(
                {
                    "year": int(yyyy),
                    "month": int(mm),
                    "day": int(dd),
                    "period": int(period),
                    "session_number": int(session_number),
                    "unit_code": unit_code.strip(),
                    "assigned_power_mw": parse_decimal(assigned_power),
                    "unused_zero": int(unused_zero),
                    "offer_type": int(offer_type),
                }
            )

    df = pd.DataFrame(rows)

    if df.empty:
        raise ValueError(f"No data rows found in {path.name}")

    rows_before_dedup = len(df)
    df = df.drop_duplicates().reset_index(drop=True)
    exact_duplicate_rows_dropped = rows_before_dedup - len(df)

    if df["session_number"].nunique() != 1:
        raise ValueError(
            f"{path.name}: multiple session_number values found in content: "
            f"{sorted(df['session_number'].unique().tolist())}"
        )

    session_number_content = int(df["session_number"].iloc[0])
    if session_number_content != meta["session_number_from_filename"]:
        raise ValueError(
            f"{path.name}: filename session_number={meta['session_number_from_filename']} "
            f"!= content session_number={session_number_content}"
        )

    df["date"] = pd.to_datetime(df[["year", "month", "day"]]).dt.date.astype(str)

    validate_content_dates(path, df)

    dup_mask = df.duplicated(subset=["date", "session_number", "period", "unit_code"])
    if dup_mask.any():
        dups = (
            df.loc[dup_mask, ["date", "session_number", "period", "unit_code"]]
            .drop_duplicates()
            .sort_values(["date", "session_number", "period", "unit_code"])
        )
        dups_preview = [tuple(x) for x in dups.head(10).to_numpy().tolist()]
        more = "..." if len(dups) > 10 else ""
        raise ValueError(
            f"{path.name}: duplicated (date, session_number, period, unit_code) rows: "
            f"{dups_preview}{more}"
        )

    mtu_minutes = infer_mtu_minutes_from_periods(df["period"])
    validate_period_values(path, df["period"], mtu_minutes)

    df = df.sort_values(["date", "session_number", "period", "unit_code"]).reset_index(drop=True)

    df["exact_duplicate_rows_dropped"] = exact_duplicate_rows_dropped
    df["mtu_minutes"] = mtu_minutes
    df["market"] = "mercado_intradiario_subastas"
    df["category"] = "programas"
    df["file_family"] = "pibca"
    df["version_suffix"] = meta["version_suffix"]
    df["source_file"] = path.name
    df["source_path"] = str(path)

    df = df[
        [
            "date",
            "session_number",
            "period",
            "unit_code",
            "assigned_power_mw",
            "unused_zero",
            "offer_type",
            "exact_duplicate_rows_dropped",
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
                    "error_message": "Filename does not match pibca pattern",
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
            df = parse_pibca_file(path)
            out_path = write_parquet_for_file(df, processed_dir, path.name)

            row = {
                "ingested_at": utc_now_iso(),
                "market": "mercado_intradiario_subastas",
                "category": "programas",
                "file_family": "pibca",
                "filename": path.name,
                "parser_name": "mtu.parsing.pibca.parse_pibca_file:v1",
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
                "market": "mercado_intradiario_subastas",
                "category": "programas",
                "file_family": "pibca",
                "filename": path.name,
                "parser_name": "mtu.parsing.pibca.parse_pibca_file:v1",
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
