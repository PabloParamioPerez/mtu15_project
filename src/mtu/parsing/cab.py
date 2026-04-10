from __future__ import annotations

import re
from datetime import date
from pathlib import Path

import pandas as pd

from mtu.parsing.omie_common import (
    append_csv_row,
    ensure_dir,
    utc_now_iso,
    visible_files,
)

# Files inside OMIE zips are uppercase: CAB_YYYYMMDD.V
# Accept both cases for robustness.
FILENAME_RE = re.compile(r"^(?:CAB|cab)_(\d{8})\.(\d+)$", re.IGNORECASE)

# Day-ahead market MTU15 reform date (1 October 2025)
_REFORM_DATE = date(2025, 10, 1)

# Fixed-width column specs (0-indexed half-open intervals), per OMIE spec section 5.1.4.1
# Field layout (post-reform, same widths pre-reform):
#   CodOferta   I10   pos  1-10  -> [0:10]
#   Version     I5    pos 11-15  -> [10:15]
#   Código      A7    pos 16-22  -> [15:22]
#   Descripción A30   pos 23-52  -> [22:52]
#   CV          A1    pos 53     -> [52:53]
#   OferPlazo   A1    pos 54     -> [53:54]
#   Fijoeuro    F17.3 pos 55-71  -> [54:71]
#   MaxPot      F7.1  pos 72-78  -> [71:78]
#   CodInt      I2    pos 79-80  -> [78:80]
#   Año         I4    pos 81-84  -> [80:84]
#   Mes         I2    pos 85-86  -> [84:86]
#   Día         I2    pos 87-88  -> [86:88]
#   Hora        I2    pos 89-90  -> [88:90]
#   Minuto      I2    pos 91-92  -> [90:92]
#   Segundo     I2    pos 93-94  -> [92:94]
_COLSPECS = [
    (0, 10),
    (10, 15),
    (15, 22),
    (22, 52),
    (52, 53),
    (53, 54),
    (54, 71),
    (71, 78),
    (78, 80),
    (80, 84),
    (84, 86),
    (86, 88),
    (88, 90),
    (90, 92),
    (92, 94),
]
_COLNAMES = [
    "offer_code",
    "version",
    "unit_code",
    "description",
    "buy_sell",
    "offer_plazo",
    "fixed_term_eur",
    "max_power_mw",
    "interconnection_code",
    "_ins_year",
    "_ins_month",
    "_ins_day",
    "_ins_hour",
    "_ins_minute",
    "_ins_second",
]


def parse_filename_metadata(path: Path) -> dict:
    m = FILENAME_RE.match(path.name)
    if not m:
        raise ValueError(f"Unexpected filename format for cab: {path.name}")

    yyyymmdd, version_suffix = m.groups()
    file_date = pd.to_datetime(yyyymmdd, format="%Y%m%d").date()

    return {
        "file_date": file_date.isoformat(),
        "version_suffix": version_suffix,
    }


def infer_mtu_minutes_from_date(session_date: date) -> int:
    return 15 if session_date >= _REFORM_DATE else 60


def parse_cab_file(path: Path) -> pd.DataFrame:
    meta = parse_filename_metadata(path)
    session_date = pd.to_datetime(meta["file_date"]).date()

    df = pd.read_fwf(
        path,
        colspecs=_COLSPECS,
        names=_COLNAMES,
        header=None,
        encoding="latin-1",
        dtype=str,
    )

    if df.empty:
        return df

    # Drop fully-empty rows (trailing newlines, etc.)
    df = df.dropna(how="all").reset_index(drop=True)

    if df.empty:
        return df

    df["offer_code"] = pd.to_numeric(df["offer_code"].str.strip(), errors="coerce").astype("Int64")
    df["version"] = pd.to_numeric(df["version"].str.strip(), errors="coerce").astype("Int64")
    df["unit_code"] = df["unit_code"].str.strip()
    df["description"] = df["description"].str.strip()
    df["buy_sell"] = df["buy_sell"].str.strip()
    df["offer_plazo"] = df["offer_plazo"].str.strip()
    df["fixed_term_eur"] = pd.to_numeric(df["fixed_term_eur"].str.strip(), errors="coerce")
    df["max_power_mw"] = pd.to_numeric(df["max_power_mw"].str.strip(), errors="coerce")
    df["interconnection_code"] = pd.to_numeric(
        df["interconnection_code"].str.strip(), errors="coerce"
    ).astype("Int64")

    # Build insertion timestamp from components
    ins_cols = ["_ins_year", "_ins_month", "_ins_day", "_ins_hour", "_ins_minute", "_ins_second"]
    ins_parts = df[ins_cols].apply(lambda s: s.str.strip())
    ins_str = (
        ins_parts["_ins_year"].str.zfill(4)
        + ins_parts["_ins_month"].str.zfill(2)
        + ins_parts["_ins_day"].str.zfill(2)
        + ins_parts["_ins_hour"].str.zfill(2)
        + ins_parts["_ins_minute"].str.zfill(2)
        + ins_parts["_ins_second"].str.zfill(2)
    )
    df["inserted_at"] = pd.to_datetime(ins_str, format="%Y%m%d%H%M%S", errors="coerce")

    df = df.drop(columns=ins_cols)

    df["row_number_in_file"] = range(1, len(df) + 1)
    df["date"] = meta["file_date"]
    df["mtu_minutes"] = infer_mtu_minutes_from_date(session_date)
    df["market"] = "mercado_diario"
    df["category"] = "ofertas"
    df["file_family"] = "cab"
    df["version_suffix"] = meta["version_suffix"]
    df["source_file"] = path.name
    df["source_path"] = str(path)

    df = df[
        [
            "date",
            "offer_code",
            "version",
            "unit_code",
            "description",
            "buy_sell",
            "offer_plazo",
            "fixed_term_eur",
            "max_power_mw",
            "interconnection_code",
            "inserted_at",
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
                    "error_message": "Filename does not match cab pattern",
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
            df = parse_cab_file(path)

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
                "market": "mercado_diario",
                "category": "ofertas",
                "file_family": "cab",
                "filename": path.name,
                "parser_name": "mtu.parsing.cab.parse_cab_file:v1",
                "raw_file_kind": "omie_fixed_width",
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
                "category": "ofertas",
                "file_family": "cab",
                "filename": path.name,
                "parser_name": "mtu.parsing.cab.parse_cab_file:v1",
                "raw_file_kind": "omie_fixed_width",
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
