from __future__ import annotations

import re
from pathlib import Path

import pandas as pd

from mtu.parsing.omie_common import (
    append_csv_row,
    ensure_dir,
    utc_now_iso,
    visible_files,
)

# Files inside OMIE zips are uppercase: DET_YYYYMMDD.V
# Accept both cases for robustness.
FILENAME_RE = re.compile(r"^(?:DET|det)_(\d{8})\.(\d+)$", re.IGNORECASE)

# ── Post-reform format (line length 60) ─────────────────────────────────────
# Adopted by OMIE on 2025-03-19 (same day as intraday MTU15 reform).
# Per OMIE spec section 5.1.4.2 (post-reform):
#   CodOferta     I10   pos  1-10  -> [0:10]
#   Version       I5    pos 11-15  -> [10:15]
#   Período       I3    pos 16-18  -> [15:18]
#   NumBlock      I2    pos 19-20  -> [18:20]
#   NumTramo      I2    pos 21-22  -> [20:22]
#   NumGrupoExcl  I2    pos 23-24  -> [22:24]
#   PrecEuro      F17.3 pos 25-41  -> [24:41]
#   Cantidad      F7.1  pos 42-48  -> [41:48]
#   MAV           F7.1  pos 49-55  -> [48:55]
#   MAR           F5.3  pos 56-60  -> [55:60]
_COLSPECS_POST = [
    (0, 10),
    (10, 15),
    (15, 18),
    (18, 20),
    (20, 22),
    (22, 24),
    (24, 41),
    (41, 48),
    (48, 55),
    (55, 60),
]
_COLNAMES_POST = [
    "offer_code",
    "version",
    "period",
    "block_number",
    "segment_number",
    "exclusive_group",
    "price_eur_mwh",
    "quantity_mw",
    "min_acceptable_volume_mw",
    "min_acceptable_ratio",
]

# ── Pre-reform format (line length 57) ──────────────────────────────────────
# Used for sessions before 2025-03-19.
#   CodOferta     I7    pos  1-7   -> [0:7]
#   Version       I3    pos  8-10  -> [7:10]
#   Período       I2    pos 11-12  -> [10:12]
#   NumTramo      I2    pos 13-14  -> [12:14]
#   PrecEuro      F17.3 pos 15-31  -> [14:31]
#   Cantidad      F17.3 pos 32-48  -> [31:48]  (wider field in old format)
#   MAV           F7.1  pos 49-55  -> [48:55]
#   _marker       A2    pos 56-57  -> [55:57]  (always 'SS'; no MAR in old format)
_COLSPECS_PRE = [
    (0, 7),
    (7, 10),
    (10, 12),
    (12, 14),
    (14, 31),
    (31, 48),
    (48, 55),
    (55, 57),
]
_COLNAMES_PRE = [
    "offer_code",
    "version",
    "period",
    "segment_number",
    "price_eur_mwh",
    "quantity_mw",
    "min_acceptable_volume_mw",
    "_marker",
]


def _detect_format(path: Path) -> str:
    """Return 'post' or 'pre' by inspecting the length of the first data line."""
    with path.open("rb") as f:
        for raw in f:
            line = raw.rstrip(b"\r\n")
            if line:
                if len(line) == 60:
                    return "post"
                if len(line) == 57:
                    return "pre"
                raise ValueError(
                    f"{path.name}: unexpected line length {len(line)} "
                    f"(expected 57 or 60). First line: {line!r}"
                )
    return "post"  # empty file — doesn't matter


def parse_filename_metadata(path: Path) -> dict:
    m = FILENAME_RE.match(path.name)
    if not m:
        raise ValueError(f"Unexpected filename format for det: {path.name}")

    yyyymmdd, version_suffix = m.groups()
    file_date = pd.to_datetime(yyyymmdd, format="%Y%m%d").date()

    return {
        "file_date": file_date.isoformat(),
        "version_suffix": version_suffix,
    }


def infer_mtu_minutes_from_periods(periods: pd.Series) -> int:
    hi = int(periods.max())

    if hi <= 25:
        return 60
    if hi <= 100:
        return 15

    raise ValueError(f"Unexpected maximum period value {hi}")


def validate_period_values(path: Path, periods: pd.Series, mtu_minutes: int) -> None:
    lo, hi = int(periods.min()), int(periods.max())

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


def parse_det_file(path: Path) -> pd.DataFrame:
    meta = parse_filename_metadata(path)
    fmt = _detect_format(path)

    if fmt == "post":
        colspecs = _COLSPECS_POST
        names = _COLNAMES_POST
    else:
        colspecs = _COLSPECS_PRE
        names = _COLNAMES_PRE

    df = pd.read_fwf(
        path,
        colspecs=colspecs,
        names=names,
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

    # ── Numeric conversions ──────────────────────────────────────────────────
    df["offer_code"] = pd.to_numeric(df["offer_code"].str.strip(), errors="coerce").astype("Int64")
    df["version"] = pd.to_numeric(df["version"].str.strip(), errors="coerce").astype("Int64")
    df["period"] = pd.to_numeric(df["period"].str.strip(), errors="coerce").astype("Int64")
    df["segment_number"] = pd.to_numeric(
        df["segment_number"].str.strip(), errors="coerce"
    ).astype("Int64")
    df["price_eur_mwh"] = pd.to_numeric(df["price_eur_mwh"].str.strip(), errors="coerce")
    df["quantity_mw"] = pd.to_numeric(df["quantity_mw"].str.strip(), errors="coerce")
    df["min_acceptable_volume_mw"] = pd.to_numeric(
        df["min_acceptable_volume_mw"].str.strip(), errors="coerce"
    )

    if fmt == "post":
        df["block_number"] = pd.to_numeric(
            df["block_number"].str.strip(), errors="coerce"
        ).astype("Int64")
        df["exclusive_group"] = pd.to_numeric(
            df["exclusive_group"].str.strip(), errors="coerce"
        ).astype("Int64")
        df["min_acceptable_ratio"] = pd.to_numeric(
            df["min_acceptable_ratio"].str.strip(), errors="coerce"
        )
    else:
        # Pre-reform format has no block_number/exclusive_group/MAR fields.
        # The '_marker' column is always 'SS' and has no numeric meaning.
        df["block_number"] = pd.array([0] * len(df), dtype="Int64")
        df["exclusive_group"] = pd.array([0] * len(df), dtype="Int64")
        df["min_acceptable_ratio"] = float("nan")
        df = df.drop(columns=["_marker"])

    mtu_minutes = infer_mtu_minutes_from_periods(df["period"])
    validate_period_values(path, df["period"], mtu_minutes)

    df["row_number_in_file"] = range(1, len(df) + 1)
    df["date"] = meta["file_date"]
    df["mtu_minutes"] = mtu_minutes
    df["market"] = "mercado_diario"
    df["category"] = "ofertas"
    df["file_family"] = "det"
    df["version_suffix"] = meta["version_suffix"]
    df["source_file"] = path.name
    df["source_path"] = str(path)

    df = df[
        [
            "date",
            "offer_code",
            "version",
            "period",
            "block_number",
            "segment_number",
            "exclusive_group",
            "price_eur_mwh",
            "quantity_mw",
            "min_acceptable_volume_mw",
            "min_acceptable_ratio",
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
                    "error_message": "Filename does not match det pattern",
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
            df = parse_det_file(path)

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
                "file_family": "det",
                "filename": path.name,
                "parser_name": "mtu.parsing.det.parse_det_file:v1",
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
                "file_family": "det",
                "filename": path.name,
                "parser_name": "mtu.parsing.det.parse_det_file:v1",
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
