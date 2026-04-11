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

# Files inside OMIE ZIPs are uppercase: IDET_YYYYMMDD.S (S = session number)
# Accept both cases for robustness.
FILENAME_RE = re.compile(r"^(?:IDET|idet)_(\d{8})\.(\d+)$", re.IGNORECASE)

# ── Post-reform format (line length 60) ──────────────────────────────────────
# From 2025-03-19 onwards (MTU15 reform).
# Per OMIE spec section 5.2.4.2 (post-reform):
#   CodOferta    I10    pos  1-10  -> [0:10]
#   Version      I3     pos 11-13  -> [10:13]
#   Codigo       A7     pos 14-20  -> [13:20]
#   NumOfer      I3     pos 21-23  -> [20:23]
#   FechaOfer    I8     pos 24-31  -> [23:31]  (YYYYMMDD, session date)
#   PeriodoOfer  I3     pos 32-34  -> [31:34]  (up to 100 for MTU15)
#   Numbloq      I2     pos 35-36  -> [34:36]
#   PrecEuro     F17.3  pos 37-53  -> [36:53]
#   Potencia     F7.1   pos 54-60  -> [53:60]
_COLSPECS_POST = [
    (0, 10),
    (10, 13),
    (13, 20),
    (20, 23),
    (23, 31),
    (31, 34),
    (34, 36),
    (36, 53),
    (53, 60),
]
_COLNAMES_POST = [
    "offer_code",
    "version",
    "unit_code",
    "offer_number",
    "_session_date",
    "period",
    "block_number",
    "price_eur_mwh",
    "quantity_mw",
]

# ── Pre-reform format (line length 76) ───────────────────────────────────────
# Before 2025-03-19. Verified by byte-level inspection.
#   CodOferta    I10    pos  1-10  -> [0:10]
#   Version      I3     pos 11-13  -> [10:13]
#   Codigo       A7     pos 14-20  -> [13:20]
#   NumOfer      I3     pos 21-23  -> [20:23]
#   FechaOfer    I8     pos 24-31  -> [23:31]  (YYYYMMDD, session date)
#   PeriodoOfer  I2     pos 32-33  -> [31:33]  (up to 25 for MTU60)
#   Numbloq      I2     pos 34-35  -> [33:35]
#   _unused      F17.3  pos 36-52  -> [35:52]  (always 0; not exposed)
#   PrecEuro     F17.3  pos 53-69  -> [52:69]
#   Potencia     F7.1   pos 70-76  -> [69:76]
_COLSPECS_PRE = [
    (0, 10),
    (10, 13),
    (13, 20),
    (20, 23),
    (23, 31),
    (31, 33),
    (33, 35),
    (35, 52),
    (52, 69),
    (69, 76),
]
_COLNAMES_PRE = [
    "offer_code",
    "version",
    "unit_code",
    "offer_number",
    "_session_date",
    "period",
    "block_number",
    "_unused",
    "price_eur_mwh",
    "quantity_mw",
]


def _detect_format(path: Path) -> str:
    """Return 'post' (60-char) or 'pre' (76-char) by inspecting the first data line."""
    with path.open("rb") as f:
        for raw in f:
            line = raw.rstrip(b"\r\n")
            if line:
                if len(line) == 60:
                    return "post"
                if len(line) == 76:
                    return "pre"
                raise ValueError(
                    f"{path.name}: unexpected line length {len(line)} "
                    f"(expected 60 or 76). First line: {line!r}"
                )
    return "post"  # empty file — doesn't matter


def parse_filename_metadata(path: Path) -> dict:
    m = FILENAME_RE.match(path.name)
    if not m:
        raise ValueError(f"Unexpected filename format for idet: {path.name}")

    yyyymmdd, session_str = m.groups()
    file_date = pd.to_datetime(yyyymmdd, format="%Y%m%d").date()

    return {
        "file_date": file_date.isoformat(),
        "session_number": int(session_str),
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


def parse_idet_file(path: Path) -> pd.DataFrame:
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

    df = df.dropna(how="all").reset_index(drop=True)

    if df.empty:
        return df

    # ── Numeric conversions ──────────────────────────────────────────────────
    df["offer_code"] = pd.to_numeric(df["offer_code"].str.strip(), errors="coerce").astype("Int64")
    df["version"] = pd.to_numeric(df["version"].str.strip(), errors="coerce").astype("Int64")
    df["unit_code"] = df["unit_code"].str.strip()
    df["offer_number"] = pd.to_numeric(
        df["offer_number"].str.strip(), errors="coerce"
    ).astype("Int64")
    df["period"] = pd.to_numeric(df["period"].str.strip(), errors="coerce").astype("Int64")
    df["block_number"] = pd.to_numeric(
        df["block_number"].str.strip(), errors="coerce"
    ).astype("Int64")
    df["price_eur_mwh"] = pd.to_numeric(df["price_eur_mwh"].str.strip(), errors="coerce")
    df["quantity_mw"] = pd.to_numeric(df["quantity_mw"].str.strip(), errors="coerce")

    # Drop format-specific columns not carried forward
    drop_cols = ["_session_date"]
    if fmt == "pre":
        drop_cols.append("_unused")
    df = df.drop(columns=drop_cols)

    mtu_minutes = infer_mtu_minutes_from_periods(df["period"])
    validate_period_values(path, df["period"], mtu_minutes)

    df["row_number_in_file"] = range(1, len(df) + 1)
    df["date"] = meta["file_date"]
    df["session_number"] = meta["session_number"]
    df["mtu_minutes"] = mtu_minutes
    df["market"] = "mercado_intradiario_subastas"
    df["category"] = "ofertas"
    df["file_family"] = "idet"
    df["source_file"] = path.name
    df["source_path"] = str(path)

    df = df[
        [
            "date",
            "session_number",
            "offer_code",
            "version",
            "unit_code",
            "offer_number",
            "period",
            "block_number",
            "price_eur_mwh",
            "quantity_mw",
            "row_number_in_file",
            "mtu_minutes",
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
                    "error_message": "Filename does not match idet pattern",
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
            df = parse_idet_file(path)

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
                "market": "mercado_intradiario_subastas",
                "category": "ofertas",
                "file_family": "idet",
                "filename": path.name,
                "parser_name": "mtu.parsing.idet.parse_idet_file:v2",
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
                "market": "mercado_intradiario_subastas",
                "category": "ofertas",
                "file_family": "idet",
                "filename": path.name,
                "parser_name": "mtu.parsing.idet.parse_idet_file:v2",
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
