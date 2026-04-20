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

# Files inside OMIE ZIPs are uppercase: ICAB_YYYYMMDD.S (S = session number)
# Accept both cases for robustness.
FILENAME_RE = re.compile(r"^(?:ICAB|icab)_(\d{8})\.(\d+)$", re.IGNORECASE)

# MTU15 intraday reform date — format changes on this day.
_REFORM_DATE = "2025-03-19"

# ── Post-reform format (line length 94) ──────────────────────────────────────
# From 2025-03-19 onwards.
# Per OMIE spec section 5.2.4.1 (post-reform):
#   CodOferta        I10    pos  1-10  -> [0:10]
#   Version          I3     pos 11-13  -> [10:13]
#   Codigo           A7     pos 14-20  -> [13:20]
#   NumOfer          I3     pos 21-23  -> [20:23]
#   Descripcion      A30    pos 24-53  -> [23:53]
#   CV               A1     pos 54     -> [53:54]
#   Oferta           A1     pos 55     -> [54:55]
#   TipoNec          A3     pos 56-58  -> [55:58]
#   PrcMedBloqOrder  F17.3  pos 59-75  -> [58:75]
#   PorcMinBloqOrder F5.3   pos 76-80  -> [75:80]
#   AñoMod           I4     pos 81-84  -> [80:84]  (4-digit year)
#   MesMod           I2     pos 85-86  -> [84:86]
#   DiaMod           I2     pos 87-88  -> [86:88]
#   HoraMod          I2     pos 89-90  -> [88:90]
#   MinMod           I2     pos 91-92  -> [90:92]
#   SegMod           I2     pos 93-94  -> [92:94]
_COLSPECS_POST = [
    (0, 10),
    (10, 13),
    (13, 20),
    (20, 23),
    (23, 53),
    (53, 54),
    (54, 55),
    (55, 58),
    (58, 75),
    (75, 80),
    (80, 84),
    (84, 86),
    (86, 88),
    (88, 90),
    (90, 92),
    (92, 94),
]
_COLNAMES_POST = [
    "offer_code",
    "version",
    "unit_code",
    "offer_number",
    "description",
    "buy_sell",
    "offer_type",
    "need_type",
    "block_order_avg_price_eur",
    "block_order_min_pct",
    "_ins_year",
    "_ins_month",
    "_ins_day",
    "_ins_hour",
    "_ins_minute",
    "_ins_second",
]

# ── Pre-reform format (line length 195) ──────────────────────────────────────
# Before 2025-03-19. Verified by byte-level inspection.
# Header fields share the same positions as post-reform [0:58].
# Positions [58:178] contain complex offer conditions — parsed but not exposed.
# CodInt       I2    pos 179-180 -> [178:180]
# Timestamp    14ch  pos 181-194 -> [180:194]  (YYYYMMDDHHMMSS, single field)
# Flag         A1    pos 195     -> [194:195]   (always 'N')
_COLSPECS_PRE = [
    (0, 10),
    (10, 13),
    (13, 20),
    (20, 23),
    (23, 53),
    (53, 54),
    (54, 55),
    (55, 58),
    (178, 180),
    (180, 194),
]
_COLNAMES_PRE = [
    "offer_code",
    "version",
    "unit_code",
    "offer_number",
    "description",
    "buy_sell",
    "offer_type",
    "need_type",
    "interconnection_code",
    "_ins_str",
]


def _detect_format(path: Path) -> str:
    """Return 'post' (94-char) or 'pre' (195-char) by inspecting the first data line."""
    with path.open("rb") as f:
        for raw in f:
            line = raw.rstrip(b"\r\n")
            if line:
                if len(line) == 94:
                    return "post"
                if len(line) == 195:
                    return "pre"
                raise ValueError(
                    f"{path.name}: unexpected line length {len(line)} "
                    f"(expected 94 or 195). First line: {line!r}"
                )
    return "post"  # empty file — doesn't matter


def parse_filename_metadata(path: Path) -> dict:
    m = FILENAME_RE.match(path.name)
    if not m:
        raise ValueError(f"Unexpected filename format for icab: {path.name}")

    yyyymmdd, session_str = m.groups()
    file_date = pd.to_datetime(yyyymmdd, format="%Y%m%d").date()

    return {
        "file_date": file_date.isoformat(),
        "session_number": int(session_str),
    }


def parse_icab_file(path: Path) -> pd.DataFrame:
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
    df["description"] = df["description"].str.strip()
    df["buy_sell"] = df["buy_sell"].str.strip()
    df["offer_type"] = df["offer_type"].str.strip()
    df["need_type"] = df["need_type"].str.strip()

    # ── Timestamp ────────────────────────────────────────────────────────────
    if fmt == "post":
        ins_cols = [
            "_ins_year", "_ins_month", "_ins_day",
            "_ins_hour", "_ins_minute", "_ins_second",
        ]
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

        df["block_order_avg_price_eur"] = pd.to_numeric(
            df["block_order_avg_price_eur"].str.strip(), errors="coerce"
        )
        df["block_order_min_pct"] = pd.to_numeric(
            df["block_order_min_pct"].str.strip(), errors="coerce"
        )
        df["interconnection_code"] = pd.array([pd.NA] * len(df), dtype="Int64")
    else:
        df["inserted_at"] = pd.to_datetime(
            df["_ins_str"].str.strip(), format="%Y%m%d%H%M%S", errors="coerce"
        )
        df["interconnection_code"] = pd.to_numeric(
            df["interconnection_code"].str.strip(), errors="coerce"
        ).astype("Int64")
        df = df.drop(columns=["_ins_str"])

        # Block-order fields not available in pre-reform format
        df["block_order_avg_price_eur"] = float("nan")
        df["block_order_min_pct"] = float("nan")

    df["row_number_in_file"] = range(1, len(df) + 1)
    df["date"] = meta["file_date"]
    df["session_number"] = meta["session_number"]
    df["market"] = "mercado_intradiario_subastas"
    df["category"] = "ofertas"
    df["file_family"] = "icab"
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
            "description",
            "buy_sell",
            "offer_type",
            "need_type",
            "block_order_avg_price_eur",
            "block_order_min_pct",
            "interconnection_code",
            "inserted_at",
            "row_number_in_file",
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
                    "error_message": "Filename does not match icab pattern",
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
            df = parse_icab_file(path)

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
                "file_family": "icab",
                "filename": path.name,
                "parser_name": "mtu.parsing.icab.parse_icab_file:v2",
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
                "file_family": "icab",
                "filename": path.name,
                "parser_name": "mtu.parsing.icab.parse_icab_file:v2",
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
