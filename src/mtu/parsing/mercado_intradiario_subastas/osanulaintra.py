from __future__ import annotations

import re
from pathlib import Path

import pandas as pd

from mtu.parsing.omie_common import (
    append_csv_row,
    ensure_dir,
    read_text_lines,
    sha256_file,
    utc_now_iso,
    visible_files,
)

FILENAME_RE = re.compile(r"^osanulaintra_(\d{8})(\d{2})\.(\d+)$")
FILE_FAMILY = "osanulaintra"
MARKET = "mercado_intradiario_subastas"

_COLS = [
    "date", "session_number", "version_suffix", "source_file",
    "file_family", "market", "año", "mes", "dia", "periodo",
    "row_order", "mtu_minutes", "parsed_at",
]


def parse_filename_metadata(path: Path) -> dict:
    m = FILENAME_RE.match(path.name)
    if not m:
        raise ValueError(f"Unexpected filename format for {FILE_FAMILY}: {path.name}")
    yyyymmdd, session_str, version_suffix = m.groups()
    file_date = pd.to_datetime(yyyymmdd, format="%Y%m%d").date()
    return {
        "file_date": file_date.isoformat(),
        "session_number": int(session_str),
        "version_suffix": version_suffix,
    }


def infer_mtu_minutes(max_periodo: int) -> int:
    if max_periodo <= 25:
        return 60
    if max_periodo <= 100:
        return 15
    raise ValueError(f"Cannot infer MTU from max periodo={max_periodo}")


def parse_osanulaintra_file(path: Path) -> pd.DataFrame:
    """
    Parse one OMIE OSANULAINTRA raw file.

    Format:
      OSANULAINTRA;
      año;mes;dia;hora_emision;minuto_emision;sesion;version;  <- metadata row (skip; some fields may be empty)
      año;mes;dia;periodo;   <- data rows (0 or more cancelled periods)
      ...
      *
    """
    meta = parse_filename_metadata(path)
    lines = read_text_lines(path)
    content_lines = [ln.strip() for ln in lines if ln.strip() and ln.strip() != "*"]
    # content_lines[0] = family identifier line
    # content_lines[1] = metadata row (7 fields, some may be empty in newer files)
    # content_lines[2:] = cancelled period rows (año;mes;dia;periodo;)

    rows = []
    for line in content_lines[2:]:
        parts = [p.strip() for p in line.rstrip(";").split(";")]
        if len(parts) != 4:
            continue
        try:
            año, mes, dia, periodo = int(parts[0]), int(parts[1]), int(parts[2]), int(parts[3])
        except ValueError:
            continue
        rows.append({"año": año, "mes": mes, "dia": dia, "periodo": periodo})

    if not rows:
        return pd.DataFrame(columns=_COLS)

    df = pd.DataFrame(rows)
    df["row_order"] = range(1, len(df) + 1)
    df["date"] = meta["file_date"]
    df["session_number"] = meta["session_number"]
    df["version_suffix"] = meta["version_suffix"]
    df["source_file"] = path.name
    df["file_family"] = FILE_FAMILY
    df["market"] = MARKET
    df["mtu_minutes"] = infer_mtu_minutes(int(df["periodo"].max()))
    df["parsed_at"] = utc_now_iso()

    return df[_COLS]


def _append_ingestion_row(
    ingestion_log_csv: Path, path: Path, status: str, n_rows: int, error_msg: str = ""
) -> None:
    append_csv_row(
        ingestion_log_csv,
        {
            "parsed_at": utc_now_iso(),
            "source_file": path.name,
            "file_family": FILE_FAMILY,
            "status": status,
            "n_rows": n_rows,
            "error_msg": error_msg,
        },
    )


def parse_folder_and_write(
    raw_dir: Path,
    processed_dir: Path,
    ingestion_log_csv: Path,
) -> pd.DataFrame:
    ensure_dir(processed_dir)
    raw_files = [p for p in visible_files(raw_dir) if FILENAME_RE.match(p.name)]

    summary_rows = []
    for path in raw_files:
        out_path = processed_dir / (path.name + ".parquet")
        if out_path.exists():
            summary_rows.append({"source_file": path.name, "status": "skipped", "n_rows": None, "error": ""})
            continue
        try:
            df = parse_osanulaintra_file(path)
            df.to_parquet(out_path, index=False)
            _append_ingestion_row(ingestion_log_csv, path, "success", len(df))
            summary_rows.append({"source_file": path.name, "status": "success", "n_rows": len(df), "error": ""})
        except Exception as e:
            _append_ingestion_row(ingestion_log_csv, path, "failed", 0, str(e))
            summary_rows.append({"source_file": path.name, "status": "failed", "n_rows": 0, "error": str(e)})

    return pd.DataFrame(summary_rows)
