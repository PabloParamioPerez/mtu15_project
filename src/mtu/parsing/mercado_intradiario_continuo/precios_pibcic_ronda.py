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

FILENAME_RE = re.compile(r"^precios_pibcic_ronda_(\d{8})\.(\d+)$")
EMISSION_RE = re.compile(r"Fecha Emisi.n\s*:(\d{2}/\d{2}/\d{4})\s*-\s*(\d{2}:\d{2})")


def parse_filename_metadata(path: Path) -> dict:
    m = FILENAME_RE.match(path.name)
    if not m:
        raise ValueError(f"Unexpected filename format for precios_pibcic_ronda: {path.name}")

    yyyymmdd, version_suffix = m.groups()
    file_date = pd.to_datetime(yyyymmdd, format="%Y%m%d").date()

    return {
        "file_date": file_date.isoformat(),
        "version_suffix": version_suffix,
    }


def parse_emission_metadata(first_line: str) -> dict:
    m = EMISSION_RE.search(first_line)
    if not m:
        return {"emission_date": "", "emission_time": ""}

    emission_date_raw, emission_time = m.groups()
    emission_date = pd.to_datetime(emission_date_raw, format="%d/%m/%Y").date().isoformat()
    return {
        "emission_date": emission_date,
        "emission_time": emission_time,
    }


def parse_precios_pibcic_ronda_file(path: Path) -> pd.DataFrame:
    meta = parse_filename_metadata(path)
    lines = [ln.rstrip("\n\r") for ln in read_text_lines(path)]
    nonempty = [ln.strip() for ln in lines if ln.strip()]

    if not nonempty:
        raise ValueError(f"Empty file: {path}")

    emission_meta = parse_emission_metadata(nonempty[0])

    header_idx = None
    header_fields = None

    for i, line in enumerate(nonempty):
        probe = line.strip().lstrip("\ufeff")
        parts = probe.split(";")
        if len(parts) >= 6 and parts[0] == "Fecha" and parts[1] == "Ronda":
            header_idx = i
            header_fields = parts
            break

    if header_idx is None or header_fields is None:
        raise ValueError(f"{path.name}: could not find column header row")

    time_col = header_fields[2].strip()
    if time_col not in {"Hora", "Periodo"}:
        raise ValueError(f"{path.name}: unexpected time column {time_col!r}")

    mtu_minutes = 60 if time_col == "Hora" else 15
    valid_period_hi = 25 if mtu_minutes == 60 else 100

    rows = []
    for line in nonempty[header_idx + 1:]:
        raw_line = line.strip()

        if raw_line == "*" or set(raw_line) <= {";"}:
            continue

        parts = raw_line.split(";")
        if parts and parts[-1] == "":
            parts = parts[:-1]

        if len(parts) != 6:
            raise ValueError(
                f"{path.name}: expected 6 fields, got {len(parts)} -> {parts!r}"
            )

        date_raw, ronda_raw, period_raw, mean_es, mean_pt, mean_mo = parts
        row_date = pd.to_datetime(date_raw, format="%d/%m/%y").date().isoformat()
        round_number = int(ronda_raw)
        period = int(period_raw)

        if round_number < 1 or round_number > 100:
            raise ValueError(f"{path.name}: invalid round {round_number}")

        if period < 1 or period > valid_period_hi:
            raise ValueError(
                f"{path.name}: invalid period {period} for MTU{mtu_minutes}"
            )

        rows.append(
            {
                "date": row_date,
                "round_number": round_number,
                "period": period,
                "price_mean_es_eur_mwh": parse_decimal(mean_es),
                "price_mean_pt_eur_mwh": parse_decimal(mean_pt),
                "price_mean_mo_eur_mwh": parse_decimal(mean_mo),
            }
        )

    df = pd.DataFrame(rows)

    if df.empty:
        raise ValueError(f"No data rows found in {path.name}")

    unique_dates = sorted(df["date"].drop_duplicates().tolist())

    if len(unique_dates) > 2:
        raise ValueError(f"{path.name}: contains too many dates {unique_dates}")

    if len(unique_dates) == 2:
        d0 = pd.to_datetime(unique_dates[0]).date()
        d1 = pd.to_datetime(unique_dates[1]).date()
        if (d1 - d0).days != 1:
            raise ValueError(f"{path.name}: spans non-adjacent dates {unique_dates}")

    if meta["file_date"] not in unique_dates:
        raise ValueError(
            f"Filename date {meta['file_date']} not present in content dates {unique_dates} in {path.name}"
        )

    dup_mask = df.duplicated(subset=["date", "round_number", "period"])
    if dup_mask.any():
        dups = (
            df.loc[dup_mask, ["date", "round_number", "period"]]
            .drop_duplicates()
            .sort_values(["date", "round_number", "period"])
        )
        dups_preview = [tuple(x) for x in dups.head(10).to_numpy().tolist()]
        more = "..." if len(dups) > 10 else ""
        raise ValueError(
            f"{path.name}: duplicated (date, round_number, period) rows: {dups_preview}{more}"
        )

    df = df.sort_values(["date", "round_number", "period"]).reset_index(drop=True)

    df["mtu_minutes"] = mtu_minutes
    df["is_partial_day_file"] = bool(
        emission_meta["emission_date"] != "" and emission_meta["emission_date"] == meta["file_date"]
    )
    df["emission_date"] = emission_meta["emission_date"]
    df["emission_time"] = emission_meta["emission_time"]
    df["market"] = "mercado_intradiario_continuo"
    df["category"] = "precios"
    df["file_family"] = "precios_pibcic_ronda"
    df["version_suffix"] = meta["version_suffix"]
    df["source_file"] = path.name
    df["source_path"] = str(path)

    df = df[
        [
            "date",
            "round_number",
            "period",
            "price_mean_es_eur_mwh",
            "price_mean_pt_eur_mwh",
            "price_mean_mo_eur_mwh",
            "mtu_minutes",
            "is_partial_day_file",
            "emission_date",
            "emission_time",
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
                    "error_message": "Filename does not match precios_pibcic_ronda pattern",
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
            df = parse_precios_pibcic_ronda_file(path)
            out_path = write_parquet_for_file(df, processed_dir, path.name)

            row = {
                "ingested_at": utc_now_iso(),
                "market": "mercado_intradiario_continuo",
                "category": "precios",
                "file_family": "precios_pibcic_ronda",
                "filename": path.name,
                "parser_name": "mtu.parsing.precios_pibcic_ronda.parse_precios_pibcic_ronda_file:v1",
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
                "category": "precios",
                "file_family": "precios_pibcic_ronda",
                "filename": path.name,
                "parser_name": "mtu.parsing.precios_pibcic_ronda.parse_precios_pibcic_ronda_file:v1",
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
        "market": "mercado_intradiario_continuo",
        "category": "precios",
        "file_family": "precios_pibcic_ronda",
        "filename": path.name,
        "size_bytes": path.stat().st_size,
        "sha256": sha256_file(path),
        "is_zip": False,
        "file_date": meta["file_date"],
        "version_suffix": meta["version_suffix"],
        "notes": "manual_download_backfill",
    }
