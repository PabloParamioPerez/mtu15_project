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

FILENAME_RE = re.compile(r"^precios_pibcic_(\d{8})\.(\d+)$")
EMISSION_RE = re.compile(r"Fecha Emisi.n\s*:(\d{2}/\d{2}/\d{4})\s*-\s*(\d{2}:\d{2})")

MTU60_ALLOWED_COUNTS = {23, 24, 25}
MTU15_ALLOWED_COUNTS = {92, 96, 100}


def parse_filename_metadata(path: Path) -> dict:
    m = FILENAME_RE.match(path.name)
    if not m:
        raise ValueError(f"Unexpected filename format for precios_pibcic: {path.name}")

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


def validate_periods(
    path: Path,
    periods: pd.Series,
    mtu_minutes: int,
    *,
    allow_partial_prefix: bool,
) -> tuple[bool, int]:
    p = sorted(set(int(x) for x in periods.tolist()))
    n_periods = len(p)

    if not p:
        raise ValueError(f"{path.name}: no periods found")

    if len(p) != len(periods):
        dup_preview = sorted(periods[periods.duplicated()].astype(int).unique().tolist())[:10]
        raise ValueError(f"{path.name}: duplicated periods found: {dup_preview}")

    lo, hi = p[0], p[-1]

    if mtu_minutes == 60:
        valid_lo, valid_hi = 1, 25
        full_day_allowed_counts = MTU60_ALLOWED_COUNTS
    elif mtu_minutes == 15:
        valid_lo, valid_hi = 1, 100
        full_day_allowed_counts = MTU15_ALLOWED_COUNTS
    else:
        raise ValueError(f"{path.name}: unsupported mtu_minutes={mtu_minutes}")

    if lo < valid_lo or hi > valid_hi:
        raise ValueError(
            f"{path.name}: period range {lo}..{hi} outside valid bounds "
            f"{valid_lo}..{valid_hi} for MTU{mtu_minutes}"
        )

    expected = list(range(lo, hi + 1))
    if p != expected:
        missing = sorted(set(expected) - set(p))
        extras = sorted(set(p) - set(expected))
        raise ValueError(
            f"{path.name}: non-contiguous periods. "
            f"Range={lo}..{hi}, missing={missing[:10]}, extras={extras[:10]}"
        )

    if n_periods in full_day_allowed_counts:
        return False, n_periods

    if allow_partial_prefix and lo == 1:
        return True, n_periods

    raise ValueError(
        f"{path.name}: MTU{mtu_minutes} but period count={n_periods} "
        f"(expected one of {sorted(full_day_allowed_counts)}"
        f"{' or a partial prefix from 1..k' if allow_partial_prefix else ''})"
    )


def parse_precios_pibcic_file(path: Path) -> pd.DataFrame:
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
        if len(parts) >= 13 and parts[0] in {"Año", "Ano"} and parts[1] == "Mes":
            header_idx = i
            header_fields = parts
            break

    if header_idx is None or header_fields is None:
        raise ValueError(f"{path.name}: could not find column header row")

    time_col = header_fields[3].strip()
    if time_col not in {"Hora", "Periodo"}:
        raise ValueError(f"{path.name}: unexpected time column {time_col!r}")

    rows = []
    for line in nonempty[header_idx + 1:]:
        raw_line = line.strip()

        if raw_line == "*" or set(raw_line) <= {";"}:
            continue

        parts = raw_line.split(";")
        if parts and parts[-1] == "":
            parts = parts[:-1]

        if len(parts) != 13:
            raise ValueError(
                f"{path.name}: expected 13 fields, got {len(parts)} -> {parts!r}"
            )

        yyyy, mm, dd, period_raw, max_es, max_pt, max_mo, min_es, min_pt, min_mo, mean_es, mean_pt, mean_mo = parts

        rows.append(
            {
                "year": int(yyyy),
                "month": int(mm),
                "day": int(dd),
                "period": int(period_raw),
                "price_max_es_eur_mwh": parse_decimal(max_es),
                "price_max_pt_eur_mwh": parse_decimal(max_pt),
                "price_max_mo_eur_mwh": parse_decimal(max_mo),
                "price_min_es_eur_mwh": parse_decimal(min_es),
                "price_min_pt_eur_mwh": parse_decimal(min_pt),
                "price_min_mo_eur_mwh": parse_decimal(min_mo),
                "price_mean_es_eur_mwh": parse_decimal(mean_es),
                "price_mean_pt_eur_mwh": parse_decimal(mean_pt),
                "price_mean_mo_eur_mwh": parse_decimal(mean_mo),
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

    # Official logic for this family:
    # - Hora    -> hourly publication
    # - Periodo -> quarter-hour contract numbering
    mtu_minutes = 60 if time_col == "Hora" else 15

    allow_partial_prefix = (
        emission_meta["emission_date"] != ""
        and emission_meta["emission_date"] == meta["file_date"]
    )

    is_partial_day_file, n_periods_in_file = validate_periods(
        path,
        df["period"],
        mtu_minutes,
        allow_partial_prefix=allow_partial_prefix,
    )

    df = df.sort_values(["date", "period"]).reset_index(drop=True)

    df["source_file"] = path.name
    df["source_path"] = str(path)
    df["file_family"] = "precios_pibcic"
    df["market"] = "mercado_intradiario_continuo"
    df["category"] = "precios"
    df["version_suffix"] = meta["version_suffix"]
    df["mtu_minutes"] = mtu_minutes
    df["n_periods_in_file"] = n_periods_in_file
    df["is_partial_day_file"] = bool(is_partial_day_file)
    df["emission_date"] = emission_meta["emission_date"]
    df["emission_time"] = emission_meta["emission_time"]

    df = df[
        [
            "date",
            "period",
            "price_max_es_eur_mwh",
            "price_max_pt_eur_mwh",
            "price_max_mo_eur_mwh",
            "price_min_es_eur_mwh",
            "price_min_pt_eur_mwh",
            "price_min_mo_eur_mwh",
            "price_mean_es_eur_mwh",
            "price_mean_pt_eur_mwh",
            "price_mean_mo_eur_mwh",
            "mtu_minutes",
            "n_periods_in_file",
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
                    "error_message": "Filename does not match precios_pibcic pattern",
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
            df = parse_precios_pibcic_file(path)
            out_path = write_parquet_for_file(df, processed_dir, path.name)

            row = {
                "ingested_at": utc_now_iso(),
                "market": "mercado_intradiario_continuo",
                "category": "precios",
                "file_family": "precios_pibcic",
                "filename": path.name,
                "parser_name": "mtu.parsing.precios_pibcic.parse_precios_pibcic_file:v3",
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
                "file_family": "precios_pibcic",
                "filename": path.name,
                "parser_name": "mtu.parsing.precios_pibcic.parse_precios_pibcic_file:v3",
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
        "file_family": "precios_pibcic",
        "filename": path.name,
        "size_bytes": path.stat().st_size,
        "sha256": sha256_file(path),
        "is_zip": False,
        "file_date": meta["file_date"],
        "version_suffix": meta["version_suffix"],
        "notes": "manual_download_backfill",
    }
