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
from mtu.parsing.orders import parse_contract

FILENAME_RE = re.compile(r"^trades_(\d{8})\.(\d+)$")

MARKET = "mercado_intradiario_continuo"
CATEGORY = "transacciones"
FILE_FAMILY = "trades"

# Expected field counts.  The spec (§5.3.2.7, v1.37) lists 16 fields with
# DiaCas/MesCas/AñoCas/HoraCas/MinutoCas/SegundCas as separate integers.
# Older files observed in practice sometimes store the casación timestamp as a
# single formatted string (DD/MM/YYYY HH:MM:SS), giving 11 fields.  Both
# layouts are accepted; the parser normalises to one ISO-format matched_at
# column.
_EXPECTED_FIELDS = {11, 16}

# Regex for the combined casación timestamp.
# Normal form:    "DD/MM/YYYY HH:MM:SS"
# Midnight form:  "DD/MM/YYYY" (OMIE omits the time component when it is 00:00:00)
_CASACION_TS_RE = re.compile(
    r"^(\d{2})/(\d{2})/(\d{4})(?:\s+(\d{2}):(\d{2}):(\d{2}))?$"
)


def parse_filename_metadata(path: Path) -> dict:
    m = FILENAME_RE.match(path.name)
    if not m:
        raise ValueError(f"Unexpected filename format for trades: {path.name}")

    yyyymmdd, version_suffix = m.groups()
    file_date = pd.to_datetime(yyyymmdd, format="%Y%m%d").date()

    return {
        "file_date": file_date.isoformat(),
        "version_suffix": version_suffix,
    }


def _casacion_to_iso(dia: str, mes: str, ano: str, hora: str, minuto: str, segund: str) -> str:
    return (
        f"{int(ano):04d}-{int(mes):02d}-{int(dia):02d}"
        f" {int(hora):02d}:{int(minuto):02d}:{int(segund):02d}"
    )


def _parse_combined_casacion(raw: str) -> str:
    """Parse 'DD/MM/YYYY HH:MM:SS' or 'DD/MM/YYYY' → 'YYYY-MM-DD HH:MM:SS'.

    OMIE omits the time component when the casación moment is exactly 00:00:00.
    """
    raw = raw.strip()
    m = _CASACION_TS_RE.match(raw)
    if not m:
        raise ValueError(f"Unrecognised casación timestamp: {raw!r}")
    dd, mm, yyyy, hh, mi, ss = m.groups()
    if hh is None:
        hh, mi, ss = "0", "0", "0"
    return _casacion_to_iso(dd, mm, yyyy, hh, mi, ss)


def parse_trades_file(path: Path) -> pd.DataFrame:
    meta = parse_filename_metadata(path)
    rows = []
    data_row_counter = 0

    with path.open("r", encoding="latin-1", errors="replace") as f:
        for i, raw_line in enumerate(f):
            line = raw_line.strip()

            if not line or line == "*":
                continue

            if i == 0:
                continue  # title: "OMIE - Mercado de electricidad;..."
            if i == 2:
                continue  # column header row

            if set(line) <= {";"}:
                continue

            parts = line.split(";")
            if parts and parts[-1] == "":
                parts = parts[:-1]

            n = len(parts)
            if n not in _EXPECTED_FIELDS:
                raise ValueError(
                    f"{path.name} line {i + 1}: expected {sorted(_EXPECTED_FIELDS)} fields, "
                    f"got {n} -> {parts!r}"
                )

            data_row_counter += 1

            if n == 16:
                (
                    fecha, contrato, agente_c, unidad_c, zona_comp,
                    agente_v, unidad_v, zona_vent, precio, cantidad,
                    dia_cas, mes_cas, ano_cas, hora_cas, minuto_cas, segund_cas,
                ) = parts
                try:
                    matched_at = _casacion_to_iso(
                        dia_cas.strip(), mes_cas.strip(), ano_cas.strip(),
                        hora_cas.strip(), minuto_cas.strip(), segund_cas.strip(),
                    )
                except (ValueError, TypeError) as exc:
                    raise ValueError(
                        f"{path.name} line {i + 1}: bad casación fields: {exc}"
                    ) from exc
            else:  # n == 11
                (
                    fecha, contrato, agente_c, unidad_c, zona_comp,
                    agente_v, unidad_v, zona_vent, precio, cantidad, casacion_ts,
                ) = parts
                try:
                    matched_at = _parse_combined_casacion(casacion_ts)
                except ValueError as exc:
                    raise ValueError(
                        f"{path.name} line {i + 1}: {exc}"
                    ) from exc

            try:
                contract_meta = parse_contract(contrato)
            except ValueError as exc:
                raise ValueError(f"{path.name} line {i + 1}: {exc}") from exc

            rows.append(
                {
                    "trade_date": pd.to_datetime(fecha.strip(), format="%d/%m/%Y").date().isoformat(),
                    "contract": contrato.strip(),
                    "delivery_date": contract_meta["delivery_date"],
                    "delivery_start": contract_meta["delivery_start"],
                    "period": contract_meta["period"],
                    "mtu_minutes": contract_meta["mtu_minutes"],
                    "dst_suffix": contract_meta["dst_suffix"],
                    "buyer_agent": agente_c.strip(),
                    "buyer_unit": unidad_c.strip(),
                    "buyer_zone": zona_comp.strip(),
                    "seller_agent": agente_v.strip(),
                    "seller_unit": unidad_v.strip(),
                    "seller_zone": zona_vent.strip(),
                    "price_eur_mwh": parse_decimal(precio),
                    "quantity_mw": parse_decimal(cantidad),
                    "matched_at": matched_at,
                    "row_number_in_file": data_row_counter,
                }
            )

    df = pd.DataFrame(rows)

    if df.empty:
        return df

    df["file_date"] = meta["file_date"]
    df["version_suffix"] = meta["version_suffix"]
    df["market"] = MARKET
    df["category"] = CATEGORY
    df["file_family"] = FILE_FAMILY
    df["source_file"] = path.name
    df["source_path"] = str(path)

    df = df[
        [
            "trade_date",
            "contract",
            "delivery_date",
            "delivery_start",
            "period",
            "mtu_minutes",
            "dst_suffix",
            "buyer_agent",
            "buyer_unit",
            "buyer_zone",
            "seller_agent",
            "seller_unit",
            "seller_zone",
            "price_eur_mwh",
            "quantity_mw",
            "matched_at",
            "row_number_in_file",
            "file_date",
            "version_suffix",
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
                    "error_message": "Filename does not match trades pattern",
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
            df = parse_trades_file(path)

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
                "market": MARKET,
                "category": CATEGORY,
                "file_family": FILE_FAMILY,
                "filename": path.name,
                "parser_name": "mtu.parsing.trades.parse_trades_file:v1",
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
                "market": MARKET,
                "category": CATEGORY,
                "file_family": FILE_FAMILY,
                "filename": path.name,
                "parser_name": "mtu.parsing.trades.parse_trades_file:v1",
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
