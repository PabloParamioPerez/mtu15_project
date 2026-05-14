"""Parse ESIOS Indisponibilidades .xls snapshots.

Archive id=105: per-snapshot Excel workbook listing all known generation
and consumption-unit outages as of the request date. The file is an
MS-CFB (old-style .xls) with sheets:

    - Índice         (cover)
    - Generación     (outages: ~150 rows per snapshot)
    - Consumo        (often empty)
    - Motivos        (reason-code legend)

Generación columns:
    Tipo unidad        UF (physical) / UP (programming)
    Fecha de inicio    outage start (Europe/Madrid local)
    Fecha de fin       outage end
    Nombre unidad      unit code or human name
    Código motivo      reason code (A95 = scheduled maintenance, B19 = forced, ...)
    Indicadores        flags (mostly NaN)
    Potencia instalada MW installed
    Potencia disponible MW available during the outage window

Each snapshot is FORWARD-LOOKING: a 2025-06-15 snapshot may list outages
ending in 2028. We tag each row with `snapshot_date` so downstream code
can dedup or pick the latest view of any (unit, start, end) outage.

Output schema (long format, one row per outage event per snapshot):
    snapshot_date         date of the .xls file
    unit_type             "UF" or "UP"
    unit_name             outage unit (Nombre unidad)
    reason_code           Código motivo (A95, B19, ...)
    flags                 Indicadores (free text or None)
    start_local           outage start (timezone-naive, interpreted as Europe/Madrid)
    end_local             outage end
    capacity_installed_mw float
    capacity_available_mw float
    source_file           input .xls filename
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd

EXPECTED_COLS = [
    "Tipo unidad", "Fecha de inicio", "Fecha de fin",
    "Nombre unidad", "Código motivo", "Indicadores",
    "Potencia instalada", "Potencia disponible",
]


def parse_indisponibilidades_xls(path: Path, snapshot_date: str) -> pd.DataFrame:
    """Read one Indisponibilidades .xls snapshot into long format.

    `snapshot_date` is ISO YYYY-MM-DD — typically derived from the filename
    (`indisponibilidades_<yyyymmdd>.xls`).
    """
    try:
        xl = pd.ExcelFile(path, engine="xlrd")
    except Exception as e:  # noqa: BLE001
        raise RuntimeError(f"cannot open {path.name}: {e}") from e

    if "Generación" not in xl.sheet_names:
        return _empty_frame()

    df = pd.read_excel(xl, sheet_name="Generación")
    if df.empty or list(df.columns)[:1] == ["El informe no ha devuelto datos"]:
        return _empty_frame()

    # Rename to a stable English-snake-case schema.
    rename = {
        "Tipo unidad":        "unit_type",
        "Fecha de inicio":    "start_local",
        "Fecha de fin":       "end_local",
        "Nombre unidad":      "unit_name",
        "Código motivo":      "reason_code",
        "Indicadores":        "flags",
        "Potencia instalada": "capacity_installed_mw",
        "Potencia disponible": "capacity_available_mw",
    }
    missing = set(EXPECTED_COLS) - set(df.columns)
    if missing:
        raise RuntimeError(f"{path.name}: missing columns {missing}")
    df = df.rename(columns=rename)[list(rename.values())].copy()

    df["start_local"] = pd.to_datetime(df["start_local"], errors="coerce")
    df["end_local"]   = pd.to_datetime(df["end_local"],   errors="coerce")
    df["capacity_installed_mw"]  = pd.to_numeric(df["capacity_installed_mw"], errors="coerce")
    df["capacity_available_mw"] = pd.to_numeric(df["capacity_available_mw"], errors="coerce")
    df["snapshot_date"] = pd.to_datetime(snapshot_date).date()
    df["source_file"]   = path.name

    cols = ["snapshot_date", "unit_type", "unit_name", "reason_code", "flags",
            "start_local", "end_local",
            "capacity_installed_mw", "capacity_available_mw", "source_file"]
    return df[cols].reset_index(drop=True)


def _empty_frame() -> pd.DataFrame:
    return pd.DataFrame({
        "snapshot_date":          pd.Series([], dtype="object"),
        "unit_type":              pd.Series([], dtype="object"),
        "unit_name":              pd.Series([], dtype="object"),
        "reason_code":            pd.Series([], dtype="object"),
        "flags":                  pd.Series([], dtype="object"),
        "start_local":            pd.Series([], dtype="datetime64[ns]"),
        "end_local":              pd.Series([], dtype="datetime64[ns]"),
        "capacity_installed_mw":  pd.Series([], dtype="float64"),
        "capacity_available_mw":  pd.Series([], dtype="float64"),
        "source_file":            pd.Series([], dtype="object"),
    })
