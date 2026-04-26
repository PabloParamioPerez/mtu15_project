"""Parse Curvas_Ofertas_aFRR daily .xls offer-curve workbook into long format.

Schema (id=234 archive, 2024-11-20 → present):

Each daily file is a .xls (OLE2 compound document) with one sheet
"Sheet1" containing the system-aggregate aFRR offer curve at PT15M
resolution. Layout:
    row 0:  ; ; ;Dia;<date>
    row 1:  Cuarto de Hora del dia;Sentido;Precio (€/MW);Indicadores;Potencia ofertada (MW)
    row 2+: data rows

`Cuarto de Hora del dia` = ISP within day (1..96, 92 on DST-spring,
100 on DST-fall). `Sentido` ∈ {"Subir", "Bajar"}. Each row is one
tranche of the system-aggregate aFRR offer curve for that ISP×side.
**No per-unit/per-firm identifier** in this archive.

Output schema (long format):
    date              ISO date (from filename)
    isp               1..96+ (Cuarto de Hora del dia)
    direction         "Subir" / "Bajar"
    price_eur_mw      tranche price (€/MW for capacity, €/MWh for energy
                      depending on REE settlement convention)
    mw                tranche quantity (MW)
    source_file       inner XLS filename
"""
from __future__ import annotations

import re
from pathlib import Path

import pandas as pd


_FILENAME_RE = re.compile(r"curvas_ofertas_afrr_(\d{8})\.xls$")


def parse_curvas_ofertas_afrr_xls(path: Path) -> pd.DataFrame:
    """Parse one daily .xls workbook. Returns long-format DataFrame."""
    m = _FILENAME_RE.search(path.name)
    if m is None:
        return pd.DataFrame()
    date = pd.to_datetime(m.group(1), format="%Y%m%d").date()

    try:
        df = pd.read_excel(path, engine="xlrd", header=1)
    except Exception as e:
        print(f"[WARN parse] {path.name}: xlrd error {e}")
        return pd.DataFrame()

    df = df.loc[:, ~df.columns.astype(str).str.startswith("Unnamed")]
    rename = {
        "Cuarto de Hora del dia": "isp",
        "Sentido": "direction",
        "Precio (€/MW)": "price_eur_mw",
        "Potencia ofertada (MW)": "mw",
    }
    df = df.rename(columns=rename)
    expected = {"isp", "direction", "price_eur_mw", "mw"}
    if not expected.issubset(df.columns):
        return pd.DataFrame()

    out = pd.DataFrame({
        "date": date,
        "isp": pd.to_numeric(df["isp"], errors="coerce").astype("Int64"),
        "direction": df["direction"].astype(str).str.strip(),
        "price_eur_mw": pd.to_numeric(df["price_eur_mw"], errors="coerce"),
        "mw": pd.to_numeric(df["mw"], errors="coerce"),
        "source_file": path.name,
    })
    return out
