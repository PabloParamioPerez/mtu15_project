"""Parse ESIOS liquicierre + liquicierresrs aFRR settlement XML files.

Two archives, paired into a continuous panel:
  - id=17 `liquicierre`     2015-01 → 2024-12-03 (legacy format, field B1)
  - id=203 `liquicierresrs` 2024-11-22 → present (post-ISP15 format, field BSP)

Each daily file contains many `SeriesTemporales` blocks, one per
(BSP, Info) combination. Inside each block, `Periodo > Intervalo`
elements carry the per-ISP quantity (`Ctd`, `CtdBaj`/`CtdSub`) and
optional price (`Precio`, `PrecioBaj`/`PrecioSub`).

Resolution is PT15M throughout (96 ISPs/day, 92 on DST-spring, 100 on
DST-fall). Even pre-ISP15-reform aFRR clearing was 15-min — the
2024-12-01 reform was about IMBALANCE settlement granularity, not
aFRR clearing granularity.

Output schema (long format):
    date              ISO date
    bsp               BSP 3-letter code (e.g. IGN, IMA, END, GN, HC)
    info              Info code (e.g. RMRSP up-reserve, NGDSUM deviation)
    period_start_utc  UTC timestamp of ISP start
    pos               1..96+ position within day
    ctd               quantity (MWh or MAW depending on UnidadMedida)
    precio            price (EUR/MWh) — None if not present
    unidad_medida     MWH or MAW
    unidad_precio     EUR:MWH or null
    source_file       inner XML filename (snapshot identity)
    archive           "liquicierre" or "liquicierresrs"

Notes on Info codes (sample observed; per REE settlement documentation):
    NGDSUM / NGDSIN — net generation deviation up/down
    RMRSP / RMRSN   — secondary reserve margin up/down
    RMRBP / RMRBN   — reserve margin balance up/down
    COEFPAR         — participation coefficient
    TCACTV/TCEMER/TCINAC/TCOFF/TCOFFR — energy categories (active /
                       emergency / inactive / off / off-reserved)
    TCALON          — alongado (extended)
    RESNUP/RESNDW   — system reserve net up/down (REE-only block)
"""
from __future__ import annotations

import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable

import pandas as pd


def _strip_ns(tag: str) -> str:
    return tag.split("}", 1)[-1] if "}" in tag else tag


def _attribs(elem: ET.Element) -> dict[str, str]:
    """Pull the standard `v` attribute off a single-attrib REE element."""
    return {k: v for k, v in elem.attrib.items()}


def parse_liquicierre_xml(path: Path, archive: str) -> pd.DataFrame:
    """Parse one daily liquicierre/liquicierresrs XML into long format.

    `archive` is "liquicierre" or "liquicierresrs" — used both as a
    column tag and to decide the BSP-field name (B1 vs BSP).
    """
    tree = ET.parse(path)
    root = tree.getroot()

    bsp_field = "B1" if archive == "liquicierre" else "BSP"

    rows: list[dict] = []
    sts = root.findall(".//{*}SeriesTemporales")

    for st in sts:
        st_fields = {
            _strip_ns(c.tag): c.attrib.get("v")
            for c in st
            if _strip_ns(c.tag) not in ("Periodo",)
        }
        bsp = st_fields.get(bsp_field)
        info = st_fields.get("Info")
        unidad_medida = st_fields.get("UnidadMedida")
        unidad_precio = st_fields.get("UnidadPrecio")

        # Iterate periods — multiple Periodo blocks possible (DST etc).
        for periodo in st.findall(".//{*}Periodo"):
            interval_str = None
            resolucion = None
            for child in periodo:
                tag = _strip_ns(child.tag)
                if tag == "IntervaloTiempo":
                    interval_str = child.attrib.get("v")
                elif tag == "Resolucion":
                    resolucion = child.attrib.get("v")

            # Parse interval start (UTC, ISO 8601)
            period_start = None
            if interval_str:
                start_str = interval_str.split("/", 1)[0].rstrip("Z")
                try:
                    period_start = datetime.fromisoformat(start_str).replace(
                        tzinfo=timezone.utc
                    )
                except ValueError:
                    period_start = None

            # Resolucion → step. Default PT15M.
            step = timedelta(minutes=15)
            if resolucion == "PT60M" or resolucion == "PT1H":
                step = timedelta(hours=1)

            for ivl in periodo.findall("{*}Intervalo"):
                pos = ivl.find("{*}Pos")
                pos_int = int(pos.attrib.get("v", 0)) if pos is not None else None

                # Quantity field has multiple possible names
                ctd_value = None
                for ctd_tag in ("Ctd", "CtdBaj", "CtdSub"):
                    e = ivl.find(f"{{*}}{ctd_tag}")
                    if e is not None:
                        try:
                            ctd_value = float(e.attrib.get("v", "nan"))
                        except (ValueError, TypeError):
                            pass
                        break

                # Price field (optional)
                precio_value = None
                for px_tag in ("Precio", "PrecioBaj", "PrecioSub"):
                    e = ivl.find(f"{{*}}{px_tag}")
                    if e is not None:
                        try:
                            precio_value = float(e.attrib.get("v", "nan"))
                        except (ValueError, TypeError):
                            pass
                        break

                # ClPto (point classifier — Schedule, Up, Down, etc.)
                clpto = ivl.find("{*}ClPto")
                clpto_v = clpto.attrib.get("v") if clpto is not None else None

                ts = (
                    period_start + step * (pos_int - 1)
                    if (period_start and pos_int)
                    else None
                )

                rows.append(
                    {
                        "bsp": bsp,
                        "info": info,
                        "clpto": clpto_v,
                        "period_start_utc": ts,
                        "pos": pos_int,
                        "ctd": ctd_value,
                        "precio": precio_value,
                        "unidad_medida": unidad_medida,
                        "unidad_precio": unidad_precio,
                        "source_file": path.name,
                        "archive": archive,
                    }
                )

    df = pd.DataFrame(rows)
    if not df.empty:
        # Pull date from filename: liquicierre_YYYYMMDD.xml etc.
        stem = path.stem.replace(".1", "").replace(".2", "")
        # filename "liquicierre_YYYYMMDD" or "liquicierresrs_YYYYMMDD.N"
        for token in stem.split("_"):
            if token.isdigit() and len(token) == 8:
                df["date"] = pd.to_datetime(token, format="%Y%m%d").date()
                break
    return df


def parse_liquicierre_dir(extracted_dir: Path, archive: str) -> pd.DataFrame:
    """Parse all XMLs in an extracted/ directory and concatenate."""
    parts = []
    for p in sorted(extracted_dir.glob("*.xml")):
        try:
            parts.append(parse_liquicierre_xml(p, archive=archive))
        except ET.ParseError as e:
            print(f"[WARN parse] {p.name}: {e}")
    if not parts:
        return pd.DataFrame()
    return pd.concat(parts, ignore_index=True)
