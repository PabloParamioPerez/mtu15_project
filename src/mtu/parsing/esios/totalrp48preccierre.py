"""Parse ESIOS totalrp48preccierre RZ technical-restriction closure-price XMLs.

Format: ENTSO-E A09 message (TipoMensaje = A09 "Total imbalances technical
and commercial"). Each daily XML contains multiple `SeriesTemporales`
blocks indexed by `TipoRedespacho` (REE redispatch type code):

    TipoRedespacho codes (REE-specific, per P.O. 7.4 / P.O. 14):
        33 = real-time technical restrictions
        34 = inter-zonal/network technical restrictions resolution
        61 = system security (RZ technical restrictions in P.O. 3.2)
        68 = reserve management
        69 = voltage control / black-start
        81 = other
        92 = mFRR activation
        94 = system balancing
    (codes preserved verbatim — interpretation per REE documentation)

Each `SeriesTemporales` has multiple `Periodo` blocks. Each Periodo has
`IntervaloTiempo` (UTC start/end), `Resolucion` (PT15M = 15-min, the
canonical resolution since 2014 for these data), and many `Intervalo`
elements with `Pos`, `CtdBaj` (quantity-down MWh), `PrecioBaj`
(price-down EUR/MWh), and possibly `CtdSub`/`PrecioSub` (up).

Output schema (long format, one row per Intervalo):
    date              ISO date (from filename)
    tipo_redespacho   redispatch type code (33/34/61/68/69/81/92/94/...)
    period_start_utc  UTC timestamp of Intervalo start
    pos               position within Periodo
    qty_down_mwh      CtdBaj (down quantity, MWh)
    qty_up_mwh        CtdSub (up quantity, MWh)
    price_down_eur    PrecioBaj (down price, EUR/MWh)
    price_up_eur      PrecioSub (up price, EUR/MWh)
    unit_qty          UnidadMedida ("MWH" typically)
    unit_price        UnidadPrecio ("EUR:MWH" typically)
    source_file       inner XML filename
"""
from __future__ import annotations

import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd


def _strip_ns(tag: str) -> str:
    return tag.split("}", 1)[-1] if "}" in tag else tag


def _attr_v(elem: ET.Element | None) -> str | None:
    if elem is None:
        return None
    return elem.attrib.get("v")


def _maybe_float(s: str | None) -> float | None:
    if s is None:
        return None
    try:
        return float(s)
    except (ValueError, TypeError):
        return None


def parse_totalrp48preccierre_xml(path: Path) -> pd.DataFrame:
    """Parse one daily totalrp48preccierre XML into long format."""
    tree = ET.parse(path)
    root = tree.getroot()

    rows: list[dict] = []

    for st in root.findall(".//{*}SeriesTemporales"):
        tipo_redespacho = _attr_v(st.find("{*}TipoRedespacho"))
        unit_qty = _attr_v(st.find("{*}UnidadMedida"))
        unit_price = _attr_v(st.find("{*}UnidadPrecio"))

        for periodo in st.findall("{*}Periodo"):
            interval_str = _attr_v(periodo.find("{*}IntervaloTiempo"))
            resolucion = _attr_v(periodo.find("{*}Resolucion"))

            period_start: datetime | None = None
            if interval_str:
                start_str = interval_str.split("/", 1)[0].rstrip("Z")
                try:
                    period_start = datetime.fromisoformat(start_str).replace(
                        tzinfo=timezone.utc
                    )
                except ValueError:
                    period_start = None

            step = timedelta(minutes=15)
            if resolucion in ("PT60M", "PT1H"):
                step = timedelta(hours=1)

            for ivl in periodo.findall("{*}Intervalo"):
                pos = _maybe_float(_attr_v(ivl.find("{*}Pos")))
                pos_int = int(pos) if pos is not None else None

                qty_down = _maybe_float(_attr_v(ivl.find("{*}CtdBaj")))
                qty_up = _maybe_float(_attr_v(ivl.find("{*}CtdSub")))
                price_down = _maybe_float(_attr_v(ivl.find("{*}PrecioBaj")))
                price_up = _maybe_float(_attr_v(ivl.find("{*}PrecioSub")))

                ts = (
                    period_start + step * (pos_int - 1)
                    if (period_start and pos_int)
                    else None
                )

                rows.append(
                    {
                        "tipo_redespacho": tipo_redespacho,
                        "period_start_utc": ts,
                        "pos": pos_int,
                        "qty_down_mwh": qty_down,
                        "qty_up_mwh": qty_up,
                        "price_down_eur": price_down,
                        "price_up_eur": price_up,
                        "unit_qty": unit_qty,
                        "unit_price": unit_price,
                        "source_file": path.name,
                    }
                )

    df = pd.DataFrame(rows)
    if not df.empty:
        for token in path.stem.split("_"):
            if token.isdigit() and len(token) == 8:
                df["date"] = pd.to_datetime(token, format="%Y%m%d").date()
                break
    return df


def parse_totalrp48preccierre_dir(extracted_dir: Path) -> pd.DataFrame:
    """Parse all XMLs in an extracted/ directory and concatenate."""
    parts = []
    for p in sorted(extracted_dir.glob("*.xml")):
        try:
            parts.append(parse_totalrp48preccierre_xml(p))
        except ET.ParseError as e:
            print(f"[WARN parse] {p.name}: {e}")
    if not parts:
        return pd.DataFrame()
    return pd.concat(parts, ignore_index=True)
