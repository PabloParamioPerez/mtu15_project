"""Parse ENTSO-E installed generation capacity aggregated (A68).

XML skeleton: `GL_MarketDocument > TimeSeries > Period > Point`, one
TimeSeries per `psrType` (B01-B25). Period resolution is `P1Y`; a single
Point per TimeSeries carries the annual capacity in MW.

Output columns:
    year           int, derived from Period/timeInterval/start
    psr_type       B01-B25 production-type code
    capacity_mw    installed capacity in MW
    unit           measurement unit (e.g. MAW)
    in_domain      EIC area code
    source_file    raw XML filename
"""

from __future__ import annotations

import xml.etree.ElementTree as ET
from pathlib import Path

import pandas as pd

from ._common import (
    find_child,
    find_children,
    local_tag,
    parse_iso_utc,
    text_of,
)


def parse_xml_bytes(xml_bytes: bytes, *, source_file: str) -> pd.DataFrame:
    root = ET.fromstring(xml_bytes)

    if local_tag(root).startswith("Acknowledgement"):
        return pd.DataFrame()

    rows: list[dict] = []
    for ts in (el for el in root.iter() if local_tag(el) == "TimeSeries"):
        unit = text_of(find_child(ts, "quantity_Measure_Unit.name"))
        psr = find_child(ts, "MktPSRType")
        psr_type = text_of(find_child(psr, "psrType")) if psr is not None else ""
        in_domain = text_of(find_child(ts, "inBiddingZone_Domain.mRID")) or \
                    text_of(find_child(ts, "in_Domain.mRID"))

        for period in find_children(ts, "Period"):
            ti = find_child(period, "timeInterval")
            if ti is None:
                continue
            period_start = parse_iso_utc(text_of(find_child(ti, "start")))
            year = period_start.year

            for point in find_children(period, "Point"):
                qty = text_of(find_child(point, "quantity"))
                capacity = float(qty) if qty else float("nan")

                rows.append({
                    "year": year,
                    "psr_type": psr_type,
                    "capacity_mw": capacity,
                    "unit": unit,
                    "in_domain": in_domain,
                    "source_file": source_file,
                })

    if not rows:
        return pd.DataFrame()

    return pd.DataFrame(rows)


def parse_file(path: Path) -> pd.DataFrame:
    return parse_xml_bytes(path.read_bytes(), source_file=path.name)
