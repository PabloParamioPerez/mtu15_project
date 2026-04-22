"""Parse ENTSO-E wind & solar day-ahead generation forecast (A69).

XML skeleton: `GL_MarketDocument > TimeSeries > Period > Point`. One
TimeSeries per `psrType` (B16 solar, B18 wind offshore, B19 wind onshore).
Point carries `quantity` in MW at the TimeSeries resolution.

Output columns:
    isp_start_utc     UTC start of the ISP
    isp_end_utc       UTC end of the ISP
    mtu_minutes       ISP length in minutes (15, 30, 60)
    position          1-based position within the Period
    psr_type          B16 Solar / B18 Wind Offshore / B19 Wind Onshore
    quantity_mw       forecast MW
    unit              measurement unit (e.g. MAW)
    source_file       raw XML filename
"""

from __future__ import annotations

import xml.etree.ElementTree as ET
from datetime import timedelta
from pathlib import Path

import pandas as pd

from ._common import (
    find_child,
    find_children,
    local_tag,
    parse_iso_utc,
    resolution_minutes,
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

        for period in find_children(ts, "Period"):
            ti = find_child(period, "timeInterval")
            if ti is None:
                continue
            period_start = parse_iso_utc(text_of(find_child(ti, "start")))
            period_end = parse_iso_utc(text_of(find_child(ti, "end")))
            res_min = resolution_minutes(text_of(find_child(period, "resolution")))
            delta = timedelta(minutes=res_min)

            for point in find_children(period, "Point"):
                position = int(text_of(find_child(point, "position")))
                isp_start = period_start + (position - 1) * delta
                isp_end = isp_start + delta
                if isp_end > period_end:
                    isp_end = period_end

                qty = text_of(find_child(point, "quantity"))
                quantity = float(qty) if qty else float("nan")

                rows.append({
                    "isp_start_utc": isp_start,
                    "isp_end_utc": isp_end,
                    "mtu_minutes": res_min,
                    "position": position,
                    "psr_type": psr_type,
                    "quantity_mw": quantity,
                    "unit": unit,
                    "source_file": source_file,
                })

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    df["isp_start_utc"] = (
        pd.to_datetime(df["isp_start_utc"], utc=True).dt.tz_convert(None)
    )
    df["isp_end_utc"] = (
        pd.to_datetime(df["isp_end_utc"], utc=True).dt.tz_convert(None)
    )
    return df


def parse_file(path: Path) -> pd.DataFrame:
    return parse_xml_bytes(path.read_bytes(), source_file=path.name)
