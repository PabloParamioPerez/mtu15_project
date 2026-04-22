"""Parse ENTSO-E activated balancing energy prices (A84).

Document shape is the same `Balancing_MarketDocument > TimeSeries > Period >
Point` used by imbalance docs; the semantics differ:

    - businessType codes the reserve type (A95 RR, A96 aFRR, A97 mFRR, A98 FCR).
    - flowDirection codes up (A01) vs down (A02) activation.
    - The price payload lives in `price.amount` (not `imbalance_Price.amount`).

One row per `(TimeSeries, ISP Point)`.

Output columns:
    isp_start_utc     UTC start of the ISP
    isp_end_utc       UTC end of the ISP
    mtu_minutes       ISP length in minutes (15, 30, 60)
    position          1-based position within the Period
    business_type     reserve family (verbatim A-code)
    flow_direction    A01 up / A02 down
    price_eur_per_mwh numeric activated-energy price (NaN if missing)
    currency          e.g. EUR
    unit              price measure unit
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
        business_type = text_of(find_child(ts, "businessType"))
        flow_direction = text_of(find_child(ts, "flowDirection.direction"))
        currency = text_of(find_child(ts, "currency_Unit.name"))
        unit = text_of(find_child(ts, "price_Measure_Unit.name"))

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

                amt = (
                    text_of(find_child(point, "activation_Price.amount"))
                    or text_of(find_child(point, "price.amount"))
                )
                price = float(amt) if amt else float("nan")

                rows.append({
                    "isp_start_utc": isp_start,
                    "isp_end_utc": isp_end,
                    "mtu_minutes": res_min,
                    "position": position,
                    "business_type": business_type,
                    "flow_direction": flow_direction,
                    "price_eur_per_mwh": price,
                    "currency": currency,
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
