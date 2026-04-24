"""Parse ENTSO-E monthly financial-balance documents (A87).

The A87 document publishes Spain's monthly balancing financial balance
under TR article 17.1.i / GL EB article 55. One TimeSeries per document,
one Period at `P1M` resolution, one Point per month carrying two
`Financial_Price` children with `direction` A01 and A02.

Semantic mapping of the two directions follows §3.13.4 of Detailed Data
Descriptions v3r4:
    direction A01 — *expenses* incurred by the TSO for procuring reserves
                    and activating balancing energy. Positive => cash from
                    TSO to BSPs; negative => cash from BSPs to TSO.
    direction A02 — *net income* to the TSO after settling imbalance
                    accounts with BRPs. Positive => cash from BRPs to TSO;
                    negative => cash from TSO to BRPs.

Note: this A01/A02 mapping is inferred from the spec's ordering of the
two regulatory items; it should be cross-checked against a published
REE aggregate (e.g. a known month with large positive net income) before
citing the numbers directionally in thesis text.

Emitted schema (one row per month per direction):
    month             first day of the settlement month (UTC date)
    direction_code    'A01' or 'A02'
    direction_label   'expenses' or 'net_income'
    amount_eur        Financial_Price.amount (float)
    currency          e.g. 'EUR'
    resolution        e.g. 'P1M'
    business_type     TimeSeries.businessType (e.g. 'A99')
    source_file       raw XML filename (snapshot identity)
"""

from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

_LOCAL = re.compile(r"^\{[^}]+\}")


def _tag(el: ET.Element) -> str:
    return _LOCAL.sub("", el.tag)


def _find(el: ET.Element, name: str) -> ET.Element | None:
    for child in el:
        if _tag(child) == name:
            return child
    return None


def _find_all(el: ET.Element, name: str) -> list[ET.Element]:
    return [c for c in el if _tag(c) == name]


def _text(el: ET.Element | None) -> str:
    if el is None or el.text is None:
        return ""
    return el.text.strip()


def _parse_iso_utc(ts: str) -> datetime:
    t = ts.rstrip("Z")
    dt = datetime.fromisoformat(t)
    return dt.replace(tzinfo=timezone.utc)


_DIRECTION_LABEL = {
    "A01": "expenses",
    "A02": "net_income",
}


def parse_xml_bytes(
    xml_bytes: bytes,
    *,
    source_file: str,
) -> pd.DataFrame:
    root = ET.fromstring(xml_bytes)
    root_tag = _tag(root)
    if root_tag.startswith("Acknowledgement"):
        return pd.DataFrame()

    rows: list[dict] = []
    time_series = [el for el in root.iter() if _tag(el) == "TimeSeries"]
    for ts in time_series:
        business_type = _text(_find(ts, "businessType"))
        currency = _text(_find(ts, "currency_Unit.name"))

        for period in _find_all(ts, "Period"):
            resolution = _text(_find(period, "resolution"))
            ti = _find(period, "timeInterval")
            if ti is None:
                continue
            period_start = _parse_iso_utc(_text(_find(ti, "start")))

            for point in _find_all(period, "Point"):
                # A87 Point carries multiple Financial_Price children, one
                # per direction. We emit one row per (point, direction).
                for fp in _find_all(point, "Financial_Price"):
                    amount = _text(_find(fp, "amount"))
                    direction = _text(_find(fp, "direction"))
                    if not amount:
                        continue
                    # Monthly timestamps are published at Spanish midnight
                    # in local time, which in UTC is 23:00 (CET) or 22:00
                    # (CEST). Converting period_start to Europe/Madrid and
                    # taking the month there avoids a DST edge-case at the
                    # March/October transitions.
                    local_start = pd.Timestamp(period_start).tz_convert(
                        "Europe/Madrid"
                    )
                    settlement_month = pd.Timestamp(
                        year=local_start.year,
                        month=local_start.month,
                        day=1,
                    ).date()

                    rows.append({
                        "month": settlement_month,
                        "direction_code": direction,
                        "direction_label": _DIRECTION_LABEL.get(direction, ""),
                        "amount_eur": float(amount),
                        "currency": currency,
                        "resolution": resolution,
                        "business_type": business_type,
                        "source_file": source_file,
                    })

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    df["month"] = pd.to_datetime(df["month"])
    return df


def parse_file(path: Path) -> pd.DataFrame:
    return parse_xml_bytes(path.read_bytes(), source_file=path.name)
