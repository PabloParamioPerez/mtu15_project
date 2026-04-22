"""Parse ENTSO-E imbalance documents (A85 prices, A86 volumes).

One module handles both document types — the XML skeleton is identical
(`Publication_MarketDocument` / `Balancing_MarketDocument` with nested
`TimeSeries` → `Period` → `Point`); the only difference is which child
element carries the numeric payload.

Key fields emitted (per ISP Point):
    kind              'prices' or 'volumes'
    isp_start_utc     UTC start of the ISP
    isp_end_utc       UTC end of the ISP
    mtu_minutes       ISP length in minutes (15, 30, 60)
    position          1-based position within the TimeSeries Period
    flow_direction    A01 = up / A02 = down (prices only; '' otherwise)
    price_eur_per_mwh numeric price (prices only; NaN otherwise)
    volume_mwh        signed volume D (volumes only; NaN otherwise)
    imbalance_flag    'A19' surplus / 'A20' deficit, when provided
    business_type     TimeSeries businessType code (verbatim)
    currency          prices only; '' otherwise
    unit              quantity unit ('MAW' for volumes), or ''
    status            intermediate/final if encoded; else ''
    source_file       raw XML filename (snapshot identity)

Output is a tidy long-format table — one row per ISP Point per series.
"""

from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

# Namespaces vary by document version; strip them with an xpath trick.
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
    """ENTSO-E emits timestamps like `2025-03-19T00:00Z` (always UTC)."""
    t = ts.rstrip("Z")
    dt = datetime.fromisoformat(t)
    return dt.replace(tzinfo=timezone.utc)


_RESOLUTION_MIN = {
    "PT15M": 15,
    "PT30M": 30,
    "PT60M": 60,
    "PT1H": 60,
}


def _resolution_minutes(code: str) -> int:
    if code not in _RESOLUTION_MIN:
        raise ValueError(f"Unsupported resolution code: {code!r}")
    return _RESOLUTION_MIN[code]


def parse_xml_bytes(
    xml_bytes: bytes,
    *,
    kind: str,
    source_file: str,
) -> pd.DataFrame:
    """Parse an A85 or A86 XML payload into a tidy DataFrame.

    `kind` must be "prices" or "volumes"; it governs which Point child
    element is expected (`price.amount` vs `quantity`) and which columns
    are populated.
    """
    if kind not in {"prices", "volumes"}:
        raise ValueError(f"kind must be 'prices' or 'volumes', got {kind!r}")

    root = ET.fromstring(xml_bytes)
    root_tag = _tag(root)

    # "No matching data found" is returned as an Acknowledgement doc.
    if root_tag.startswith("Acknowledgement"):
        return pd.DataFrame()

    rows: list[dict] = []
    time_series = [el for el in root.iter() if _tag(el) == "TimeSeries"]
    for ts in time_series:
        business_type = _text(_find(ts, "businessType"))
        flow_direction = _text(_find(ts, "flowDirection.direction"))
        currency = _text(_find(ts, "currency_Unit.name"))
        # Prices use `price_Measure_Unit.name`, volumes use
        # `quantity_Measure_Unit.name`. Try both, keep whichever exists.
        unit = (
            _text(_find(ts, "quantity_Measure_Unit.name"))
            or _text(_find(ts, "price_Measure_Unit.name"))
        )

        for period in _find_all(ts, "Period"):
            ti = _find(period, "timeInterval")
            if ti is None:
                continue
            period_start = _parse_iso_utc(_text(_find(ti, "start")))
            period_end = _parse_iso_utc(_text(_find(ti, "end")))
            res_min = _resolution_minutes(_text(_find(period, "resolution")))
            delta = timedelta(minutes=res_min)

            for point in _find_all(period, "Point"):
                position = int(_text(_find(point, "position")))
                isp_start = period_start + (position - 1) * delta
                isp_end = isp_start + delta
                if isp_end > period_end:
                    # Defensive: should not happen with well-formed payloads.
                    isp_end = period_end

                imbalance_flag = _text(_find(point, "imbalance_Price.category"))
                # Status / quality flags differ between national TSOs; we keep
                # whichever is present without assuming a specific tag name.
                status = (
                    _text(_find(point, "quality")) or
                    _text(_find(point, "secondaryQuantity"))
                )

                row = {
                    "kind": kind,
                    "isp_start_utc": isp_start,
                    "isp_end_utc": isp_end,
                    "mtu_minutes": res_min,
                    "position": position,
                    "flow_direction": flow_direction,
                    "price_eur_per_mwh": float("nan"),
                    "volume_mwh": float("nan"),
                    "imbalance_flag": imbalance_flag,
                    "business_type": business_type,
                    "currency": currency,
                    "unit": unit,
                    "status": status,
                    "source_file": source_file,
                }

                if kind == "prices":
                    # A85 uses `imbalance_Price.amount`. Some TSOs also
                    # emit `price.amount` for activated-energy-style
                    # docs — fall back to it defensively.
                    amt = (
                        _text(_find(point, "imbalance_Price.amount"))
                        or _text(_find(point, "price.amount"))
                    )
                    if amt:
                        row["price_eur_per_mwh"] = float(amt)
                else:
                    qty = _text(_find(point, "quantity"))
                    if qty:
                        row["volume_mwh"] = float(qty)

                rows.append(row)

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    # Store as naive UTC timestamps (column name encodes the timezone);
    # avoids a pytz dependency in DuckDB readers downstream.
    df["isp_start_utc"] = (
        pd.to_datetime(df["isp_start_utc"], utc=True).dt.tz_convert(None)
    )
    df["isp_end_utc"] = (
        pd.to_datetime(df["isp_end_utc"], utc=True).dt.tz_convert(None)
    )
    return df


def parse_file(path: Path, *, kind: str) -> pd.DataFrame:
    """Read a raw XML file and return its parsed DataFrame."""
    return parse_xml_bytes(path.read_bytes(), kind=kind, source_file=path.name)
