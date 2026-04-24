"""Parse ENTSO-E A44 day-ahead price documents.

One Point per market time unit (MTU). Schema:
  isp_start_utc, isp_end_utc, mtu_minutes, in_domain, out_domain,
  position, price_eur_per_mwh, source_file
"""
from __future__ import annotations
import re
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from pathlib import Path
import pandas as pd

_LOCAL = re.compile(r"^\{[^}]+\}")


def _tag(el):
    return _LOCAL.sub("", el.tag)


def _find(el, name):
    for c in el:
        if _tag(c) == name:
            return c
    return None


def _find_all(el, name):
    return [c for c in el if _tag(c) == name]


def _text(el):
    return (el.text or '').strip() if el is not None else ''


_RES = {'PT15M': 15, 'PT30M': 30, 'PT60M': 60, 'PT1H': 60}


def parse_xml_bytes(xml_bytes, *, source_file, domain_label=''):
    root = ET.fromstring(xml_bytes)
    if _tag(root).startswith('Acknowledgement'):
        return pd.DataFrame()
    rows = []
    for ts in [e for e in root.iter() if _tag(e) == 'TimeSeries']:
        in_dom = _text(_find(ts, 'in_Domain.mRID'))
        out_dom = _text(_find(ts, 'out_Domain.mRID'))
        for period in _find_all(ts, 'Period'):
            ti = _find(period, 'timeInterval')
            p_start = datetime.fromisoformat(
                _text(_find(ti, 'start')).rstrip('Z')
            ).replace(tzinfo=timezone.utc)
            res = _RES[_text(_find(period, 'resolution'))]
            delta = timedelta(minutes=res)
            for pt in _find_all(period, 'Point'):
                pos = int(_text(_find(pt, 'position')))
                amt = _text(_find(pt, 'price.amount'))
                if not amt:
                    continue
                isp_start = p_start + (pos - 1) * delta
                rows.append({
                    'isp_start_utc': isp_start,
                    'isp_end_utc': isp_start + delta,
                    'mtu_minutes': res,
                    'in_domain': in_dom,
                    'out_domain': out_dom,
                    'domain_label': domain_label,
                    'position': pos,
                    'price_eur_per_mwh': float(amt),
                    'source_file': source_file,
                })
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df['isp_start_utc'] = pd.to_datetime(df['isp_start_utc'], utc=True).dt.tz_convert(None)
    df['isp_end_utc'] = pd.to_datetime(df['isp_end_utc'], utc=True).dt.tz_convert(None)
    return df


def parse_file(path, *, domain_label=''):
    return parse_xml_bytes(
        path.read_bytes(), source_file=Path(path).name, domain_label=domain_label
    )
