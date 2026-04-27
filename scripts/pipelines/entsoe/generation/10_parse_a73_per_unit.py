"""Parse ENTSO-E A73 per-unit actual generation XMLs into per-ISP records.

A73 returns one TimeSeries per generation unit. Each TimeSeries carries:
  - registeredResource.mRID  (the unit EIC code)
  - registeredResource.name  (the operator's plant name)
  - MktPSRType.psrType      (B04 CCGT, B10 PS, B11 RoR, B12 reservoir, B14 nuclear, ...)
  - Period > Point with position + quantity (MW per ISP)

The download is split per psrType into raw subdirs (a73_b04/, a73_b10/,
a73_b11/, a73_b12/, a73_b14/). We parse each subdir and write one
parquet per (psr_type, month) into data/processed/entsoe/generation/a73/.
"""
from __future__ import annotations

import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[4]
RAW_BASE = PROJECT_ROOT / "data/raw/entsoe/generation"
OUT_DIR = PROJECT_ROOT / "data/processed/entsoe/generation/a73"

# psrType subdirs we expect.
PSR_DIRS = ["a73_b04", "a73_b10", "a73_b11", "a73_b12", "a73_b14"]


def _localname(tag: str) -> str:
    return tag.split("}", 1)[1] if "}" in tag else tag


def parse_one(path: Path) -> pd.DataFrame:
    try:
        tree = ET.parse(path)
    except ET.ParseError:
        return pd.DataFrame()
    root = tree.getroot()
    rows = []
    for ts in root.iter():
        if _localname(ts.tag) != "TimeSeries":
            continue
        unit_eic = unit_name = psr_type = None
        for child in ts.iter():
            ln = _localname(child.tag)
            txt = child.text
            if ln == "registeredResource.mRID" and unit_eic is None:
                unit_eic = txt
            elif ln == "registeredResource.name" and unit_name is None:
                unit_name = txt
            elif ln == "psrType" and psr_type is None:
                psr_type = txt
        for period in ts.iter():
            if _localname(period.tag) != "Period":
                continue
            start_el = res_el = None
            for c in period:
                ln = _localname(c.tag)
                if ln == "timeInterval":
                    for cc in c:
                        if _localname(cc.tag) == "start":
                            start_el = cc.text
                elif ln == "resolution":
                    res_el = c.text
            if not start_el or not res_el:
                continue
            t0 = datetime.fromisoformat(start_el.replace("Z", "+00:00"))
            step_min = 60 if res_el == "PT60M" else (15 if res_el == "PT15M" else 30)
            for pt in period.iter():
                if _localname(pt.tag) != "Point":
                    continue
                pos = qty = None
                for cc in pt:
                    ln = _localname(cc.tag)
                    if ln == "position":
                        try:
                            pos = int(cc.text)
                        except (ValueError, TypeError):
                            pass
                    elif ln == "quantity":
                        try:
                            qty = float(cc.text)
                        except (ValueError, TypeError):
                            pass
                if pos is None or qty is None:
                    continue
                t = t0 + timedelta(minutes=step_min * (pos - 1))
                rows.append({
                    "isp_start_utc": t.replace(tzinfo=None),
                    "mtu_minutes": step_min,
                    "unit_eic": unit_eic or "UNKNOWN",
                    "unit_name": unit_name or "UNKNOWN",
                    "psr_type": psr_type or "UNKNOWN",
                    "quantity_mw": qty,
                })
    return pd.DataFrame(rows)


def parse_dir(subdir: str) -> int:
    raw = RAW_BASE / subdir
    if not raw.exists():
        return 0
    out_sub = OUT_DIR / subdir
    out_sub.mkdir(parents=True, exist_ok=True)
    n = 0
    for f in sorted(raw.glob("*.xml")):
        if f.stat().st_size < 50 or f.read_bytes()[:8] == b"<empty/>":
            continue
        df = parse_one(f)
        if df.empty:
            continue
        df.to_parquet(out_sub / f"{f.stem}.parquet", index=False)
        n += 1
    return n


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    for sub in PSR_DIRS:
        n = parse_dir(sub)
        print(f"{sub}: {n} monthly files parsed")


if __name__ == "__main__":
    main()
