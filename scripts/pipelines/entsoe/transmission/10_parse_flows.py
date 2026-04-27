"""Parse ENTSO-E cross-border flow documents (A11 physical, A09 scheduled, A61 NTC).

These all share the GL_MarketDocument schema with quantity per ISP. We
parse all three subdirs into per-ISP records.

Output:
  data/processed/entsoe/transmission/{subdir}_all.parquet
"""
from __future__ import annotations

import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[4]


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
        in_dom = out_dom = None
        for child in ts:
            ln = _localname(child.tag)
            if ln == "in_Domain.mRID":
                in_dom = child.text
            elif ln == "out_Domain.mRID":
                out_dom = child.text
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
                    "in_domain": in_dom,
                    "out_domain": out_dom,
                    "quantity_mw": qty,
                })
    return pd.DataFrame(rows)


def parse_dir(subdir: str) -> None:
    raw = PROJECT_ROOT / f"data/raw/entsoe/{subdir}"
    out_dir = PROJECT_ROOT / "data/processed/entsoe/transmission"
    out_dir.mkdir(parents=True, exist_ok=True)
    if not raw.exists():
        return
    dfs = []
    for f in sorted(raw.glob("*.xml")):
        if f.stat().st_size < 50 or f.read_bytes()[:8] == b"<empty/>":
            continue
        df = parse_one(f)
        if not df.empty:
            dfs.append(df)
    if not dfs:
        print(f"{subdir}: no data")
        return
    df = pd.concat(dfs, ignore_index=True).drop_duplicates(["isp_start_utc"])
    out = out_dir / f"{subdir}_all.parquet"
    df.to_parquet(out, index=False)
    print(f"{subdir}: {len(df):,} rows → {out}")


def main() -> None:
    for sub in [
        "flows_physical_fr_to_es",
        "flows_physical_es_to_fr",
        "flows_scheduled_fr_to_es",
        "flows_scheduled_es_to_fr",
        "ntc_fr_to_es",
        "ntc_es_to_fr",
    ]:
        parse_dir(sub)


if __name__ == "__main__":
    main()
