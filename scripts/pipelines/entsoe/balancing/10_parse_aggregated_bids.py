"""Parse ENTSO-E A24 aggregated balancing energy bids (mFRR, processType A47).

A24 returns one Balancing_MarketDocument per month containing one
TimeSeries per flow direction (A01 = up reserve, A02 = down reserve).
Each Point carries `quantity` (MW of bids available) at MTU resolution.

Output:
  data/processed/entsoe/balancing/aggregated_bids_all.parquet
  Schema: isp_start_utc, mtu_minutes, flow_direction, quantity_mw

Note: ENTSO-E only publishes A24 for Spain from ~2022 onwards; earlier
months return Acknowledgement-only documents (no data).
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
    if _localname(root.tag) == "Acknowledgement_MarketDocument":
        return pd.DataFrame()
    rows = []
    for ts in root.iter():
        if _localname(ts.tag) != "TimeSeries":
            continue
        flow = None
        for c in ts:
            if _localname(c.tag) == "flowDirection.direction":
                flow = c.text
                break
        for period in ts.iter():
            if _localname(period.tag) != "Period":
                continue
            t0 = res = None
            for c in period:
                ln = _localname(c.tag)
                if ln == "timeInterval":
                    for cc in c:
                        if _localname(cc.tag) == "start":
                            t0 = datetime.fromisoformat(cc.text.replace("Z", "+00:00"))
                elif ln == "resolution":
                    res = c.text
            if t0 is None or res is None:
                continue
            step = 60 if res == "PT60M" else (15 if res == "PT15M" else 30)
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
                t = t0 + timedelta(minutes=step * (pos - 1))
                rows.append({
                    "isp_start_utc": t.replace(tzinfo=None),
                    "mtu_minutes": step,
                    "flow_direction": flow or "",
                    "quantity_mw": qty,
                })
    return pd.DataFrame(rows)


def main() -> None:
    raw = PROJECT_ROOT / "data/raw/entsoe/balancing_aggregated_bids"
    out = PROJECT_ROOT / "data/processed/entsoe/balancing/aggregated_bids_all.parquet"
    out.parent.mkdir(parents=True, exist_ok=True)
    if not raw.exists():
        print(f"skip — {raw} not found")
        return
    dfs = []
    n_data = n_empty = 0
    for f in sorted(raw.glob("*.xml")):
        if f.stat().st_size < 1500:
            n_empty += 1
            continue
        df = parse_one(f)
        if df.empty:
            n_empty += 1
            continue
        dfs.append(df)
        n_data += 1
    if not dfs:
        print("no data")
        return
    big = pd.concat(dfs).drop_duplicates(["isp_start_utc", "flow_direction"]).sort_values("isp_start_utc")
    big.to_parquet(out, index=False)
    print(f"{len(big):,} rows from {n_data} months ({n_empty} acknowledgement-only).")
    print(f"flow_direction: {big['flow_direction'].value_counts().to_dict()}")
    print(f"range: {big.isp_start_utc.min()} -> {big.isp_start_utc.max()}")
    print(f"wrote {out}")


if __name__ == "__main__":
    main()
