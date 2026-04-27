"""Parse ENTSO-E A85 imbalance prices for France.

Output: data/processed/entsoe/balancing/imbalance_prices_fr_all.parquet

Schema:
  isp_start_utc, mtu_minutes, flag (A04=up, A05=down), price_eur_mwh

Note: France uses 30-min ISP through 2025; switched to 15-min in Q4 2025.
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
        flag = None
        for c in ts.iter():
            if _localname(c.tag) == "imbalance_Price.category":
                flag = c.text
                break
        if flag is None:
            for c in ts.iter():
                if _localname(c.tag) == "flowDirection.direction":
                    flag = c.text
                    break
        for period in ts.iter():
            if _localname(period.tag) != "Period":
                continue
            t0 = res = None
            for c in period:
                if _localname(c.tag) == "timeInterval":
                    for cc in c:
                        if _localname(cc.tag) == "start":
                            t0 = datetime.fromisoformat(cc.text.replace("Z", "+00:00"))
                elif _localname(c.tag) == "resolution":
                    res = c.text
            if t0 is None or res is None:
                continue
            step = 60 if res == "PT60M" else (15 if res == "PT15M" else 30)
            for pt in period.iter():
                if _localname(pt.tag) != "Point":
                    continue
                pos = price = None
                for cc in pt:
                    ln = _localname(cc.tag)
                    if ln == "position":
                        try:
                            pos = int(cc.text)
                        except (ValueError, TypeError):
                            pass
                    elif ln in ("imbalance_Price.amount", "price.amount"):
                        try:
                            price = float(cc.text)
                        except (ValueError, TypeError):
                            pass
                if pos is None or price is None:
                    continue
                t = t0 + timedelta(minutes=step * (pos - 1))
                rows.append({
                    "isp_start_utc": t.replace(tzinfo=None),
                    "mtu_minutes": step,
                    "flag": flag or "",
                    "price_eur_mwh": price,
                })
    return pd.DataFrame(rows)


def main() -> None:
    raw = PROJECT_ROOT / "data/raw/entsoe/imbalance_prices_fr"
    out = PROJECT_ROOT / "data/processed/entsoe/balancing/imbalance_prices_fr_all.parquet"
    out.parent.mkdir(parents=True, exist_ok=True)
    if not raw.exists():
        print(f"skip — {raw} not found")
        return
    dfs = []
    for f in sorted(raw.glob("*.xml")):
        if f.stat().st_size < 50 or f.read_bytes()[:8] == b"<empty/>":
            continue
        df = parse_one(f)
        if not df.empty:
            dfs.append(df)
    if not dfs:
        print("no data")
        return
    big = pd.concat(dfs).drop_duplicates(["isp_start_utc", "flag"]).sort_values("isp_start_utc")
    big.to_parquet(out, index=False)
    print(f"{len(big):,} rows; flags {big['flag'].value_counts().to_dict()}")
    print(f"wrote {out}")


if __name__ == "__main__":
    main()
