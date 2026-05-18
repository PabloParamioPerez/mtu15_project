"""Parse ENTSO-E A75 actual system generation per type (all 20 PSR types).

Distinct from A73 per-unit data: A75 returns one TimeSeries per PSR type
(B01-B20) carrying the SYSTEM-AGGREGATE generation per ISP. This covers
the technologies for which Spain does not publish per-unit dispatch
(wind, solar, biomass, waste, etc.) AND complements the A73 per-unit
data with system totals for cross-check.

Output: data/processed/entsoe/generation/gen_actual_per_type_all.parquet
"""
from __future__ import annotations

import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[4]
NS = {"e": "urn:iec62325.351:tc57wg16:451-6:generationloaddocument:3:0"}


def parse_one(path: Path) -> pd.DataFrame:
    tree = ET.parse(path)
    root = tree.getroot()
    rows = []
    for ts in root.findall("e:TimeSeries", NS):
        psr_el = ts.find("e:MktPSRType/e:psrType", NS)
        if psr_el is None:
            continue
        psr_type = psr_el.text
        for period in ts.findall("e:Period", NS):
            start_el = period.find("e:timeInterval/e:start", NS)
            res_el = period.find("e:resolution", NS)
            if start_el is None or res_el is None:
                continue
            t0 = datetime.fromisoformat(start_el.text.replace("Z", "+00:00"))
            res = res_el.text
            step = 60 if res == "PT60M" else (15 if res == "PT15M" else 30)
            for pt in period.findall("e:Point", NS):
                pos = pt.find("e:position", NS)
                q = pt.find("e:quantity", NS)
                if pos is None or q is None:
                    continue
                t = t0 + timedelta(minutes=step * (int(pos.text) - 1))
                rows.append({
                    "isp_start_utc": t.replace(tzinfo=None),
                    "mtu_minutes": step,
                    "psr_type": psr_type,
                    "quantity_mw": float(q.text),
                })
    return pd.DataFrame(rows)


def main() -> None:
    raw = PROJECT_ROOT / "data/raw/entsoe/generation/actual_per_type"
    out = PROJECT_ROOT / "data/processed/entsoe/generation/gen_actual_per_type_all.parquet"
    out.parent.mkdir(parents=True, exist_ok=True)
    if not raw.exists():
        print(f"skip — {raw} not found")
        return
    dfs = []
    for f in sorted(raw.glob("*.xml")):
        if f.stat().st_size < 50 or f.read_bytes()[:8] == b"<empty/>":
            continue
        try:
            df = parse_one(f)
        except ET.ParseError:
            continue
        if not df.empty:
            dfs.append(df)
    if not dfs:
        print("no data")
        return
    big = pd.concat(dfs).drop_duplicates(["isp_start_utc", "psr_type"]).sort_values(["isp_start_utc", "psr_type"])
    big.to_parquet(out, index=False)
    print(f"{len(big):,} rows; psr_types {sorted(big['psr_type'].unique())}")
    print(f"wrote {out}")


if __name__ == "__main__":
    main()
