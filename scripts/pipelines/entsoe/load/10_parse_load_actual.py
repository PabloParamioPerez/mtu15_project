"""Parse ENTSO-E A65 actual total load (Spain) into per-ISP records.

Output: data/processed/entsoe/load/load_actual/load_actual_{YYYYMM}.parquet
"""
from __future__ import annotations

import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[4]
RAW = PROJECT_ROOT / "data/raw/entsoe/load/load_actual"
OUT_DIR = PROJECT_ROOT / "data/processed/entsoe/load/load_actual"
NS = {"e": "urn:iec62325.351:tc57wg16:451-6:generationloaddocument:3:0"}


def parse_one(path: Path) -> pd.DataFrame:
    tree = ET.parse(path)
    root = tree.getroot()
    rows = []
    for ts in root.findall("e:TimeSeries", NS):
        for period in ts.findall("e:Period", NS):
            start_el = period.find("e:timeInterval/e:start", NS)
            res_el = period.find("e:resolution", NS)
            if start_el is None or res_el is None:
                continue
            t0 = datetime.fromisoformat(start_el.text.replace("Z", "+00:00"))
            res = res_el.text
            step_min = 60 if res == "PT60M" else (15 if res == "PT15M" else 30)
            for pt in period.findall("e:Point", NS):
                pos_el = pt.find("e:position", NS)
                q_el = pt.find("e:quantity", NS)
                if pos_el is None or q_el is None:
                    continue
                pos = int(pos_el.text)
                qty = float(q_el.text)
                t = t0 + timedelta(minutes=step_min * (pos - 1))
                rows.append({
                    "isp_start_utc": t.replace(tzinfo=None),
                    "mtu_minutes": step_min,
                    "load_mw": qty,
                })
    return pd.DataFrame(rows)


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    n = 0
    for f in sorted(RAW.glob("*.xml")):
        if f.read_bytes()[:8] == b"<empty/>":
            continue
        df = parse_one(f)
        if df.empty:
            continue
        out = OUT_DIR / f"load_actual_{f.stem}.parquet"
        df.to_parquet(out, index=False)
        n += 1
    print(f"parsed {n} monthly files")


if __name__ == "__main__":
    main()
