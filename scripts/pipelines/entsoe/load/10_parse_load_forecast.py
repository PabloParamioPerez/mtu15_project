"""Parse ENTSO-E A65 day-ahead forecast load and week-ahead forecast load.

Inputs:
  data/raw/entsoe/load/load_forecast_da/{YYYYMM}.xml  (process A01)
  data/raw/entsoe/load/load_forecast_wa/{YYYYMM}.xml  (process A31)

Outputs (concatenated all-month):
  data/processed/entsoe/load/load_forecast_da_all.parquet
  data/processed/entsoe/load/load_forecast_wa_all.parquet
"""
from __future__ import annotations

import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[4]
NS = {"e": "urn:iec62325.351:tc57wg16:451-6:generationloaddocument:3:0"}

SUBDIRS = {
    "load_forecast_da": "load_forecast_da_all",
    "load_forecast_wa": "load_forecast_wa_all",
}


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
                t = t0 + timedelta(minutes=step_min * (int(pos_el.text) - 1))
                rows.append({
                    "isp_start_utc": t.replace(tzinfo=None),
                    "mtu_minutes": step_min,
                    "load_forecast_mw": float(q_el.text),
                })
    return pd.DataFrame(rows)


def build(subdir: str, out_stem: str) -> None:
    raw = PROJECT_ROOT / "data/raw/entsoe" / subdir
    if not raw.exists():
        print(f"skip {subdir} (not found)")
        return
    out_dir = PROJECT_ROOT / "data/processed/entsoe/load"
    out_dir.mkdir(parents=True, exist_ok=True)
    dfs = []
    for f in sorted(raw.glob("*.xml")):
        if f.read_bytes()[:8] == b"<empty/>":
            continue
        try:
            df = parse_one(f)
        except ET.ParseError:
            continue
        if not df.empty:
            dfs.append(df)
    if not dfs:
        print(f"{subdir}: no data")
        return
    big = pd.concat(dfs).drop_duplicates("isp_start_utc").sort_values("isp_start_utc")
    out = out_dir / f"{out_stem}.parquet"
    big.to_parquet(out, index=False)
    print(f"{subdir}: {len(big):,} rows → {out}")


def main() -> None:
    for sub, stem in SUBDIRS.items():
        build(sub, stem)


if __name__ == "__main__":
    main()
