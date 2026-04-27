"""Build CCGT per-firm dispatch panel by joining A73 to the EIC-OMIE mapping.

Inputs:
  data/processed/entsoe/generation/a73_per_unit_all.parquet  (B04 rows)
  data/external/omie_reference/ccgt_eic_to_omie.csv          (firm column)

Output:
  data/processed/entsoe/generation/ccgt_per_firm_panel.parquet

Schema: isp_start_utc, mtu_minutes, unit_eic, omie_code, firm, psr_type,
        quantity_mw, mwh
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[4]


def main() -> None:
    panel = pd.read_parquet(PROJECT_ROOT / "data/processed/entsoe/generation/a73_per_unit_all.parquet")
    mp = pd.read_csv(PROJECT_ROOT / "data/external/omie_reference/ccgt_eic_to_omie.csv")

    ccgt = panel[panel["psr_type"] == "B04"].copy()
    merged = ccgt.merge(mp[["entsoe_eic", "omie_code", "firm"]],
                        left_on="unit_eic", right_on="entsoe_eic", how="left")
    merged["firm"] = merged["firm"].fillna("UNKNOWN")
    merged["mwh"] = merged["quantity_mw"] * merged["mtu_minutes"] / 60.0

    out_path = PROJECT_ROOT / "data/processed/entsoe/generation/ccgt_per_firm_panel.parquet"
    cols = ["isp_start_utc", "mtu_minutes", "unit_eic", "omie_code", "firm",
            "psr_type", "quantity_mw", "mwh"]
    merged[cols].to_parquet(out_path, index=False)
    print(f"{len(merged):,} rows -> {out_path}")
    print(f"firm rowcount: {merged['firm'].value_counts().to_dict()}")


if __name__ == "__main__":
    main()
