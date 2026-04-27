# STATUS: ALIVE
# LAST-AUDIT: 2026-04-27
# FEEDS: F16
# CLAIM: Per-firm CCGT supply-curve slope (MW per €/MWh) with hour + month FE; IB strategic-posture break at blackout
"""F16 audit script — per-firm CCGT supply slope across post-MTU15-IDA regimes.

Regression: CCGT_MW_firm_t ~ price_da_eur_t + month-FE + hour-FE
Run separately for each (firm × regime) combination across:
  - DA60/ID15 PRE-blackout (2025-03-19 → 2025-04-27, n≈953)
  - DA60/ID15 POST-blackout (2025-04-28 → 2025-09-30, n≈3622)
  - DA15/ID15 (2025-10-01 onwards, n≈18471)

Result: IB slope was 0.95 MW/€ pre-blackout (price-INELASTIC, strategic
posture) and jumped 4.8× to 4.55 MW/€ post-blackout (operational dispatch
under operación reforzada P.O. 3.2 voltage-support recommitments). GN
consistently most price-responsive (merit-order); GE essentially price-
inelastic (slope ~0).

What this is NOT: a causal supply-elasticity estimate (price endogenous).
What it IS: a robust conditional correlation that quantifies the firm-
by-firm strategic-vs-operational dispatch posture and shows IB's posture
broke specifically with operación reforzada — not with MTU15-IDA itself.

Output: data/derived/results/ccgt_supply_slope_by_firm_regime.csv
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd
import statsmodels.api as sm

PROJECT = Path(__file__).resolve().parents[3]


def assign_regime(d: pd.Timestamp) -> str:
    if d < pd.Timestamp("2024-06-14"):
        return "pre-IDA"
    if d < pd.Timestamp("2024-12-01"):
        return "3-sess"
    if d < pd.Timestamp("2025-03-19"):
        return "ISP15-win"
    if d < pd.Timestamp("2025-04-28"):
        return "DA60 PRE-blackout"
    if d < pd.Timestamp("2025-10-01"):
        return "DA60 POST-blackout"
    return "DA15/ID15"


def main() -> None:
    panel = pd.read_parquet(PROJECT / "data/derived/panels/entsoe_system_panel.parquet")
    ccgt = pd.read_parquet(PROJECT / "data/processed/entsoe/generation/ccgt_per_firm_panel.parquet")

    ccgt_h = ccgt.pivot_table(
        index="isp_start_utc", columns="firm", values="quantity_mw",
        aggfunc="sum", fill_value=0,
    ).reset_index()

    mg = panel.merge(ccgt_h, on="isp_start_utc", how="inner")
    mg = mg.dropna(subset=["price_da_eur"])
    mg["ts"] = pd.to_datetime(mg["isp_start_utc"])
    mg["hour"] = mg["ts"].dt.hour
    mg["month_num"] = mg["ts"].dt.month
    mg["regime"] = mg["ts"].apply(assign_regime)

    rows = []
    print(f'{"Regime":22} {"Firm":4} {"n":>6} {"slope":>8} {"se":>7} {"R²":>6} {"intercept":>10}')
    for reg in ["DA60 PRE-blackout", "DA60 POST-blackout", "DA15/ID15"]:
        sub = mg[mg.regime == reg].copy()
        if len(sub) < 100:
            continue
        Xd = pd.concat([
            sub[["price_da_eur"]].astype(float).reset_index(drop=True),
            pd.get_dummies(sub["month_num"], prefix="m", drop_first=True).astype(int).reset_index(drop=True),
            pd.get_dummies(sub["hour"], prefix="h", drop_first=True).astype(int).reset_index(drop=True),
        ], axis=1)
        Xd = sm.add_constant(Xd)
        for f in ["IB", "GN", "GE", "HC"]:
            if f not in sub.columns:
                continue
            m = sm.OLS(sub[f].reset_index(drop=True), Xd).fit()
            b = m.params["price_da_eur"]
            se = m.bse["price_da_eur"]
            rows.append({
                "regime": reg, "firm": f, "n": len(sub),
                "slope": b, "se": se, "r2": m.rsquared,
                "intercept": m.params["const"],
            })
            print(f"{reg:22} {f:4} {len(sub):>6} {b:>8.2f} {se:>7.2f} {m.rsquared:>6.3f} {m.params['const']:>10.0f}")

    out_path = PROJECT / "data/derived/results/ccgt_supply_slope_by_firm_regime.csv"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(out_path, index=False)
    print(f"\nwrote {out_path}")


if __name__ == "__main__":
    main()
