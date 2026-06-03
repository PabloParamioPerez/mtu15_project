# STATUS: ALIVE
# LAST-AUDIT: 2026-05-31
# CLAIM: Big-4 per-(firm, tech) Spec C metrics (sigma_p, N_eff) at
#        *preliminary*'s window-and-market-specific bandwidth (DA15
#        DA h=50; DA15 IDA h=58). Pre window 2025-04-28 .. 2025-09-30
#        (reforzada-era pre-DA15); post window 2025-10-01 .. 2025-11-09
#        (panel ends; consistent with *preliminary*'s Spec C scope).
#
# Per-firm reading of CCGT DA sigma_p DiD (post crit-flat minus pre
# crit-flat) at h=50 reproduces the *preliminary* §A.4 attribution:
#   GN +3.72  (largest -- the "Naturgy primary" reading)
#   IB +0.96  (the "Iberdrola to a lesser extent" reading)
#   HC +0.83  (small but positive)
#   GE -0.10  (essentially zero -- the "Endesa does not contribute")
# The per-tech leader differs: GN leads on CCGT, IB on hydro and pump,
# GE leads on pump alone. The pooled CCGT DiD attributed to "primarily
# Naturgy" in the preliminary memo is structurally a per-(firm, tech)
# concentration result.
#
# IN:  data/derived/panels/per_curve_windowed/per_curve_DA15_real_{DA,IDA}_h*.parquet
# OUT: results/regressions/firm/per_firm_tech_bid_shape/per_firm_tech_specc_h_window.csv

from pathlib import Path

import duckdb
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
PANEL_DA = REPO / "data/derived/panels/per_curve_windowed/per_curve_DA15_real_DA_h50.parquet"
PANEL_IDA = REPO / "data/derived/panels/per_curve_windowed/per_curve_DA15_real_IDA_h58.parquet"
OUT = REPO / "results/regressions/firm/per_firm_tech_bid_shape"
OUT.mkdir(parents=True, exist_ok=True)

PRE = ("2025-07-01", "2025-09-30")
POST = ("2025-10-01", "2025-12-31")  # panel covers up to 2025-11-09


def cells(panel, label):
    q = f"""
    SELECT
      firm, tech, hour_class,
      CASE WHEN d BETWEEN TIMESTAMP '{PRE[0]}' AND TIMESTAMP '{PRE[1]}'  THEN 'pre'
           WHEN d BETWEEN TIMESTAMP '{POST[0]}' AND TIMESTAMP '{POST[1]}' THEN 'post' END AS win,
      ROUND(AVG(sigma_p), 3) AS sigma_p,
      ROUND(AVG(n_eff), 3) AS n_eff,
      COUNT(*) AS n_curves
    FROM '{panel}'
    WHERE firm IN ('IB','GN','GE','HC')
      AND hour_class IN ('Critical','Flat')
      AND d BETWEEN TIMESTAMP '{PRE[0]}' AND TIMESTAMP '{POST[1]}'
    GROUP BY firm, tech, hour_class, win
    """
    df = duckdb.sql(q).df()
    df = df.dropna(subset=["win"])
    df["market"] = label
    return df


def make_did(df):
    """Compute crit-flat differential DiD per (firm, tech, market)."""
    out = []
    for (firm, tech, market), g in df.groupby(["firm", "tech", "market"]):
        d = {(r.win, r.hour_class): r for r in g.itertuples()}
        if all(k in d for k in [("pre","Critical"), ("pre","Flat"),
                                 ("post","Critical"), ("post","Flat")]):
            sig_did = (d[("post","Critical")].sigma_p - d[("post","Flat")].sigma_p) \
                    - (d[("pre","Critical")].sigma_p - d[("pre","Flat")].sigma_p)
            neff_did = (d[("post","Critical")].n_eff - d[("post","Flat")].n_eff) \
                     - (d[("pre","Critical")].n_eff - d[("pre","Flat")].n_eff)
            out.append({
                "firm": firm, "tech": tech, "market": market,
                "sigma_p_did": round(sig_did, 3),
                "n_eff_did": round(neff_did, 3),
                "sigma_p_post_crit": d[("post","Critical")].sigma_p,
                "sigma_p_post_flat": d[("post","Flat")].sigma_p,
                "n_eff_post_crit": d[("post","Critical")].n_eff,
                "n_eff_post_flat": d[("post","Flat")].n_eff,
                "n_curves_post_crit": d[("post","Critical")].n_curves,
            })
    return pd.DataFrame(out)


def main():
    da = cells(PANEL_DA, "DA (h=50)")
    ida = cells(PANEL_IDA, "IDA (h=58)")
    cells_df = pd.concat([da, ida], ignore_index=True)

    print("=== Spec C cells per (firm, tech, hour_class, win, market) ===")
    print(cells_df.to_string(index=False))

    did = make_did(cells_df)
    did.to_csv(OUT / "per_firm_tech_specc_h_window.csv", index=False)
    print("\n=== DiD per (firm, tech, market) ===")
    print(did.to_string(index=False))

    print("\n=== Compact DA pivot (sigma_p DiD) ===")
    da_did = did[did.market == "DA (h=50)"]
    print(da_did.pivot(index="firm", columns="tech", values="sigma_p_did").to_string())

    print("\n=== Compact DA pivot (n_eff DiD) ===")
    print(da_did.pivot(index="firm", columns="tech", values="n_eff_did").to_string())

    print(f"\nwrote: {OUT}/per_firm_tech_specc_h_window.csv")


if __name__ == "__main__":
    main()
