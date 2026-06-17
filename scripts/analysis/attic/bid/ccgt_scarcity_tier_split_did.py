# STATUS: ALIVE
# LAST-AUDIT: 2026-05-26
# FEEDS: advisor_memo.tex sec 5(d) reforzada channel evidence (CCGT scarcity-
#        tier split: withholding share +3.7pp, competing-tier N_eff +0.18). Per-curve
#        sigma_p and N_eff for CCGT split into the competing tier (bid <= 200
#        EUR/MWh, the cleared portion) and the scarcity tier (bid > 200, the
#        parked portion that does not clear the day-ahead auction but is
#        recalled in Fase I redispatch). Run the same critical/flat DiD
#        on each tier separately for MTU15-DA.
#
# Tier cutoff: SCARCITY = 200 EUR/MWh, the clearing-price ceiling (DA price
# exceeds it in only 0.1% of periods). Same convention as the descriptive_facts
# two-tier bid curve figure.
#
# Outcomes per (unit, date, period):
#   - sigma_p_comp = MW-weighted SD of tranche prices in [0, 200]
#   - sigma_p_scarc = MW-weighted SD of tranche prices in (200, max]
#   - n_eff_comp, n_eff_scarc = inverse-Herfindahl tranche counts per tier
#   - comp_mw, scarc_mw = total MW per tier
#   - comp_share = comp_mw / (comp_mw + scarc_mw)
#
# OUT: results/regressions/bid/mtu15_critical_flat/ccgt_scarcity_tier_split.csv

from pathlib import Path
import sys

import duckdb
import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO / "scripts/analysis/bid"))
from mtu15_critical_flat_did import (  # noqa: E402
    clustered_ols, hour_class_label, CRITICAL, FLAT, WINDOWS,
)

DET = REPO / "data/processed/omie/mercado_diario/ofertas/det_all.parquet"
CAB = REPO / "data/processed/omie/mercado_diario/ofertas/cab_all.parquet"
UNITS = REPO / "data/external/omie_reference/lista_unidades.csv"
OUT = REPO / "results/regressions/bid/mtu15_critical_flat/ccgt_scarcity_tier_split.csv"
OUT.parent.mkdir(parents=True, exist_ok=True)

SCARCITY = 200.0  # bid > this cannot clear in the day-ahead (descriptive_facts sec 9)


def build_ccgt_two_tier_panel(lo, hi):
    """Per (unit, date, period) CCGT bid-shape functionals SPLIT into:
       - competing tier (bid <= SCARCITY=200): sigma_p_comp, n_eff_comp, comp_mw
       - scarcity tier  (bid >  SCARCITY=200): sigma_p_scarc, n_eff_scarc, scarc_mw
       Also reports the withholding share (scarc_mw / total)."""
    units = pd.read_csv(UNITS)
    units = units[units["technology"].str.lower().str.contains("ciclo combinado", na=False)][
        ["unit_code"]].drop_duplicates()
    con = duckdb.connect()
    con.execute("SET memory_limit='10GB'")
    con.execute("SET threads=4")
    con.register("u", units)
    sql = f"""
    WITH cab_l AS (
      SELECT d, offer_code, unit_code FROM (
        SELECT CAST(date AS DATE) d, offer_code, unit_code,
               ROW_NUMBER() OVER (PARTITION BY CAST(date AS DATE), offer_code, unit_code
                                  ORDER BY version DESC) rn
        FROM '{CAB}' WHERE date BETWEEN '{lo}' AND '{hi}' AND buy_sell='V'
      ) WHERE rn=1
    ),
    det AS (
      SELECT CAST(date AS DATE) d, offer_code, period,
             price_eur_mwh p, quantity_mw q, COALESCE(mtu_minutes, 60) AS mtu
      FROM '{DET}' WHERE date BETWEEN '{lo}' AND '{hi}' AND quantity_mw > 0
    ),
    joined AS (
      SELECT dv.d, dv.period, c.unit_code, dv.q, dv.p,
             CASE WHEN dv.mtu = 60 THEN dv.period - 1
                  ELSE CAST(FLOOR((dv.period - 1) / 4.0) AS INT) END AS clock_hour,
             CASE WHEN dv.p <= {SCARCITY} THEN 'comp' ELSE 'scarc' END AS tier
      FROM det dv JOIN cab_l c ON dv.d=c.d AND dv.offer_code=c.offer_code
        JOIN u ON c.unit_code = u.unit_code
    )
    SELECT d, period, clock_hour, unit_code, tier,
           SUM(q) sum_w, SUM(q*p) sum_wp, SUM(q*p*p) sum_wp2,
           SUM(q*q) sum_w2, COUNT(*) n_tranche
    FROM joined GROUP BY 1,2,3,4,5 HAVING SUM(q) > 0
    """
    df = con.execute(sql).fetchdf()
    df["d"] = pd.to_datetime(df["d"])
    mean_p = df["sum_wp"] / df["sum_w"]
    var_p = df["sum_wp2"] / df["sum_w"] - mean_p ** 2
    df["sigma_p"] = np.sqrt(var_p.clip(lower=0))
    df["n_eff"] = df["sum_w"] ** 2 / df["sum_w2"]
    df["hour_class"] = df["clock_hour"].map(hour_class_label)

    # Wide format: one row per (unit, date, period) with tier-specific columns
    out = df.pivot_table(index=["d", "period", "clock_hour", "unit_code", "hour_class"],
                        columns="tier", values=["sigma_p", "n_eff", "sum_w"],
                        aggfunc="first").reset_index()
    out.columns = ["_".join(c).strip("_") if isinstance(c, tuple) else c for c in out.columns]
    # Rename: sigma_p_comp, sigma_p_scarc, etc.
    rename = {"sum_w_comp": "comp_mw", "sum_w_scarc": "scarc_mw"}
    out = out.rename(columns=rename)
    for c in ["comp_mw", "scarc_mw"]:
        if c not in out.columns: out[c] = 0
    out["comp_mw"] = out["comp_mw"].fillna(0)
    out["scarc_mw"] = out["scarc_mw"].fillna(0)
    out["total_mw"] = out["comp_mw"] + out["scarc_mw"]
    out["scarc_share"] = out["scarc_mw"] / out["total_mw"].clip(lower=1e-6)
    return out


def run_did_on_tier(panel, reform, outcome):
    """Spec A DiD on a tier-specific outcome, restricting to (unit, date, period)
    cells where that tier has data (sum_w > 0)."""
    w = WINDOWS[reform]
    pre_lo, pre_hi = pd.Timestamp(w["pre_lo"]), pd.Timestamp(w["pre_hi"])
    post_lo, post_hi = pd.Timestamp(w["post_lo"]), pd.Timestamp(w["post_hi"])
    p = panel.copy()
    in_pre = (p["d"] >= pre_lo) & (p["d"] <= pre_hi)
    in_post = (p["d"] >= post_lo) & (p["d"] <= post_hi)
    p = p[(in_pre | in_post) & p["hour_class"].isin(["Critical", "Flat"])].copy()
    p = p.dropna(subset=[outcome])
    if len(p) < 50:
        return None
    p["post"] = (p["d"] >= post_lo).astype(int)
    p["crit"] = (p["hour_class"] == "Critical").astype(int)
    p["post_crit"] = p["post"] * p["crit"]
    cell = p.groupby(["post", "crit"])[outcome].mean().unstack()
    gm = p.groupby("unit_code")[outcome].transform("mean")
    p["y_w"] = p[outcome] - gm
    for c in ["post", "crit", "post_crit"]:
        gmc = p.groupby("unit_code")[c].transform("mean")
        p[c + "_w"] = p[c] - gmc
    X = np.column_stack([np.ones(len(p)), p["post_w"].values,
                         p["crit_w"].values, p["post_crit_w"].values])
    beta, se = clustered_ols(p["y_w"].values, X, p["d"].astype(str).values)
    pre_crit_v = cell.loc[(0, 1)] if (0, 1) in cell.stack().index else np.nan
    post_crit_v = cell.loc[(1, 1)] if (1, 1) in cell.stack().index else np.nan
    pre_flat_v = cell.loc[(0, 0)] if (0, 0) in cell.stack().index else np.nan
    post_flat_v = cell.loc[(1, 0)] if (1, 0) in cell.stack().index else np.nan
    return {"outcome": outcome, "n": len(p),
            "DiD": beta[3], "se": se[3], "t": beta[3] / se[3],
            "pre_crit": pre_crit_v, "post_crit": post_crit_v,
            "pre_flat": pre_flat_v, "post_flat": post_flat_v}


def main():
    rows = []
    outcomes = ["sigma_p_comp", "sigma_p_scarc",
                "n_eff_comp", "n_eff_scarc",
                "scarc_share", "comp_mw", "scarc_mw"]

    # ===== Baseline DA15 (Jul-Sep 25 vs Oct-Dec 25, cross-season) =====
    print("=== (1) DA15 BASELINE window (Jul-Sep 25 vs Oct-Dec 25, cross-season) ===")
    da_lo = WINDOWS["DA15"]["pre_lo"]
    da_hi = WINDOWS["DA15"]["post_hi"]
    panel = build_ccgt_two_tier_panel(da_lo, da_hi)
    print(f"  {len(panel):,} (unit, date, period) cells")
    print(f"  Withholding share: mean={panel['scarc_share'].mean():.1%}, "
          f"median={panel['scarc_share'].median():.1%}")
    for outcome in outcomes:
        r = run_did_on_tier(panel, "DA15", outcome)
        if r is None: continue
        rows.append({"spec": "DA15_baseline", **r})
        print(f"  {outcome:20s}  DiD={r['DiD']:+10.4f}  se={r['se']:9.4f}  t={r['t']:+6.2f}  n={r['n']:,}")

    # ===== Same-calendar-month DA15 (Oct-Dec 24 vs Oct-Dec 25) =====
    print("\n=== (2) DA15 SAME-CAL window (Oct-Dec 24 vs Oct-Dec 25) ===")
    WINDOWS["DA15_samecal"] = {
        "pre_lo": "2024-10-01", "pre_hi": "2024-12-31",
        "post_lo": "2025-10-01", "post_hi": "2025-12-31",
        "reform_date": pd.Timestamp("2025-10-01"),
    }
    panel_sc = build_ccgt_two_tier_panel("2024-10-01", "2025-12-31")
    panel_sc = panel_sc[panel_sc["d"].dt.month.isin([10, 11, 12])].copy()
    print(f"  {len(panel_sc):,} (unit, date, period) cells")
    print(f"  Withholding share: mean={panel_sc['scarc_share'].mean():.1%}, "
          f"median={panel_sc['scarc_share'].median():.1%}")
    for outcome in outcomes:
        r = run_did_on_tier(panel_sc, "DA15_samecal", outcome)
        if r is None: continue
        rows.append({"spec": "DA15_samecal", **r})
        print(f"  {outcome:20s}  DiD={r['DiD']:+10.4f}  se={r['se']:9.4f}  t={r['t']:+6.2f}  n={r['n']:,}")

    pd.DataFrame(rows).to_csv(OUT, index=False)
    print(f"\nWrote {OUT}")


if __name__ == "__main__":
    main()
