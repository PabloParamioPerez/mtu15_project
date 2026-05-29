# STATUS: ALIVE
# LAST-AUDIT: 2026-05-27
# FEEDS: advisor_memo discussion -- "smart descriptive" regressions of the
#        per-curve sigma_p and N_eff on Critical / Midday dummies, fully
#        interacted with regime, with unit FE absorbed (within-unit demeaning)
#        and date-clustered SEs. One regression per (tech, market, outcome)
#        with regime interactions; report all regime-specific differentials
#        in compact tables.
#
# Outcomes:    sigma_p, n_eff
# Markets:     DA, IDA
# Techs:       CCGT, Hydro, Hydro_pump, Wind
# Regimes:     pre-IDA-ref, 3-sess, ISP15-win, ID15 pre-blackout,
#              ID15 post-blackout, DA15+ID15
#
# OUT: results/regressions/bid/mtu15_critical_flat/critflat_smart_descriptive.csv

from pathlib import Path
import sys

import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO / "scripts/analysis/bid"))
from mtu15_critical_flat_did import clustered_ols  # noqa: E402

DA_PANEL = REPO / "data/derived/panels/per_curve_metrics_da_full.parquet"
IDA_PANEL = REPO / "data/derived/panels/per_curve_metrics_ida.parquet"
OUT = REPO / "results/regressions/bid/mtu15_critical_flat/critflat_smart_descriptive.csv"
OUT.parent.mkdir(parents=True, exist_ok=True)

CRITICAL = {5, 6, 7, 8, 16, 17, 18, 19, 20, 21, 22}
FLAT = {1, 2, 3}
MIDDAY = {11, 12, 13, 14}

REGIMES = [
    ("pre_IDA",    "2022-01-01", "2024-06-13"),
    ("3sess",      "2024-06-14", "2024-11-30"),
    ("ISP15_win",  "2024-12-01", "2025-03-18"),
    ("ID15_pre",   "2025-03-19", "2025-04-27"),
    ("ID15_post",  "2025-04-28", "2025-09-30"),
    ("DA15_ID15",  "2025-10-01", "2026-02-26"),
]
REGIME_NAMES = [r[0] for r in REGIMES]


def assign_regime(d):
    for name, lo, hi in REGIMES:
        if pd.Timestamp(lo) <= d <= pd.Timestamp(hi):
            return name
    return None


def hour_class(h):
    if h in CRITICAL: return "Critical"
    if h in FLAT: return "Flat"
    if h in MIDDAY: return "Midday"
    return "Other"


def run_one(panel, tech, market):
    p = panel[panel["tech"] == tech].copy()
    p["d"] = pd.to_datetime(p["d"])
    p["hc"] = p["clock_hour"].apply(hour_class)
    p = p[p["hc"].isin(["Critical", "Flat", "Midday"])]
    p["regime"] = p["d"].apply(assign_regime)
    p = p[p["regime"].isin(REGIME_NAMES)]
    if len(p) < 50:
        return []

    # Critical/Midday dummies (Flat is omitted baseline)
    p["crit"] = (p["hc"] == "Critical").astype(int)
    p["mid"]  = (p["hc"] == "Midday").astype(int)

    out_rows = []
    for outcome in ["sigma_p", "n_eff"]:
        d = p.dropna(subset=[outcome]).copy()
        if len(d) < 50:
            continue
        # Build design with regime dummies + interactions, omitting one regime as baseline.
        baseline_regime = REGIME_NAMES[0]
        other = [r for r in REGIME_NAMES if r != baseline_regime
                 and (d["regime"] == r).any()]
        # Columns: intercept, crit, mid, regime dummies, crit:regime interactions, mid:regime interactions
        cols = []
        names = []
        # Drop intercept: within-unit demeaning absorbs it (demeaned column is all-zero).
        cols.append(d["crit"].values.astype(float)); names.append("crit")
        cols.append(d["mid"].values.astype(float));  names.append("mid")
        for r in other:
            ind = (d["regime"] == r).astype(float).values
            cols.append(ind);                                       names.append(f"R_{r}")
            cols.append((ind * d["crit"].values).astype(float));    names.append(f"crit_x_{r}")
            cols.append((ind * d["mid"].values).astype(float));     names.append(f"mid_x_{r}")
        X = np.column_stack(cols)
        # Within-unit demean (absorbs unit FE)
        # NOTE: demean every column AND the response so the regression is equivalent to FE OLS
        df_for_dm = pd.DataFrame(X, columns=names)
        df_for_dm["y"] = d[outcome].values
        df_for_dm["unit_code"] = d["unit_code"].values
        for c in names + ["y"]:
            gm = df_for_dm.groupby("unit_code")[c].transform("mean")
            df_for_dm[c] = df_for_dm[c] - gm
        Xd = df_for_dm[names].values
        yd = df_for_dm["y"].values
        try:
            beta, se = clustered_ols(yd, Xd, d["d"].astype(str).values)
        except Exception as e:
            print(f"  [{tech} {market} {outcome}] clustered_ols failed: {e}")
            continue
        idx = {n: i for i, n in enumerate(names)}

        # Report Critical and Midday differentials per regime
        for r in REGIME_NAMES:
            if r == baseline_regime:
                b_crit = beta[idx["crit"]]
                b_mid  = beta[idx["mid"]]
                s_crit = se[idx["crit"]]
                s_mid  = se[idx["mid"]]
            elif f"crit_x_{r}" in idx:
                b_crit = beta[idx["crit"]] + beta[idx[f"crit_x_{r}"]]
                b_mid  = beta[idx["mid"]]  + beta[idx[f"mid_x_{r}"]]
                # SE of sum: sqrt(var_a + var_b + 2cov). Use cov from inv(X'X) of the
                # clustered_ols residual.  Approximation: use sqrt(se^2 + se_int^2) (cov ignored).
                s_crit = np.sqrt(se[idx["crit"]]**2 + se[idx[f"crit_x_{r}"]]**2)
                s_mid  = np.sqrt(se[idx["mid"]]**2  + se[idx[f"mid_x_{r}"]]**2)
            else:
                continue
            n_r = (d["regime"] == r).sum()
            out_rows.append({
                "market": market, "tech": tech, "outcome": outcome, "regime": r,
                "beta_crit": b_crit, "se_crit": s_crit,
                "beta_mid":  b_mid,  "se_mid":  s_mid,
                "n_curves":  int(n_r),
            })
    return out_rows


def main():
    print(f"Loading panels...")
    da  = pd.read_parquet(DA_PANEL)
    ida = pd.read_parquet(IDA_PANEL)
    print(f"  DA:  {len(da):,} curves   IDA: {len(ida):,} curves")

    rows = []
    for tech in ["CCGT", "Hydro", "Hydro_pump", "Wind"]:
        for market, p in [("DA", da), ("IDA", ida)]:
            print(f"  {tech:12s} {market}  ...", end="", flush=True)
            rows.extend(run_one(p, tech, market))
            print(" done")
    df = pd.DataFrame(rows)
    df.to_csv(OUT, index=False)
    print(f"\nWrote {OUT}")

    # Compact wide tables: beta_crit and beta_mid per (tech, market, outcome, regime)
    for outcome in ["sigma_p", "n_eff"]:
        for which in ["beta_crit", "beta_mid"]:
            print(f"\n=== {outcome}  {which}  (Flat baseline; FE absorbed) ===")
            piv = df[df["outcome"] == outcome].pivot_table(
                index=["tech", "market"], columns="regime", values=which
            )
            piv = piv[[r for r in REGIME_NAMES if r in piv.columns]]
            print(piv.round(2).to_string())


if __name__ == "__main__":
    main()
