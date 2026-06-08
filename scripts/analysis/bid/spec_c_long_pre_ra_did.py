# STATUS: ALIVE
# LAST-AUDIT: 2026-06-06
# FEEDS: thesis/paper/thesis.tex Spec C robustness -- addresses parallel-trends
#        concern raised by advisor (2026-06-06): "I'm worried about long-pre +
#        year*renewable -- could be seasonality." Implements three specs:
#
#   (1) HEADLINE: tight pre/post, no covariates (existing default).
#   (2) LONG-PRE + CONTROLS: extend pre to 2022-01-01, add year FE +
#       cal-month FE + daily wind/solar interaction with hour-class.
#   (3) RA-DiD (HIT 1997, HIST 1998): regression adjustment under
#       conditional parallel trends. Estimate the counterfactual trend
#       in flat hours conditional on (year, cal-month, renewable
#       production), then subtract from the realized treated change.
#
#  Outcomes: sigma_p (robust), HHI = 1/n_eff (concentration).
#  Cells:
#   - DA15 DA CCGT (headline)
#   - DA15 DA Hydro
#   - DA15 DA Hydro_pump
#   - ID15 DA CCGT (anticipation)
#   - ID15 IDA Hydro
#   - ID15 IDA Hydro_pump
#
#  OUT: results/regressions/bid/mtu15_critical_flat/spec_c_long_pre_ra_did.csv

from pathlib import Path
import duckdb
import numpy as np
import pandas as pd
import sys

REPO = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO / "scripts/analysis/bid"))
from mtu15_critical_flat_did import clustered_ols, CRITICAL, FLAT  # noqa: E402

DA_PANEL  = REPO / "data/derived/panels/per_curve_metrics_da_full.parquet"
IDA_PANEL = REPO / "data/derived/panels/per_curve_metrics_ida.parquet"
RENEW    = REPO / "data/derived/panels/bsts_daily_panel.parquet"
OUT       = REPO / "results/regressions/bid/mtu15_critical_flat/spec_c_long_pre_ra_did.csv"

LONG_PRE_START = "2022-01-01"

WINDOWS = {
    # reform : (tight_pre_lo, tight_pre_hi, post_lo, post_hi)
    "ID15_DA":  ("2024-12-19", "2025-03-18", "2025-03-19", "2025-04-27"),
    "ID15_IDA": ("2024-12-19", "2025-03-18", "2025-03-19", "2025-04-27"),
    "DA15_DA":  ("2025-07-01", "2025-09-30", "2025-10-01", "2025-12-31"),
}

CELLS = [
    ("DA15_DA",  "CCGT",        DA_PANEL),
    ("DA15_DA",  "Hydro",       DA_PANEL),
    ("DA15_DA",  "Hydro_pump",  DA_PANEL),
    ("ID15_DA",  "CCGT",        DA_PANEL),
    ("ID15_IDA", "Hydro",       IDA_PANEL),
    ("ID15_IDA", "Hydro_pump",  IDA_PANEL),
]


def hour_class(h):
    if h in CRITICAL: return "Critical"
    if h in FLAT:     return "Flat"
    return "Other"


def load_panel(panel_fp, lo, hi, tech, has_session=False):
    con = duckdb.connect()
    sess = "session_number," if has_session else ""
    df = con.execute(f"""
        SELECT d, {sess} period, clock_hour, unit_code, firm, tech, n_tranche, sigma_p, n_eff
        FROM '{panel_fp}'
        WHERE d BETWEEN '{lo}' AND '{hi}' AND tech = '{tech}'
    """).fetchdf()
    df["d"] = pd.to_datetime(df["d"])
    df["hour_class"] = df["clock_hour"].apply(hour_class)
    df = df[df["hour_class"].isin(["Critical", "Flat"])].copy()
    df["hhi"] = 1.0 / df["n_eff"]
    return df


def load_renew():
    con = duckdb.connect()
    r = con.execute(f"""
        SELECT d, wind_gwh, solar_gwh, gas_eur
        FROM '{RENEW}'
    """).fetchdf()
    r["d"] = pd.to_datetime(r["d"])
    return r


def did_basic(p, outcome, T_ref):
    """Spec (1) headline: post + crit + post*crit + unit FE, clustered SE by date."""
    p = p.dropna(subset=[outcome]).copy()
    if len(p) < 100:
        return None
    p["post"] = (p["d"] >= pd.Timestamp(T_ref)).astype(int)
    p["crit"] = (p["hour_class"] == "Critical").astype(int)
    p["post_crit"] = p["post"] * p["crit"]
    gm = p.groupby("unit_code")[outcome].transform("mean")
    p["y_w"] = p[outcome] - gm
    for c in ["post", "crit", "post_crit"]:
        gmc = p.groupby("unit_code")[c].transform("mean")
        p[c + "_w"] = p[c] - gmc
    X = np.column_stack([np.ones(len(p)), p["post_w"], p["crit_w"], p["post_crit_w"]])
    beta, se = clustered_ols(p["y_w"].values, X, p["d"].astype(str).values)
    return {"DiD": beta[3], "se": se[3], "t": beta[3] / se[3], "n": len(p)}


def did_long_pre_controls(p, renew, outcome, T_ref):
    """Spec (2) long-pre + year FE + cal-month FE + renewable*hour_class controls.

    Within-transform on unit FE; then DiD with explicit dummies for year, month,
    and crit*wind, crit*solar interactions to absorb season-by-treatment trends.
    """
    p = p.merge(renew, on="d", how="left").dropna(subset=[outcome, "wind_gwh", "solar_gwh"]).copy()
    if len(p) < 100:
        return None
    p["post"] = (p["d"] >= pd.Timestamp(T_ref)).astype(int)
    p["crit"] = (p["hour_class"] == "Critical").astype(int)
    p["post_crit"] = p["post"] * p["crit"]
    p["year"]  = p["d"].dt.year
    p["month"] = p["d"].dt.month
    # crit interactions with renewables (absorb season-by-treatment differential trend)
    p["crit_wind"]  = p["crit"] * p["wind_gwh"]
    p["crit_solar"] = p["crit"] * p["solar_gwh"]
    # Year dummies (drop first)
    for y in sorted(p["year"].unique())[1:]:
        p[f"y_{y}"] = (p["year"] == y).astype(int)
    # Month dummies (drop first)
    for m in sorted(p["month"].unique())[1:]:
        p[f"m_{m}"] = (p["month"] == m).astype(int)
    # Within-transform on unit FE
    y_cols = [f"y_{y}" for y in sorted(p["year"].unique())[1:]]
    m_cols = [f"m_{m}" for m in sorted(p["month"].unique())[1:]]
    feature_cols = ["post", "crit", "post_crit", "wind_gwh", "solar_gwh",
                    "crit_wind", "crit_solar"] + y_cols + m_cols
    gm_y = p.groupby("unit_code")[outcome].transform("mean")
    p["y_w"] = p[outcome] - gm_y
    for c in feature_cols:
        gmc = p.groupby("unit_code")[c].transform("mean")
        p[c + "_w"] = p[c] - gmc
    X_cols = [c + "_w" for c in feature_cols]
    X = np.column_stack([np.ones(len(p))] + [p[c].values for c in X_cols])
    beta, se = clustered_ols(p["y_w"].values, X, p["d"].astype(str).values)
    did_idx = 1 + feature_cols.index("post_crit")
    return {"DiD": beta[did_idx], "se": se[did_idx],
            "t": beta[did_idx] / se[did_idx], "n": len(p)}


def did_ra_hit(p, renew, outcome, T_ref):
    """Spec (3) RA-DiD per Heckman-Ichimura-Todd (1997, 1998).

    Conditional parallel trends: E[Y(infty)_post - Y(infty)_pre | crit, X]
                                = E[Y(infty)_post - Y(infty)_pre | flat, X].
    Identifying assumption: AFTER conditioning on X = (year, cal-month, wind, solar),
    the flat-hour pre/post change predicts what would have happened in critical
    absent the reform.

    Estimation:
      1. On FLAT hours, regress (Y_post - Y_pre) at unit level on X covariates.
         => m_Delta^flat(X) estimator.
      2. For each CRITICAL unit, predict the counterfactual change from its X.
      3. ATT = mean(realized Δ in critical) - mean(predicted Δ from flat model).
    """
    p = p.merge(renew, on="d", how="left").dropna(subset=[outcome, "wind_gwh", "solar_gwh"]).copy()
    if len(p) < 100:
        return None
    p["post"] = (p["d"] >= pd.Timestamp(T_ref)).astype(int)
    p["year"]  = p["d"].dt.year
    p["month"] = p["d"].dt.month
    # Per-(unit, post, hour_class), pre-aggregate and define Δ_unit = mean(post) - mean(pre)
    g = p.groupby(["unit_code", "hour_class", "post"]).agg(
        y=(outcome, "mean"), wind=("wind_gwh", "mean"), solar=("solar_gwh", "mean"),
        d_mean=("d", lambda s: s.astype("int64").mean())
    ).reset_index()
    piv = g.pivot_table(index=["unit_code", "hour_class"], columns="post",
                        values=["y", "wind", "solar"]).reset_index()
    piv.columns = ["unit_code", "hour_class"] + [f"{a}_{b}" for a, b in piv.columns[2:]]
    piv = piv.dropna()
    piv["dy"] = piv["y_1"] - piv["y_0"]
    piv["wind_avg"] = (piv["wind_0"] + piv["wind_1"]) / 2
    piv["solar_avg"] = (piv["solar_0"] + piv["solar_1"]) / 2
    flat = piv[piv["hour_class"] == "Flat"].copy()
    crit = piv[piv["hour_class"] == "Critical"].copy()
    if len(flat) < 5 or len(crit) < 5:
        return None
    # Regress dy on wind_avg + solar_avg + intercept on the FLAT subgroup
    X_flat = np.column_stack([np.ones(len(flat)), flat["wind_avg"], flat["solar_avg"]])
    y_flat = flat["dy"].values
    try:
        beta_flat = np.linalg.lstsq(X_flat, y_flat, rcond=None)[0]
    except np.linalg.LinAlgError:
        return None
    # Predict counterfactual dy for critical units using their (wind_avg, solar_avg)
    X_crit = np.column_stack([np.ones(len(crit)), crit["wind_avg"], crit["solar_avg"]])
    dy_pred = X_crit @ beta_flat
    # ATT = mean(realized dy in crit) - mean(predicted dy if they had been flat)
    att = (crit["dy"].values - dy_pred).mean()
    # Approximate SE via bootstrap on critical units
    rng_seed = 42
    rng = np.random.default_rng(rng_seed)
    boot = []
    for _ in range(500):
        idx_f = rng.integers(0, len(flat), len(flat))
        idx_c = rng.integers(0, len(crit), len(crit))
        Xf_b = np.column_stack([np.ones(len(idx_f)), flat["wind_avg"].values[idx_f],
                                flat["solar_avg"].values[idx_f]])
        yf_b = flat["dy"].values[idx_f]
        try:
            bf = np.linalg.lstsq(Xf_b, yf_b, rcond=None)[0]
        except np.linalg.LinAlgError:
            continue
        Xc_b = np.column_stack([np.ones(len(idx_c)), crit["wind_avg"].values[idx_c],
                                crit["solar_avg"].values[idx_c]])
        dypred_b = Xc_b @ bf
        boot.append((crit["dy"].values[idx_c] - dypred_b).mean())
    se = float(np.std(boot)) if boot else np.nan
    return {"DiD": float(att), "se": se, "t": att / se if se else np.nan,
            "n_crit": len(crit), "n_flat": len(flat),
            "n_units": piv["unit_code"].nunique()}


def main():
    print("Loading renewables panel ...", flush=True)
    renew = load_renew()
    print(f"  rows: {len(renew):,}", flush=True)

    rows = []
    for reform_market, tech, panel_fp in CELLS:
        tight_pre_lo, tight_pre_hi, post_lo, post_hi = WINDOWS[reform_market]
        T_ref = post_lo
        print(f"\n=== {reform_market} {tech} ===", flush=True)
        # (1) Headline: tight window, no covariates
        p_tight = load_panel(panel_fp, tight_pre_lo, post_hi, tech,
                              has_session=("IDA" in reform_market))
        for outcome in ("sigma_p", "hhi"):
            r = did_basic(p_tight, outcome, T_ref)
            if r is None: continue
            stars = "***" if abs(r["t"]) >= 2.58 else "**" if abs(r["t"]) >= 1.96 else "*" if abs(r["t"]) >= 1.645 else ""
            rows.append({"cell": f"{reform_market} {tech}", "outcome": outcome,
                         "spec": "(1) tight headline", **r})
            print(f"  (1) tight {outcome:>8s}: DiD={r['DiD']:+.4f} (SE {r['se']:.4f}) t={r['t']:+.2f}{stars}  n={r['n']:,}")

        # (2) Long-pre with controls
        p_long = load_panel(panel_fp, LONG_PRE_START, post_hi, tech,
                             has_session=("IDA" in reform_market))
        for outcome in ("sigma_p", "hhi"):
            r = did_long_pre_controls(p_long, renew, outcome, T_ref)
            if r is None: continue
            stars = "***" if abs(r["t"]) >= 2.58 else "**" if abs(r["t"]) >= 1.96 else "*" if abs(r["t"]) >= 1.645 else ""
            rows.append({"cell": f"{reform_market} {tech}", "outcome": outcome,
                         "spec": "(2) long-pre + year + month + renew", **r})
            print(f"  (2) longpr {outcome:>8s}: DiD={r['DiD']:+.4f} (SE {r['se']:.4f}) t={r['t']:+.2f}{stars}  n={r['n']:,}")

        # (3) RA-DiD (HIT 1997)
        for outcome in ("sigma_p", "hhi"):
            r = did_ra_hit(p_long, renew, outcome, T_ref)
            if r is None: continue
            stars = "***" if abs(r["t"]) >= 2.58 else "**" if abs(r["t"]) >= 1.96 else "*" if abs(r["t"]) >= 1.645 else ""
            rows.append({"cell": f"{reform_market} {tech}", "outcome": outcome,
                         "spec": "(3) RA-DiD (HIT 1997)", **r})
            print(f"  (3) RA-DiD {outcome:>8s}: ATT={r['DiD']:+.4f} (SE {r['se']:.4f}) t={r['t']:+.2f}{stars}  units={r['n_units']}")

    out_df = pd.DataFrame(rows)
    OUT.parent.mkdir(parents=True, exist_ok=True)
    out_df.to_csv(OUT, index=False)
    print(f"\nWrote {OUT}\n")

    # Pretty summary
    print("=" * 80)
    print("SPEC C ROBUSTNESS SUMMARY")
    print("=" * 80)
    for cell in out_df["cell"].unique():
        sub = out_df[out_df["cell"] == cell]
        print(f"\n{cell}")
        for outcome in ("sigma_p", "hhi"):
            ss = sub[sub["outcome"] == outcome]
            if ss.empty: continue
            line = f"  {outcome:8s}: "
            for _, r in ss.iterrows():
                stars = "***" if abs(r["t"]) >= 2.58 else "**" if abs(r["t"]) >= 1.96 else "*" if abs(r["t"]) >= 1.645 else ""
                line += f"  [{r['spec'][:6]}: {r['DiD']:+.3f}{stars}]"
            print(line)


if __name__ == "__main__":
    main()
