# STATUS: ALIVE
# LAST-AUDIT: 2026-05-25
# FEEDS: advisor_memo.tex headline-reading correction; descriptive_facts.tex
#        methodology link.
# CLAIM: Implements the *proper* Fourier seasonal adjustment per the
#        descriptive_facts.tex Frisch-Waugh recipe in src/mtu/analysis/sa_fwl.py:
#          (1) fit y_t = alpha + regime FE + Fourier(K=4) + DOW dummies on
#              the post-2022 daily series PER HOUR-CLASS (2022-01-01 onwards,
#              consistent with the descriptive_facts canonical START) -- identity
#              link so SA-DiD coefficients land in the outcome's natural units
#              (EUR/MWh for prices, MW for quantities), directly comparable to
#              the level baseline and the same-calendar-month estimates.
#          (2) predict the seasonal-cycle component (intercept + Fourier + DOW,
#              ZEROING regime effects) at each date in the regression window
#          (3) subtract the predicted seasonal-cycle from y -> SA residual
#          (4) run the critical/flat DiD on the SA residual
#        Crucially, regime dummies ARE in the fit (to absorb pre-IDA / 3-sess /
#        ISP15-win / MTU15-IDA-pre-blk / MTU15-IDA-post-blk / DA15-ID15 level
#        shifts so the Fourier coefficients are not biased by regime mix) but
#        ZEROED in the prediction (so the SA residual retains the regime-level
#        shifts -- which is what the DiD is supposed to test).
#
#        This script uses src/mtu/analysis/sa_fwl.py helpers exactly. Window
#        shortened to 2022-01-01..2026-05-15 (the same as
#        bidshape_deseasonalized_by_*.py).
#
# Outcomes covered: DA clearing price, IDA clearing price, pump-storage cleared
#                   MW (gen only), CCGT cleared MW (gen only).
#
# OUT: results/regressions/bid/mtu15_critical_flat/proper_fourier_sa_did.csv

from pathlib import Path
import sys

import duckdb
import numpy as np
import pandas as pd
import statsmodels.api as sm

REPO = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO / "src"))
from mtu.analysis.sa_fwl import (  # noqa: E402
    attach_design_columns, fourier_terms, dow_dummies, DEFAULT_K,
)

OUT = REPO / "results/regressions/bid/mtu15_critical_flat/proper_fourier_sa_did.csv"
OUT.parent.mkdir(parents=True, exist_ok=True)

MPDBC = REPO / "data/processed/omie/mercado_diario/precios/marginalpdbc_all.parquet"
MPIBC = REPO / "data/processed/omie/mercado_intradiario_subastas/precios/marginalpibc_all.parquet"
PDBC = REPO / "data/processed/omie/mercado_diario/programas/pdbc_all.parquet"
UNITS = REPO / "data/external/omie_reference/lista_unidades.csv"

CRITICAL = {5, 6, 7, 8, 16, 17, 18, 19, 20, 21, 22}
FLAT = {1, 2, 3}

# Fit window: 2022-01-01 to most-recent date in panel. Same convention as
# scripts/analysis/bid/bidshape_deseasonalized_by_firm_hour_class.py.
START = pd.Timestamp("2022-01-01")
END = pd.Timestamp("2026-05-15")
K = DEFAULT_K

# Canonical 5-regime taxonomy from src/mtu/analysis/sa_fwl.py users.
REGIME_DATES = [
    ("3sess",         pd.Timestamp("2024-06-14"), pd.Timestamp("2024-11-30")),
    ("ISP15win",      pd.Timestamp("2024-12-01"), pd.Timestamp("2025-03-18")),
    ("MTU15IDA_pre",  pd.Timestamp("2025-03-19"), pd.Timestamp("2025-04-27")),
    ("MTU15IDA_post", pd.Timestamp("2025-04-28"), pd.Timestamp("2025-09-30")),
    ("DA15_ID15",     pd.Timestamp("2025-10-01"), END),
]
FOURIER_COLS = [f"{p}_{k}" for k in range(1, K + 1) for p in ("cos", "sin")]
DOW_COLS = [f"dow_{i}" for i in range(1, 7)]
REGIME_COLS = [f"D_{tup[0]}" for tup in REGIME_DATES]


def hour_class(h):
    if h in CRITICAL: return "Critical"
    if h in FLAT: return "Flat"
    return "Other"


def clustered_ols(y, X, cluster):
    XtX_inv = np.linalg.inv(X.T @ X)
    beta = XtX_inv @ (X.T @ y)
    e = y - X @ beta
    meat = np.zeros((X.shape[1], X.shape[1]))
    for g in np.unique(cluster):
        m = cluster == g; s = X[m].T @ e[m]; meat += np.outer(s, s)
    G = len(np.unique(cluster)); n, k = X.shape
    adj = (G / (G - 1)) * ((n - 1) / (n - k))
    V = adj * (XtX_inv @ meat @ XtX_inv)
    return beta, np.sqrt(np.diag(V))


def fit_sa_per_hour_class(daily_panel, value_col, transform="log"):
    """Fit log(y) ~ const + regime + Fourier + DOW on the 2022-2026 daily series
    per hour-class. Return a dict {hour_class: predict_fn} where predict_fn(d)
    returns the seasonal-cycle component (intercept + Fourier + DOW, regime
    ZEROED) for each date in the input."""
    out = {}
    for hc in ["Critical", "Flat"]:
        d = daily_panel[daily_panel["hour_class"] == hc].copy()
        d = d[(d["d"] >= START) & (d["d"] <= END)]
        d = attach_design_columns(d, REGIME_DATES, K=K, date_col="d")
        if transform == "log":
            # ensure positivity
            shift = 1.0 - d[value_col].min() if d[value_col].min() <= 0 else 0.0
            d["y_link"] = np.log(d[value_col] + shift + 1e-6)
        else:
            d["y_link"] = d[value_col]
            shift = 0.0
        cols = REGIME_COLS + FOURIER_COLS + DOW_COLS
        X = sm.add_constant(d[cols].astype(float))
        fit = sm.OLS(d["y_link"].astype(float), X).fit()
        coefs = dict(fit.params)
        n_fit = int(len(d))
        r2_fit = float(fit.rsquared)
        print(f"    fit {hc}: n={n_fit:,}  R2={r2_fit:.3f}  shift={shift:.1f}  transform={transform}")

        def make_predict(coefs=coefs, shift=shift, transform=transform):
            def predict(dates):
                dates = pd.to_datetime(dates)
                doy = dates.dayofyear.values
                fk = fourier_terms(doy, K=K)
                dw = dow_dummies(pd.Series(dates))
                # Build prediction: intercept + Fourier + DOW (regime zeroed)
                pred = np.full(len(dates), coefs.get("const", 0.0), dtype=float)
                for c in FOURIER_COLS:
                    pred += coefs.get(c, 0.0) * fk[c].values
                for c in DOW_COLS:
                    pred += coefs.get(c, 0.0) * dw[c].values
                return pred, shift, transform
            return predict

        out[hc] = make_predict()
    return out


def deseasonalize_panel(panel, predict_dict, value_col):
    """Add a `<value_col>_sa` column = log(y+shift) - predict (per hour_class)."""
    p = panel.copy()
    p["__sa__"] = np.nan
    for hc, predict in predict_dict.items():
        m = p["hour_class"] == hc
        if not m.any():
            continue
        pred, shift, transform = predict(p.loc[m, "d"].values)
        if transform == "log":
            p.loc[m, "__sa__"] = np.log(p.loc[m, value_col].clip(lower=1e-6) + shift) - pred
        else:
            p.loc[m, "__sa__"] = p.loc[m, value_col] - pred
    p[f"{value_col}_sa"] = p["__sa__"]
    return p.drop(columns=["__sa__"])


def run_did(panel, reform_date, pre_lo, pre_hi, post_lo, post_hi, outcome_col):
    p = panel.copy()
    p["d"] = pd.to_datetime(p["d"])
    in_pre = (p["d"] >= pre_lo) & (p["d"] <= pre_hi)
    in_post = (p["d"] >= post_lo) & (p["d"] <= post_hi)
    p = p[(in_pre | in_post) & p["hour_class"].isin(["Critical", "Flat"])].copy()
    p = p.dropna(subset=[outcome_col])
    p["post"] = (p["d"] >= reform_date).astype(int)
    p["crit"] = (p["hour_class"] == "Critical").astype(int)
    p["post_crit"] = p["post"] * p["crit"]
    dd = pd.get_dummies(p["d"].astype(str), prefix="d", drop_first=True).astype(float)
    X = np.column_stack([np.ones(len(p)), p["crit"].values.astype(float),
                         p["post_crit"].values.astype(float), dd.values])
    y = p[outcome_col].values.astype(float)
    beta, se = clustered_ols(y, X, p["d"].astype(str).values)
    return {"n": len(p), "DiD": beta[2], "se": se[2], "t": beta[2] / se[2]}


def build_price_panel(parquet_path, lo, hi):
    con = duckdb.connect()
    sql = f"""SELECT CAST(date AS DATE) d, period, price_es_eur_mwh p_clear,
                     COALESCE(mtu_minutes, 60) mtu
              FROM '{parquet_path}'
              WHERE date BETWEEN '{lo.date()}' AND '{hi.date()}'
                AND price_es_eur_mwh IS NOT NULL"""
    df = con.execute(sql).fetchdf()
    df["d"] = pd.to_datetime(df["d"])
    df["clock_hour"] = np.where(df["mtu"] == 60, df["period"] - 1,
                                ((df["period"] - 1) // 4).astype(int))
    df["hour_class"] = df["clock_hour"].map(hour_class)
    return df


def build_tech_cleared_mw_panel(tech_substring, lo, hi):
    units = pd.read_csv(UNITS)
    units = units[units["technology"].str.lower().str.contains(tech_substring, na=False)][
        ["unit_code"]].drop_duplicates()
    con = duckdb.connect()
    con.register("u", units)
    sql = f"""
    SELECT CAST(p.date AS DATE) d, p.period,
           SUM(CASE WHEN p.assigned_power_mw > 0 THEN p.assigned_power_mw ELSE 0 END) AS gen,
           COALESCE(p.mtu_minutes, 60) mtu
    FROM '{PDBC}' p JOIN u ON p.unit_code = u.unit_code
    WHERE p.date BETWEEN '{lo.date()}' AND '{hi.date()}'
    GROUP BY 1, p.period, mtu
    """
    df = con.execute(sql).fetchdf()
    df["d"] = pd.to_datetime(df["d"])
    df["clock_hour"] = np.where(df["mtu"] == 60, df["period"] - 1,
                                ((df["period"] - 1) // 4).astype(int))
    df["hour_class"] = df["clock_hour"].map(hour_class)
    return df


def daily_aggregate(panel, value_col):
    return (panel.groupby(["d", "hour_class"], as_index=False)[value_col].mean())


def main():
    rows = []

    # ============ (1) DA15 clearing price ============
    print("\n=== (1) DA15 clearing price (proper SA, 2022-2026 fit window) ===")
    DA15_REFORM = pd.Timestamp("2025-10-01")
    DA15_PRE_LO = pd.Timestamp("2025-07-01")
    DA15_PRE_HI = pd.Timestamp("2025-09-30")
    DA15_POST_HI = pd.Timestamp("2025-12-31")
    panel = build_price_panel(MPDBC, START, DA15_POST_HI)
    print(f"  panel n={len(panel):,}")
    daily = daily_aggregate(panel, "p_clear")
    predict = fit_sa_per_hour_class(daily, "p_clear", transform="identity")
    panel_sa = deseasonalize_panel(panel, predict, "p_clear")
    for spec_label, outcome in [("baseline (raw price)", "p_clear"),
                                 ("Fourier-SA (identity, 2022-fit, regime FE)", "p_clear_sa")]:
        r = run_did(panel_sa, DA15_REFORM, DA15_PRE_LO, DA15_PRE_HI,
                    DA15_REFORM, DA15_POST_HI, outcome)
        print(f"  DA15 price  {spec_label:46s}  DiD={r['DiD']:+8.3f}  se={r['se']:6.3f}  t={r['t']:+6.2f}")
        rows.append({"outcome": "p_clear_DA15", "spec": spec_label, **r})

    # ============ (2) ID15 IDA clearing price ============
    print("\n=== (2) ID15 IDA clearing price (proper SA, 2022-2026 fit window) ===")
    ID15_REFORM = pd.Timestamp("2025-03-19")
    ID15_PRE_LO = pd.Timestamp("2024-12-19")
    ID15_PRE_HI = pd.Timestamp("2025-03-18")
    ID15_POST_HI = pd.Timestamp("2025-04-27")
    panel = build_price_panel(MPIBC, START, END)
    print(f"  panel n={len(panel):,}")
    daily = daily_aggregate(panel, "p_clear")
    predict = fit_sa_per_hour_class(daily, "p_clear", transform="identity")
    panel_sa = deseasonalize_panel(panel, predict, "p_clear")
    for spec_label, outcome in [("baseline (raw price)", "p_clear"),
                                 ("Fourier-SA (identity, 2022-fit, regime FE)", "p_clear_sa")]:
        r = run_did(panel_sa, ID15_REFORM, ID15_PRE_LO, ID15_PRE_HI,
                    ID15_REFORM, ID15_POST_HI, outcome)
        print(f"  ID15 IDA price  {spec_label:46s}  DiD={r['DiD']:+8.3f}  se={r['se']:6.3f}  t={r['t']:+6.2f}")
        rows.append({"outcome": "p_clear_ID15", "spec": spec_label, **r})

    # ============ (3) DA15 pump-storage cleared MW ============
    print("\n=== (3) DA15 pump-storage cleared MW (proper SA, 2022-2026 fit) ===")
    panel = build_tech_cleared_mw_panel("bombeo", START, DA15_POST_HI)
    print(f"  panel n={len(panel):,}")
    daily = daily_aggregate(panel, "gen")
    predict = fit_sa_per_hour_class(daily, "gen", transform="identity")
    panel_sa = deseasonalize_panel(panel, predict, "gen")
    for spec_label, outcome in [("baseline (raw gen MW)", "gen"),
                                 ("Fourier-SA (identity, 2022-fit, regime FE)", "gen_sa")]:
        r = run_did(panel_sa, DA15_REFORM, DA15_PRE_LO, DA15_PRE_HI,
                    DA15_REFORM, DA15_POST_HI, outcome)
        print(f"  DA15 pump-storage gen  {spec_label:46s}  DiD={r['DiD']:+9.3f}  se={r['se']:7.3f}  t={r['t']:+6.2f}")
        rows.append({"outcome": "pump_gen_mw_DA15", "spec": spec_label, **r})

    # ============ (4) DA15 CCGT cleared MW ============
    print("\n=== (4) DA15 CCGT cleared MW (proper SA, 2022-2026 fit) ===")
    panel = build_tech_cleared_mw_panel("ciclo combinado", START, DA15_POST_HI)
    print(f"  panel n={len(panel):,}")
    daily = daily_aggregate(panel, "gen")
    predict = fit_sa_per_hour_class(daily, "gen", transform="identity")
    panel_sa = deseasonalize_panel(panel, predict, "gen")
    for spec_label, outcome in [("baseline (raw gen MW)", "gen"),
                                 ("Fourier-SA (identity, 2022-fit, regime FE)", "gen_sa")]:
        r = run_did(panel_sa, DA15_REFORM, DA15_PRE_LO, DA15_PRE_HI,
                    DA15_REFORM, DA15_POST_HI, outcome)
        print(f"  DA15 CCGT gen  {spec_label:46s}  DiD={r['DiD']:+9.3f}  se={r['se']:7.3f}  t={r['t']:+6.2f}")
        rows.append({"outcome": "ccgt_gen_mw_DA15", "spec": spec_label, **r})

    pd.DataFrame(rows).to_csv(OUT, index=False)
    print(f"\nWrote {OUT}")


if __name__ == "__main__":
    main()
