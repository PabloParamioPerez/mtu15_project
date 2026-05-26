# STATUS: ALIVE
# LAST-AUDIT: 2026-05-24
# FEEDS: thesis/provisional/advisor_memo.tex sec 4 results (per-tech cleared MW
#        SA correction); descriptive_facts.tex link.
# CLAIM: After absorbing the (critical-flat) seasonal differential via Fourier
#        x crit interactions (4 annual harmonics + 6 DOW dummies, all
#        interacted with the critical-hour indicator), the per-tech DA15
#        cleared-MW DiDs collapse for Solar PV (the entire system-wide
#        '-2115 MW withholding' baseline is the Jul-Sep -> Oct-Dec solar
#        seasonal collapse) and for CCGT (the +1548 MW critical-hour rise
#        also goes away when seasonality is absorbed).
#
# Why interactions: with date FE delta_d the date-level Fourier and DOW main
# effects are absorbed, so they identify nothing on their own. The DiD theta
# on the post*crit interaction is exactly the (crit-flat) shift across the
# cutover; only Fourier and DOW interacted with crit can absorb the seasonal
# (crit-flat) differential trend that drives the headline.
#
# OUT: results/regressions/bid/mtu15_critical_flat/fourier_sa_cleared_mw.csv

from pathlib import Path
import duckdb
import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
OUT = REPO / "results/regressions/bid/mtu15_critical_flat/fourier_sa_cleared_mw.csv"
OUT.parent.mkdir(parents=True, exist_ok=True)

CRITICAL = {5, 6, 7, 8, 16, 17, 18, 19, 20, 21, 22}
FLAT = {1, 2, 3}

PRE_LO, PRE_HI = "2025-07-01", "2025-09-30"
POST_LO, POST_HI = "2025-10-01", "2025-12-31"
REFORM_DATE = pd.Timestamp("2025-10-01")


def hour_class(h):
    if h in CRITICAL: return "Critical"
    if h in FLAT: return "Flat"
    return "Other"


def tech_bucket_full(t):
    t = str(t).lower()
    if "ciclo combinado" in t:                        return "CCGT"
    if "hidráulica generación" in t:                  return "Hydro_run"
    if "bombeo" in t:                                 return "Hydro_pump"
    if "re mercado hidráulica" in t:                  return "Hydro_RE"
    if "eólica" in t:                                 return "Wind"
    if "fotovolt" in t:                               return "Solar_PV"
    if "solar térmica" in t or "solar termica" in t:  return "Solar_thermal"
    if "nuclear" in t:                                return "Nuclear"
    if "térmica no renovab" in t:                     return "Coal_other_thermal"
    if "térmica renovable" in t:                      return "Biomass_RE"
    return "Other"


def clustered_ols(y, X, cluster):
    XtX_inv = np.linalg.inv(X.T @ X)
    beta = XtX_inv @ (X.T @ y)
    e = y - X @ beta
    meat = np.zeros((X.shape[1], X.shape[1]))
    for g in np.unique(cluster):
        m = cluster == g
        s = X[m].T @ e[m]
        meat += np.outer(s, s)
    G = len(np.unique(cluster))
    n, k = X.shape
    adj = (G / (G - 1)) * ((n - 1) / (n - k))
    V = adj * (XtX_inv @ meat @ XtX_inv)
    return beta, np.sqrt(np.diag(V))


def build_per_tech_cleared_mw():
    PDBC = REPO / "data/processed/omie/mercado_diario/programas/pdbc_all.parquet"
    UNITS = REPO / "data/external/omie_reference/lista_unidades.csv"
    units = pd.read_csv(UNITS)
    units["tech_full"] = units["technology"].apply(tech_bucket_full)
    units = units[units["tech_full"] != "Other"][["unit_code", "tech_full"]]
    units = units.drop_duplicates("unit_code")
    con = duckdb.connect()
    con.register("u", units)
    sql = f"""
    SELECT CAST(p.date AS DATE) d, p.period, u.tech_full AS tech,
           SUM(p.assigned_power_mw) AS cleared_mw,
           COALESCE(p.mtu_minutes, 60) mtu
    FROM '{PDBC}' p JOIN u ON p.unit_code = u.unit_code
    WHERE p.date BETWEEN '{PRE_LO}' AND '{POST_HI}'
      AND p.assigned_power_mw > 0
    GROUP BY 1, period, tech, mtu_minutes
    """
    df = con.execute(sql).fetchdf()
    df["d"] = pd.to_datetime(df["d"])
    df["clock_hour"] = np.where(df["mtu"] == 60, df["period"] - 1,
                                ((df["period"] - 1) // 4).astype(int))
    df["hour_class"] = df["clock_hour"].map(hour_class)
    return df


def run_did(panel, tech, sa):
    p = panel[panel["tech"] == tech].copy()
    p = p[p["hour_class"].isin(["Critical", "Flat"])].copy()
    if p.empty:
        return None
    p["post"] = (p["d"] >= REFORM_DATE).astype(int)
    p["crit"] = (p["hour_class"] == "Critical").astype(int)
    p["post_crit"] = p["post"] * p["crit"]
    # Date FE via dummies (parsimonious — drop first)
    dd = pd.get_dummies(p["d"].astype(str), prefix="d", drop_first=True).astype(float)
    cols = [p["crit"].values.astype(float), p["post_crit"].values.astype(float)]
    names = ["crit", "post_crit"]
    if sa:
        doy = p["d"].dt.dayofyear.values
        for k in range(1, 5):
            cols.append(p["crit"].values.astype(float) *
                        np.cos(2 * np.pi * k * doy / 365.0))
            cols.append(p["crit"].values.astype(float) *
                        np.sin(2 * np.pi * k * doy / 365.0))
            names += [f"crit_c{k}", f"crit_s{k}"]
        dow = p["d"].dt.dayofweek.values
        for j in range(1, 7):  # Tue..Sun; Mon dropped
            cols.append(p["crit"].values.astype(float) * (dow == j).astype(float))
            names.append(f"crit_dow{j}")
    X = np.column_stack([np.ones(len(p))] + cols + [dd.values])
    y = p["cleared_mw"].values.astype(float)
    try:
        beta, se = clustered_ols(y, X, p["d"].astype(str).values)
    except np.linalg.LinAlgError:
        return None
    # post_crit is column index 2 (1 intercept + 1 crit + 1 post_crit) regardless of SA
    idx = 2
    return {"tech": tech, "spec": "fourier_SA" if sa else "baseline",
            "n": len(p), "DiD": beta[idx], "se": se[idx], "t": beta[idx]/se[idx]}


def main():
    print("Building per-tech DA cleared-MW panel (Jul 2025 -> Dec 2025)...")
    panel = build_per_tech_cleared_mw()
    print(f"  {len(panel):,} (date, period, tech) rows")
    techs = ["CCGT", "Hydro_run", "Hydro_pump", "Hydro_RE", "Wind",
             "Solar_PV", "Solar_thermal", "Nuclear",
             "Coal_other_thermal", "Biomass_RE"]
    rows = []
    for tech in techs:
        for sa in [False, True]:
            r = run_did(panel, tech, sa)
            if r is None: continue
            rows.append(r)
        if len(rows) >= 2:
            b, s = rows[-2], rows[-1]
            print(f"  {tech:20s}  baseline DiD={b['DiD']:+9.1f} (t={b['t']:+6.2f})  "
                  f"->  Fourier-SA DiD={s['DiD']:+9.1f} (t={s['t']:+6.2f})")
    pd.DataFrame(rows).to_csv(OUT, index=False)
    print(f"\nWrote {OUT}")


if __name__ == "__main__":
    main()
