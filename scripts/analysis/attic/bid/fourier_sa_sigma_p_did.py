# STATUS: ALIVE
# LAST-AUDIT: 2026-05-26
# FEEDS: thesis/provisional/advisor_memo.tex sec 5 Fourier-SA cross-check for
#        the sigma_p DiD; descriptive_facts.tex methodological link.
# CLAIM: After absorbing seasonality at the date level via the descriptive_facts
#        Fourier spec (4 annual harmonics + 6 day-of-week dummies), the σ_p DiD
#        in Spec A survives. This is the closest the bid-tranche data permits
#        to the daily-aggregate Fourier SA of descriptive_facts (which cannot
#        be applied at the tranche level without destroying the non-decreasing
#        ladder structure).
#
# Method: Augment the Spec A design matrix with day-of-year sinusoids
#         {c_k(d), s_k(d)}_{k=1..4} and 6 DOW dummies (drop Monday).
#         Within-unit demean as before. Compare θ to the headline.
#
# OUT: results/regressions/bid/mtu15_critical_flat/fourier_sa_robustness.csv

from pathlib import Path
import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
PANELS = REPO / "data/derived/panels"
OUT = REPO / "results/regressions/bid/mtu15_critical_flat/fourier_sa_robustness.csv"
OUT.parent.mkdir(parents=True, exist_ok=True)

CRITICAL = {5, 6, 7, 8, 16, 17, 18, 19, 20, 21, 22}
FLAT = {1, 2, 3}
WINDOWS = {
    "ID15": {"pre_lo": "2024-12-19", "pre_hi": "2025-03-18",
             "post_lo": "2025-03-19", "post_hi": "2025-04-27"},
    "DA15": {"pre_lo": "2025-07-01", "pre_hi": "2025-09-30",
             "post_lo": "2025-10-01", "post_hi": "2025-12-31"},
}


def hour_class(h):
    if h in CRITICAL: return "Critical"
    if h in FLAT:     return "Flat"
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


def add_fourier_dow(df):
    """Append 4 annual-harmonic sinusoids and 6 DOW dummies (drop Monday)."""
    d = df.copy()
    doy = d["d"].dt.dayofyear.values
    for k in range(1, 5):
        d[f"c{k}"] = np.cos(2 * np.pi * k * doy / 365.0)
        d[f"s{k}"] = np.sin(2 * np.pi * k * doy / 365.0)
    dow = d["d"].dt.dayofweek.values
    for j in range(1, 7):  # Tue..Sun; Mon (=0) dropped
        d[f"dow{j}"] = (dow == j).astype(float)
    return d


def run_spec_A_with_fourier(panel, reform, tech_filter, add_fourier):
    w = WINDOWS[reform]
    pre_lo, pre_hi = pd.Timestamp(w["pre_lo"]), pd.Timestamp(w["pre_hi"])
    post_lo, post_hi = pd.Timestamp(w["post_lo"]), pd.Timestamp(w["post_hi"])
    p = panel.copy()
    p["d"] = pd.to_datetime(p["d"])
    if tech_filter is not None:
        p = p[p["tech"] == tech_filter].copy()
    p["hour_class"] = p["clock_hour"].map(hour_class)
    in_pre = (p["d"] >= pre_lo) & (p["d"] <= pre_hi)
    in_post = (p["d"] >= post_lo) & (p["d"] <= post_hi)
    p = p[(in_pre | in_post) & p["hour_class"].isin(["Critical", "Flat"])].copy()
    if p.empty:
        return None
    p["post"] = (p["d"] >= post_lo).astype(int)
    p["crit"] = (p["hour_class"] == "Critical").astype(int)
    p["post_crit"] = p["post"] * p["crit"]
    p = add_fourier_dow(p) if add_fourier else p
    out = []
    for outcome in ["sigma_p", "n_eff"]:
        d = p.dropna(subset=[outcome]).copy()
        if len(d) < 50:
            continue
        cols = ["post", "crit", "post_crit"]
        if add_fourier:
            cols += [f"c{k}" for k in range(1, 5)] + [f"s{k}" for k in range(1, 5)]
            cols += [f"dow{j}" for j in range(1, 7)]
        # within-unit demean
        gm = d.groupby("unit_code")[outcome].transform("mean")
        d["y_w"] = d[outcome] - gm
        for c in cols:
            gmc = d.groupby("unit_code")[c].transform("mean")
            d[c + "_w"] = d[c] - gmc
        X = np.column_stack([np.ones(len(d))] + [d[c + "_w"].values for c in cols])
        beta, se = clustered_ols(d["y_w"].values.astype(float),
                                  X.astype(float),
                                  d["d"].astype(str).values)
        # DiD = beta on post_crit (idx 3: 1=intercept, 1=post, 2=crit, 3=post_crit)
        out.append({"outcome": outcome, "n": len(d),
                    "DiD": beta[3], "se": se[3], "t": beta[3] / se[3]})
    return pd.DataFrame(out)


def main():
    # Re-build the panels by importing from the main DiD script's panel
    # builders. We import lazily to avoid the heavy panel build if cached.
    print("Importing panel builders from mtu15_critical_flat_did...")
    import sys
    sys.path.insert(0, str(REPO / "scripts/analysis/bid"))
    from mtu15_critical_flat_did import build_ida_panel, WINDOWS as WMAIN
    import duckdb
    # Build the IDA panel for ID15 window
    print("Building IDA panel for ID15 (Dec 2024 -> Apr 2025)...")
    ida = build_ida_panel(WMAIN["ID15"]["pre_lo"], WMAIN["ID15"]["post_hi"])
    print(f"  {len(ida):,} IDA in-band curves")

    # Build the DA panel for DA15 window
    print("Building DA panel for DA15 (Jul 2025 -> Dec 2025)...")
    DET = REPO / "data/processed/omie/mercado_diario/ofertas/det_all.parquet"
    CAB = REPO / "data/processed/omie/mercado_diario/ofertas/cab_all.parquet"
    MPDBC = REPO / "data/processed/omie/mercado_diario/precios/marginalpdbc_all.parquet"
    UNITS = REPO / "data/external/omie_reference/lista_unidades.csv"
    units = pd.read_csv(UNITS)

    def _bucket_tech(t):
        t = str(t).lower()
        if "ciclo combinado" in t: return "CCGT"
        if "hidráulica generación" in t: return "Hydro"
        if "bombeo" in t: return "Hydro_pump"
        return "Other"

    def _bucket_firm(o):
        o = str(o).lower()
        if "iberdrola" in o: return "IB"
        if "endesa" in o: return "GE"
        if "naturgy" in o or "gas natural" in o: return "GN"
        if "edp" in o or "hidroel" in o: return "HC"
        return "OTH"

    units["tech"] = units["technology"].apply(_bucket_tech)
    units["firm"] = units["owner_agent"].apply(_bucket_firm)
    units = units[units["tech"].isin(["CCGT", "Hydro", "Hydro_pump"]) &
                  units["firm"].isin(["IB", "GE", "GN", "HC"])][
        ["unit_code", "firm", "tech"]].drop_duplicates("unit_code")

    H = 140.0
    da_lo, da_hi = WMAIN["DA15"]["pre_lo"], WMAIN["DA15"]["post_hi"]
    con = duckdb.connect()
    con.execute("SET memory_limit='12GB'")
    con.execute("SET threads=4")
    con.register("u", units)
    sql = f"""
    WITH cab_l AS (
      SELECT d, offer_code, unit_code FROM (
        SELECT CAST(date AS DATE) d, offer_code, unit_code,
               ROW_NUMBER() OVER (PARTITION BY CAST(date AS DATE), offer_code, unit_code
                                  ORDER BY version DESC) rn
        FROM '{CAB}' WHERE date BETWEEN '{da_lo}' AND '{da_hi}' AND buy_sell='V'
      ) WHERE rn=1
    ),
    det AS (
      SELECT CAST(date AS DATE) d, offer_code, period,
             price_eur_mwh p, quantity_mw q, COALESCE(mtu_minutes, 60) AS mtu
      FROM '{DET}' WHERE date BETWEEN '{da_lo}' AND '{da_hi}' AND quantity_mw > 0
    ),
    mp AS (
      SELECT CAST(date AS DATE) d, period, price_es_eur_mwh p_clear,
             COALESCE(mtu_minutes, 60) mtu_p
      FROM '{MPDBC}' WHERE date BETWEEN '{da_lo}' AND '{da_hi}' AND price_es_eur_mwh IS NOT NULL
    ),
    inband AS (
      SELECT mp.d, mp.period, c.unit_code, dv.q, dv.p,
             CASE WHEN COALESCE(mp.mtu_p, dv.mtu) = 60 THEN mp.period - 1
                  ELSE CAST(FLOOR((mp.period - 1) / 4.0) AS INT) END AS clock_hour
      FROM det dv JOIN cab_l c ON dv.d=c.d AND dv.offer_code=c.offer_code
        JOIN mp ON mp.d=dv.d AND mp.period=dv.period
      WHERE dv.p BETWEEN mp.p_clear - {H} AND mp.p_clear + {H}
    )
    SELECT i.d, i.period, i.clock_hour, i.unit_code, u.firm, u.tech,
           SUM(i.q) sum_w, SUM(i.q*i.p) sum_wp, SUM(i.q*i.p*i.p) sum_wp2,
           SUM(i.q*i.q) sum_w2, COUNT(*) n_tranche
    FROM inband i JOIN u ON i.unit_code = u.unit_code
    GROUP BY 1,2,3,4,5,6 HAVING SUM(i.q) > 0
    """
    da = con.execute(sql).fetchdf()
    da["d"] = pd.to_datetime(da["d"])
    mean_p = da["sum_wp"] / da["sum_w"]
    var_p = da["sum_wp2"] / da["sum_w"] - mean_p ** 2
    da["sigma_p"] = np.sqrt(var_p.clip(lower=0))
    da["n_eff"] = da["sum_w"] ** 2 / da["sum_w2"]
    print(f"  {len(da):,} DA in-band curves")

    rows = []
    for reform, panel in [("ID15", ida), ("DA15", da)]:
        for tech in [None, "CCGT", "Hydro", "Hydro_pump"]:
            print(f"\n  {reform}  tech={tech or 'pooled'}")
            for label, add in [("baseline", False), ("fourier_dow", True)]:
                r = run_spec_A_with_fourier(panel, reform, tech, add)
                if r is None:
                    continue
                for _, row in r.iterrows():
                    rows.append({"reform": reform, "tech": tech or "All",
                                 "spec": label, **row.to_dict()})
                    print(f"    {label:12s} {row['outcome']:8s}  "
                          f"DiD={row['DiD']:+8.3f}  se={row['se']:6.3f}  "
                          f"t={row['t']:+6.2f}")
    pd.DataFrame(rows).to_csv(OUT, index=False)
    print(f"\nWrote {OUT}")


if __name__ == "__main__":
    main()
