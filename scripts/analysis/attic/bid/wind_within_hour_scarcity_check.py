# STATUS: ALIVE
# LAST-AUDIT: 2026-05-22
# FEEDS: thesis/provisional/descriptive_facts.tex sec 9 (wind within-hour bids)
# CLAIM: Tests whether the within-hour variation in wind day-ahead sell bids
#        is STRATEGIC (responding to within-hour demand/scarcity) or merely
#        FORECAST-DRIVEN. The outcome is THE FUNCTIONAL PRICE METRIC of the
#        document -- sigma_p, the MW-weighted SD of the in-band tranche prices
#        on each (unit, date, period) sell curve (band half-width H = 140 as
#        in per_curve_metrics.py, but centered on the CLOCK-HOUR-MEAN clearing
#        price so the band is fixed within the hour -- see the query comment).
#        A strategic wind aggregator
#        steepens its in-band price ladder (sigma_p up) in the scarce quarter
#        of the clock-hour; a pure price-taker does not. The MW-weighted mean
#        in-band bid price p_bid (the level) is kept as a secondary outcome.
#
#        Design (DA15/ID15, post-MTU15-DA quarter periods):
#          outcome    sigma_p[u,d,q] -- functional price metric (EUR/MWh)
#          regressor  scar[d,q]      -- the quarter's scarcity proxy minus its
#                       clock-hour mean (GW). Scarcity = DA load forecast
#                       MINUS DA solar forecast (ENTSO-E A65 + A75 B16):
#                       residual demand EXCLUDING wind, so exogenous to any
#                       wind unit AND the wind fleet -- this addresses the
#                       endogeneity of plain residual demand. Plain load is
#                       kept as a robustness regressor.
#          FE  unit x date x clock-hour  -- beta is identified PURELY off
#                       across-quarter, within-hour variation.
#        Spec 2 adds the unit's own within-hour offered-MW deviation. SEs
#        clustered by date. Run pooled, by hour-class, by clock-hour, by
#        aggregator.
#
# OUT: results/regressions/bid/wind_within_hour/wind_scarcity_check.csv
#      figures/working/wind_within_hour_scarcity.pdf

from pathlib import Path
import duckdb
import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
DET = REPO / "data/processed/omie/mercado_diario/ofertas/det_all.parquet"
CAB = REPO / "data/processed/omie/mercado_diario/ofertas/cab_all.parquet"
MPDBC = REPO / "data/processed/omie/mercado_diario/precios/marginalpdbc_all.parquet"
LOADF = REPO / "data/processed/entsoe/load/load_forecast_da_all.parquet"
WSF = REPO / "data/processed/entsoe/generation/wind_solar_forecast_all.parquet"
UNITS = REPO / "data/external/omie_reference/lista_unidades.csv"
OUT = REPO / "results/regressions/bid/wind_within_hour/wind_scarcity_check.csv"
OUT.parent.mkdir(parents=True, exist_ok=True)
FIG = REPO / "figures/working/wind_within_hour_scarcity.pdf"
FIG.parent.mkdir(parents=True, exist_ok=True)

LO, HI = "2025-10-01", "2026-04-22"      # DA15/ID15 ∩ 15-min load+solar coverage
DST = ("2025-10-26", "2026-03-29")        # drop clock-change days
H = 140.0                                 # strategic band, per per_curve_metrics.py
# Hour-class clock-hours, matching per_curve_metrics.py (the functionals table).
CRITICAL = {5, 6, 7, 8, 16, 17, 18, 19, 20, 21, 22}
FLAT = {1, 2, 3}


def agg_bucket(o):
    o = str(o).upper()
    if "GESTERNOVA" in o: return "Gesternova"
    if "AXPO" in o: return "AXPO"
    if "NEXUS" in o: return "NEXUS"
    if "ENGIE" in o: return "ENGIE"
    if "IGNIS" in o: return "IGNIS"
    return "Other"


def clustered_ols(y, X, cluster):
    """OLS with cluster-robust SEs. X already includes an intercept column."""
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


def main():
    con = duckdb.connect()
    con.execute("SET memory_limit='12GB'")
    con.execute("SET threads=4")
    con.execute("SET preserve_insertion_order=false")

    u = pd.read_csv(UNITS)
    u = u[u["technology"].astype(str).str.lower()
          .str.contains("eólica|eolica", na=False)].copy()
    u["agg"] = u["owner_agent"].apply(agg_bucket)
    u = u[["unit_code", "agg"]].drop_duplicates("unit_code")
    con.register("u", u)

    # --- wind DA sell curves: per-curve functionals, in band |p-MCP| <= H ----
    # sigma_p = MW-weighted SD of in-band tranche prices (the functional price
    # metric); n_eff = inverse-Herfindahl tranche count; p_bid = in-band mean.
    # The band is centered on the CLOCK-HOUR-MEAN clearing price, not on each
    # quarter's own MCP: a quarter-specific band shifts with within-hour
    # scarcity (scarce quarter -> higher MCP -> band slides up -> different
    # tranches in-band), which would mechanically move sigma_p. Centering on
    # the hour-mean MCP fixes the band within the clock-hour, so sigma_p's
    # cross-quarter variation is the firm's ladder shaping, not a band artefact.
    bids = con.execute(f"""
    WITH cab_l AS (
      SELECT d, offer_code, unit_code FROM (
        SELECT CAST(date AS DATE) d, offer_code, unit_code,
               ROW_NUMBER() OVER (PARTITION BY CAST(date AS DATE),offer_code,unit_code
                                  ORDER BY version DESC) rn
        FROM '{CAB}' WHERE date BETWEEN '{LO}' AND '{HI}' AND buy_sell='V') WHERE rn=1),
    det AS (
      SELECT CAST(date AS DATE) d, offer_code, period,
             CAST(FLOOR((period - 1) / 4.0) AS INT) AS clock_hour,
             price_eur_mwh p, quantity_mw q
      FROM '{DET}' WHERE date BETWEEN '{LO}' AND '{HI}'
        AND quantity_mw > 0 AND period BETWEEN 1 AND 96),
    mp_h AS (
      SELECT CAST(date AS DATE) d,
             CAST(FLOOR((period - 1) / 4.0) AS INT) AS clock_hour,
             AVG(price_es_eur_mwh) AS p_clear_h
      FROM '{MPDBC}' WHERE date BETWEEN '{LO}' AND '{HI}'
        AND price_es_eur_mwh IS NOT NULL
      GROUP BY 1, 2),
    inband AS (
      SELECT c.unit_code, u.agg, dv.d, dv.clock_hour,
             ((dv.period - 1) % 4) AS quarter, dv.q, dv.p
      FROM det dv JOIN cab_l c ON dv.d=c.d AND dv.offer_code=c.offer_code
        JOIN u    ON c.unit_code = u.unit_code
        JOIN mp_h ON mp_h.d=dv.d AND mp_h.clock_hour=dv.clock_hour
      WHERE dv.p BETWEEN mp_h.p_clear_h - {H} AND mp_h.p_clear_h + {H})
    SELECT unit_code, agg, d, clock_hour, quarter,
           SUM(q)              AS sum_w,
           SUM(q * p)          AS sum_wp,
           SUM(q * p * p)      AS sum_wp2,
           SUM(q * q)          AS sum_w2
    FROM inband
    GROUP BY 1,2,3,4,5
    HAVING SUM(q) > 0
    """).fetchdf()

    # per-curve functionals
    bids["p_bid"] = bids["sum_wp"] / bids["sum_w"]
    var_p = bids["sum_wp2"] / bids["sum_w"] - bids["p_bid"] ** 2
    bids["sigma_p"] = np.sqrt(var_p.clip(lower=0))
    bids["n_eff"] = bids["sum_w"] ** 2 / bids["sum_w2"]
    bids["q_own"] = bids["sum_w"]

    # --- scarcity proxy: DA load forecast minus DA solar forecast ------------
    scar = con.execute(f"""
    WITH ld AS (
      SELECT (isp_start_utc AT TIME ZONE 'UTC') AT TIME ZONE 'Europe/Madrid' AS loc,
             load_forecast_mw AS load_mw
      FROM '{LOADF}'
      WHERE isp_start_utc BETWEEN '{LO}' AND TIMESTAMP '{HI}' + INTERVAL 2 DAY
        AND mtu_minutes = 15),
    sol AS (
      SELECT (isp_start_utc AT TIME ZONE 'UTC') AT TIME ZONE 'Europe/Madrid' AS loc,
             quantity_mw AS solar_mw
      FROM '{WSF}'
      WHERE isp_start_utc BETWEEN '{LO}' AND TIMESTAMP '{HI}' + INTERVAL 2 DAY
        AND mtu_minutes = 15 AND psr_type = 'B16')
    SELECT CAST(ld.loc AS DATE) AS d, HOUR(ld.loc) AS clock_hour,
           CAST(MINUTE(ld.loc) / 15 AS INT) AS quarter,
           ld.load_mw / 1000.0 AS load_gw,
           (ld.load_mw - sol.solar_mw) / 1000.0 AS scar_gw
    FROM ld JOIN sol ON ld.loc = sol.loc
    """).fetchdf()

    df = bids.merge(scar, on=["d", "clock_hour", "quarter"], how="inner")
    df = df[~df["d"].astype(str).isin(DST)].copy()
    df["g"] = (df["unit_code"].astype(str) + "|" + df["d"].astype(str)
               + "|" + df["clock_hour"].astype(str))

    # keep clock-hours with all 4 quarters present (a full within-hour profile)
    df = df[df.groupby("g")["quarter"].transform("nunique") == 4].copy()

    # within-(unit,date,hour) demeaning == absorbing the FE
    for c in ["sigma_p", "p_bid", "n_eff", "scar_gw", "load_gw", "q_own"]:
        df[f"{c}_w"] = df[c] - df.groupby("g")[c].transform("mean")

    sigma_curve_sd = df.groupby("g")["sigma_p"].transform("std")
    df["date_cl"] = df["d"].astype(str)
    df["hour_class"] = np.where(df["clock_hour"].isin(CRITICAL), "Critical",
                       np.where(df["clock_hour"].isin(FLAT), "Flat", "Other"))
    n_obs, n_cells, n_units = len(df), df["g"].nunique(), df["unit_code"].nunique()
    share_graded = (df["sigma_p"] > 0.01).mean()
    share_sigvar = (sigma_curve_sd > 0.01).groupby(df["g"]).first().mean()

    rows = []

    def run(sub, label, xcol, ycol, with_qown, verbose=True):
        s = sub[sub[f"{xcol}_w"].abs() > 1e-9]
        if len(s) < 50:
            return None
        cols = [f"{xcol}_w"] + (["q_own_w"] if with_qown else [])
        X = np.column_stack([np.ones(len(s))] + [s[c].values for c in cols])
        beta, se = clustered_ols(s[f"{ycol}_w"].values, X, s["date_cl"].values)
        t = beta[1] / se[1]
        r = (np.corrcoef(s[f"{xcol}_w"], s[f"{ycol}_w"])[0, 1]
             if not with_qown else np.nan)
        rows.append(dict(sample=label, outcome=ycol, regressor=xcol,
                          spec="+q_own" if with_qown else "base",
                          beta=beta[1], se=se[1], t=t, partial_r=r, n=len(s)))
        if verbose:
            print(f"  {label:22s} y={ycol:8s} {xcol:8s} "
                  f"{'+q_own' if with_qown else 'base':7s} "
                  f"beta={beta[1]:+8.3f}  se={se[1]:6.3f}  t={t:+6.2f}  "
                  f"r={r:+.3f}  n={len(s):,}")
        return beta[1], se[1], len(s)

    print(f"\n=== Wind within-hour FUNCTIONAL response to scarcity ({LO}..{HI}) ===")
    print(f"  {n_obs:,} unit-date-quarter in-band curves | {n_cells:,} full "
          f"clock-hours | {n_units} wind units")
    print(f"  wind curves with a graded in-band ladder (sigma_p>0): "
          f"{share_graded:5.1%}")
    print(f"  clock-hours where sigma_p varies across the 4 quarters: "
          f"{share_sigvar:5.1%}")
    print("  outcome: within-hour-demeaned functional price metric sigma_p "
          "(EUR/MWh)")
    print("  scar = load - solar forecast (residual demand ex-wind); "
          "beta in (EUR/MWh of sigma_p) per GW\n")
    run(df, "all wind", "scar_gw", "sigma_p", False)
    run(df, "all wind", "scar_gw", "sigma_p", True)
    run(df, "all wind", "load_gw", "sigma_p", False)        # robustness
    run(df, "all wind", "scar_gw", "n_eff", False)          # quantity functional
    run(df, "all wind", "scar_gw", "p_bid", False)          # bid-price level
    act = df[sigma_curve_sd > 0.01]
    run(act, "sigma_p-varying hrs", "scar_gw", "sigma_p", False)
    print()
    for hc in ["Critical", "Flat"]:
        sub = df[df["hour_class"] == hc]
        print(f"  [{hc}: within-hour scarcity SD {sub['scar_gw_w'].std():.3f} GW]")
        run(sub, f"{hc} hours", "scar_gw", "sigma_p", False)
        run(sub, f"{hc} hours", "scar_gw", "sigma_p", True)
    print()
    print("  beta by clock-hour (y=sigma_p, scar_gw, base):")
    hour_b = {}
    for h in range(24):
        res = run(df[df["clock_hour"] == h], f"hour {h:02d}", "scar_gw",
                  "sigma_p", False, verbose=False)
        if res is None:
            continue
        b, se, n = res
        hour_b[h] = (b, se)
        flag = "CRIT" if h in CRITICAL else ("flat" if h in FLAT else "")
        print(f"    {h:02d}  beta={b:+7.3f}  se={se:6.3f}  "
              f"t={b/se:+6.2f}  n={n:,}  {flag}")
    print()
    for a in ["Gesternova", "AXPO", "NEXUS", "ENGIE", "IGNIS", "Other"]:
        run(df[df["agg"] == a], a, "scar_gw", "sigma_p", False)

    pd.DataFrame(rows).to_csv(OUT, index=False)
    print(f"\nwrote {OUT}")

    # --- figure -------------------------------------------------------------
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    fig, axes = plt.subplots(1, 2, figsize=(12, 4.4))
    # Left: critical vs flat hours -- sigma_p deviation vs scarcity deviation.
    titles = []
    for hc, col in [("Critical", "#d62728"), ("Flat", "#1f77b4")]:
        s = df[(df["hour_class"] == hc) & (df["scar_gw_w"].abs() > 1e-9)]
        qs = pd.qcut(s["scar_gw_w"], 20, duplicates="drop")
        gb = s.groupby(qs, observed=True)
        axes[0].scatter(gb["scar_gw_w"].mean(), gb["sigma_p_w"].mean(),
                        s=24, color=col, zorder=3, label=f"{hc} hours")
        X = np.column_stack([np.ones(len(s)), s["scar_gw_w"].values])
        b, _ = clustered_ols(s["sigma_p_w"].values, X, s["date_cl"].values)
        xx = np.linspace(s["scar_gw_w"].min(), s["scar_gw_w"].max(), 50)
        axes[0].plot(xx, b[0] + b[1] * xx, color=col, lw=1.8)
        titles.append(rf"{hc} $\beta={b[1]:+.2f}$")
    axes[0].axhline(0, color="grey", lw=0.6)
    axes[0].axvline(0, color="grey", lw=0.6)
    axes[0].set_xlabel("within-hour scarcity deviation, load$-$solar (GW)",
                       fontsize=9)
    axes[0].set_ylabel(r"within-hour $\sigma_p$ deviation (EUR/MWh)", fontsize=9)
    axes[0].set_title(r"Wind functional $\sigma_p$ vs scarcity, by hour-class"
                      "\n" + "  ".join(titles) + " (both flat)", fontsize=9.5)
    axes[0].legend(fontsize=8, loc="upper right")
    axes[0].grid(alpha=0.3, lw=0.5)
    # Right: beta for every clock-hour of the day.
    hh = sorted(hour_b)
    bv = [hour_b[h][0] for h in hh]
    ev = [1.96 * hour_b[h][1] for h in hh]
    cols = ["#d62728" if h in CRITICAL else
            ("#1f77b4" if h in FLAT else "#999999") for h in hh]
    axes[1].bar(hh, bv, yerr=ev, color=cols, capsize=2)
    axes[1].axhline(0, color="black", lw=0.7)
    axes[1].set_xticks(range(0, 24, 2))
    axes[1].set_xlabel("clock-hour of day", fontsize=9)
    axes[1].set_ylabel(r"$\beta$ on $\sigma_p$ (EUR/MWh per GW)", fontsize=9)
    axes[1].set_title(r"$\sigma_p$ scarcity response, by clock-hour"
                      "\n(red = critical, blue = flat; 95% CI, date-clustered)",
                      fontsize=9.5)
    axes[1].grid(alpha=0.3, lw=0.5, axis="y")
    fig.suptitle(r"Wind within-hour functional price metric $\sigma_p$ vs "
                 "within-hour scarcity (DA15/ID15, unit$\\times$date$\\times$"
                 "hour FE)", fontsize=10.5)
    fig.tight_layout(rect=[0, 0, 1, 0.93])
    fig.savefig(FIG, bbox_inches="tight", dpi=130)
    plt.close(fig)
    print(f"wrote {FIG}")


if __name__ == "__main__":
    main()
