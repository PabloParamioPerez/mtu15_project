# STATUS: ALIVE
# LAST-AUDIT: 2026-05-22
# FEEDS: thesis/provisional/descriptive_facts.tex sec 9 (wind within-hour bids)
# CLAIM: Tests whether the within-hour variation in wind day-ahead sell bids
#        is STRATEGIC (responding to within-hour demand/scarcity) or merely
#        FORECAST-DRIVEN. Wind is a near-zero-cost price-taker: under pure
#        price-taking the four within-hour quarter bids differ only in
#        QUANTITY (the sub-hourly forecast), and the bid PRICE is flat. A
#        strategic wind aggregator instead raises its bid price in the scarce
#        quarter of the clock-hour.
#
#        Design (DA15/ID15, post-MTU15-DA quarter periods):
#          outcome    p_bid[u,d,q]  -- MW-weighted mean DA sell-bid price
#          regressor  scar[d,q]     -- the quarter's scarcity proxy minus its
#                       clock-hour mean (GW). Scarcity = DA load forecast
#                       MINUS DA solar forecast (ENTSO-E A65 + A75 B16):
#                       residual demand EXCLUDING wind. It contains no wind,
#                       so it is exogenous to any single wind unit AND to the
#                       wind fleet -- this is what addresses the endogeneity
#                       of plain residual demand (which contains wind). The
#                       within-hour swing is driven by the solar ramp, the
#                       real sub-hourly scarcity signal. Plain load is kept
#                       as a robustness regressor.
#          FE  unit x date x clock-hour  -- beta is identified PURELY off
#                       across-quarter, within-hour variation.
#        Spec 2 adds the unit's own within-hour offered-MW deviation (its
#        forecast shape, predetermined by meteorology): if beta survives, the
#        price response is not just the unit tracking its own forecast.
#        SEs clustered by date.
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
LOADF = REPO / "data/processed/entsoe/load/load_forecast_da_all.parquet"
WSF = REPO / "data/processed/entsoe/generation/wind_solar_forecast_all.parquet"
UNITS = REPO / "data/external/omie_reference/lista_unidades.csv"
OUT = REPO / "results/regressions/bid/wind_within_hour/wind_scarcity_check.csv"
OUT.parent.mkdir(parents=True, exist_ok=True)
FIG = REPO / "figures/working/wind_within_hour_scarcity.pdf"
FIG.parent.mkdir(parents=True, exist_ok=True)

LO, HI = "2025-10-01", "2026-04-22"      # DA15/ID15 ∩ 15-min load+solar coverage
DST = ("2025-10-26", "2026-03-29")        # drop clock-change days


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

    # --- wind DA sell bids: per (unit, date, quarter period) -----------------
    bids = con.execute(f"""
    WITH cab_l AS (
      SELECT d, offer_code, unit_code FROM (
        SELECT CAST(date AS DATE) d, offer_code, unit_code,
               ROW_NUMBER() OVER (PARTITION BY CAST(date AS DATE),offer_code,unit_code
                                  ORDER BY version DESC) rn
        FROM '{CAB}' WHERE date BETWEEN '{LO}' AND '{HI}' AND buy_sell='V') WHERE rn=1),
    det AS (
      SELECT CAST(date AS DATE) d, offer_code, period,
             price_eur_mwh p, quantity_mw q
      FROM '{DET}' WHERE date BETWEEN '{LO}' AND '{HI}'
        AND quantity_mw > 0 AND period BETWEEN 1 AND 96)
    SELECT c.unit_code, u.agg, dv.d,
           CAST((dv.period - 1) / 4 AS INT) AS clock_hour,
           ((dv.period - 1) % 4)            AS quarter,
           SUM(dv.q)                        AS q_own,
           SUM(dv.q * dv.p) / SUM(dv.q)     AS p_bid
    FROM det dv JOIN cab_l c ON dv.d=c.d AND dv.offer_code=c.offer_code
      JOIN u ON c.unit_code = u.unit_code
    GROUP BY 1,2,3,4,5
    """).fetchdf()

    # --- scarcity proxy: DA load forecast minus DA solar forecast ------------
    # residual demand EXCLUDING wind -> exogenous to any wind unit/fleet.
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

    # keep clock-hours with all 4 quarters bid (a full within-hour profile)
    df = df[df.groupby("g")["quarter"].transform("nunique") == 4].copy()

    # within-(unit,date,hour) demeaning == absorbing the FE
    for c in ["p_bid", "scar_gw", "load_gw", "q_own"]:
        df[f"{c}_w"] = df[c] - df.groupby("g")[c].transform("mean")

    price_sd = df.groupby("g")["p_bid"].transform("std")
    share_active = (price_sd > 0.01).groupby(df["g"]).first().mean()
    df["date_cl"] = df["d"].astype(str)
    n_obs, n_cells, n_units = len(df), df["g"].nunique(), df["unit_code"].nunique()

    rows = []

    def run(sub, label, xcol, with_qown):
        s = sub[sub[f"{xcol}_w"].abs() > 1e-9]
        if len(s) < 50:
            return
        cols = [f"{xcol}_w"] + (["q_own_w"] if with_qown else [])
        X = np.column_stack([np.ones(len(s))] + [s[c].values for c in cols])
        beta, se = clustered_ols(s["p_bid_w"].values, X, s["date_cl"].values)
        t = beta[1] / se[1]
        r = (np.corrcoef(s[f"{xcol}_w"], s["p_bid_w"])[0, 1]
             if not with_qown else np.nan)
        rows.append(dict(sample=label, regressor=xcol,
                          spec="+q_own" if with_qown else "base",
                          beta=beta[1], se=se[1], t=t, partial_r=r, n=len(s)))
        print(f"  {label:22s} {xcol:8s} {'+q_own' if with_qown else 'base':7s} "
              f"beta={beta[1]:+8.3f}  se={se[1]:6.3f}  t={t:+6.2f}  "
              f"r={r:+.3f}  n={len(s):,}")

    print(f"\n=== Wind within-hour bid-price response to scarcity ({LO}..{HI}) ===")
    print(f"  {n_obs:,} unit-date-quarter obs | {n_cells:,} full clock-hours | "
          f"{n_units} wind units")
    print(f"  clock-hours with any within-hour bid-price variation: "
          f"{share_active:5.1%}")
    print("  outcome: within-hour-demeaned MW-weighted bid price (EUR/MWh)")
    print("  scar = load - solar forecast (residual demand ex-wind); "
          "beta in EUR/MWh per GW\n")
    run(df, "all wind", "scar_gw", False)
    run(df, "all wind", "scar_gw", True)
    run(df, "all wind", "load_gw", False)        # robustness: plain load
    act = df[price_sd > 0.01]
    run(act, "price-varying hours", "scar_gw", False)
    run(act, "price-varying hours", "scar_gw", True)
    print()
    for a in ["Gesternova", "AXPO", "NEXUS", "ENGIE", "IGNIS", "Other"]:
        run(df[df["agg"] == a], a, "scar_gw", False)

    # falsification benchmark: does offered MW (the forecast) track scarcity?
    s = df[df["scar_gw_w"].abs() > 1e-9]
    X = np.column_stack([np.ones(len(s)), s["scar_gw_w"].values])
    bq, sq = clustered_ols(s["q_own_w"].values, X, s["date_cl"].values)
    print(f"\n  benchmark -- offered MW vs within-hour scarcity: "
          f"beta = {bq[1]:+.2f} MW per GW  (t={bq[1]/sq[1]:+.2f})")

    pd.DataFrame(rows).to_csv(OUT, index=False)
    print(f"\nwrote {OUT}")

    # --- figure -------------------------------------------------------------
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    fig, axes = plt.subplots(1, 2, figsize=(12, 4.4))
    s = df[df["scar_gw_w"].abs() > 1e-9]
    qs = pd.qcut(s["scar_gw_w"], 20, duplicates="drop")
    gb = s.groupby(qs, observed=True)
    axes[0].scatter(gb["scar_gw_w"].mean(), gb["p_bid_w"].mean(),
                    s=28, color="#1f77b4", zorder=3)
    X = np.column_stack([np.ones(len(s)), s["scar_gw_w"].values])
    b, _ = clustered_ols(s["p_bid_w"].values, X, s["date_cl"].values)
    xx = np.linspace(s["scar_gw_w"].min(), s["scar_gw_w"].max(), 50)
    axes[0].plot(xx, b[0] + b[1] * xx, color="#d62728", lw=1.8)
    axes[0].axhline(0, color="grey", lw=0.6)
    axes[0].axvline(0, color="grey", lw=0.6)
    axes[0].set_xlabel("within-hour scarcity deviation, load$-$solar (GW)",
                       fontsize=9)
    axes[0].set_ylabel("within-hour bid-price deviation (EUR/MWh)", fontsize=9)
    axes[0].set_title(f"All wind: bid price vs scarcity\n"
                      rf"$\beta={b[1]:+.2f}$ EUR/MWh per GW (essentially flat)",
                      fontsize=9.5)
    axes[0].grid(alpha=0.3, lw=0.5)

    aggs = ["Gesternova", "AXPO", "NEXUS", "ENGIE", "IGNIS", "Other"]
    bb, ee = [], []
    for a in aggs:
        s = df[(df["agg"] == a) & (df["scar_gw_w"].abs() > 1e-9)]
        if len(s) < 50:
            bb.append(0.0); ee.append(0.0); continue
        X = np.column_stack([np.ones(len(s)), s["scar_gw_w"].values])
        b, se = clustered_ols(s["p_bid_w"].values, X, s["date_cl"].values)
        bb.append(b[1]); ee.append(1.96 * se[1])
    axes[1].bar(range(len(aggs)), bb, yerr=ee, color="#4c72b0", capsize=3)
    axes[1].axhline(0, color="black", lw=0.7)
    axes[1].set_xticks(range(len(aggs)))
    axes[1].set_xticklabels(aggs, rotation=25, ha="right", fontsize=8.5)
    axes[1].set_ylabel(r"$\beta$ (EUR/MWh per GW)", fontsize=9)
    axes[1].set_title("Within-hour scarcity response, by wind aggregator\n"
                      "(95% CI, date-clustered)", fontsize=9.5)
    axes[1].grid(alpha=0.3, lw=0.5, axis="y")
    fig.suptitle("Wind within-hour bid price vs within-hour scarcity "
                 "(DA15/ID15, unit$\\times$date$\\times$hour FE)", fontsize=10.5)
    fig.tight_layout(rect=[0, 0, 1, 0.93])
    fig.savefig(FIG, bbox_inches="tight", dpi=130)
    plt.close(fig)
    print(f"wrote {FIG}")


if __name__ == "__main__":
    main()
