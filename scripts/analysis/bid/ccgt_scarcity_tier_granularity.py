# STATUS: ALIVE
# LAST-AUDIT: 2026-05-22
# FEEDS: thesis/provisional/descriptive_facts.tex sec 9 (CCGT scarcity tier)
# CLAIM: Asks whether CCGT exploits day-ahead granularity in its SCARCITY
#        TIER -- the high-priced block withheld from the day-ahead auction.
#        That capacity is recalled later in pre-IDA Fase I, but Fase I is paid
#        pay-as-bid via a SEPARATE restriction offer (PO 14.4 apartado 20.1) --
#        NOT at this day-ahead price. So the DA scarcity-tier price is a
#        non-clearing marker, not the Fase I payment; this script measures the
#        withholding decision (quantity parked, within-hour shaping), not a
#        price the firm earns. The granularity ranking
#        (granularity_responsiveness_by_tech.py) used only the in-band
#        competing tier, so it cannot see this withheld margin.
#
#        Each CCGT quarter sell curve is split at the band edge (band centered
#        on the CLOCK-HOUR-MEAN clearing price c_h, fixed within the hour):
#          competing tier  |p - c_h| <= H         -- clears the auction
#          scarcity  tier  p > c_h + H            -- withheld; Fase I recall
#        with H = 140. Per (unit, date, quarter) we take each tier's
#        MW-weighted mean price and MW, plus scar_share = scarcity MW / total
#        offered MW. Two questions:
#        (1) DESCRIPTIVE -- is the scarcity tier shaped across the 4 within-
#            hour quarters MORE than the competing tier? D_price (SD across
#            quarters of the tier price) and D_share (SD of scar_share).
#        (2) STRATEGIC  -- does the scarcity tier respond to within-hour
#            scarcity? Regress scar_share / scarcity-tier price on a within-
#            hour scarcity proxy (DA load forecast minus solar; residual
#            demand ex-wind), unit x date x hour FE, date-clustered SEs.
#
# OUT: results/regressions/bid/granularity_response/ccgt_scarcity_tier.csv
#      figures/working/ccgt_scarcity_tier_granularity.pdf

from pathlib import Path
import duckdb
import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
P = REPO / "data/processed/omie"
DET = P / "mercado_diario/ofertas/det_all.parquet"
CAB = P / "mercado_diario/ofertas/cab_all.parquet"
MPDBC = P / "mercado_diario/precios/marginalpdbc_all.parquet"
LOADF = REPO / "data/processed/entsoe/load/load_forecast_da_all.parquet"
WSF = REPO / "data/processed/entsoe/generation/wind_solar_forecast_all.parquet"
UNITS = REPO / "data/external/omie_reference/lista_unidades.csv"
OUT = REPO / "results/regressions/bid/granularity_response/ccgt_scarcity_tier.csv"
OUT.parent.mkdir(parents=True, exist_ok=True)
FIG = REPO / "figures/working/ccgt_scarcity_tier_granularity.pdf"
FIG.parent.mkdir(parents=True, exist_ok=True)

LO, HI = "2025-10-01", "2026-05-15"       # layer 1 (full DA15/ID15)
HI2 = "2026-04-22"                         # layer 2 (load+solar coverage)
DST = ("2025-10-26", "2026-03-29")
H = 140.0
CRITICAL = {5, 6, 7, 8, 16, 17, 18, 19, 20, 21, 22}
FLAT = {1, 2, 3}
FIRMS = ["IB", "GE", "GN", "HC"]


def tech_bucket(t):
    return "CCGT" if "ciclo combinado" in str(t).lower() else "Other"


def firm_bucket(o):
    o = str(o).lower()
    if "iberdrola" in o: return "IB"
    if "endesa" in o: return "GE"
    if "naturgy" in o or "gas natural" in o: return "GN"
    if "edp" in o or "hidroel" in o: return "HC"
    return "OTH"


def clustered_ols(y, X, cluster):
    XtX_inv = np.linalg.inv(X.T @ X)
    beta = XtX_inv @ (X.T @ y)
    e = y - X @ beta
    meat = np.zeros((X.shape[1], X.shape[1]))
    for g in np.unique(cluster):
        m = cluster == g
        s = X[m].T @ e[m]
        meat += np.outer(s, s)
    G, (n, k) = len(np.unique(cluster)), X.shape
    adj = (G / (G - 1)) * ((n - 1) / (n - k))
    V = adj * (XtX_inv @ meat @ XtX_inv)
    return beta, np.sqrt(np.diag(V))


def cv(x):
    x = x.values
    m = x.mean()
    return np.std(x, ddof=1) / m if m > 0 else np.nan


def main():
    con = duckdb.connect()
    con.execute("SET memory_limit='12GB'")
    con.execute("SET threads=4")
    con.execute("SET preserve_insertion_order=false")

    u = pd.read_csv(UNITS)
    u["tech"] = u["technology"].apply(tech_bucket)
    u["firm"] = u["owner_agent"].apply(firm_bucket)
    u = u[u["tech"] == "CCGT"][["unit_code", "firm"]].drop_duplicates("unit_code")
    con.register("u", u)
    dst = "(" + ",".join(f"'{d}'" for d in DST) + ")"

    # --- per (unit, date, quarter): competing vs scarcity tier --------------
    q = con.execute(f"""
    WITH cab_l AS (
      SELECT d, offer_code, unit_code FROM (
        SELECT CAST(date AS DATE) d, offer_code, unit_code,
               ROW_NUMBER() OVER (PARTITION BY CAST(date AS DATE),offer_code,unit_code
                                  ORDER BY version DESC) rn
        FROM '{CAB}' WHERE date BETWEEN '{LO}' AND '{HI}' AND buy_sell='V') WHERE rn=1),
    det AS (
      SELECT CAST(date AS DATE) d, offer_code,
             CAST(FLOOR((period - 1) / 4.0) AS INT) AS clock_hour,
             ((period - 1) % 4) AS quarter,
             price_eur_mwh p, quantity_mw q
      FROM '{DET}' WHERE date BETWEEN '{LO}' AND '{HI}'
        AND quantity_mw > 0 AND period BETWEEN 1 AND 96),
    mp_h AS (
      SELECT CAST(date AS DATE) d,
             CAST(FLOOR((period - 1) / 4.0) AS INT) AS clock_hour,
             AVG(price_es_eur_mwh) AS c_h
      FROM '{MPDBC}' WHERE date BETWEEN '{LO}' AND '{HI}'
        AND price_es_eur_mwh IS NOT NULL GROUP BY 1, 2),
    tr AS (
      SELECT c.unit_code, u.firm, dv.d, dv.clock_hour, dv.quarter, dv.q, dv.p,
             mp_h.c_h
      FROM det dv JOIN cab_l c ON dv.d=c.d AND dv.offer_code=c.offer_code
        JOIN u    ON c.unit_code = u.unit_code
        JOIN mp_h ON mp_h.d=dv.d AND mp_h.clock_hour=dv.clock_hour
      WHERE dv.d NOT IN {dst})
    SELECT unit_code, firm, d, clock_hour, quarter,
           SUM(q)                                                  AS m_tot,
           SUM(q) FILTER (p BETWEEN c_h - {H} AND c_h + {H})        AS m_comp,
           SUM(q*p) FILTER (p BETWEEN c_h - {H} AND c_h + {H})      AS wp_comp,
           SUM(q) FILTER (p > c_h + {H})                            AS m_scar,
           SUM(q*p) FILTER (p > c_h + {H})                          AS wp_scar
    FROM tr
    GROUP BY 1,2,3,4,5
    """).fetchdf()

    for c in ["m_comp", "wp_comp", "m_scar", "wp_scar"]:
        q[c] = q[c].fillna(0.0)
    q["p_comp"] = np.where(q["m_comp"] > 0, q["wp_comp"] / q["m_comp"], np.nan)
    q["p_scar"] = np.where(q["m_scar"] > 0, q["wp_scar"] / q["m_scar"], np.nan)
    q["scar_share"] = q["m_scar"] / q["m_tot"]

    # --- within-hour shaping: one row per (unit, date, clock-hour) ----------
    q["g"] = (q["unit_code"].astype(str) + "|" + q["d"].astype(str)
              + "|" + q["clock_hour"].astype(str))
    full = q.groupby("g")["quarter"].transform("nunique") == 4
    h = q[full].groupby(["unit_code", "firm", "d", "clock_hour", "g"],
                        observed=True)
    cell = h.agg(
        scar_share_mean=("scar_share", "mean"),
        D_share=("scar_share", lambda x: np.std(x, ddof=1)),
        D_qty_comp=("m_comp", cv),
        D_price_comp=("p_comp", lambda x: np.std(x.dropna(), ddof=1)
                      if x.notna().sum() >= 2 else np.nan),
        D_price_scar=("p_scar", lambda x: np.std(x.dropna(), ddof=1)
                      if x.notna().sum() >= 2 else np.nan),
        n_scar_q=("m_scar", lambda x: (x > 0).sum()),
    ).reset_index()
    cell["hour_class"] = np.where(cell["clock_hour"].isin(CRITICAL), "Critical",
                          np.where(cell["clock_hour"].isin(FLAT), "Flat", "Other"))

    print(f"\n=== CCGT scarcity-tier granularity ({LO}..{HI}, DA sell) ===")
    print(f"  {len(cell):,} (unit,date,hour) cells with all 4 quarters | "
          f"{cell['unit_code'].nunique()} CCGT units")
    print("  competing tier |p-c_h|<=140 ; scarcity tier p>c_h+140 "
          "(c_h = clock-hour-mean clearing price)\n")
    print(f"  {'firm':6s} {'n_cells':>9s}  {'scar_share':>10s}  "
          f"{'D_share':>8s}  {'D_qty_comp':>10s}  {'D_p_comp':>9s}  "
          f"{'D_p_scar':>9s}")
    rows = []
    for fm in FIRMS + ["OTH", "ALL"]:
        c = cell if fm == "ALL" else cell[cell["firm"] == fm]
        if c.empty:
            continue
        rec = dict(firm=fm, n_cells=len(c),
                   scar_share=c["scar_share_mean"].mean(),
                   D_share=c["D_share"].mean(),
                   D_qty_comp=c["D_qty_comp"].mean(),
                   D_price_comp=c["D_price_comp"].mean(),
                   D_price_scar=c["D_price_scar"].mean())
        rows.append(rec)
        print(f"  {fm:6s} {len(c):>9,}  {rec['scar_share']:>9.1%}  "
              f"{rec['D_share']:>8.3f}  {rec['D_qty_comp']:>10.3f}  "
              f"{rec['D_price_comp']:>9.2f}  {rec['D_price_scar']:>9.1f}")
    pd.DataFrame(rows).to_csv(OUT, index=False)

    # critical vs flat (ALL CCGT)
    print("\n  within-hour scarcity-tier shaping, critical vs flat hours:")
    for hc in ["Critical", "Flat"]:
        c = cell[cell["hour_class"] == hc]
        print(f"    {hc:9s}  D_share {c['D_share'].mean():.3f}  "
              f"scar_share {c['scar_share_mean'].mean():.1%}  "
              f"D_price_scar {c['D_price_scar'].mean():.1f}  (n={len(c):,})")

    # --- layer 2: does the scarcity tier respond to within-hour scarcity? ---
    scar = con.execute(f"""
    WITH ld AS (
      SELECT (isp_start_utc AT TIME ZONE 'UTC') AT TIME ZONE 'Europe/Madrid' loc,
             load_forecast_mw lf FROM '{LOADF}'
      WHERE isp_start_utc BETWEEN '{LO}' AND TIMESTAMP '{HI2}' + INTERVAL 2 DAY
        AND mtu_minutes = 15),
    sol AS (
      SELECT (isp_start_utc AT TIME ZONE 'UTC') AT TIME ZONE 'Europe/Madrid' loc,
             quantity_mw sm FROM '{WSF}'
      WHERE isp_start_utc BETWEEN '{LO}' AND TIMESTAMP '{HI2}' + INTERVAL 2 DAY
        AND mtu_minutes = 15 AND psr_type='B16')
    SELECT CAST(ld.loc AS DATE) AS d, HOUR(ld.loc) AS clock_hour,
           CAST(MINUTE(ld.loc)/15 AS INT) AS quarter,
           (ld.lf - sol.sm)/1000.0 AS scar_gw
    FROM ld JOIN sol ON ld.loc=sol.loc
    """).fetchdf()

    r = q[full].merge(scar, on=["d", "clock_hour", "quarter"], how="inner")
    r = r[r["d"].astype(str) <= HI2].copy()
    r["date_cl"] = r["d"].astype(str)
    for c in ["scar_share", "m_scar", "scar_gw"]:
        r[f"{c}_w"] = r[c] - r.groupby("g")[c].transform("mean")
    # price of the withheld block: only where the unit has a scarcity tier
    rp = r[r["m_scar"] > 0].copy()
    rp["p_scar_w"] = rp["p_scar"] - rp.groupby("g")["p_scar"].transform("mean")

    print("\n  --- layer 2: scarcity tier vs within-hour scarcity proxy ---")
    print("  outcome regressed on within-hour-demeaned (load-solar), GW; "
          "unit x date x hour FE; date-clustered SE\n")
    reg_rows = []
    for label, sub, ycol in [
            ("scar_share", r, "scar_share"),
            ("scar MW",    r, "m_scar"),
            ("scar price", rp, "p_scar")]:
        s = sub[sub["scar_gw_w"].abs() > 1e-9]
        X = np.column_stack([np.ones(len(s)), s["scar_gw_w"].values])
        b, se = clustered_ols(s[f"{ycol}_w"].values, X, s["date_cl"].values)
        reg_rows.append(dict(outcome=ycol, beta=b[1], se=se[1],
                             t=b[1]/se[1], n=len(s)))
        print(f"    {label:12s}  beta={b[1]:+10.4f}  se={se[1]:8.4f}  "
              f"t={b[1]/se[1]:+6.2f}  n={len(s):,}")
    # per firm: scar_share response
    print("\n  scar_share response by firm:")
    for fm in FIRMS:
        s = r[(r["firm"] == fm) & (r["scar_gw_w"].abs() > 1e-9)]
        if len(s) < 50:
            continue
        X = np.column_stack([np.ones(len(s)), s["scar_gw_w"].values])
        b, se = clustered_ols(s["scar_share_w"].values, X, s["date_cl"].values)
        print(f"    {fm}:  beta={b[1]:+9.4f}  se={se[1]:7.4f}  "
              f"t={b[1]/se[1]:+6.2f}  n={len(s):,}")

    pd.DataFrame(reg_rows).to_csv(
        OUT.with_name("ccgt_scarcity_tier_layer2.csv"), index=False)

    # --- layer 3: do constrained-zone CCGTs bid a HIGHER scarcity tier? -----
    # p_scar = MW-weighted mean price of the withheld block (the Fase I-recall
    # capacity). Constrained corridor = the voltage-fragile southern zones
    # with the heaviest PO-3.2 / operacion reforzada redispatch (sec geo).
    ZFIRM = (REPO / "results/regressions/regulatory/ccgt_zonal_competition/"
             "ccgt_zonal_firm.csv")
    CONSTRAINED = {"Sur", "Levante", "Cataluna"}
    zf = pd.read_csv(ZFIRM)[["unit_code", "zone", "firm"]].drop_duplicates(
        "unit_code")
    zf["constrained"] = zf["zone"].isin(CONSTRAINED)
    # drop q's coarse firm bucket; use the zonal map's firm for the FE
    qz = q[q["m_scar"] > 0].drop(columns=["firm"]).merge(
        zf, on="unit_code", how="inner")

    print("\n  --- layer 3: scarcity-tier PRICE LEVEL by zone ---")
    print(f"  p_scar = MW-weighted mean price of the withheld block (EUR/MWh); "
          f"constrained corridor = {sorted(CONSTRAINED)}\n")
    bz = (qz.groupby("zone")
            .agg(n_units=("unit_code", "nunique"), n_cells=("p_scar", "size"),
                 p_scar=("p_scar", "mean"), scar_share=("scar_share", "mean"))
            .reset_index().sort_values("p_scar", ascending=False))
    bz["constrained"] = bz["zone"].isin(CONSTRAINED)
    for _, z in bz.iterrows():
        flag = "CONSTRAINED" if z["zone"] in CONSTRAINED else ""
        print(f"    {z['zone']:10s}  units {int(z['n_units']):2d}  "
              f"cells {int(z['n_cells']):>8,}  p_scar {z['p_scar']:>8.1f}  "
              f"scar_share {z['scar_share']:>6.1%}  {flag}")
    for grp, lab in [(True, "CONSTRAINED  "), (False, "unconstrained")]:
        s = qz[qz["constrained"] == grp]
        print(f"  {lab}: p_scar {s['p_scar'].mean():8.1f} EUR/MWh  "
              f"(n={len(s):,}, {s['unit_code'].nunique()} units)")

    # firm x zone-type overlap: beta(constrained) with firm FE is identified
    # ONLY off firms that operate plants in BOTH zone types. If firms were
    # perfectly nested in zones (firm == zone) it would not be identified.
    units = (qz.groupby(["unit_code", "firm", "zone", "constrained"],
                        observed=True)
               .agg(p_scar=("p_scar", "mean"), n_cells=("p_scar", "size"))
               .reset_index())
    ct = (units.groupby(["firm", "constrained"]).size()
               .unstack(fill_value=0)
               .reindex(columns=[False, True], fill_value=0))
    print("\n  firm x zone-type overlap (CCGT units with a scarcity tier):")
    for fm, row in ct.iterrows():
        nu, nc = int(row[False]), int(row[True])
        tag = "<-- SPANS both (identifies beta)" if (nu and nc) else ""
        print(f"    {fm:14s}  unconstrained {nu:2d}   constrained {nc:2d}   {tag}")
    nspan = int(((ct[False] > 0) & (ct[True] > 0)).sum())
    span_u = units[units["firm"].isin(ct.index[(ct[False] > 0) & (ct[True] > 0)])]
    print(f"  -> {nspan} firms span both; within-firm estimate uses "
          f"{span_u['unit_code'].nunique()} units across those firms")
    units.to_csv(OUT.with_name("ccgt_scarcity_tier_by_unit.csv"), index=False)

    # within-firm constrained premium: firm FE, SE clustered by unit
    fd = pd.get_dummies(qz["firm"], prefix="f", drop_first=True).astype(float)
    X = np.column_stack([np.ones(len(qz)),
                         qz["constrained"].astype(float).values, fd.values])
    b, se = clustered_ols(qz["p_scar"].values, X, qz["unit_code"].values)
    print(f"\n  within-firm constrained premium (firm FE, cluster by unit):")
    print(f"    beta(constrained) = {b[1]:+.1f} EUR/MWh   se {se[1]:.1f}   "
          f"t {b[1]/se[1]:+.2f}")
    print("  per firm, mean p_scar (constrained vs unconstrained zones):")
    for fm in sorted(qz["firm"].unique()):
        s = qz[qz["firm"] == fm]
        c = s[s["constrained"]]["p_scar"]
        nc = s[~s["constrained"]]["p_scar"]
        cs = f"{c.mean():8.1f}" if len(c) else "      --"
        ncs = f"{nc.mean():8.1f}" if len(nc) else "      --"
        print(f"    {fm:14s} constrained {cs}  unconstrained {ncs}")
    bz.to_csv(OUT.with_name("ccgt_scarcity_tier_by_zone.csv"), index=False)
    print(f"\nwrote {OUT}")

    # --- figure -------------------------------------------------------------
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    fig, axes = plt.subplots(1, 3, figsize=(16.5, 4.6))
    d = pd.DataFrame(rows).set_index("firm")
    order = [f for f in FIRMS + ["OTH"] if f in d.index]
    x = np.arange(len(order))
    # left: competing-tier quantity shaping vs scarcity-tier share shaping
    axes[0].bar(x - 0.2, d.loc[order, "D_qty_comp"], 0.38,
                color="#4c72b0", label="competing tier  $D_{qty}$")
    axes[0].bar(x + 0.2, d.loc[order, "D_share"], 0.38,
                color="#c44e52", label="scarcity tier  $D_{share}$")
    axes[0].set_xticks(x); axes[0].set_xticklabels(order)
    axes[0].set_ylabel("within-hour dispersion (CV / SD of share)", fontsize=9)
    axes[0].set_title("Within-hour QUANTITY shaping: competing vs scarcity tier",
                      fontsize=9.5)
    axes[0].legend(fontsize=8)
    axes[0].grid(alpha=0.3, lw=0.5, axis="y")
    # middle: scarcity-tier share level and within-hour SD
    axes[1].bar(x - 0.2, d.loc[order, "scar_share"], 0.38,
                color="#7a7a7a", label="mean scarcity share")
    axes[1].bar(x + 0.2, d.loc[order, "D_share"], 0.38,
                color="#c44e52", label="within-hour SD of scarcity share")
    axes[1].set_xticks(x); axes[1].set_xticklabels(order)
    axes[1].set_ylabel("fraction of offered MW", fontsize=9)
    axes[1].set_title("Scarcity tier: how much is withheld, how much it moves",
                      fontsize=9.5)
    axes[1].legend(fontsize=8)
    axes[1].grid(alpha=0.3, lw=0.5, axis="y")
    # right: per-unit scarcity price, unconstrained vs constrained, by firm.
    # Each firm's two-point line is flat => no within-firm zone effect; firms
    # appearing in BOTH columns show the estimate is identified, not nested.
    fcol = {"IB": "#1f77b4", "GE": "#2ca02c", "GN": "#d62728", "HC": "#9467bd",
            "Engie": "#ff7f0e", "Moeve": "#8c564b", "OTHER": "#999999",
            "TotalEnergies": "#17becf"}
    rng = np.random.default_rng(0)
    for fm in sorted(units["firm"].unique()):
        fu = units[units["firm"] == fm]
        xs = fu["constrained"].astype(int) + rng.uniform(-0.07, 0.07, len(fu))
        axes[2].scatter(xs, fu["p_scar"], s=42, color=fcol.get(fm, "#999"),
                        edgecolors="black", linewidths=0.4, zorder=3, label=fm)
        mu = fu.groupby("constrained")["p_scar"].mean()
        if len(mu) == 2:
            axes[2].plot([0, 1], [mu[False], mu[True]], color=fcol.get(fm, "#999"),
                         lw=1.6, alpha=0.75, zorder=2)
    axes[2].set_xticks([0, 1])
    axes[2].set_xticklabels(["unconstrained\nzones", "constrained\ncorridor"])
    axes[2].set_xlim(-0.4, 1.4)
    axes[2].set_ylabel("unit mean scarcity-tier price (EUR/MWh)", fontsize=9)
    axes[2].set_title("Per-unit scarcity price by firm:\neach firm flat across "
                      "zones (lines = firm means)", fontsize=9.5)
    axes[2].legend(fontsize=6.5, ncol=2)
    axes[2].grid(alpha=0.3, lw=0.5, axis="y")
    fig.suptitle("CCGT scarcity tier (withheld, Fase I-recalled): within-hour "
                 "granularity use and zonal price level (DA15/ID15)",
                 fontsize=10.5)
    fig.tight_layout(rect=[0, 0, 1, 0.94])
    fig.savefig(FIG, bbox_inches="tight", dpi=130)
    plt.close(fig)
    print(f"wrote {FIG}")


if __name__ == "__main__":
    main()
