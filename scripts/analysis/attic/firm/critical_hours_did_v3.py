# STATUS: ALIVE
# LAST-AUDIT: 2026-05-04
# FEEDS: Critical-hours v3: bid-quarter differentiation + q1 DiD +
#        bootstrap pre-blackout
# CLAIM: (1) Firms differentiate bids across the 4 within-hour quarters
#        more in critical hours than in flat hours — direct behavioural
#        evidence of the granularity-targeting mechanism. (2) q1 (DA
#        forward sale) is HIGHER in critical hours under DA60/ID15
#        (hedging hypothesis confirmed). (3) The DA60/ID15-pre-blackout
#        DiD coefficient survives a wild-cluster bootstrap.
"""Critical-hours v3 — three tests:

(A) Bid-quarter-differentiation rate, critical vs flat hours.
    For each (unit, date, hour) post-MTU15-DA, classify whether the unit
    submitted identical bid signatures across the 4 within-hour quarters
    or differentiated bids. The DiD identification assumes flat hours
    are unaffected by granularity — the cleanest test is direct
    behavioural verification: does the differentiation rate rise in
    critical hours? If yes, this is the most direct evidence of the
    granularity-targeting mechanism.

(B) q1 DiD: same identification framework on q1 = DA cleared sell volume.
    Hypothesis (Candidate 1, hedging): under DA60/ID15, firms commit MORE
    in DA in critical hours to hedge within-hour ISP imbalance risk
    → q1 critical-flat differential is POSITIVE. Under DA15/ID15, no
    hedging concern (DA also 15-min) → q1 differential is zero or
    negative (firm allocates MORE to spot withholding instead).

(C) Wild-cluster bootstrap of the DA60/ID15-pre-blackout DiD coefficient.
    Pre-blackout window has G ≈ 40 date clusters, borderline Cameron-
    Miller. Bootstrap to verify the −32 coefficient is robust.

Output:
  results/regressions/critical_hours_bid_differentiation.csv
  results/regressions/critical_hours_q1_did.csv
  results/regressions/critical_hours_bootstrap_preblackout.csv
  figures/working/critical_hours_bid_differentiation.png
  figures/working/critical_hours_q1_did.png
"""
from __future__ import annotations

from pathlib import Path

import duckdb
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import statsmodels.api as sm

PROJECT = Path(__file__).resolve().parents[3]
PIBCIE  = PROJECT / "data" / "processed" / "omie" / "mercado_intradiario_subastas" / "programas" / "pibcie_all.parquet"
PDBCE   = PROJECT / "data" / "processed" / "omie" / "mercado_diario" / "programas" / "pdbce_all.parquet"
DET     = PROJECT / "data" / "processed" / "omie" / "mercado_diario" / "ofertas" / "det_all.parquet"
CAB     = PROJECT / "data" / "processed" / "omie" / "mercado_diario" / "ofertas" / "cab_all.parquet"
LISTA   = PROJECT / "data" / "external" / "omie_reference" / "lista_unidades.csv"

OUT_DIR_R = PROJECT / "results" / "regressions"
OUT_DIR_F = PROJECT / "figures" / "working"

REGIMES_6 = ["pre-IDA", "3-sess", "ISP15-win", "DA60/ID15-pre-blackout",
             "DA60/ID15-reforzada", "DA15/ID15"]
BIG4 = ["IB", "GE", "GN", "HC"]
MTU15_IDA_DATE = pd.Timestamp("2025-03-19")
MTU15_DA_DATE  = pd.Timestamp("2025-10-01")
BLACKOUT_DATE  = pd.Timestamp("2025-04-28")
CRITICAL_HOURS = [7, 8, 16, 17, 18]


def assign_regime6(d) -> str:
    d = pd.Timestamp(d)
    if d < pd.Timestamp("2024-06-14"): return "pre-IDA"
    if d < pd.Timestamp("2024-12-01"): return "3-sess"
    if d < pd.Timestamp("2025-03-19"): return "ISP15-win"
    if d < pd.Timestamp("2025-04-28"): return "DA60/ID15-pre-blackout"
    if d < pd.Timestamp("2025-10-01"): return "DA60/ID15-reforzada"
    return "DA15/ID15"


def fit_did(panel, regimes, demean_col=None, group_for_dummies=None,
            outcome="q_mwh"):
    if demean_col is not None:
        panel = panel.copy()
        panel["_outcome_dm"] = panel[outcome] - panel.groupby(demean_col)[outcome].transform("mean")
        outcome = "_outcome_dm"
    cols = {"const": np.ones(len(panel))}
    cols["critical"] = panel["critical"].values.astype(float)
    for r in regimes[1:]:
        cols[f"R_{r}"] = (panel["regime"] == r).astype(float).values
    for r in regimes[1:]:
        cols[f"crit×R_{r}"] = ((panel["critical"]) * (panel["regime"] == r)).astype(float).values
    if group_for_dummies is not None:
        for g in group_for_dummies:
            for v in sorted(panel[g].unique())[1:]:
                cols[f"{g}_{v}"] = (panel[g] == v).astype(float).values
    for m_ in range(2, 13):
        cols[f"M{m_}"] = (panel["month"] == m_).astype(float).values
    years = sorted(panel["year"].unique())
    for yr in years[1:]:
        cols[f"Y{yr}"] = (panel["year"] == yr).astype(float).values
    for d_ in range(1, 7):
        cols[f"DOW{d_}"] = (panel["dow"] == d_).astype(float).values

    X = pd.DataFrame(cols, index=panel.index)
    y = panel[outcome].values
    cluster = panel["date"].dt.strftime("%Y%m%d").astype(np.int64).values
    m = sm.OLS(y, X.values).fit(cov_type="cluster", cov_kwds={"groups": cluster})
    return dict(coefs=pd.Series(m.params, index=X.columns),
                ses=pd.Series(m.bse, index=X.columns),
                pvals=pd.Series(m.pvalues, index=X.columns),
                n=len(panel), n_clusters=len(np.unique(cluster)),
                rsq=m.rsquared, X=X, y=y, cluster=cluster, model=m)


def main() -> None:
    con = duckdb.connect()
    con.execute("SET memory_limit='8GB'")
    con.execute("SET threads=4")

    print("[setup] mappings…", flush=True)
    firms = con.execute(f"""
        SELECT unit_code, grupo_empresarial AS firm FROM (
          SELECT unit_code, grupo_empresarial,
                 ROW_NUMBER() OVER (PARTITION BY unit_code ORDER BY date DESC) AS rn
          FROM '{PDBCE}' WHERE grupo_empresarial IS NOT NULL) WHERE rn = 1
    """).df()
    lista = pd.read_csv(LISTA)[["unit_code","technology"]].drop_duplicates(subset=["unit_code"])
    map_uf = firms.merge(lista, on="unit_code", how="left")

    def tech_group(t):
        if not isinstance(t, str): return "Other"
        tl = t.lower()
        if "gas" in tl or "ciclo" in tl: return "CCGT"
        if "nuclear" in tl: return "Nuclear"
        if "ombeo" in tl or "idráulica" in tl: return "Hydro"
        return "Other"
    map_uf["tech"] = map_uf["technology"].apply(tech_group)
    con.register("uf", map_uf[["unit_code","firm","tech"]])

    # ================================================================
    # PIECE A — Bid-quarter-differentiation rate (post-MTU15-DA only)
    # ================================================================
    print()
    print("[A] Bid-quarter-differentiation post-MTU15-DA (the cleanest test)…", flush=True)
    print("    For each Big-4 unit-day-hour, check if bid signature differs across 4 quarters.")

    # Build per-(unit, date, period) bid signature: median of tranche prices weighted by quantity
    # Then group to (unit, date, hour) and count distinct signatures across 4 quarters.
    # Restrict to post-MTU15-DA where DA periods are 1..96.
    # DET uses NUMERIC offer_code; CAB maps offer_code → unit_code (alphanumeric).
    # Join DET → CAB → uf to get firm and tech. CAB has buy_sell flag (V = sell, C = buy).
    bid_sig = con.execute(f"""
        WITH cab AS (
          SELECT CAST(date AS DATE) AS date, offer_code, version, unit_code, buy_sell
          FROM '{CAB}'
          WHERE buy_sell = 'V'                              -- sell-side only
            AND CAST(date AS DATE) >= DATE '2025-10-01'
        ),
        bids AS (
          SELECT CAST(d.date AS DATE) AS date, d.period, c.unit_code,
                 ROUND(SUM(d.price_eur_mwh * d.quantity_mw) / NULLIF(SUM(d.quantity_mw), 0), 1) AS bid_avg_price,
                 SUM(d.quantity_mw) AS bid_total_mw,
                 COUNT(*) AS n_tranches
          FROM '{DET}' d
            JOIN cab c ON CAST(d.date AS DATE) = c.date
                       AND d.offer_code = c.offer_code
                       AND d.version = c.version
            JOIN uf ON c.unit_code = uf.unit_code
          WHERE uf.firm IN ('IB','GE','GN','HC')
            AND d.quantity_mw IS NOT NULL
            AND d.quantity_mw > 0
            AND CAST(d.date AS DATE) >= DATE '2025-10-01'
            AND d.period BETWEEN 1 AND 96
          GROUP BY 1, 2, 3
        )
        SELECT * FROM bids
    """).df()
    bid_sig["date"] = pd.to_datetime(bid_sig["date"])
    bid_sig["hour"] = ((bid_sig["period"] - 1) // 4).astype(int).clip(0, 23)
    bid_sig["q_in_h"] = ((bid_sig["period"] - 1) % 4).astype(int)
    print(f"    bid signatures (unit-period rows): {len(bid_sig):,}", flush=True)

    # For each (date, unit, hour), count distinct bid_avg_price values
    # Need exactly 4 quarter observations per hour to qualify (full hour observation)
    grp = bid_sig.groupby(["date","unit_code","hour"]).agg(
        n_quarters=("q_in_h", "nunique"),
        n_distinct_prices=("bid_avg_price", "nunique"),
        n_distinct_volumes=("bid_total_mw", "nunique"),
        n_distinct_tranches=("n_tranches", "nunique"),
    ).reset_index()
    grp = grp[grp["n_quarters"] == 4]  # require complete hour
    grp["differentiated_price"] = (grp["n_distinct_prices"] > 1).astype(int)
    grp["differentiated_volume"] = (grp["n_distinct_volumes"] > 1).astype(int)
    grp["differentiated_any"] = ((grp["n_distinct_prices"] > 1) |
                                  (grp["n_distinct_volumes"] > 1)).astype(int)
    grp = grp.merge(map_uf[["unit_code","firm","tech"]], on="unit_code", how="left")
    grp["critical"] = grp["hour"].isin(CRITICAL_HOURS).astype(int)
    print(f"    full-hour unit-hour observations: {len(grp):,}", flush=True)

    # Cross-tabulate: differentiation rate by critical/flat × tech
    print()
    print("=" * 100)
    print("PIECE A — Bid-quarter-differentiation rate, post-MTU15-DA")
    print("=" * 100)
    print()
    print("Differentiation rate (% of unit-hours with non-identical bids across the 4 quarters):")
    print()
    cross = grp.groupby(["tech","critical"]).agg(
        diff_price_rate=("differentiated_price", "mean"),
        diff_volume_rate=("differentiated_volume", "mean"),
        diff_any_rate=("differentiated_any", "mean"),
        n=("differentiated_any", "size"),
    ).reset_index()
    cross["critical_label"] = cross["critical"].map({0: "flat", 1: "critical"})
    print(cross[["tech","critical_label","diff_price_rate","diff_volume_rate",
                 "diff_any_rate","n"]].to_string(index=False, float_format=lambda x: f"{x:.3f}"))

    # Aggregate Big-4 (any tech)
    agg_all = grp.groupby("critical").agg(
        diff_price_rate=("differentiated_price","mean"),
        diff_volume_rate=("differentiated_volume","mean"),
        diff_any_rate=("differentiated_any","mean"),
        n=("differentiated_any","size"),
    ).reset_index()
    agg_all["critical_label"] = agg_all["critical"].map({0: "flat", 1: "critical"})
    print()
    print("Aggregate Big-4:")
    print(agg_all.to_string(index=False, float_format=lambda x: f"{x:.3f}"))
    grp.to_csv(OUT_DIR_R / "critical_hours_bid_differentiation.csv", index=False)

    # Figure: differentiation rate by tech × critical
    fig, ax = plt.subplots(figsize=(8, 4.5))
    techs = ["Hydro","CCGT","Nuclear"]
    width = 0.35
    x = np.arange(len(techs))
    flat_rates = [cross[(cross.tech==t) & (cross.critical==0)]["diff_any_rate"].iloc[0] if
                  len(cross[(cross.tech==t) & (cross.critical==0)]) else 0 for t in techs]
    crit_rates = [cross[(cross.tech==t) & (cross.critical==1)]["diff_any_rate"].iloc[0] if
                  len(cross[(cross.tech==t) & (cross.critical==1)]) else 0 for t in techs]
    ax.bar(x - width/2, flat_rates, width, label="Flat hours", color="tab:gray")
    ax.bar(x + width/2, crit_rates, width, label="Critical hours", color="tab:red")
    for i, (f, c) in enumerate(zip(flat_rates, crit_rates)):
        ax.text(i - width/2, f + 0.005, f"{f:.1%}", ha="center", fontsize=9)
        ax.text(i + width/2, c + 0.005, f"{c:.1%}", ha="center", fontsize=9)
    ax.set_xticks(x)
    ax.set_xticklabels(techs)
    ax.set_ylabel("Differentiation rate (any across 4 quarters)")
    ax.set_title("Big-4 bid-quarter differentiation rate, post-MTU15-DA\n"
                 "Direct behavioural evidence of the granularity-targeting mechanism")
    ax.legend()
    plt.tight_layout()
    OUT_DIR_F.mkdir(parents=True, exist_ok=True)
    plt.savefig(OUT_DIR_F / "critical_hours_bid_differentiation.png", dpi=110, bbox_inches="tight")
    plt.close()

    # ================================================================
    # PIECE B — q1 DiD (test of Candidate 1: hedging)
    # ================================================================
    print()
    print("[B] q1 DiD (DA forward sale, test of Candidate 1)…", flush=True)
    q1 = con.execute(f"""
        SELECT CAST(p.date AS DATE) AS date, p.period, uf.firm,
               SUM(CASE WHEN p.offer_type = 1 AND p.assigned_power_mw > 0
                        THEN p.assigned_power_mw * p.mtu_minutes / 60.0
                        ELSE 0 END) AS q1_mwh
        FROM '{PDBCE}' p JOIN uf USING (unit_code)
        WHERE uf.firm IN ('IB','GE','GN','HC')
        GROUP BY 1, 2, 3
    """).df()
    q1["date"] = pd.to_datetime(q1["date"])
    is_post_da15 = q1["date"] >= MTU15_DA_DATE
    q1["hour"] = np.where(is_post_da15,
                          ((q1["period"] - 1) // 4).astype(int),
                          (q1["period"] - 1).astype(int)).clip(0, 23)
    q1h = q1.groupby(["date","firm","hour"], as_index=False)["q1_mwh"].sum()
    q1h["regime"] = q1h["date"].apply(assign_regime6)
    q1h["critical"] = q1h["hour"].isin(CRITICAL_HOURS).astype(int)
    q1h["dow"] = q1h["date"].dt.dayofweek
    q1h["month"] = q1h["date"].dt.month
    q1h["year"] = q1h["date"].dt.year
    q1h["q_mwh"] = q1h["q1_mwh"]  # rename for fit_did
    print(f"    q1 firm-day-hour rows: {len(q1h):,}", flush=True)

    res_q1 = fit_did(q1h, REGIMES_6, group_for_dummies=["firm"], outcome="q_mwh")
    print()
    print("=" * 100)
    print("PIECE B — q1 (DA forward sale) DiD: critical × regime")
    print("=" * 100)
    print(f"  N = {res_q1['n']:,};  G = {res_q1['n_clusters']:,};  R² = {res_q1['rsq']:.3f}")
    print(f"  q1 critical main effect (pre-IDA baseline): β = {res_q1['coefs']['critical']:+.1f} "
          f"(SE {res_q1['ses']['critical']:.1f}, p={res_q1['pvals']['critical']:.2e})")
    print()
    print("  q1 DiD interactions (critical × regime, change vs pre-IDA):")
    rows_q1 = []
    for r in REGIMES_6[1:]:
        k = f"crit×R_{r}"
        b = res_q1["coefs"][k]; se = res_q1["ses"][k]; p = res_q1["pvals"][k]
        net = res_q1["coefs"]["critical"] + b
        print(f"    {r:30s}: δ_q1 = {b:+8.2f}  (SE {se:5.2f}, p={p:.2e})  → net = {net:+.1f}")
        rows_q1.append({"regime": r, "delta_q1": b, "se": se, "p": p,
                         "net_critical_effect": net})
    pd.DataFrame(rows_q1).to_csv(OUT_DIR_R / "critical_hours_q1_did.csv", index=False)

    # Figure: q1 DiD coefficients by regime
    fig, ax = plt.subplots(figsize=(10, 5))
    deltas = [res_q1["coefs"][f"crit×R_{r}"] for r in REGIMES_6[1:]]
    ses    = [res_q1["ses"][f"crit×R_{r}"]   for r in REGIMES_6[1:]]
    base   = res_q1["coefs"]["critical"]
    x = np.arange(len(REGIMES_6[1:]))
    colors = ["tab:gray","tab:gray","tab:blue","tab:orange","tab:red"]
    ax.bar(x, deltas, yerr=[1.96*s for s in ses], capsize=5, color=colors)
    ax.axhline(0, color="black", lw=0.6)
    labels = ["3-sess","ISP15-win","DA60/ID15\npre-blackout","DA60/ID15\nreforzada","DA15/ID15"]
    ax.set_xticks(x)
    ax.set_xticklabels(labels, fontsize=9)
    ax.set_ylabel(r"q1 DiD δ$_R$: critical-hour DA forward sale change vs pre-IDA (MWh / firm-hour)")
    ax.set_title("q1 (DA forward sale) DiD by regime — testing the hedging hypothesis\n"
                 "(positive δ at DA60/ID15 = firms commit MORE in DA in critical hours; 95% CI shown)")
    plt.tight_layout()
    plt.savefig(OUT_DIR_F / "critical_hours_q1_did.png", dpi=110, bbox_inches="tight")
    plt.close()

    # ================================================================
    # PIECE C — Wild-cluster bootstrap on DA60/ID15-pre-blackout
    # ================================================================
    print()
    print("[C] Wild-cluster bootstrap on DA60/ID15-pre-blackout coefficient…", flush=True)

    # Re-run aggregate q2 DiD with blackout split
    q2 = con.execute(f"""
        SELECT CAST(date AS DATE) AS date, period,
               COALESCE(grupo_empresarial, 'NA') AS firm,
               SUM(assigned_power_mw * mtu_minutes / 60.0) AS q_mwh
        FROM '{PIBCIE}'
        WHERE assigned_power_mw IS NOT NULL
          AND grupo_empresarial IN ('IB','GE','GN','HC')
        GROUP BY 1, 2, 3
    """).df()
    q2["date"] = pd.to_datetime(q2["date"])
    is_post_ida = q2["date"] >= MTU15_IDA_DATE
    q2["hour"] = np.where(is_post_ida,
                          ((q2["period"] - 1) // 4).astype(int),
                          (q2["period"] - 1).astype(int)).clip(0, 23)
    q2h = q2.groupby(["date","firm","hour"], as_index=False)["q_mwh"].sum()
    q2h["regime"] = q2h["date"].apply(assign_regime6)
    q2h["critical"] = q2h["hour"].isin(CRITICAL_HOURS).astype(int)
    q2h["dow"] = q2h["date"].dt.dayofweek
    q2h["month"] = q2h["date"].dt.month
    q2h["year"] = q2h["date"].dt.year

    res_q2 = fit_did(q2h, REGIMES_6, group_for_dummies=["firm"], outcome="q_mwh")
    target_coef = res_q2["coefs"]["crit×R_DA60/ID15-pre-blackout"]
    target_se   = res_q2["ses"]["crit×R_DA60/ID15-pre-blackout"]
    print(f"   Original estimate: δ = {target_coef:+.2f} (SE {target_se:.2f})")

    # Wild-cluster bootstrap (Rademacher weights at cluster level)
    np.random.seed(42)
    n_boot = 1000
    X = res_q2["X"].values
    y = res_q2["y"]
    cluster = res_q2["cluster"]
    unique_clusters = np.unique(cluster)
    # Get residuals from main model
    yhat = X @ res_q2["coefs"].values
    u = y - yhat
    # Get index of the target coefficient
    target_idx = list(res_q2["coefs"].index).index("crit×R_DA60/ID15-pre-blackout")

    # Cluster-to-rows map
    cluster_to_rows = {c: np.where(cluster == c)[0] for c in unique_clusters}

    boot_coefs = np.zeros(n_boot)
    XtX_inv = np.linalg.inv(X.T @ X)
    print(f"   Running {n_boot} wild-cluster bootstrap replications…", flush=True)
    for b in range(n_boot):
        # Generate Rademacher weights, one per cluster
        weights = np.random.choice([-1, 1], size=len(unique_clusters))
        w_full = np.zeros(len(y))
        for i, c in enumerate(unique_clusters):
            w_full[cluster_to_rows[c]] = weights[i]
        # Construct bootstrap y* = yhat + w * u
        y_star = yhat + w_full * u
        # Refit
        beta_b = XtX_inv @ (X.T @ y_star)
        boot_coefs[b] = beta_b[target_idx]

    p_boot = np.mean(np.abs(boot_coefs - boot_coefs.mean()) >= np.abs(target_coef - boot_coefs.mean()))
    ci_low, ci_high = np.percentile(boot_coefs, [2.5, 97.5])
    print(f"   Bootstrap 95% CI: [{ci_low:+.2f}, {ci_high:+.2f}]")
    print(f"   Bootstrap p-value: {p_boot:.4f}")
    print(f"   Bootstrap mean: {boot_coefs.mean():+.2f}, std: {boot_coefs.std():.2f}")

    pd.DataFrame({
        "estimator": ["original_DiD","bootstrap_mean","bootstrap_2.5pct","bootstrap_97.5pct","bootstrap_pvalue"],
        "value": [target_coef, boot_coefs.mean(), ci_low, ci_high, p_boot],
    }).to_csv(OUT_DIR_R / "critical_hours_bootstrap_preblackout.csv", index=False)

    print()
    print(f"wrote {OUT_DIR_F / 'critical_hours_bid_differentiation.png'}")
    print(f"wrote {OUT_DIR_F / 'critical_hours_q1_did.png'}")
    print(f"wrote {OUT_DIR_R / 'critical_hours_bid_differentiation.csv'}")
    print(f"wrote {OUT_DIR_R / 'critical_hours_q1_did.csv'}")
    print(f"wrote {OUT_DIR_R / 'critical_hours_bootstrap_preblackout.csv'}")
    print("Done.")


if __name__ == "__main__":
    main()
