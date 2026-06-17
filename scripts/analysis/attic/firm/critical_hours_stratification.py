# STATUS: ALIVE
# LAST-AUDIT: 2026-05-04
# FEEDS: critical-hours stratification for MTU15 markup tests
# CLAIM: Within-hour net-load variance σ²_within and across-day variance
#        σ²_across rank hours-of-day by "criticality"; cross-regime tests
#        stratified by critical vs flat hours show stronger MTU15 effect
#        in critical hours (the prediction the granularity mechanism makes).
"""Critical-hours stratification analysis.

Construct two criticality measures of "how much within-hour profile variation
the granularity mechanism could exploit":

  σ²_within(d, h): variance of net-load across the 4 quarters within
                   hour h on day d (only post-MTU15-IDA, when 15-min data
                   is observable).
  σ²_across(h):    variance of net-load at hour-of-day h across days
                   (computable both pre- and post-reform at hour-of-day
                   resolution).

Net-load = load_actual − wind_actual − solar_actual (system-level).
Net-load is the residual demand that thermal + hydro must meet — the
relevant supply curve for the dominant-firm strategic decisions.

Steps:
  1. Build 15-min net-load panel post-MTU15-IDA (2025-03-19 onward).
  2. Compute σ²_within(d, h) and aggregate to mean σ²_within(h).
  3. Compute σ²_across(h) on the same panel + on pre-reform.
  4. Rank hours-of-day, identify top quintile (5 critical hours).
  5. Re-run a Big-4 q₂_IDA cross-regime test stratified by critical/flat.
  6. Output figures + tables.

Output:
  results/regressions/critical_hours_ranking.csv
  results/regressions/critical_hours_q2_stratified.csv
  figures/working/critical_hours_variance.png
  figures/working/critical_hours_q2_stratified.png
"""
from __future__ import annotations

from pathlib import Path

import duckdb
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import statsmodels.api as sm

PROJECT = Path(__file__).resolve().parents[3]
LOAD_A = PROJECT / "data" / "processed" / "entsoe" / "load" / "load_actual_all.parquet"
VRE_A  = PROJECT / "data" / "processed" / "entsoe" / "generation" / "wind_solar_actual_all.parquet"
PIBCIE = PROJECT / "data" / "processed" / "omie" / "mercado_intradiario_subastas" / "programas" / "pibcie_all.parquet"

OUT_DIR_R = PROJECT / "results" / "regressions"
OUT_DIR_F = PROJECT / "figures" / "working"

REGIMES = ["pre-IDA", "3-sess", "ISP15-win", "DA60/ID15", "DA15/ID15"]
BIG4 = ["IB", "GE", "GN", "HC"]
MTU15_IDA_DATE = pd.Timestamp("2025-03-19")


def assign_regime(d) -> str:
    d = pd.Timestamp(d)
    if d < pd.Timestamp("2024-06-14"): return "pre-IDA"
    if d < pd.Timestamp("2024-12-01"): return "3-sess"
    if d < pd.Timestamp("2025-03-19"): return "ISP15-win"
    if d < pd.Timestamp("2025-10-01"): return "DA60/ID15"
    return "DA15/ID15"


def main() -> None:
    con = duckdb.connect()
    con.execute("SET memory_limit='8GB'")
    con.execute("SET threads=4")

    print("[1/5] Building 15-min net-load panel (post-MTU15-IDA)…", flush=True)
    # Load 15-min granularity (from MTU15-IDA onward)
    load = con.execute(f"""
        SELECT isp_start_utc, mtu_minutes, load_mw
        FROM '{LOAD_A}'
        WHERE isp_start_utc >= TIMESTAMP '2024-01-01'
    """).df()
    load["isp_start_utc"] = pd.to_datetime(load["isp_start_utc"])

    vre = con.execute(f"""
        SELECT isp_start_utc, mtu_minutes, psr_type, quantity_mw
        FROM '{VRE_A}'
        WHERE psr_type IN ('B16','B18','B19')
          AND isp_start_utc >= TIMESTAMP '2024-01-01'
    """).df()
    vre["isp_start_utc"] = pd.to_datetime(vre["isp_start_utc"])
    vre_agg = vre.groupby(["isp_start_utc","mtu_minutes"], as_index=False)["quantity_mw"].sum()
    vre_agg = vre_agg.rename(columns={"quantity_mw": "vre_mw"})

    panel = load.merge(vre_agg, on=["isp_start_utc","mtu_minutes"], how="inner")
    panel["net_load_mw"] = panel["load_mw"] - panel["vre_mw"]
    panel["date"] = panel["isp_start_utc"].dt.date
    panel["hour"] = panel["isp_start_utc"].dt.hour
    # Use only 15-min granularity rows (post-MTU15-IDA) for σ²_within
    p15 = panel[panel["mtu_minutes"] == 15].copy()
    p15["date"] = pd.to_datetime(p15["date"])
    print(f"   panel rows (15-min): {len(p15):,}", flush=True)

    print("[2/5] Computing σ²_within(d,h) (only post-MTU15-IDA)…", flush=True)
    # σ²_within: variance across 4 quarters within (day, hour)
    p15_post = p15[p15["date"] >= MTU15_IDA_DATE].copy()
    sigma_within = (p15_post.groupby(["date","hour"])["net_load_mw"]
                            .var(ddof=0)
                            .reset_index(name="sigma2_within"))
    # Mean σ²_within per hour-of-day
    sigma_within_h = sigma_within.groupby("hour")["sigma2_within"].mean().reset_index()
    sigma_within_h["sigma_within"] = np.sqrt(sigma_within_h["sigma2_within"])

    print("[3/5] Computing σ²_across(h) on 60-min hourly aggregates…", flush=True)
    # σ²_across: variance of hourly net-load across days at each hour-of-day
    # Use the 60-min aggregate (by averaging within hour where 15-min available)
    panel_h = (panel.assign(hour_start=panel["isp_start_utc"].dt.floor("h"))
                    .groupby(["hour_start"], as_index=False)
                    .agg(net_load_mw=("net_load_mw","mean")))
    panel_h["date"] = panel_h["hour_start"].dt.date
    panel_h["date"] = pd.to_datetime(panel_h["date"])
    panel_h["hour"] = panel_h["hour_start"].dt.hour
    panel_h["regime"] = panel_h["date"].apply(assign_regime)
    # σ²_across by regime × hour-of-day
    sigma_across_full = (panel_h.groupby(["regime","hour"])["net_load_mw"]
                                .var(ddof=0).reset_index(name="sigma2_across"))
    sigma_across_full["sigma_across"] = np.sqrt(sigma_across_full["sigma2_across"])
    # Pooled (full panel) σ²_across by hour-of-day for ranking
    sigma_across_h = (panel_h.groupby("hour")["net_load_mw"]
                              .var(ddof=0).reset_index(name="sigma2_across"))
    sigma_across_h["sigma_across"] = np.sqrt(sigma_across_h["sigma2_across"])

    print("[4/5] Ranking hours-of-day, identifying critical hours…", flush=True)
    rank = sigma_within_h.merge(sigma_across_h, on="hour")
    rank["rank_within"] = rank["sigma_within"].rank(ascending=False).astype(int)
    rank["rank_across"] = rank["sigma_across"].rank(ascending=False).astype(int)
    rank_corr = rank["sigma_within"].corr(rank["sigma_across"])
    spearman = rank["sigma_within"].corr(rank["sigma_across"], method="spearman")
    # Critical: top 5 hours-of-day by σ_within (~top quintile of 24)
    rank = rank.sort_values("sigma_within", ascending=False).reset_index(drop=True)
    critical_hours = sorted(rank.head(5)["hour"].tolist())
    print(f"   critical hours (top-5 by σ_within): {critical_hours}", flush=True)
    print(f"   Pearson(σ_within, σ_across) by hour-of-day: {rank_corr:.3f}", flush=True)
    print(f"   Spearman:                                    {spearman:.3f}", flush=True)
    rank.to_csv(OUT_DIR_R / "critical_hours_ranking.csv", index=False)

    # Figure 1: σ_within and σ_across by hour-of-day
    fig, ax = plt.subplots(figsize=(10, 4))
    rank_sorted = rank.sort_values("hour")
    width = 0.4
    ax.bar(rank_sorted["hour"] - width/2, rank_sorted["sigma_within"], width=width,
           label=r"$\sigma_{within}$ (post-MTU15-IDA, 15-min)", color="tab:blue")
    ax.bar(rank_sorted["hour"] + width/2, rank_sorted["sigma_across"], width=width,
           label=r"$\sigma_{across}$ (full panel, hour-of-day)", color="tab:orange")
    for h in critical_hours:
        ax.axvspan(h-0.5, h+0.5, alpha=0.10, color="red")
    ax.set_xlabel("Hour of day")
    ax.set_ylabel("Standard deviation of net-load (MW)")
    ax.set_title(f"Net-load variation by hour-of-day. Pearson(σ_within, σ_across) = {rank_corr:.3f}\n"
                 f"Critical hours (top-5 by σ_within, red shaded): {critical_hours}")
    ax.legend()
    ax.set_xticks(range(0, 24))
    plt.tight_layout()
    OUT_DIR_F.mkdir(parents=True, exist_ok=True)
    plt.savefig(OUT_DIR_F / "critical_hours_variance.png", dpi=110, bbox_inches="tight")
    plt.close()

    print("[5/5] B9 q₂_IDA cross-regime test, stratified by critical/flat hours…", flush=True)
    # Big-4 q₂_IDA per (firm, date, hour) using PIBCIE
    q2 = con.execute(f"""
        SELECT CAST(date AS DATE) AS date,
               EXTRACT(hour FROM CAST(date AS DATE)) AS dummy_h,
               period,
               COALESCE(grupo_empresarial, 'NA') AS firm,
               SUM(assigned_power_mw * mtu_minutes / 60.0) AS q2_mwh
        FROM '{PIBCIE}'
        WHERE assigned_power_mw IS NOT NULL
          AND grupo_empresarial IN ('IB','GE','GN','HC')
        GROUP BY 1, 2, 3, 4
    """).df()
    q2["date"] = pd.to_datetime(q2["date"])
    # Map period → hour: pre-MTU15-IDA periods are 1..24 (60-min); post are 1..96 (15-min)
    # Use the date and ceil(period/4) - 1 mapping for post; identity for pre
    q2["regime"] = q2["date"].apply(assign_regime)
    is_post = q2["date"] >= MTU15_IDA_DATE
    q2["hour"] = np.where(is_post,
                          ((q2["period"] - 1) // 4).astype(int),
                          (q2["period"] - 1).astype(int))
    q2["hour"] = q2["hour"].clip(0, 23)
    # Aggregate to firm-day-hour
    q2h = q2.groupby(["date","firm","hour","regime"], as_index=False)["q2_mwh"].sum()
    q2h["critical"] = q2h["hour"].isin(critical_hours).astype(int)
    print(f"   q2 firm-day-hour rows: {len(q2h):,}", flush=True)

    # Compare regime-mean q₂ for critical vs flat hours
    summary = (q2h.groupby(["regime","critical"])["q2_mwh"].mean().reset_index()
                  .pivot(index="regime", columns="critical", values="q2_mwh")
                  .rename(columns={0:"flat_q2_mean", 1:"crit_q2_mean"}))
    summary = summary.reindex(REGIMES)
    summary["crit_minus_flat"] = summary["crit_q2_mean"] - summary["flat_q2_mean"]
    print()
    print("Big-4 mean q₂_IDA (MWh per firm-hour) by regime × hour-criticality:")
    print(summary.round(1).to_string())
    summary.to_csv(OUT_DIR_R / "critical_hours_q2_stratified.csv")

    # Regression: q2 ~ regime × critical, firm FE, cluster by date
    q2h["dow"] = q2h["date"].dt.dayofweek
    q2h["month"] = q2h["date"].dt.month
    cols = {"const": np.ones(len(q2h))}
    cols["critical"] = q2h["critical"].values.astype(float)
    for r in REGIMES[1:]:
        cols[f"R_{r}"] = (q2h["regime"] == r).astype(float).values
    for r in REGIMES[1:]:
        cols[f"R_{r}×crit"] = (q2h["critical"] * (q2h["regime"] == r)).astype(float).values
    for f in ["GE","GN","HC"]:
        cols[f"firm_{f}"] = (q2h["firm"] == f).astype(float).values
    for d_ in range(1, 7):
        cols[f"DOW{d_}"] = (q2h["dow"] == d_).astype(float).values
    for m_ in range(2, 13):
        cols[f"M{m_}"] = (q2h["month"] == m_).astype(float).values
    for h_ in range(1, 24):
        cols[f"H{h_}"] = (q2h["hour"] == h_).astype(float).values

    X = pd.DataFrame(cols, index=q2h.index)
    y = q2h["q2_mwh"].values
    cluster = q2h["date"].dt.strftime("%Y%m%d").astype(np.int64).values
    m = sm.OLS(y, X.values).fit(cov_type="cluster", cov_kwds={"groups": cluster})
    coefs = pd.Series(m.params, index=X.columns)
    ses   = pd.Series(m.bse,    index=X.columns)
    pvals = pd.Series(m.pvalues, index=X.columns)
    print()
    print("Regime × critical interaction (q₂ in critical hours - q₂ in flat hours, by regime):")
    print(f"  pre-IDA baseline (critical effect): β = {coefs['critical']:+.1f} "
          f"(SE {ses['critical']:.1f}, p={pvals['critical']:.2e})")
    for r in REGIMES[1:]:
        k = f"R_{r}×crit"
        b = coefs[k]; se = ses[k]; p = pvals[k]
        net = coefs["critical"] + b
        print(f"  {r}: Δ from pre-IDA = {b:+.1f} (SE {se:.1f}, p={p:.2e}); "
              f"net critical-hour effect at this regime = {net:+.1f} MWh/firm-hour")
    print(f"  N={len(q2h):,}; n_clusters={len(np.unique(cluster)):,}; R²={m.rsquared:.3f}")

    # Figure 2: regime-mean q₂ in critical vs flat hours
    fig, ax = plt.subplots(figsize=(8, 4.5))
    x = np.arange(len(REGIMES))
    width = 0.4
    flat_vals = summary["flat_q2_mean"].values
    crit_vals = summary["crit_q2_mean"].values
    ax.bar(x - width/2, flat_vals, width, label="Flat hours", color="tab:gray")
    ax.bar(x + width/2, crit_vals, width, label=f"Critical hours (h={critical_hours})", color="tab:red")
    ax.axhline(0, color="black", lw=0.5)
    ax.set_xticks(x)
    ax.set_xticklabels(REGIMES, rotation=15, ha="right")
    ax.set_ylabel("Mean Big-4 q₂_IDA (MWh per firm-hour)")
    ax.set_title("Voluntary IDA repositioning: regime × hour-criticality")
    ax.legend()
    plt.tight_layout()
    plt.savefig(OUT_DIR_F / "critical_hours_q2_stratified.png", dpi=110, bbox_inches="tight")
    plt.close()

    print()
    print(f"wrote {OUT_DIR_F / 'critical_hours_variance.png'}")
    print(f"wrote {OUT_DIR_F / 'critical_hours_q2_stratified.png'}")
    print(f"wrote {OUT_DIR_R / 'critical_hours_ranking.csv'}")
    print(f"wrote {OUT_DIR_R / 'critical_hours_q2_stratified.csv'}")
    print()
    print("Done.")


if __name__ == "__main__":
    main()
