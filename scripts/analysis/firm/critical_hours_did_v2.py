# STATUS: ALIVE
# LAST-AUDIT: 2026-05-04
# FEEDS: Critical-hours DiD v2 — fix Nuclear NaN + DA60/ID15 pre/post blackout
#        + load-only criticality robustness
# CLAIM: The DA60/ID15 negative critical-hour effect appears only in the
#        POST-blackout sub-window. Reforzada limits dominant-firm market
#        power in critical hours by forcing supply via PO-3.2 RRTT.
"""Critical-hours DiD v2.

Three corrections to v1 (critical_hours_did.py):

1. Per-tech FE: use UNIT FE (within-demeaning) instead of FIRM FE.
   Avoids the Nuclear NaN-SE issue (1 unit each for GN/GE) and is
   the standard panel-FE approach.

2. DA60/ID15 split: separate pre-blackout (2025-03-19 to 2025-04-27,
   clean MTU15-IDA, no reforzada) and post-blackout (2025-04-28
   onwards, reforzada). Tests the "stability vs market power tradeoff"
   hypothesis: forced CCGT/nuclear supply via PO-3.2 RRTT depresses
   dominant-firm market power in critical hours by suppressing
   pivotality.

3. Load-only criticality: define critical hours by within-hour σ²
   of LOAD (not net-load), to check robustness. Net-load mixes demand
   and renewable variation; load isolates the demand-side ramp.

Output:
  results/regressions/critical_hours_did_v2.csv
  results/regressions/critical_hours_did_v2_per_tech.csv
  figures/working/critical_hours_did_v2.png
  figures/working/critical_hours_did_v2_per_tech.png
  figures/working/critical_hours_load_only_ranking.png
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
PIBCI   = PROJECT / "data" / "processed" / "omie" / "mercado_intradiario_subastas" / "programas" / "pibci_all.parquet"
PDBCE   = PROJECT / "data" / "processed" / "omie" / "mercado_diario" / "programas" / "pdbce_all.parquet"
PRICES  = PROJECT / "data" / "processed" / "omie" / "mercado_diario" / "precios" / "marginalpdbc_all.parquet"
LOAD_A  = PROJECT / "data" / "processed" / "entsoe" / "load" / "load_actual_all.parquet"
LISTA   = PROJECT / "data" / "external" / "omie_reference" / "lista_unidades.csv"

OUT_DIR_R = PROJECT / "results" / "regressions"
OUT_DIR_F = PROJECT / "figures" / "working"

REGIMES_5 = ["pre-IDA", "3-sess", "ISP15-win", "DA60/ID15", "DA15/ID15"]
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
            outcome="q2_mwh"):
    """DiD: outcome ~ critical + regime + critical*regime + cal-month + year + DOW.
    If demean_col, demean outcome by that column (within-FE absorption)."""
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
                rsq=m.rsquared)


def main() -> None:
    con = duckdb.connect()
    con.execute("SET memory_limit='8GB'")
    con.execute("SET threads=4")

    # Mappings
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

    # ----------------------------------------------------------------
    # PIECE 1 — Aggregate DiD with DA60/ID15 pre/post-blackout split
    # ----------------------------------------------------------------
    print("[1/3] Big-4 q₂ aggregate DiD with blackout split…", flush=True)
    q2 = con.execute(f"""
        SELECT CAST(date AS DATE) AS date, period,
               COALESCE(grupo_empresarial, 'NA') AS firm,
               SUM(assigned_power_mw * mtu_minutes / 60.0) AS q2_mwh
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
    q2h = q2.groupby(["date","firm","hour"], as_index=False)["q2_mwh"].sum()
    q2h["regime"] = q2h["date"].apply(assign_regime6)
    q2h["critical"] = q2h["hour"].isin(CRITICAL_HOURS).astype(int)
    q2h["dow"] = q2h["date"].dt.dayofweek
    q2h["month"] = q2h["date"].dt.month
    q2h["year"] = q2h["date"].dt.year
    print(f"   panel rows: {len(q2h):,}", flush=True)
    print(f"   regime sizes:")
    for r in REGIMES_6:
        n = (q2h["regime"] == r).sum()
        print(f"     {r:30s}: {n:>8,} firm-hour obs")

    res = fit_did(q2h, REGIMES_6, group_for_dummies=["firm"])
    print()
    print("=" * 100)
    print("PIECE 1 — Big-4 DiD with DA60/ID15 split into pre-blackout and reforzada")
    print("=" * 100)
    print(f"  N = {res['n']:,};  G (date clusters) = {res['n_clusters']:,};  R² = {res['rsq']:.3f}")
    print()
    print(f"  Critical main effect (pre-IDA baseline): β = {res['coefs']['critical']:+.1f} "
          f"(SE {res['ses']['critical']:.1f}, p={res['pvals']['critical']:.2e})")
    print()
    print("  DiD interactions (critical × regime, change vs pre-IDA):")
    for r in REGIMES_6[1:]:
        k = f"crit×R_{r}"
        b = res["coefs"][k]; se = res["ses"][k]; p = res["pvals"][k]
        net = res["coefs"]["critical"] + b
        print(f"    {r:30s}: δ = {b:+8.2f}  (SE {se:5.2f}, p={p:.2e})  "
              f"→ net = {net:+.1f} MWh/firm-hour")

    out1 = pd.DataFrame({"term": res["coefs"].index, "coef": res["coefs"].values,
                         "se": res["ses"].values, "p": res["pvals"].values})
    keep_terms = ["critical"] + [f"R_{r}" for r in REGIMES_6[1:]] + [f"crit×R_{r}" for r in REGIMES_6[1:]]
    out1[out1.term.isin(keep_terms)].to_csv(OUT_DIR_R / "critical_hours_did_v2.csv", index=False)

    # Figure 1: aggregate DiD δ with blackout split
    fig, ax = plt.subplots(figsize=(10, 5))
    deltas = [res["coefs"][f"crit×R_{r}"] for r in REGIMES_6[1:]]
    ses    = [res["ses"][f"crit×R_{r}"]   for r in REGIMES_6[1:]]
    base   = res["coefs"]["critical"]
    x = np.arange(len(REGIMES_6[1:]))
    colors = ["tab:gray","tab:gray","tab:blue","tab:orange","tab:red"]
    bar = ax.bar(x, deltas, yerr=[1.96*s for s in ses], capsize=5, color=colors)
    ax.axhline(0, color="black", lw=0.6)
    labels = ["3-sess","ISP15-win","DA60/ID15\npre-blackout\n(no reforzada)",
              "DA60/ID15\nreforzada","DA15/ID15"]
    ax.set_xticks(x)
    ax.set_xticklabels(labels, fontsize=9)
    ax.set_ylabel(r"DiD δ$_R$: critical-hour effect change vs pre-IDA (MWh / firm-hour)")
    ax.set_title("Big-4 q₂ DiD coefficient by regime — DA60/ID15 split into pre-blackout vs reforzada\n"
                 f"(critical main effect at pre-IDA baseline = {base:+.1f} MWh/firm-hour, 95% CI shown)")
    plt.tight_layout()
    plt.savefig(OUT_DIR_F / "critical_hours_did_v2.png", dpi=110, bbox_inches="tight")
    plt.close()

    # ----------------------------------------------------------------
    # PIECE 2 — Per-tech DiD with UNIT FE (within-demeaning)
    # ----------------------------------------------------------------
    print()
    print("[2/3] Per-tech DiD using PIBCI unit-level + UNIT FE…", flush=True)
    q2u = con.execute(f"""
        SELECT CAST(p.date AS DATE) AS date, p.period, p.unit_code, uf.firm, uf.tech,
               SUM(p.assigned_power_mw * p.mtu_minutes / 60.0) AS q2_mwh
        FROM '{PIBCI}' p JOIN uf USING (unit_code)
        WHERE p.assigned_power_mw IS NOT NULL
          AND uf.firm IN ('IB','GE','GN','HC')
          AND uf.tech IN ('CCGT','Nuclear','Hydro')
        GROUP BY 1, 2, 3, 4, 5
    """).df()
    q2u["date"] = pd.to_datetime(q2u["date"])
    is_post_ida_u = q2u["date"] >= MTU15_IDA_DATE
    q2u["hour"] = np.where(is_post_ida_u,
                           ((q2u["period"] - 1) // 4).astype(int),
                           (q2u["period"] - 1).astype(int)).clip(0, 23)
    q2ut = q2u.groupby(["date","unit_code","firm","tech","hour"],
                       as_index=False)["q2_mwh"].sum()
    q2ut["regime"] = q2ut["date"].apply(assign_regime6)
    q2ut["critical"] = q2ut["hour"].isin(CRITICAL_HOURS).astype(int)
    q2ut["dow"] = q2ut["date"].dt.dayofweek
    q2ut["month"] = q2ut["date"].dt.month
    q2ut["year"] = q2ut["date"].dt.year
    print(f"   unit-day-hour rows: {len(q2ut):,}", flush=True)

    rows_pertech = []
    print()
    print("=" * 100)
    print("PIECE 2 — Per-tech DiD with UNIT FE (within-demeaning) and blackout split")
    print("=" * 100)
    for tech in ["Hydro","CCGT","Nuclear"]:
        sub = q2ut[q2ut["tech"] == tech].copy()
        n_units = sub["unit_code"].nunique()
        if len(sub) < 100: continue
        # Use unit FE (within-demeaning)
        res_t = fit_did(sub, REGIMES_6, demean_col="unit_code")
        print()
        print(f"  {tech} (N={res_t['n']:,}, n_units={n_units}, G={res_t['n_clusters']:,}, "
              f"R²={res_t['rsq']:.3f})")
        for r in REGIMES_6[1:]:
            k = f"crit×R_{r}"
            b = res_t["coefs"][k]; se = res_t["ses"][k]; p = res_t["pvals"][k]
            print(f"    {r:30s}: δ = {b:+8.2f}  (SE {se:5.2f}, p={p:.2e})")
            rows_pertech.append({"tech": tech, "regime": r,
                                  "delta_did": b, "se": se, "p": p,
                                  "n_units": n_units,
                                  "n": res_t["n"], "n_clusters": res_t["n_clusters"]})
    pd.DataFrame(rows_pertech).to_csv(OUT_DIR_R / "critical_hours_did_v2_per_tech.csv", index=False)

    # Figure 2: per-tech DiD with blackout split
    fig, ax = plt.subplots(figsize=(13, 5))
    df_t = pd.DataFrame(rows_pertech)
    techs = ["Hydro","CCGT","Nuclear"]
    width = 0.27
    x = np.arange(len(REGIMES_6[1:]))
    colors = {"Hydro":"tab:blue","CCGT":"tab:red","Nuclear":"tab:green"}
    for i, tech in enumerate(techs):
        sub = df_t[df_t["tech"] == tech].set_index("regime")
        d = [sub.loc[r, "delta_did"] if r in sub.index else 0 for r in REGIMES_6[1:]]
        s = [sub.loc[r, "se"]        if r in sub.index else 0 for r in REGIMES_6[1:]]
        ax.bar(x + (i-1)*width, d, width, yerr=[1.96*v for v in s], capsize=3,
               label=tech, color=colors[tech])
    ax.axhline(0, color="black", lw=0.6)
    labels = ["3-sess","ISP15-win","DA60/ID15\npre-blackout","DA60/ID15\nreforzada","DA15/ID15"]
    ax.set_xticks(x)
    ax.set_xticklabels(labels, fontsize=9)
    ax.set_ylabel(r"DiD δ$_R$ by tech (MWh / unit-hour, within-unit demeaned)")
    ax.set_title("Per-tech DiD coefficient by regime — PIBCI with UNIT FE + blackout split\n"
                 "(Big-4 q₂ critical-hour effect, change vs pre-IDA baseline; 95% CI shown)")
    ax.legend()
    plt.tight_layout()
    plt.savefig(OUT_DIR_F / "critical_hours_did_v2_per_tech.png", dpi=110, bbox_inches="tight")
    plt.close()

    # ----------------------------------------------------------------
    # PIECE 3 — Load-only criticality robustness
    # ----------------------------------------------------------------
    print()
    print("[3/3] Load-only criticality robustness…", flush=True)
    load = con.execute(f"""
        SELECT CAST(isp_start_utc AS DATE) AS date,
               EXTRACT(hour FROM isp_start_utc) AS hour,
               mtu_minutes, load_mw
        FROM '{LOAD_A}'
        WHERE isp_start_utc >= TIMESTAMP '2024-01-01'
          AND mtu_minutes = 15
    """).df()
    load["date"] = pd.to_datetime(load["date"])
    load_post = load[load["date"] >= MTU15_IDA_DATE].copy()

    # σ²_within for LOAD-ONLY
    sigma_within_load = (load_post.groupby(["date","hour"])["load_mw"]
                                   .var(ddof=0)
                                   .reset_index(name="sigma2_within"))
    sigma_within_load_h = sigma_within_load.groupby("hour")["sigma2_within"].mean().reset_index()
    sigma_within_load_h["sigma_within"] = np.sqrt(sigma_within_load_h["sigma2_within"])
    sigma_within_load_h = sigma_within_load_h.sort_values("hour")

    # Compare to net-load ranking (already in results)
    nl_rank = pd.read_csv(OUT_DIR_R / "critical_hours_ranking.csv")[["hour","sigma_within"]]
    nl_rank.columns = ["hour","sigma_within_netload"]
    cmp = sigma_within_load_h[["hour","sigma_within"]].rename(columns={"sigma_within": "sigma_within_load"}).merge(
              nl_rank, on="hour")
    cmp_corr = cmp["sigma_within_load"].corr(cmp["sigma_within_netload"])
    cmp["rank_load"] = cmp["sigma_within_load"].rank(ascending=False).astype(int)
    cmp["rank_netload"] = cmp["sigma_within_netload"].rank(ascending=False).astype(int)
    print()
    print(f"  Pearson(σ_within load, σ_within net-load) = {cmp_corr:.3f}")
    print()
    print("  Top 5 critical hours by each criterion:")
    print("    LOAD-only:    ", cmp.sort_values("sigma_within_load", ascending=False).head(5)["hour"].tolist())
    print("    NET-LOAD:     ", cmp.sort_values("sigma_within_netload", ascending=False).head(5)["hour"].tolist())

    # Plot comparison
    fig, ax = plt.subplots(figsize=(11, 4))
    width = 0.4
    x = cmp["hour"].values
    ax.bar(x - width/2, cmp["sigma_within_load"]/1000, width, label="σ_within (load only)", color="tab:purple")
    ax.bar(x + width/2, cmp["sigma_within_netload"]/1000, width, label="σ_within (net-load)", color="tab:blue")
    for h in CRITICAL_HOURS:
        ax.axvspan(h-0.5, h+0.5, alpha=0.10, color="red")
    ax.set_xticks(range(0, 24))
    ax.set_xlabel("Hour of day")
    ax.set_ylabel("σ_within (GW)")
    ax.set_title(f"Within-hour σ: load only vs net-load. Pearson = {cmp_corr:.3f}\n"
                 f"Net-load critical hours (red): {CRITICAL_HOURS}")
    ax.legend()
    plt.tight_layout()
    plt.savefig(OUT_DIR_F / "critical_hours_load_only_ranking.png", dpi=110, bbox_inches="tight")
    plt.close()

    print()
    print(f"wrote {OUT_DIR_F / 'critical_hours_did_v2.png'}")
    print(f"wrote {OUT_DIR_F / 'critical_hours_did_v2_per_tech.png'}")
    print(f"wrote {OUT_DIR_F / 'critical_hours_load_only_ranking.png'}")
    print(f"wrote {OUT_DIR_R / 'critical_hours_did_v2.csv'}")
    print(f"wrote {OUT_DIR_R / 'critical_hours_did_v2_per_tech.csv'}")
    print("Done.")


if __name__ == "__main__":
    main()
