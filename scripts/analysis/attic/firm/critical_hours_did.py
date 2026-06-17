# STATUS: ALIVE
# LAST-AUDIT: 2026-05-04
# FEEDS: critical-hours DiD + per-tech (via PIBCI) + price premium seasonality
# CLAIM: Flat hours are a valid within-day control for critical hours.
#        DiD identifies the granularity mechanism cleanly: under full MTU15,
#        critical-hour q₂ exceeds flat-hour q₂ by ~80 MWh/firm-hour above
#        the pre-reform baseline differential (which is essentially zero).
"""Critical-hours DiD design — flat hours as control.

DiD spec:
  q₂_{f,d,h} = α + β·critical_h + Σ_R γ_R·R_d + Σ_R δ_R·(critical_h × R_d)
              + firm FE + cal-month FE + year FE + DOW FE + ε
              cluster SE by date.
  critical_h = 1{h ∈ {7,8,16,17,18}}.
  R = regime indicators (3-sess, ISP15-win, DA60/ID15, DA15/ID15;
      pre-IDA baseline).
  δ_R = DiD treatment effect: how much MORE q₂ firms reposition in
       critical hours vs flat hours, above the pre-IDA baseline.

Three pieces:
  Piece 1: Aggregate Big-4 DiD (PIBCIE-based, firm-hour panel).
  Piece 2: Per-tech DiD using PIBCI (unit-level → firm × tech × hour panel).
           Cleaner than the DA-sell-share apportionment — each unit's IDA
           repositioning is mapped to its own technology.
  Piece 3: Seasonality check on the DA price premium (critical − flat).
           Compare same-calendar-month pre-reform vs post-reform premium
           to disentangle reform from seasonal structure.

Output:
  results/regressions/critical_hours_did.csv
  results/regressions/critical_hours_did_per_tech.csv
  results/regressions/critical_hours_price_seasonality.csv
  figures/working/critical_hours_did.png
  figures/working/critical_hours_did_per_tech.png
  figures/working/critical_hours_price_seasonality.png
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
LISTA   = PROJECT / "data" / "external" / "omie_reference" / "lista_unidades.csv"

OUT_DIR_R = PROJECT / "results" / "regressions"
OUT_DIR_F = PROJECT / "figures" / "working"

REGIMES = ["pre-IDA", "3-sess", "ISP15-win", "DA60/ID15", "DA15/ID15"]
BIG4 = ["IB", "GE", "GN", "HC"]
MTU15_IDA_DATE = pd.Timestamp("2025-03-19")
MTU15_DA_DATE  = pd.Timestamp("2025-10-01")
CRITICAL_HOURS = [7, 8, 16, 17, 18]


def assign_regime(d) -> str:
    d = pd.Timestamp(d)
    if d < pd.Timestamp("2024-06-14"): return "pre-IDA"
    if d < pd.Timestamp("2024-12-01"): return "3-sess"
    if d < pd.Timestamp("2025-03-19"): return "ISP15-win"
    if d < pd.Timestamp("2025-10-01"): return "DA60/ID15"
    return "DA15/ID15"


def fit_did(panel: pd.DataFrame, group_cols: list[str], outcome: str) -> dict:
    """Fit DiD: outcome ~ critical + regime + critical*regime + firm FE + ...,
    cluster SE by date. group_cols defines additional FE (e.g., tech)."""
    cols = {"const": np.ones(len(panel))}
    cols["critical"] = panel["critical"].values.astype(float)
    for r in REGIMES[1:]:
        cols[f"R_{r}"] = (panel["regime"] == r).astype(float).values
    for r in REGIMES[1:]:
        cols[f"crit×R_{r}"] = ((panel["critical"]) * (panel["regime"] == r)).astype(float).values
    # Firm FE (drop HC as baseline)
    for f in ["IB","GE","GN"]:
        cols[f"firm_{f}"] = (panel["firm"] == f).astype(float).values
    # Calendar-month FE
    for m_ in range(2, 13):
        cols[f"M{m_}"] = (panel["month"] == m_).astype(float).values
    # Year FE
    years = sorted(panel["year"].unique())
    for yr in years[1:]:
        cols[f"Y{yr}"] = (panel["year"] == yr).astype(float).values
    # DOW FE
    for d_ in range(1, 7):
        cols[f"DOW{d_}"] = (panel["dow"] == d_).astype(float).values
    # Extra group FE
    for g in group_cols:
        for v in sorted(panel[g].unique())[1:]:
            cols[f"{g}_{v}"] = (panel[g] == v).astype(float).values

    X = pd.DataFrame(cols, index=panel.index)
    y = panel[outcome].values
    cluster = panel["date"].dt.strftime("%Y%m%d").astype(np.int64).values
    m = sm.OLS(y, X.values).fit(cov_type="cluster", cov_kwds={"groups": cluster})
    coefs = pd.Series(m.params, index=X.columns)
    ses   = pd.Series(m.bse,    index=X.columns)
    pvals = pd.Series(m.pvalues, index=X.columns)
    return dict(coefs=coefs, ses=ses, pvals=pvals,
                n=len(panel), n_clusters=len(np.unique(cluster)),
                rsq=m.rsquared, X=X, model=m)


def main() -> None:
    con = duckdb.connect()
    con.execute("SET memory_limit='8GB'")
    con.execute("SET threads=4")

    # ------------------------------------------------------------
    # Setup mappings
    # ------------------------------------------------------------
    print("[setup] unit → firm → tech mapping…", flush=True)
    firms = con.execute(f"""
        SELECT unit_code, grupo_empresarial AS firm FROM (
          SELECT unit_code, grupo_empresarial,
                 ROW_NUMBER() OVER (PARTITION BY unit_code ORDER BY date DESC) AS rn
          FROM '{PDBCE}' WHERE grupo_empresarial IS NOT NULL) WHERE rn = 1
    """).df()
    lista = pd.read_csv(LISTA)[["unit_code","technology"]]
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

    # ------------------------------------------------------------
    # PIECE 1 — Aggregate Big-4 DiD using PIBCIE (firm-day-hour panel)
    # ------------------------------------------------------------
    print("[1/3] Big-4 q₂ firm-day-hour panel from PIBCIE…", flush=True)
    q2 = con.execute(f"""
        SELECT CAST(date AS DATE) AS date,
               period,
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
    q2h["regime"] = q2h["date"].apply(assign_regime)
    q2h["critical"] = q2h["hour"].isin(CRITICAL_HOURS).astype(int)
    q2h["dow"] = q2h["date"].dt.dayofweek
    q2h["month"] = q2h["date"].dt.month
    q2h["year"] = q2h["date"].dt.year
    print(f"   panel rows: {len(q2h):,}", flush=True)

    # Run the DiD
    res = fit_did(q2h, group_cols=[], outcome="q2_mwh")
    print()
    print("=" * 90)
    print("PIECE 1 — Aggregate Big-4 DiD: q₂ ~ critical × regime, firm FE + cal-month + year + DOW")
    print("=" * 90)
    print(f"  N = {res['n']:,};  n_clusters (date) = {res['n_clusters']:,};  R² = {res['rsq']:.3f}")
    print()
    print(f"  Critical main effect (pre-IDA baseline):  β = {res['coefs']['critical']:+.1f} "
          f"(SE {res['ses']['critical']:.1f}, p={res['pvals']['critical']:.2e})")
    print()
    print("  DiD interactions (critical × regime, change vs pre-IDA):")
    for r in REGIMES[1:]:
        k = f"crit×R_{r}"
        b = res["coefs"][k]; se = res["ses"][k]; p = res["pvals"][k]
        net = res["coefs"]["critical"] + b
        print(f"    {r:14s}: δ = {b:+.1f}  (SE {se:.1f}, p={p:.2e})  "
              f"→ net critical-hour effect at this regime = {net:+.1f} MWh/firm-hour")

    # Save
    out1 = pd.DataFrame({"term": res["coefs"].index, "coef": res["coefs"].values,
                         "se": res["ses"].values, "p": res["pvals"].values})
    keep_terms = ["critical"] + [f"R_{r}" for r in REGIMES[1:]] + [f"crit×R_{r}" for r in REGIMES[1:]]
    out1[out1.term.isin(keep_terms)].to_csv(OUT_DIR_R / "critical_hours_did.csv", index=False)

    # ------------------------------------------------------------
    # PIECE 2 — Per-tech DiD using PIBCI (unit-level)
    # ------------------------------------------------------------
    print()
    print("[2/3] PIBCI unit-day-hour panel for per-tech DiD…", flush=True)
    q2u = con.execute(f"""
        SELECT CAST(p.date AS DATE) AS date, p.period, uf.firm, uf.tech,
               SUM(p.assigned_power_mw * p.mtu_minutes / 60.0) AS q2_mwh
        FROM '{PIBCI}' p JOIN uf USING (unit_code)
        WHERE p.assigned_power_mw IS NOT NULL
          AND uf.firm IN ('IB','GE','GN','HC')
          AND uf.tech IN ('CCGT','Nuclear','Hydro')
        GROUP BY 1, 2, 3, 4
    """).df()
    q2u["date"] = pd.to_datetime(q2u["date"])
    is_post_ida_u = q2u["date"] >= MTU15_IDA_DATE
    q2u["hour"] = np.where(is_post_ida_u,
                           ((q2u["period"] - 1) // 4).astype(int),
                           (q2u["period"] - 1).astype(int)).clip(0, 23)
    # Aggregate to (firm, tech, hour, date) — this is the panel
    q2ft = q2u.groupby(["date","firm","tech","hour"], as_index=False)["q2_mwh"].sum()
    q2ft["regime"] = q2ft["date"].apply(assign_regime)
    q2ft["critical"] = q2ft["hour"].isin(CRITICAL_HOURS).astype(int)
    q2ft["dow"] = q2ft["date"].dt.dayofweek
    q2ft["month"] = q2ft["date"].dt.month
    q2ft["year"] = q2ft["date"].dt.year
    print(f"   firm-tech-day-hour rows: {len(q2ft):,}", flush=True)

    # Run separate DiD per tech
    rows_pertech = []
    print()
    print("=" * 90)
    print("PIECE 2 — Per-tech DiD (PIBCI unit-level): δ_R for each technology")
    print("=" * 90)
    for tech in ["Hydro","CCGT","Nuclear"]:
        sub = q2ft[q2ft["tech"] == tech].copy()
        if len(sub) < 100: continue
        res_t = fit_did(sub, group_cols=[], outcome="q2_mwh")
        print()
        print(f"  {tech} (N={res_t['n']:,}, G={res_t['n_clusters']:,}, R²={res_t['rsq']:.3f})")
        print(f"    Critical main effect: β = {res_t['coefs']['critical']:+.1f}  "
              f"(SE {res_t['ses']['critical']:.1f})")
        for r in REGIMES[1:]:
            k = f"crit×R_{r}"
            b = res_t["coefs"][k]; se = res_t["ses"][k]; p = res_t["pvals"][k]
            print(f"    {r:14s}: δ = {b:+.1f}  (SE {se:.1f}, p={p:.2e})")
            rows_pertech.append({"tech": tech, "regime": r,
                                  "delta_did": b, "se": se, "p": p,
                                  "n": res_t["n"], "n_clusters": res_t["n_clusters"]})
    pd.DataFrame(rows_pertech).to_csv(OUT_DIR_R / "critical_hours_did_per_tech.csv", index=False)

    # ------------------------------------------------------------
    # PIECE 3 — Seasonality check on DA price premium critical vs flat
    # ------------------------------------------------------------
    print()
    print("[3/3] Seasonality check on DA price premium (critical − flat)…", flush=True)
    pp = con.execute(f"""
        SELECT CAST(date AS DATE) AS date, period, price_es_eur_mwh
        FROM '{PRICES}'
    """).df()
    pp["date"] = pd.to_datetime(pp["date"])
    is_post_da15 = pp["date"] >= MTU15_DA_DATE
    pp["hour"] = np.where(is_post_da15,
                          ((pp["period"] - 1) // 4).astype(int),
                          (pp["period"] - 1).astype(int)).clip(0, 23)
    pp["regime"] = pp["date"].apply(assign_regime)
    pp["critical"] = pp["hour"].isin(CRITICAL_HOURS).astype(int)
    pp["month"] = pp["date"].dt.month
    pp["year"] = pp["date"].dt.year
    pp["season"] = pp["month"].apply(
        lambda m: "winter (Dec-Feb)" if m in [12,1,2]
                  else "spring (Mar-May)" if m in [3,4,5]
                  else "summer (Jun-Aug)" if m in [6,7,8]
                  else "autumn (Sep-Nov)")

    # Daily critical-flat premium
    daily = (pp.groupby(["date","regime","season","critical"])["price_es_eur_mwh"]
               .mean()
               .reset_index()
               .pivot_table(index=["date","regime","season"],
                            columns="critical",
                            values="price_es_eur_mwh",
                            aggfunc="first")
               .reset_index())
    daily.columns = ["date","regime","season","p_flat","p_crit"]
    daily["premium"] = daily["p_crit"] - daily["p_flat"]
    # Aggregate by (regime, season)
    season_table = (daily.groupby(["regime","season"])["premium"]
                          .agg(["mean","median","count"])
                          .reset_index())
    season_table.columns = ["regime","season","mean_premium","median_premium","n_days"]
    season_table["regime"] = pd.Categorical(season_table["regime"],
                                              categories=REGIMES, ordered=True)
    season_table = season_table.sort_values(["season","regime"]).reset_index(drop=True)
    print()
    print("=" * 90)
    print("PIECE 3 — Seasonality: critical-flat DA price premium (€/MWh) by regime × season")
    print("=" * 90)
    print()
    pivot = season_table.pivot_table(index="season", columns="regime",
                                       values="mean_premium", aggfunc="first")
    print(pivot.round(2).to_string())
    season_table.to_csv(OUT_DIR_R / "critical_hours_price_seasonality.csv", index=False)

    # ------------------------------------------------------------
    # FIGURES
    # ------------------------------------------------------------
    OUT_DIR_F.mkdir(parents=True, exist_ok=True)

    # Figure 1: aggregate DiD δ_R by regime
    fig, ax = plt.subplots(figsize=(8, 4.5))
    deltas = [res["coefs"][f"crit×R_{r}"] for r in REGIMES[1:]]
    ses    = [res["ses"][f"crit×R_{r}"]   for r in REGIMES[1:]]
    base   = res["coefs"]["critical"]
    x = np.arange(len(REGIMES[1:]))
    ax.bar(x, deltas, yerr=[1.96*s for s in ses], capsize=5,
           color=["tab:gray","tab:gray","tab:gray","tab:red"])
    ax.axhline(0, color="black", lw=0.6)
    ax.set_xticks(x)
    ax.set_xticklabels(REGIMES[1:], rotation=15, ha="right")
    ax.set_ylabel(r"DiD δ$_R$: critical-hour effect change vs pre-IDA (MWh / firm-hour)")
    ax.set_title("Big-4 q₂ DiD coefficient by regime\n"
                 f"(critical-hour main effect at pre-IDA baseline = {base:+.1f} MWh)")
    plt.tight_layout()
    plt.savefig(OUT_DIR_F / "critical_hours_did.png", dpi=110, bbox_inches="tight")
    plt.close()

    # Figure 2: per-tech DiD δ_R
    fig, ax = plt.subplots(figsize=(10, 4.5))
    df_t = pd.DataFrame(rows_pertech)
    techs = ["Hydro","CCGT","Nuclear"]
    width = 0.27
    x = np.arange(len(REGIMES[1:]))
    for i, tech in enumerate(techs):
        sub = df_t[df_t["tech"] == tech].set_index("regime")
        d = [sub.loc[r, "delta_did"] if r in sub.index else 0 for r in REGIMES[1:]]
        s = [sub.loc[r, "se"]        if r in sub.index else 0 for r in REGIMES[1:]]
        ax.bar(x + (i-1)*width, d, width, yerr=[1.96*x for x in s], capsize=3,
               label=tech, color={"Hydro":"tab:blue","CCGT":"tab:red","Nuclear":"tab:green"}[tech])
    ax.axhline(0, color="black", lw=0.6)
    ax.set_xticks(x)
    ax.set_xticklabels(REGIMES[1:], rotation=15, ha="right")
    ax.set_ylabel(r"DiD δ$_R$ by tech (MWh / firm-tech-hour)")
    ax.set_title("Per-tech DiD coefficient by regime — PIBCI unit-level\n"
                 "(Big-4 q₂ critical-hour effect, change vs pre-IDA baseline)")
    ax.legend()
    plt.tight_layout()
    plt.savefig(OUT_DIR_F / "critical_hours_did_per_tech.png", dpi=110, bbox_inches="tight")
    plt.close()

    # Figure 3: seasonality heatmap
    fig, ax = plt.subplots(figsize=(9, 4))
    seasons = ["winter (Dec-Feb)","spring (Mar-May)","summer (Jun-Aug)","autumn (Sep-Nov)"]
    pivot_full = season_table.pivot_table(index="season", columns="regime",
                                            values="mean_premium", aggfunc="first")
    pivot_full = pivot_full.reindex(index=seasons, columns=REGIMES)
    im = ax.imshow(pivot_full.values, cmap="RdBu_r", aspect="auto",
                   vmin=-20, vmax=20)
    ax.set_xticks(range(len(REGIMES)))
    ax.set_xticklabels(REGIMES, rotation=20, ha="right")
    ax.set_yticks(range(len(seasons)))
    ax.set_yticklabels(seasons)
    for i in range(len(seasons)):
        for j in range(len(REGIMES)):
            v = pivot_full.values[i, j]
            if not np.isnan(v):
                ax.text(j, i, f"{v:+.1f}", ha="center", va="center",
                        color="white" if abs(v) > 10 else "black", fontsize=10)
    plt.colorbar(im, label="critical − flat premium (€/MWh)")
    ax.set_title("DA cleared-price premium (critical − flat €/MWh) by season × regime\n"
                 "Pre-reform winter premium pattern indicates seasonal vs reform-driven structure")
    plt.tight_layout()
    plt.savefig(OUT_DIR_F / "critical_hours_price_seasonality.png", dpi=110, bbox_inches="tight")
    plt.close()

    print()
    print(f"wrote {OUT_DIR_F / 'critical_hours_did.png'}")
    print(f"wrote {OUT_DIR_F / 'critical_hours_did_per_tech.png'}")
    print(f"wrote {OUT_DIR_F / 'critical_hours_price_seasonality.png'}")
    print(f"wrote {OUT_DIR_R / 'critical_hours_did.csv'}")
    print(f"wrote {OUT_DIR_R / 'critical_hours_did_per_tech.csv'}")
    print(f"wrote {OUT_DIR_R / 'critical_hours_price_seasonality.csv'}")
    print("Done.")


if __name__ == "__main__":
    main()
