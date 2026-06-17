# STATUS: ALIVE
# LAST-AUDIT: 2026-05-04
# FEEDS: Critical-hours stratification — per-firm-tech contrasts +
#        cross-regime DA price premium
# CLAIM: Hydro shows the strongest critical-hour q₂ activation under
#        MTU15 (consistent with the bid-flex × dispatch-flex prediction);
#        the DA cleared-price premium between critical and flat hours
#        widens at full MTU15.
"""Critical-hours stratification — extended.

Builds on critical_hours_stratification.py (which identified critical
hours = 7, 8, 16, 17, 18 from net-load σ²_within) and pushes the test in
two directions:

  Test A — Per-firm × tech q₂ stratification.
    For each (Big-4 firm, technology, regime), compute mean q₂_IDA in
    critical vs flat hours. Predicts hydro > CCGT > nuclear in critical-
    flat differential under MTU15 (cross-tech flexibility heterogeneity).

  Test B — Critical-hour DA price premium by regime.
    For each day, compute mean DA cleared price in critical hours and
    in flat hours. Premium = (p_crit − p_flat) / p_flat. Compare across
    regimes. Under the granularity mechanism, the premium should widen
    once firms can target their pricing within hours (MTU15-IDA onwards),
    and most strongly at full MTU15-DA.

Output:
  results/regressions/critical_hours_q2_by_firm_tech.csv
  results/regressions/critical_hours_da_price_premium.csv
  figures/working/critical_hours_q2_by_firm_tech.png
  figures/working/critical_hours_da_price_premium.png
"""
from __future__ import annotations

from pathlib import Path

import duckdb
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

PROJECT = Path(__file__).resolve().parents[3]
PIBCIE  = PROJECT / "data" / "processed" / "omie" / "mercado_intradiario_subastas" / "programas" / "pibcie_all.parquet"
PDBCE   = PROJECT / "data" / "processed" / "omie" / "mercado_diario" / "programas" / "pdbce_all.parquet"
PRICES  = PROJECT / "data" / "processed" / "omie" / "mercado_diario" / "precios" / "marginalpdbc_all.parquet"
LISTA   = PROJECT / "data" / "external" / "omie_reference" / "lista_unidades.csv"

OUT_DIR_R = PROJECT / "results" / "regressions"
OUT_DIR_F = PROJECT / "figures" / "working"

REGIMES = ["pre-IDA", "3-sess", "ISP15-win", "DA60/ID15", "DA15/ID15"]
BIG4 = ["IB", "GE", "GN", "HC"]
MTU15_IDA_DATE = pd.Timestamp("2025-03-19")  # intraday/PIBCIE goes 15-min
MTU15_DA_DATE  = pd.Timestamp("2025-10-01")  # day-ahead/marginalpdbc goes 15-min
CRITICAL_HOURS = [7, 8, 16, 17, 18]  # from critical_hours_stratification.py


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

    # ----------------------------------------------------------------------
    # Setup: unit → firm → tech mapping
    # ----------------------------------------------------------------------
    print("[setup] unit → firm → tech mapping…", flush=True)
    firms = con.execute(f"""
        SELECT unit_code, grupo_empresarial AS firm FROM (
          SELECT unit_code, grupo_empresarial,
                 ROW_NUMBER() OVER (PARTITION BY unit_code ORDER BY date DESC) AS rn
          FROM '{PDBCE}' WHERE grupo_empresarial IS NOT NULL) WHERE rn = 1
    """).df()
    lista = pd.read_csv(LISTA)[["unit_code", "technology"]]
    map_uf = firms.merge(lista, on="unit_code", how="left")

    def tech_group(t):
        if not isinstance(t, str): return "Other"
        tl = t.lower()
        if "gas" in tl or "ciclo" in tl: return "CCGT"
        if "nuclear" in tl: return "Nuclear"
        if "ombeo" in tl or "idráulica" in tl: return "Hydro"
        return "Other"

    map_uf["tech_group"] = map_uf["technology"].apply(tech_group)

    # PIBCIE assigns at the firm level, not unit level. We need firm-level tech
    # disaggregation — which we cannot do directly from PIBCIE. Instead, use
    # PDBC sell volumes by (firm × tech × period) to get tech composition,
    # then weight q₂ by tech share. Cleaner: derive technology from offer codes
    # in PIBCIE if they include unit-codes; otherwise approximate via PDBC.
    #
    # For this analysis: use PIBCIE at firm level (assigned_power_mw is signed
    # net IDA repositioning by firm). The TECH split must come from elsewhere.
    # Approach: compute q₂ per Big-4 firm-day-hour, then ALSO compute, per
    # firm-day, the share of DA cleared sell volume by technology (from PDBCE).
    # Apportion the firm-day q₂ to technologies using DA sell-share weights.
    #
    # This is an approximation but defensible: it assumes IDA repositioning
    # follows the DA sell-side technology mix, which is the most reasonable
    # default in the absence of finer data.

    # ----------------------------------------------------------------------
    # TEST A — per-firm-tech q₂ stratification (using DA-sell weighting)
    # ----------------------------------------------------------------------
    print("[A1] firm-day DA sell-side share by tech (Big-4)…", flush=True)
    con.register("uf", map_uf[["unit_code","firm","tech_group"]])
    da_sell = con.execute(f"""
        SELECT CAST(p.date AS DATE) AS date, uf.firm, uf.tech_group,
               SUM(CASE WHEN p.offer_type = 1 AND p.assigned_power_mw > 0
                        THEN p.assigned_power_mw * p.mtu_minutes / 60.0
                        ELSE 0 END) AS sell_mwh
        FROM '{PDBCE}' p JOIN uf USING (unit_code)
        WHERE uf.firm IN ('IB','GE','GN','HC')
          AND uf.tech_group IN ('CCGT','Nuclear','Hydro')
        GROUP BY 1, 2, 3
    """).df()
    da_sell["date"] = pd.to_datetime(da_sell["date"])
    # Compute share within firm-day
    da_sell["firm_day_total"] = da_sell.groupby(["date","firm"])["sell_mwh"].transform("sum")
    da_sell["tech_share"] = np.where(da_sell["firm_day_total"] > 0,
                                     da_sell["sell_mwh"] / da_sell["firm_day_total"], 0)
    print(f"   firm-day-tech rows: {len(da_sell):,}", flush=True)

    print("[A2] firm-day-hour q₂_IDA from PIBCIE (Big-4)…", flush=True)
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
    is_post = q2["date"] >= MTU15_IDA_DATE
    q2["hour"] = np.where(is_post,
                          ((q2["period"] - 1) // 4).astype(int),
                          (q2["period"] - 1).astype(int))
    q2["hour"] = q2["hour"].clip(0, 23)
    q2["regime"] = q2["date"].apply(assign_regime)
    q2["critical"] = q2["hour"].isin(CRITICAL_HOURS).astype(int)
    q2h = q2.groupby(["date","firm","hour","regime","critical"], as_index=False)["q2_mwh"].sum()

    # Apportion q₂ by DA-sell tech share (firm-day join)
    q2h_tech = q2h.merge(da_sell[["date","firm","tech_group","tech_share"]],
                         on=["date","firm"], how="left")
    q2h_tech["q2_apportioned"] = q2h_tech["q2_mwh"] * q2h_tech["tech_share"]

    # Aggregate: mean apportioned q₂ by (firm, tech_group, regime, critical)
    summary_A = (q2h_tech.dropna(subset=["tech_group"])
                          .groupby(["firm","tech_group","regime","critical"])["q2_apportioned"]
                          .mean()
                          .reset_index())
    pivot_A = summary_A.pivot_table(index=["firm","tech_group","regime"],
                                     columns="critical",
                                     values="q2_apportioned",
                                     aggfunc="first").reset_index()
    pivot_A.columns = ["firm","tech_group","regime","flat_q2","crit_q2"]
    pivot_A["crit_minus_flat"] = pivot_A["crit_q2"] - pivot_A["flat_q2"]
    pivot_A["regime"] = pd.Categorical(pivot_A["regime"], categories=REGIMES, ordered=True)
    pivot_A = pivot_A.sort_values(["firm","tech_group","regime"]).reset_index(drop=True)

    print()
    print("Test A: Big-4 q₂ apportioned by tech (mean MWh per firm-hour), critical vs flat:")
    print(pivot_A.round(1).to_string(index=False))
    pivot_A.to_csv(OUT_DIR_R / "critical_hours_q2_by_firm_tech.csv", index=False)

    # Figure A: critical-flat differential by firm × tech × regime
    OUT_DIR_F.mkdir(parents=True, exist_ok=True)
    fig, axes = plt.subplots(1, 4, figsize=(15, 4), sharey=True)
    for ax, firm in zip(axes, BIG4):
        sub = pivot_A[pivot_A["firm"] == firm].copy()
        techs = ["Hydro","CCGT","Nuclear"]
        x = np.arange(len(REGIMES))
        width = 0.27
        for i, tech in enumerate(techs):
            t = sub[sub["tech_group"] == tech].set_index("regime")["crit_minus_flat"].reindex(REGIMES)
            ax.bar(x + (i-1)*width, t.values, width=width, label=tech,
                   color={"Hydro":"tab:blue","CCGT":"tab:red","Nuclear":"tab:green"}[tech])
        ax.axhline(0, color="black", lw=0.5)
        ax.set_xticks(x)
        ax.set_xticklabels(REGIMES, rotation=20, ha="right", fontsize=8)
        ax.set_title(firm)
        if firm == "IB":
            ax.set_ylabel("Critical−flat q₂ apportioned (MWh / firm-hour)")
        if firm == "HC":
            ax.legend(loc="best", fontsize=8)
    fig.suptitle("Critical-hour q₂_IDA activation, by firm × tech × regime\n"
                 "(positive = firm reposits more in critical hours; "
                 "tech-apportioned via DA sell-side share)", fontsize=10)
    plt.tight_layout()
    plt.savefig(OUT_DIR_F / "critical_hours_q2_by_firm_tech.png", dpi=110, bbox_inches="tight")
    plt.close()

    # ----------------------------------------------------------------------
    # TEST B — DA cleared-price premium critical vs flat by regime
    # ----------------------------------------------------------------------
    print()
    print("[B] DA cleared-price premium critical vs flat by regime…", flush=True)
    pp = con.execute(f"""
        SELECT CAST(date AS DATE) AS date, period, price_es_eur_mwh
        FROM '{PRICES}'
    """).df()
    pp["date"] = pd.to_datetime(pp["date"])
    # DA price went 15-min only at MTU15-DA (Oct 2025). Pre that, DA periods 1..24.
    is_post_da15 = pp["date"] >= MTU15_DA_DATE
    pp["hour"] = np.where(is_post_da15,
                          ((pp["period"] - 1) // 4).astype(int),
                          (pp["period"] - 1).astype(int))
    pp["hour"] = pp["hour"].clip(0, 23)
    pp["regime"] = pp["date"].apply(assign_regime)
    pp["critical"] = pp["hour"].isin(CRITICAL_HOURS).astype(int)
    # Compute mean price per (date, critical)
    daily_split = (pp.groupby(["date","regime","critical"])["price_es_eur_mwh"]
                     .mean()
                     .reset_index()
                     .pivot_table(index=["date","regime"],
                                  columns="critical",
                                  values="price_es_eur_mwh",
                                  aggfunc="first")
                     .reset_index())
    daily_split.columns = ["date","regime","p_flat","p_crit"]
    daily_split["premium_eur"] = daily_split["p_crit"] - daily_split["p_flat"]
    daily_split["premium_pct"] = (daily_split["p_crit"] - daily_split["p_flat"]) / daily_split["p_flat"] * 100
    # Aggregate by regime
    by_regime = (daily_split.groupby("regime").agg(
                    mean_p_flat=("p_flat","mean"),
                    mean_p_crit=("p_crit","mean"),
                    mean_premium_eur=("premium_eur","mean"),
                    median_premium_eur=("premium_eur","median"),
                    mean_premium_pct=("premium_pct","mean"),
                    n_days=("date","nunique"),
                ).reset_index())
    by_regime["regime"] = pd.Categorical(by_regime["regime"], categories=REGIMES, ordered=True)
    by_regime = by_regime.sort_values("regime").reset_index(drop=True)
    print()
    print("Test B: DA cleared-price premium (critical − flat hours) by regime:")
    print(by_regime.round(2).to_string(index=False))
    by_regime.to_csv(OUT_DIR_R / "critical_hours_da_price_premium.csv", index=False)

    # Figure B: critical-flat price premium by regime
    fig, ax = plt.subplots(figsize=(8, 4.5))
    x = np.arange(len(REGIMES))
    flat_vals = by_regime["mean_p_flat"].values
    crit_vals = by_regime["mean_p_crit"].values
    width = 0.4
    ax.bar(x - width/2, flat_vals, width, label="Flat hours", color="tab:gray")
    ax.bar(x + width/2, crit_vals, width, label=f"Critical hours (h={CRITICAL_HOURS})", color="tab:red")
    for xi, prem in zip(x, by_regime["mean_premium_eur"]):
        ax.text(xi, max(crit_vals[xi], flat_vals[xi]) + 1, f"+€{prem:.1f}",
                ha="center", fontsize=9, color="darkred")
    ax.set_xticks(x)
    ax.set_xticklabels(REGIMES, rotation=15, ha="right")
    ax.set_ylabel("Mean DA cleared price (€/MWh)")
    ax.set_title("DA cleared-price level: critical vs flat hours, by regime\n"
                 "Annotation = mean critical-flat premium per day")
    ax.legend()
    plt.tight_layout()
    plt.savefig(OUT_DIR_F / "critical_hours_da_price_premium.png", dpi=110, bbox_inches="tight")
    plt.close()

    print()
    print(f"wrote {OUT_DIR_F / 'critical_hours_q2_by_firm_tech.png'}")
    print(f"wrote {OUT_DIR_F / 'critical_hours_da_price_premium.png'}")
    print(f"wrote {OUT_DIR_R / 'critical_hours_q2_by_firm_tech.csv'}")
    print(f"wrote {OUT_DIR_R / 'critical_hours_da_price_premium.csv'}")
    print()
    print("Done.")


if __name__ == "__main__":
    main()
