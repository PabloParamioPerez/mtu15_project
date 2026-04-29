# STATUS: ALIVE
# LAST-AUDIT: 2026-04-29
# FEEDS: B9 firm-shape proxy ρ_i — Prop 5 (model_v2.tex) cross-firm test
# CLAIM: Per the structural model (Prop 5), firms with larger intra-hour
#        shape exposure ρ_i should have larger Φ_i,r in asymmetric regimes,
#        therefore deeper compression in q₂. We compute ρ_i empirically as
#        the within-day std of the firm's hourly net schedule (PDBCE) over
#        its mean, per regime, and test whether ρ_i correlates with the
#        per-firm q₂ compression depth.
"""B9 firm-shape proxy ρ_i — cross-firm test of Prop 5 heterogeneity.

ρ_i ≈ std(firm's hourly schedule pattern) / mean(daily schedule)
    measures how much the firm's load shape varies within the day.

Prediction (Prop 5): firms with larger ρ_i should have larger Φ in asymmetric
regimes → deeper q₂ compression at ISP15-win and DA60/ID15.

We compute:
  - ρ_i per firm per regime (from PDBCE per-firm-period schedule)
  - q₂ compression depth per firm = q₂(pre-IDA) − q₂(ISP15-win)
  - cross-firm correlation between ρ_i and compression depth

Output: per-firm ρ table; rank correlation; scatter plot.
"""
from __future__ import annotations
from pathlib import Path
import time
import duckdb
import numpy as np
import pandas as pd

PROJECT  = Path(__file__).resolve().parents[3]
PDBCE    = PROJECT / "data" / "processed" / "omie" / "mercado_diario" / "programas" / "pdbce_all.parquet"
B9_PERFIRM = PROJECT / "data" / "derived" / "results" / "b9_replicated_isp_grain_perfirm.csv"
OUTDIR   = PROJECT / "data" / "derived" / "results" / "b9_firm_shape_rho"
OUTDIR.mkdir(parents=True, exist_ok=True)

REGIMES = ["pre-IDA", "3-sess", "ISP15-win", "DA60/ID15", "DA15/ID15"]
BIG4 = ["GE", "IB", "GN", "HC"]


def main() -> None:
    t0 = time.time()
    print(f"[{time.strftime('%H:%M:%S')}] Starting firm-shape ρ_i computation…", flush=True)

    con = duckdb.connect()
    con.execute("SET memory_limit='6GB'")
    con.execute("SET threads=4")
    con.execute("SET preserve_insertion_order=false")

    big4_sql = "(" + ",".join(f"'{f}'" for f in BIG4) + ")"

    # ============================================================
    # 1. Per-firm-day-hour schedule (Big-4)
    # PDBCE is per-firm-period; aggregate by firm × date × hour
    # ============================================================
    print("[1/3] Building Big-4 firm-day-hour cleared MWh from PDBCE…", flush=True)
    sched = con.execute(f"""
        SELECT date,
               grupo_empresarial AS firm,
               CASE WHEN mtu_minutes = 15 THEN CEIL(period / 4.0)::INT
                    ELSE period END AS hour,
               SUM(assigned_power_mw * mtu_minutes / 60.0) AS mwh
        FROM '{PDBCE}'
        WHERE grupo_empresarial IN {big4_sql}
          AND assigned_power_mw IS NOT NULL
        GROUP BY 1, 2, 3
    """).df()
    sched["date"] = pd.to_datetime(sched["date"])
    print(f"   firm-day-hour rows: {len(sched):,}", flush=True)

    def assign_regime(d):
        d = pd.Timestamp(d)
        if d < pd.Timestamp("2024-06-14"): return "pre-IDA"
        if d < pd.Timestamp("2024-12-01"): return "3-sess"
        if d < pd.Timestamp("2025-03-19"): return "ISP15-win"
        if d < pd.Timestamp("2025-10-01"): return "DA60/ID15"
        return "DA15/ID15"
    sched["regime"] = sched["date"].apply(assign_regime)

    # ============================================================
    # 2. Compute firm-day shape statistic, then average across days per regime
    # ρ_{i,d} = std(MWh across 24 hours of day d) / |mean(MWh across day d)|
    # ============================================================
    print("[2/3] Computing per-firm-day shape statistic ρ…", flush=True)
    shape = (sched.groupby(["firm", "date", "regime"], observed=True)["mwh"]
                  .agg(["mean", "std", "count"])
                  .reset_index())
    shape = shape[shape["count"] >= 20].copy()  # require enough hours
    shape["rho_d"] = shape["std"] / shape["mean"].abs().clip(lower=1e-3)
    print(f"   firm-day shape rows: {len(shape):,}", flush=True)

    # Per-firm regime mean of ρ
    rho_table = (shape.groupby(["firm", "regime"], observed=True)["rho_d"]
                       .agg(["mean", "median", "count"])
                       .reset_index())
    rho_table["regime"] = pd.Categorical(rho_table["regime"], categories=REGIMES, ordered=True)
    rho_table = rho_table.sort_values(["firm", "regime"])
    rho_pv = rho_table.pivot(index="firm", columns="regime", values="mean").reindex(BIG4).reindex(REGIMES, axis=1)
    print("Firm × regime mean ρ_i (within-day shape coefficient of variation):", flush=True)
    print(rho_pv.round(3).to_string(), flush=True)
    print()

    # Use pre-IDA as the baseline ρ_i for each firm
    rho_pre = rho_pv["pre-IDA"]
    print(f"Big-4 ρ_i (pre-IDA baseline):", flush=True)
    print(rho_pre.round(3).to_string(), flush=True)
    print()

    rho_table.to_csv(OUTDIR / "rho_per_firm_per_regime.csv", index=False)

    # ============================================================
    # 3. Cross-firm test: correlate ρ_i with q₂ compression depth
    # ============================================================
    print("[3/3] Cross-firm test: ρ_i vs q₂ compression depth…", flush=True)
    q2 = pd.read_csv(B9_PERFIRM, index_col=0).reindex(BIG4)
    q2["compression"] = q2["pre-IDA"] - q2["ISP15-win"]
    q2["recovery"]    = q2["DA15/ID15"] - q2["ISP15-win"]
    q2["proportional_compression"] = q2["compression"] / q2["pre-IDA"]
    q2["rho_i"] = rho_pre
    print("Cross-firm panel:", flush=True)
    print(q2[["pre-IDA", "ISP15-win", "DA15/ID15", "compression",
              "proportional_compression", "rho_i"]].round(2).to_string(), flush=True)
    print()

    # Spearman + Pearson correlations
    if q2["rho_i"].notna().sum() >= 4:
        corr_p_abs = q2["rho_i"].corr(q2["compression"])
        corr_p_pct = q2["rho_i"].corr(q2["proportional_compression"])
        corr_s_abs = q2["rho_i"].corr(q2["compression"], method="spearman")
        corr_s_pct = q2["rho_i"].corr(q2["proportional_compression"], method="spearman")
        print(f"  Pearson  (ρ_i, |compression|)         = {corr_p_abs:+.3f}", flush=True)
        print(f"  Pearson  (ρ_i, proportional)          = {corr_p_pct:+.3f}", flush=True)
        print(f"  Spearman (ρ_i, |compression|)         = {corr_s_abs:+.3f}", flush=True)
        print(f"  Spearman (ρ_i, proportional)          = {corr_s_pct:+.3f}", flush=True)
        print(flush=True)
        print("Reading: positive correlation supports Prop 5 (peakier firms compress more).", flush=True)
        print("With only 4 Big-4 firms, p-values are not informative — sign is what matters.", flush=True)

    q2.to_csv(OUTDIR / "rho_vs_compression.csv")

    # Scatter plot
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        FIG_DIR = PROJECT / "thesis" / "figures"
        FIG_DIR.mkdir(parents=True, exist_ok=True)
        fig, ax = plt.subplots(figsize=(8, 5))
        for i, firm in enumerate(BIG4):
            x = q2.loc[firm, "rho_i"]
            y = q2.loc[firm, "compression"]
            if not (np.isnan(x) or np.isnan(y)):
                ax.scatter(x, y, s=120)
                ax.annotate(firm, (x, y), xytext=(8, 4), textcoords="offset points", fontsize=11)
        ax.set_xlabel("ρ_i — within-day shape coefficient of variation (pre-IDA)", fontsize=11)
        ax.set_ylabel("q₂ compression: q₂(pre-IDA) − q₂(ISP15-win), MWh per firm-ISP", fontsize=11)
        ax.set_title("Prop 5 cross-firm test: shape exposure ρ_i vs Big-4 q₂ compression depth",
                     fontsize=11)
        ax.grid(True, alpha=0.3)
        plt.tight_layout()
        plt.savefig(FIG_DIR / "fig_prop5_rho_vs_compression.pdf", bbox_inches="tight")
        plt.savefig(FIG_DIR / "fig_prop5_rho_vs_compression.png", bbox_inches="tight")
        print(f"Wrote {FIG_DIR / 'fig_prop5_rho_vs_compression.pdf'}", flush=True)
    except Exception as e:
        print(f"Plot failed (non-fatal): {e}", flush=True)

    print(f"\nTotal runtime: {(time.time() - t0)/60:.1f} min", flush=True)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        import traceback
        print(f"ERROR: {type(e).__name__}: {e}", flush=True)
        print(traceback.format_exc(), flush=True)
        raise
