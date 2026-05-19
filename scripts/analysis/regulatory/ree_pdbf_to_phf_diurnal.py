# STATUS: ALIVE
# LAST-AUDIT: 2026-05-19
# CLAIM: Diurnal (24-hour) and distributional view of the post-DA gap
#        (PHF_last - PDBC). Replaces the Critical / Flat / Midday hour-class
#        aggregation with the full hour-of-day pattern + quantile distribution.
#
#        (A) Heatmap: per regime, rows = (tech, firm), cols = hour 0-23,
#            color = mean GWh/day at that (firm, tech, hour) cell.
#        (B) Quantile table: per (tech, firm, regime), p10/p25/p50/p75/p90 of
#            the daily post-DA gap across (unit-day-hour) cells. Reveals
#            the heavy upward tails on critical hours that drive the mean.
#
# OUT:
#   results/regressions/regulatory/pdbf_to_phf_diurnal/
#     per_firm_tech_hour_regime.csv
#     quantiles_per_firm_tech_regime.csv
#     tab_post_da_gap_quantiles.tex
#   figures/working/
#     post_da_gap_diurnal_heatmap.pdf

from __future__ import annotations
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

REPO = Path(__file__).resolve().parents[3]
PDBC = REPO / "data/processed/omie/mercado_diario/programas/pdbc_all.parquet"
PHF  = REPO / "data/processed/omie/mercado_intradiario_subastas/programas/phf_all.parquet"
UNITS_CSV = REPO / "data/external/omie_reference/lista_unidades.csv"
OUT_DIR = REPO / "results/regressions/regulatory/pdbf_to_phf_diurnal"
FIG_DIR = REPO / "figures/working"
OUT_DIR.mkdir(parents=True, exist_ok=True)
FIG_DIR.mkdir(parents=True, exist_ok=True)

START = "2024-06-14"
END = "2026-02-28"

REGIME_DATES = [
    ("3sess",         pd.Timestamp("2024-06-14"), pd.Timestamp("2024-11-30"), "3-sess"),
    ("ISP15win",      pd.Timestamp("2024-12-01"), pd.Timestamp("2025-03-18"), "ISP15-win"),
    ("MTU15IDA_pre",  pd.Timestamp("2025-03-19"), pd.Timestamp("2025-04-27"), "DA60/ID15 pre-blk"),
    ("MTU15IDA_post", pd.Timestamp("2025-04-28"), pd.Timestamp("2025-09-30"), "DA60/ID15 post-blk"),
    ("DA15_ID15",     pd.Timestamp("2025-10-01"), pd.Timestamp("2026-02-28"), "DA15/ID15"),
]
TECHS = ["CCGT", "Hydro", "Hydro_pump"]
FIRMS = ["IB", "GE", "GN", "HC", "REP"]


def tech_bucket(t):
    if t is None: return "Other"
    t = str(t).lower()
    if "ciclo combinado" in t: return "CCGT"
    if "hidráulica generación" in t: return "Hydro"
    if "bombeo" in t: return "Hydro_pump"
    return "Other"


def firm_bucket(o):
    if not isinstance(o, str): return "OTH"
    o = o.lower()
    if "iberdrola" in o: return "IB"
    if "endesa" in o: return "GE"
    if "naturgy" in o or "gas natural" in o: return "GN"
    if "edp" in o or "hidroel" in o: return "HC"
    if "repsol" in o: return "REP"
    return "OTH"


def main():
    print("Loading units...")
    units = pd.read_csv(UNITS_CSV)
    units["tech"] = units["technology"].apply(tech_bucket)
    units["firm"] = units["owner_agent"].apply(firm_bucket)
    units = units[(units["tech"].isin(TECHS)) & (units["firm"].isin(FIRMS))][
        ["unit_code", "firm", "tech"]
    ].drop_duplicates("unit_code")

    con = duckdb.connect()
    con.execute("SET memory_limit='12GB'")
    con.execute("SET threads=4")
    con.register("u", units)

    print("Building per (firm, tech, day, hour, unit) PHF-last vs PDBC gap (MWh)...")
    sql = f"""
    WITH pdbc AS (
      SELECT CAST(date AS DATE) AS d, period, unit_code,
             assigned_power_mw AS pdbc_mw,
             COALESCE(mtu_minutes, 60) AS mtu,
             CASE WHEN COALESCE(mtu_minutes, 60) = 60 THEN period - 1
                  ELSE CAST(FLOOR((period - 1) / 4.0) AS INT) END AS clock_hour
      FROM read_parquet('{PDBC}')
      WHERE date >= '{START}' AND date <= '{END}'
    ),
    phf_last AS (
      SELECT CAST(date AS DATE) AS d, period, unit_code,
             MAX(session_number) AS last_session
      FROM read_parquet('{PHF}')
      WHERE date >= '{START}' AND date <= '{END}'
      GROUP BY 1, 2, 3
    ),
    phf AS (
      SELECT CAST(p.date AS DATE) AS d, p.period, p.unit_code,
             p.assigned_power_mw AS phf_mw,
             COALESCE(p.mtu_minutes, 60) AS mtu,
             CASE WHEN COALESCE(p.mtu_minutes, 60) = 60 THEN p.period - 1
                  ELSE CAST(FLOOR((p.period - 1) / 4.0) AS INT) END AS clock_hour
      FROM read_parquet('{PHF}') p
        JOIN phf_last pl ON CAST(p.date AS DATE)=pl.d AND p.period=pl.period
                         AND p.unit_code=pl.unit_code AND p.session_number=pl.last_session
      WHERE p.date >= '{START}' AND p.date <= '{END}'
    ),
    j AS (
      SELECT COALESCE(pdbc.d, phf.d) AS d,
             COALESCE(pdbc.period, phf.period) AS period,
             COALESCE(pdbc.unit_code, phf.unit_code) AS unit_code,
             COALESCE(pdbc.clock_hour, phf.clock_hour) AS clock_hour,
             COALESCE(pdbc.pdbc_mw, 0) AS pdbc_mw,
             COALESCE(phf.phf_mw, 0) AS phf_mw,
             COALESCE(pdbc.mtu, phf.mtu, 60) AS mtu
      FROM pdbc FULL OUTER JOIN phf
        ON pdbc.d=phf.d AND pdbc.period=phf.period AND pdbc.unit_code=phf.unit_code
    )
    SELECT j.d, j.clock_hour, u.firm, u.tech, j.unit_code,
           SUM((j.phf_mw - j.pdbc_mw) * j.mtu/60.0) / 1000.0 AS gap_gwh
    FROM j JOIN u ON j.unit_code = u.unit_code
    GROUP BY 1, 2, 3, 4, 5
    """
    df = con.execute(sql).fetchdf()
    df["d"] = pd.to_datetime(df["d"])
    df = df[(df["clock_hour"] >= 0) & (df["clock_hour"] <= 23)].copy()
    print(f"  {len(df):,} (unit, day, hour) rows")

    # Regime label
    df["regime"] = "other"
    for label, lo, hi, _ in REGIME_DATES:
        m = (df["d"] >= lo) & (df["d"] <= hi)
        df.loc[m, "regime"] = label
    df = df[df["regime"] != "other"].copy()

    # Per (firm, tech, regime, hour) mean gap GWh/day (sum across units → already done at unit level; want firm-tech aggregate)
    # First aggregate by (date, firm, tech, hour) summing across units
    daily = df.groupby(["d", "firm", "tech", "clock_hour", "regime"])["gap_gwh"].sum().reset_index()
    # Then mean across days
    heatmap = (daily.groupby(["firm", "tech", "regime", "clock_hour"])["gap_gwh"].mean()
                       .reset_index())
    heatmap.to_csv(OUT_DIR / "per_firm_tech_hour_regime.csv", index=False)
    print(f"  wrote per_firm_tech_hour_regime.csv ({len(heatmap):,} rows)")

    # Quantiles across (day-hour) cells per (firm, tech, regime) — daily aggregates
    quantiles = (daily.groupby(["firm", "tech", "regime"])["gap_gwh"]
                       .quantile([0.10, 0.25, 0.50, 0.75, 0.90])
                       .unstack().reset_index())
    quantiles.columns = ["firm", "tech", "regime", "p10", "p25", "p50", "p75", "p90"]
    quantiles.to_csv(OUT_DIR / "quantiles_per_firm_tech_regime.csv", index=False)

    # Tex table: quantiles per (tech, firm, regime)
    rows = [r"\begin{tabular}{l l l r r r r r}", r"\toprule",
            r"Tech & Firm & Regime & p10 & p25 & p50 (median) & p75 & p90 \\",
            r"\midrule"]
    last_tech, last_firm = None, None
    for tech in TECHS:
        for firm in FIRMS:
            for r_lab, _, _, r_disp in REGIME_DATES:
                sub = quantiles[(quantiles["tech"] == tech) &
                                (quantiles["firm"] == firm) &
                                (quantiles["regime"] == r_lab)]
                if sub.empty:
                    continue
                row = sub.iloc[0]
                tech_lbl = tech.replace("_", " ") if tech != last_tech else ""
                firm_lbl = firm if (tech != last_tech or firm != last_firm) else ""
                if tech != last_tech and last_tech is not None:
                    rows.append(r"\addlinespace")
                last_tech, last_firm = tech, firm
                rows.append(" & ".join([
                    tech_lbl, firm_lbl, r_disp,
                    f"{row['p10']:+.3f}", f"{row['p25']:+.3f}",
                    f"{row['p50']:+.3f}", f"{row['p75']:+.3f}", f"{row['p90']:+.3f}",
                ]) + r" \\")
    rows.extend([r"\bottomrule", r"\end{tabular}"])
    (OUT_DIR / "tab_post_da_gap_quantiles.tex").write_text(
        "% Quantiles of daily post-DA gap (GWh/day) per (firm, tech, regime) across (day, hour) cells.\n"
        + "\n".join(rows))
    print(f"  wrote tab_post_da_gap_quantiles.tex")

    # Heatmap figure
    CRIT = {5, 6, 7, 8, 16, 17, 18, 19, 20, 21, 22}
    MID  = {11, 12, 13, 14}
    pivot = heatmap.pivot_table(index=["tech", "firm"], columns=["regime", "clock_hour"], values="gap_gwh")
    row_keys = []
    for tech in TECHS:
        for firm in FIRMS:
            if (tech, firm) in pivot.index:
                row_keys.append((tech, firm))

    fig, axes = plt.subplots(1, len(REGIME_DATES), figsize=(20, 4.5), sharey=True)
    fig.suptitle("Post-DA gap (PHF$_{\\text{last}}$ $-$ PDBC, GWh/day) — diurnal pattern per (tech, firm)", fontsize=11)
    # set common scale based on data
    vmax = pivot.max().max() * 1.0
    vmin = pivot.min().min() * 1.0
    abs_max = max(abs(vmin), abs(vmax))
    for ax, (r_lab, _, _, r_disp) in zip(axes, REGIME_DATES):
        mat = np.full((len(row_keys), 24), np.nan)
        for i, (tech, firm) in enumerate(row_keys):
            if (tech, firm) not in pivot.index:
                continue
            row = pivot.loc[(tech, firm)]
            for h in range(24):
                try:
                    v = row[(r_lab, h)]
                    if pd.notna(v):
                        mat[i, h] = v
                except KeyError:
                    pass
        im = ax.imshow(mat, aspect="auto", cmap="RdBu_r", vmin=-abs_max, vmax=abs_max)
        ax.set_xticks(range(0, 24, 4))
        ax.set_xticks(range(24), minor=True)
        ax.set_xlabel("Hour of day")
        ax.set_title(r_disp, fontsize=10)
        for h in range(24):
            if h in CRIT:
                ax.axvline(h, color="red", lw=0.4, alpha=0.20)
            elif h in MID:
                ax.axvline(h, color="green", lw=0.4, alpha=0.20)
    axes[0].set_yticks(range(len(row_keys)))
    axes[0].set_yticklabels([f"{t.replace('_',' ')} | {f}" for t, f in row_keys], fontsize=8)
    fig.colorbar(im, ax=axes, label="Post-DA gap (GWh/day)", shrink=0.7, pad=0.02)
    fig.savefig(FIG_DIR / "post_da_gap_diurnal_heatmap.pdf", bbox_inches="tight", dpi=120)
    plt.close(fig)
    print(f"  wrote {FIG_DIR}/post_da_gap_diurnal_heatmap.pdf")


if __name__ == "__main__":
    main()
