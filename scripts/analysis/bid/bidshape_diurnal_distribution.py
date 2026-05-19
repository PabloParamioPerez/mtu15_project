# STATUS: ALIVE
# LAST-AUDIT: 2026-05-19
# CLAIM: Diurnal (24-hour) and distributional view of in-band MW share, replacing
#        the Critical / Flat / Midday hour-class aggregation:
#
#        (A) Heatmap: per regime, rows = (tech, firm), cols = hour 0-23,
#            color = mean in-band MW share at that (firm, tech, hour) cell.
#            Reveals the diurnal pattern of price-setting concentration
#            within each (firm, tech) that hour-class means flatten.
#
#        (B) Quantile table: per (tech, firm, regime), p10/p25/p50/p75/p90
#            of the in-band share across all (unit, day, hour) cells.
#            Reveals heavy tails and bimodality the mean hides.
#
#        Computed on the DA market for the three price-setting techs.
#
# OUT:
#   results/regressions/bid/bidshape_diurnal/
#     per_firm_tech_hour_regime.csv
#     quantiles_per_firm_tech_regime.csv
#     tab_bidshape_quantiles_per_firm_tech_regime.tex
#   figures/working/
#     bidshape_diurnal_heatmap_<regime>.pdf  (5 regime PDFs)

from __future__ import annotations
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

REPO = Path(__file__).resolve().parents[3]
DET = REPO / "data/processed/omie/mercado_diario/ofertas/det_all.parquet"
CAB = REPO / "data/processed/omie/mercado_diario/ofertas/cab_all.parquet"
MPDBC = REPO / "data/processed/omie/mercado_diario/precios/marginalpdbc_all.parquet"
UNITS = REPO / "data/external/omie_reference/lista_unidades.csv"
OUT_DIR = REPO / "results/regressions/bid/bidshape_diurnal"
FIG_DIR = REPO / "figures/working"
OUT_DIR.mkdir(parents=True, exist_ok=True)
FIG_DIR.mkdir(parents=True, exist_ok=True)

H = 50.0
START = "2024-06-14"
END = "2026-05-15"

REGIME_DATES = [
    ("3sess",         pd.Timestamp("2024-06-14"), pd.Timestamp("2024-11-30"), "3-sess"),
    ("ISP15win",      pd.Timestamp("2024-12-01"), pd.Timestamp("2025-03-18"), "ISP15-win"),
    ("MTU15IDA_pre",  pd.Timestamp("2025-03-19"), pd.Timestamp("2025-04-27"), "DA60/ID15 pre-blk"),
    ("MTU15IDA_post", pd.Timestamp("2025-04-28"), pd.Timestamp("2025-09-30"), "DA60/ID15 post-blk"),
    ("DA15_ID15",     pd.Timestamp("2025-10-01"), pd.Timestamp("2026-05-15"), "DA15/ID15"),
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


def build_panel():
    """Per (date, firm, tech, unit_code, clock_hour) daily in-band share."""
    con = duckdb.connect()
    con.execute("SET memory_limit='12GB'")
    con.execute("SET threads=4")
    units = pd.read_csv(UNITS)
    units["tech"] = units["technology"].apply(tech_bucket)
    units["firm"] = units["owner_agent"].apply(firm_bucket)
    units = units[units["tech"].isin(TECHS) & units["firm"].isin(FIRMS)][
        ["unit_code", "firm", "tech"]
    ].drop_duplicates("unit_code")
    con.register("u", units)

    sql = f"""
    WITH cab_last AS (
      SELECT CAST(date AS DATE) AS d, offer_code, unit_code,
             ROW_NUMBER() OVER (PARTITION BY CAST(date AS DATE), offer_code, unit_code
                                ORDER BY version DESC) AS rn
      FROM read_parquet('{CAB}')
      WHERE date >= '{START}' AND date <= '{END}' AND buy_sell='V'
    ),
    cab_l AS (SELECT d, offer_code, unit_code FROM cab_last WHERE rn=1),
    det AS (
      SELECT CAST(date AS DATE) AS d, offer_code, period,
             price_eur_mwh AS p, quantity_mw AS q, COALESCE(mtu_minutes, 60) AS mtu
      FROM read_parquet('{DET}')
      WHERE date >= '{START}' AND date <= '{END}' AND quantity_mw > 0
    ),
    mp AS (
      SELECT CAST(date AS DATE) AS d, period, price_es_eur_mwh AS p_clear,
             COALESCE(mtu_minutes, 60) AS mtu_p
      FROM read_parquet('{MPDBC}')
      WHERE date >= '{START}' AND date <= '{END}' AND price_es_eur_mwh IS NOT NULL
    ),
    joined AS (
      SELECT mp.d, mp.period, c.unit_code, dv.q, mp.p_clear,
             (dv.p BETWEEN mp.p_clear - {H} AND mp.p_clear + {H})::INT AS in_band,
             COALESCE(mp.mtu_p, dv.mtu) AS mtu_minutes,
             CASE WHEN COALESCE(mp.mtu_p, dv.mtu) = 60 THEN mp.period - 1
                  ELSE CAST(FLOOR((mp.period - 1) / 4.0) AS INT) END AS clock_hour
      FROM det dv JOIN cab_l c ON dv.d=c.d AND dv.offer_code=c.offer_code
        JOIN mp ON mp.d=dv.d AND mp.period=dv.period
    ),
    per_unit_hour_day AS (
      SELECT d, clock_hour, unit_code,
             SUM(q * mtu_minutes/60.0) AS mw_total,
             SUM(q * mtu_minutes/60.0 * in_band) AS mw_in
      FROM joined GROUP BY 1, 2, 3
    )
    SELECT p.d, u.firm, u.tech, p.unit_code, p.clock_hour,
           p.mw_in / NULLIF(p.mw_total, 0) AS in_band_share,
           p.mw_total
    FROM per_unit_hour_day p JOIN u ON p.unit_code = u.unit_code
    WHERE p.mw_total > 0
    """
    df = con.execute(sql).fetchdf()
    df["d"] = pd.to_datetime(df["d"])

    # Drop midnight (clock_hour=0) which won't appear in critical/flat/midday anyway, plus hour > 23
    df = df[(df["clock_hour"] >= 0) & (df["clock_hour"] <= 23)].copy()
    return df


def assign_regime(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["regime"] = "other"
    for label, lo, hi, _ in REGIME_DATES:
        m = (df["d"] >= lo) & (df["d"] <= hi)
        df.loc[m, "regime"] = label
    return df[df["regime"] != "other"].copy()


def main():
    print("Building per (firm, tech, unit, day, hour) panel...")
    df = build_panel()
    print(f"  {len(df):,} (unit, day, hour) cells")
    df = assign_regime(df)
    print(f"  {len(df):,} in-regime cells, {df['regime'].nunique()} regimes")

    # Per (firm, tech, regime, hour) mean share — for heatmap
    heatmap_data = (df.groupby(["firm", "tech", "regime", "clock_hour"])["in_band_share"]
                      .mean().reset_index())
    heatmap_data.to_csv(OUT_DIR / "per_firm_tech_hour_regime.csv", index=False)
    print(f"  wrote per_firm_tech_hour_regime.csv ({len(heatmap_data):,} rows)")

    # Per (firm, tech, regime) quantiles across all (unit, day, hour) cells
    quantiles = (df.groupby(["firm", "tech", "regime"])["in_band_share"]
                   .quantile([0.10, 0.25, 0.50, 0.75, 0.90])
                   .unstack().reset_index())
    quantiles.columns = ["firm", "tech", "regime", "p10", "p25", "p50", "p75", "p90"]
    quantiles.to_csv(OUT_DIR / "quantiles_per_firm_tech_regime.csv", index=False)
    print(f"  wrote quantiles_per_firm_tech_regime.csv")

    # ===== Tex table: quantiles per (tech, firm, regime) =====
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
                    f"{row['p10']*100:.1f}",
                    f"{row['p25']*100:.1f}",
                    f"{row['p50']*100:.1f}",
                    f"{row['p75']*100:.1f}",
                    f"{row['p90']*100:.1f}",
                ]) + r" \\")
    rows.extend([r"\bottomrule", r"\end{tabular}"])
    (OUT_DIR / "tab_bidshape_quantiles_per_firm_tech_regime.tex").write_text(
        "% Quantiles of daily in-band MW share (%) per (firm, tech, regime) across (unit, day, hour) cells.\n"
        + "\n".join(rows))
    print(f"  wrote tab_bidshape_quantiles_per_firm_tech_regime.tex")

    # ===== Heatmap figures =====
    # Layout: one figure per regime; rows = (tech, firm); cols = hour 0-23; color = mean share.
    # Critical hours marked with red box, flat with blue, midday with white.
    CRIT = {5, 6, 7, 8, 16, 17, 18, 19, 20, 21, 22}
    FLAT = {1, 2, 3}
    MID  = {11, 12, 13, 14}

    # Aggregate to (firm, tech, hour, regime) mean
    pivot = (heatmap_data.pivot_table(index=["tech", "firm"],
                                        columns=["regime", "clock_hour"],
                                        values="in_band_share"))

    # Build (tech, firm) row order
    row_keys = []
    for tech in TECHS:
        for firm in FIRMS:
            if (tech, firm) in pivot.index:
                row_keys.append((tech, firm))

    fig, axes = plt.subplots(1, len(REGIME_DATES), figsize=(20, 4.5), sharey=True)
    fig.suptitle("In-band MW share (%, $\\pm$50 EUR/MWh around MCP) — diurnal pattern per (tech, firm) per regime",
                 fontsize=11)
    for ax, (r_lab, _, _, r_disp) in zip(axes, REGIME_DATES):
        # build (n_rows, 24) matrix
        mat = np.full((len(row_keys), 24), np.nan)
        for i, (tech, firm) in enumerate(row_keys):
            if (tech, firm) not in pivot.index:
                continue
            row = pivot.loc[(tech, firm)]
            for h in range(24):
                try:
                    v = row[(r_lab, h)]
                    if pd.notna(v):
                        mat[i, h] = v * 100
                except KeyError:
                    pass
        im = ax.imshow(mat, aspect="auto", cmap="viridis", vmin=0, vmax=100)
        ax.set_xticks(range(0, 24, 4))
        ax.set_xticks(range(24), minor=True)
        ax.set_xlabel("Hour of day")
        ax.set_title(r_disp, fontsize=10)
        # mark critical hours below
        for h in range(24):
            if h in CRIT:
                ax.axvline(h, color="red", lw=0.4, alpha=0.25)
            elif h in MID:
                ax.axvline(h, color="white", lw=0.4, alpha=0.4)
    axes[0].set_yticks(range(len(row_keys)))
    axes[0].set_yticklabels([f"{t.replace('_',' ')} | {f}" for t, f in row_keys], fontsize=8)
    fig.colorbar(im, ax=axes, label="In-band share (%)", shrink=0.7, pad=0.02)
    fig.savefig(FIG_DIR / "bidshape_diurnal_heatmap.pdf", bbox_inches="tight", dpi=120)
    plt.close(fig)
    print(f"  wrote {FIG_DIR}/bidshape_diurnal_heatmap.pdf")


if __name__ == "__main__":
    main()
