# STATUS: ALIVE
# LAST-AUDIT: 2026-05-19
# CLAIM: Estimated probability density (Gaussian KDE) of daily in-band MW share
#        per (tech, firm), with regimes overlaid as different-colored curves.
#        Replaces the quantile tables with smooth densities -- exposes bimodality
#        (CCGT bidding strategies often have mass at 0 and at 1 simultaneously)
#        and the post-reform shift of the WHOLE distribution.
#
# Source: same per-(unit, date, hour) panel as bidshape_diurnal_distribution.py.
#
# OUT: figures/working/bidshape_kde_per_firm_tech.pdf

from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
from scipy.stats import gaussian_kde
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

REPO = Path(__file__).resolve().parents[3]
DET = REPO / "data/processed/omie/mercado_diario/ofertas/det_all.parquet"
CAB = REPO / "data/processed/omie/mercado_diario/ofertas/cab_all.parquet"
MPDBC = REPO / "data/processed/omie/mercado_diario/precios/marginalpdbc_all.parquet"
UNITS = REPO / "data/external/omie_reference/lista_unidades.csv"
FIG_DIR = REPO / "figures/working"
FIG_DIR.mkdir(parents=True, exist_ok=True)

H = 50.0
START = "2024-06-14"
END = "2026-05-15"

REGIME_DATES = [
    ("3sess",         pd.Timestamp("2024-06-14"), pd.Timestamp("2024-11-30"), "3-sess",       "#1f77b4"),
    ("ISP15win",      pd.Timestamp("2024-12-01"), pd.Timestamp("2025-03-18"), "ISP15-win",    "#ff7f0e"),
    ("MTU15IDA_pre",  pd.Timestamp("2025-03-19"), pd.Timestamp("2025-04-27"), "DA60/ID15 pre",  "#2ca02c"),
    ("MTU15IDA_post", pd.Timestamp("2025-04-28"), pd.Timestamp("2025-09-30"), "DA60/ID15 post", "#d62728"),
    ("DA15_ID15",     pd.Timestamp("2025-10-01"), pd.Timestamp("2026-05-15"), "DA15/ID15",    "#9467bd"),
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
             COALESCE(mp.mtu_p, dv.mtu) AS mtu_minutes
      FROM det dv JOIN cab_l c ON dv.d=c.d AND dv.offer_code=c.offer_code
        JOIN mp ON mp.d=dv.d AND mp.period=dv.period
    ),
    per_cell AS (
      SELECT d, period, unit_code,
             SUM(q * mtu_minutes/60.0) AS mw_total,
             SUM(q * mtu_minutes/60.0 * in_band) AS mw_in
      FROM joined GROUP BY 1, 2, 3
    )
    SELECT p.d, u.firm, u.tech,
           p.mw_in / NULLIF(p.mw_total, 0) AS in_band_share
    FROM per_cell p JOIN u ON p.unit_code = u.unit_code
    WHERE p.mw_total > 0
    """
    df = con.execute(sql).fetchdf()
    df["d"] = pd.to_datetime(df["d"])
    return df


def assign_regime(df):
    df = df.copy()
    df["regime"] = "other"
    for label, lo, hi, _, _ in REGIME_DATES:
        m = (df["d"] >= lo) & (df["d"] <= hi)
        df.loc[m, "regime"] = label
    return df[df["regime"] != "other"].copy()


def main():
    print("Building per-cell panel...")
    df = build_panel()
    df = assign_regime(df)
    print(f"  {len(df):,} cells across {df['regime'].nunique()} regimes")

    n_techs = len(TECHS)
    n_firms = len(FIRMS)
    fig, axes = plt.subplots(n_techs, n_firms, figsize=(15, 8), sharex=True, sharey=True)
    fig.suptitle("In-band MW share ($\\pm$50 EUR/MWh around DA MCP) — estimated density per (tech, firm), regimes overlaid", fontsize=11)

    x = np.linspace(0.001, 0.999, 200)
    for i, tech in enumerate(TECHS):
        for j, firm in enumerate(FIRMS):
            ax = axes[i, j] if n_techs > 1 else axes[j]
            sub_tf = df[(df["tech"] == tech) & (df["firm"] == firm)]
            for r_lab, _, _, r_disp, color in REGIME_DATES:
                vals = sub_tf[sub_tf["regime"] == r_lab]["in_band_share"].dropna().values
                if len(vals) < 100:
                    continue
                # Clip to (0.001, 0.999) for KDE (avoid boundary issues)
                vals = np.clip(vals, 0.001, 0.999)
                try:
                    kde = gaussian_kde(vals, bw_method=0.08)
                    ax.plot(x, kde(x), color=color, lw=1.5, label=r_disp)
                except (np.linalg.LinAlgError, ValueError):
                    pass
            if i == 0:
                ax.set_title(firm, fontsize=10)
            if j == 0:
                ax.set_ylabel(tech.replace("_", " "), fontsize=10)
            if i == n_techs - 1:
                ax.set_xlabel("In-band share")
            ax.set_xlim(0, 1)
            ax.grid(alpha=0.25, lw=0.4)
            if i == 0 and j == n_firms - 1:
                ax.legend(loc="upper right", fontsize=7)
    plt.tight_layout(rect=[0, 0, 1, 0.96])
    out = FIG_DIR / "bidshape_kde_per_firm_tech.pdf"
    fig.savefig(out, bbox_inches="tight", dpi=120)
    plt.close(fig)
    print(f"wrote {out}")


if __name__ == "__main__":
    main()
