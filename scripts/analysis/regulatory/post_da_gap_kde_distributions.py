# STATUS: ALIVE
# LAST-AUDIT: 2026-05-19
# CLAIM: Estimated probability density (Gaussian KDE) of the daily post-DA gap
#        per (tech, firm), with regimes overlaid. Replaces the quantile table
#        for the post-DA gap distribution.
#
# OUT: figures/working/post_da_gap_kde_per_firm_tech.pdf

from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
from scipy.stats import gaussian_kde
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

REPO = Path(__file__).resolve().parents[3]
PDBC = REPO / "data/processed/omie/mercado_diario/programas/pdbc_all.parquet"
PHF  = REPO / "data/processed/omie/mercado_intradiario_subastas/programas/phf_all.parquet"
UNITS_CSV = REPO / "data/external/omie_reference/lista_unidades.csv"
FIG_DIR = REPO / "figures/working"
FIG_DIR.mkdir(parents=True, exist_ok=True)

START = "2024-06-14"
END = "2026-02-28"

REGIME_DATES = [
    ("3sess",         pd.Timestamp("2024-06-14"), pd.Timestamp("2024-11-30"), "3-sess",       "#1f77b4"),
    ("ISP15win",      pd.Timestamp("2024-12-01"), pd.Timestamp("2025-03-18"), "ISP15-win",    "#ff7f0e"),
    ("MTU15IDA_pre",  pd.Timestamp("2025-03-19"), pd.Timestamp("2025-04-27"), "DA60/ID15 pre",  "#2ca02c"),
    ("MTU15IDA_post", pd.Timestamp("2025-04-28"), pd.Timestamp("2025-09-30"), "DA60/ID15 post", "#d62728"),
    ("DA15_ID15",     pd.Timestamp("2025-10-01"), pd.Timestamp("2026-02-28"), "DA15/ID15",    "#9467bd"),
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
    units = units[units["tech"].isin(TECHS) & units["firm"].isin(FIRMS)][
        ["unit_code", "firm", "tech"]
    ].drop_duplicates("unit_code")
    con = duckdb.connect(); con.execute("SET memory_limit='12GB'"); con.execute("SET threads=4")
    con.register("u", units)

    print("Building daily (firm, tech) post-DA gap...")
    sql = f"""
    WITH pdbc AS (
      SELECT CAST(date AS DATE) AS d, period, unit_code,
             assigned_power_mw AS pdbc_mw, COALESCE(mtu_minutes, 60) AS mtu
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
             COALESCE(p.mtu_minutes, 60) AS mtu
      FROM read_parquet('{PHF}') p
        JOIN phf_last pl ON CAST(p.date AS DATE)=pl.d AND p.period=pl.period
                         AND p.unit_code=pl.unit_code AND p.session_number=pl.last_session
      WHERE p.date >= '{START}' AND p.date <= '{END}'
    ),
    j AS (
      SELECT COALESCE(pdbc.d, phf.d) AS d,
             COALESCE(pdbc.period, phf.period) AS period,
             COALESCE(pdbc.unit_code, phf.unit_code) AS unit_code,
             COALESCE(pdbc.pdbc_mw, 0) AS pdbc_mw,
             COALESCE(phf.phf_mw, 0) AS phf_mw,
             COALESCE(pdbc.mtu, phf.mtu, 60) AS mtu
      FROM pdbc FULL OUTER JOIN phf
        ON pdbc.d=phf.d AND pdbc.period=phf.period AND pdbc.unit_code=phf.unit_code
    )
    SELECT j.d, u.firm, u.tech,
           SUM((j.phf_mw - j.pdbc_mw) * j.mtu/60.0) / 1000.0 AS gap_gwh
    FROM j JOIN u ON j.unit_code = u.unit_code
    GROUP BY 1, 2, 3
    """
    df = con.execute(sql).fetchdf()
    df["d"] = pd.to_datetime(df["d"])
    df["regime"] = "other"
    for label, lo, hi, _, _ in REGIME_DATES:
        m = (df["d"] >= lo) & (df["d"] <= hi)
        df.loc[m, "regime"] = label
    df = df[df["regime"] != "other"].copy()
    print(f"  {len(df):,} (date, firm, tech) cells")

    n_techs = len(TECHS)
    n_firms = len(FIRMS)
    fig, axes = plt.subplots(n_techs, n_firms, figsize=(15, 8), sharex="row", sharey=False)
    fig.suptitle("Post-DA gap (PHF$_{\\text{last}}$ $-$ PDBC, GWh/day) — estimated density per (tech, firm), regimes overlaid", fontsize=11)

    # Determine x range per tech (the gap magnitude differs across techs)
    x_range_per_tech = {}
    for tech in TECHS:
        vals_tech = df[df["tech"] == tech]["gap_gwh"].dropna().values
        if len(vals_tech) == 0:
            x_range_per_tech[tech] = (-1, 1)
            continue
        q01, q99 = np.quantile(vals_tech, [0.01, 0.99])
        # Pad a bit
        x_range_per_tech[tech] = (q01 - 0.2 * abs(q01 + 0.1), q99 + 0.2 * abs(q99 + 0.1))

    for i, tech in enumerate(TECHS):
        lo_x, hi_x = x_range_per_tech[tech]
        x = np.linspace(lo_x, hi_x, 200)
        for j, firm in enumerate(FIRMS):
            ax = axes[i, j] if n_techs > 1 else axes[j]
            sub_tf = df[(df["tech"] == tech) & (df["firm"] == firm)]
            for r_lab, _, _, r_disp, color in REGIME_DATES:
                vals = sub_tf[sub_tf["regime"] == r_lab]["gap_gwh"].dropna().values
                if len(vals) < 30:
                    continue
                try:
                    kde = gaussian_kde(vals, bw_method=0.15)
                    ax.plot(x, kde(x), color=color, lw=1.5, label=r_disp)
                except (np.linalg.LinAlgError, ValueError):
                    pass
            ax.axvline(0, color="black", lw=0.5, alpha=0.5)
            if i == 0:
                ax.set_title(firm, fontsize=10)
            if j == 0:
                ax.set_ylabel(tech.replace("_", " "), fontsize=10)
            if i == n_techs - 1:
                ax.set_xlabel("Gap (GWh/day)")
            ax.set_xlim(lo_x, hi_x)
            ax.grid(alpha=0.25, lw=0.4)
            if i == 0 and j == n_firms - 1:
                ax.legend(loc="upper right", fontsize=7)
    plt.tight_layout(rect=[0, 0, 1, 0.96])
    out = FIG_DIR / "post_da_gap_kde_per_firm_tech.pdf"
    fig.savefig(out, bbox_inches="tight", dpi=120)
    plt.close(fig)
    print(f"wrote {out}")


if __name__ == "__main__":
    main()
