# STATUS: ALIVE
# LAST-AUDIT: 2026-05-19
# CLAIM: Build a LaTeX table of per-(tech, regime) in-band bid-share with
#        two values per cell — the raw mean (default colour) and the
#        seasonality-adjusted mean from the Spec A FWL regression in
#        seasonality_adjusted_all_outcomes.py (in colour).
#
#        "Seasonality-adjusted" = predicted share at the regime's β plus
#        Fourier(K=4) evaluated at AVERAGE doy of the entire sample
#        (so the regime values are comparable on a common-seasonality basis).
#
# OUTPUT: results/regressions/bid/seasonality_adjusted/tab_bidshape_raw_vs_deseasonalized.tex

from __future__ import annotations
from pathlib import Path
import duckdb
import pandas as pd
import numpy as np

REPO = Path(__file__).resolve().parents[3]
COEFS = REPO / "results/regressions/bid/seasonality_adjusted/all_outcomes_coefs.csv"
OUT_TEX = REPO / "results/regressions/bid/seasonality_adjusted/tab_bidshape_raw_vs_deseasonalized.tex"
DET = REPO / "data/processed/omie/mercado_diario/ofertas/det_all.parquet"
CAB = REPO / "data/processed/omie/mercado_diario/ofertas/cab_all.parquet"
MPDBC = REPO / "data/processed/omie/mercado_diario/precios/marginalpdbc_all.parquet"
UNITS = REPO / "data/external/omie_reference/lista_unidades.csv"

START = "2022-01-01"
END = "2026-05-15"
REGIME_DATES = [
    ("3sess",         pd.Timestamp("2024-06-14"), pd.Timestamp("2024-11-30")),
    ("ISP15win",      pd.Timestamp("2024-12-01"), pd.Timestamp("2025-03-18")),
    ("MTU15IDA_pre",  pd.Timestamp("2025-03-19"), pd.Timestamp("2025-04-27")),
    ("MTU15IDA_post", pd.Timestamp("2025-04-28"), pd.Timestamp("2025-09-30")),
    ("DA15_ID15",     pd.Timestamp("2025-10-01"), pd.Timestamp("2026-05-15")),
]
SHORT_REGIME = {
    "3sess": "3-sess",
    "ISP15win": "ISP15-win",
    "MTU15IDA_pre": "MTU15-IDA pre-blk",
    "MTU15IDA_post": "MTU15-IDA post-blk",
    "DA15_ID15": "DA15/ID15",
}
TECHS = ["CCGT", "Wind", "Solar_PV", "Hydro", "Hydro_pump", "Nuclear"]
TECH_LABEL = {
    "CCGT": "CCGT", "Wind": "Wind", "Solar_PV": "Solar PV",
    "Hydro": "Hydro", "Hydro_pump": "Hydro pump", "Nuclear": "Nuclear",
}


def tech_bucket(t):
    if t is None: return "Other"
    t = str(t).lower()
    if "ciclo combinado" in t: return "CCGT"
    if "nuclear" in t: return "Nuclear"
    if "hidráulica generación" in t: return "Hydro"
    if "bombeo" in t: return "Hydro_pump"
    if "re mercado eólica" in t: return "Wind"
    if "re mercado solar fotovolt" in t: return "Solar_PV"
    if "re mercado solar térmica" in t: return "Solar_Thermal"
    return "Other"


def build_raw_means():
    """Per-(tech, regime) raw mean in-band share — same data as the regression."""
    con = duckdb.connect()
    con.execute("SET memory_limit='12GB'")
    units = pd.read_csv(UNITS)[["unit_code", "technology"]]
    units["tech"] = units["technology"].apply(tech_bucket)
    con.register("u", units[["unit_code", "tech"]])
    H = 50.0
    sql = f"""
    WITH cab_last AS (
      SELECT CAST(date AS DATE) AS d, offer_code, unit_code,
             ROW_NUMBER() OVER (PARTITION BY CAST(date AS DATE), offer_code, unit_code
                                ORDER BY version DESC) AS rn
      FROM read_parquet('{CAB}') WHERE buy_sell='V' AND date >= '{START}' AND date <= '{END}'
    ),
    cab_l AS (SELECT d, offer_code, unit_code FROM cab_last WHERE rn=1),
    det AS (
      SELECT CAST(date AS DATE) AS d, offer_code, period, price_eur_mwh AS p,
             quantity_mw AS q, COALESCE(mtu_minutes, 60) AS mtu_minutes
      FROM read_parquet('{DET}') WHERE date >= '{START}' AND date <= '{END}' AND quantity_mw > 0
    ),
    mp AS (
      SELECT CAST(date AS DATE) AS d, period, price_es_eur_mwh AS p_clear,
             COALESCE(mtu_minutes, 60) AS mtu_p
      FROM read_parquet('{MPDBC}') WHERE date >= '{START}' AND date <= '{END}' AND price_es_eur_mwh IS NOT NULL
    ),
    joined AS (
      SELECT mp.d, mp.period, c.unit_code, dv.q, mp.p_clear,
             (dv.p BETWEEN mp.p_clear - {H} AND mp.p_clear + {H})::INT AS in_band,
             COALESCE(mp.mtu_p, dv.mtu_minutes) AS mtu_minutes
      FROM det dv JOIN cab_l c ON dv.d=c.d AND dv.offer_code=c.offer_code
      JOIN mp ON mp.d=dv.d AND mp.period=dv.period
    ),
    per_cell AS (
      SELECT d, period, unit_code,
             SUM(q * mtu_minutes/60.0) AS mw_total,
             SUM(q * mtu_minutes/60.0 * in_band) AS mw_in
      FROM joined GROUP BY 1, 2, 3
    ),
    per_tech AS (
      SELECT pc.d, u.tech,
             SUM(pc.mw_total) AS mw_total, SUM(pc.mw_in) AS mw_in
      FROM per_cell pc JOIN u ON pc.unit_code=u.unit_code
      GROUP BY 1, 2
    )
    SELECT d, tech, mw_in/NULLIF(mw_total,0) AS in_band_share
    FROM per_tech WHERE mw_total > 0 ORDER BY d, tech
    """
    df = con.execute(sql).fetchdf()
    df["d"] = pd.to_datetime(df["d"])
    df["regime"] = "preIDA"
    for lab, lo, hi in REGIME_DATES:
        m = (df["d"] >= lo) & (df["d"] <= hi)
        df.loc[m, "regime"] = lab
    return df.groupby(["tech", "regime"])["in_band_share"].mean().reset_index()


def main():
    coefs = pd.read_csv(COEFS)
    spA = coefs[(coefs["spec"] == "A_seasonality_only") & (coefs["outcome"] == "bidshape_in_band_share")]
    # spA has columns tech, regime, regime_level (deseasonalized)
    des = spA.set_index(["tech", "regime"])["regime_level"].unstack("regime")

    raw = build_raw_means()
    raw = raw.set_index(["tech", "regime"])["in_band_share"].unstack("regime")

    # Build LaTeX
    lines = []
    lines.append(r"% auto-built by build_deseasonalized_bidshape_table.py")
    lines.append(r"% Cells: raw mean / \textcolor{seasoncol}{deseasonalized mean from Spec A}")
    lines.append(r"\begin{tabular}{l " + "r " * len(REGIME_DATES) + r"}")
    lines.append(r"\toprule")
    header = " & ".join([r""] + [r"\textbf{" + SHORT_REGIME[r[0]] + "}" for r in REGIME_DATES]) + r" \\"
    lines.append(header)
    lines.append(r"\midrule")
    for tech in TECHS:
        cells = [TECH_LABEL[tech]]
        for r_lab, _, _ in REGIME_DATES:
            try:
                raw_val = raw.loc[tech, r_lab] * 100
                des_val = des.loc[tech, r_lab] * 100
                cells.append(f"{raw_val:.1f}~{{\\color{{seasoncol}}\\scriptsize[{des_val:.1f}]}}")
            except KeyError:
                cells.append("--")
        lines.append(" & ".join(cells) + r" \\")
    lines.append(r"\bottomrule")
    lines.append(r"\end{tabular}")

    tex = "\n".join(lines) + "\n"
    OUT_TEX.parent.mkdir(parents=True, exist_ok=True)
    OUT_TEX.write_text(tex)
    print(f"wrote {OUT_TEX}")
    print(tex)


if __name__ == "__main__":
    main()
