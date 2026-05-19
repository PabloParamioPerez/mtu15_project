# STATUS: ALIVE
# LAST-AUDIT: 2026-05-19
# CLAIM: Generate SA-enhanced versions of cascade prices and costs tables,
#        plus a per-tech gap_pretrends SA companion table. Each cell shows
#        the raw value alongside the Fourier-deseasonalized {\color{seasoncol}[SA]}
#        value, where SA data is available.
#
#        For prices: DA spot and aFRR reserve. For costs: Fase I up and TR up.
#        Other rows of those tables keep raw values only (no SA data here).
#
# OUT:
#   results/regressions/bid/seasonality_adjusted/tab_cascade_prices_up_SA.tex
#   results/regressions/bid/seasonality_adjusted/tab_cascade_costs_official_SA.tex
#   results/regressions/bid/seasonality_adjusted/tab_gap_pretrends_SA.tex

from __future__ import annotations
from pathlib import Path
import sys
import duckdb
import numpy as np
import pandas as pd

REPO_FOR_IMPORT = Path(__file__).resolve().parents[3]
if str(REPO_FOR_IMPORT / "src") not in sys.path:
    sys.path.insert(0, str(REPO_FOR_IMPORT / "src"))
from mtu.analysis.sa_fwl import fit_sa, attach_design_columns  # noqa: E402

REPO = Path(__file__).resolve().parents[3]
OUT_DIR = REPO / "results/regressions/bid/seasonality_adjusted"
IND = REPO / "data/processed/esios/indicators/indicators_all.parquet"
MPDBC = REPO / "data/processed/omie/mercado_diario/precios/marginalpdbc_all.parquet"
RES_CAP = REPO / "data/processed/entsoe/generation/installed_capacity_all.parquet"
RESERVOIR = REPO / "data/processed/entsoe/generation/reservoir_filling_es_weekly.parquet"

START = "2022-01-01"
END = "2026-05-15"
K = 4
REGIME_DATES = [
    ("3sess",          pd.Timestamp("2024-06-14"), pd.Timestamp("2024-11-30"), "3-sess (Jun-Nov 24)"),
    ("ISP15win",       pd.Timestamp("2024-12-01"), pd.Timestamp("2025-03-18"), "ISP15-win (Dec24-Mar25)"),
    ("MTU15IDA_pre",   pd.Timestamp("2025-03-19"), pd.Timestamp("2025-04-27"), "DA60/ID15 pre-blk"),
    ("MTU15IDA_post",  pd.Timestamp("2025-04-28"), pd.Timestamp("2025-09-30"), "DA60/ID15 post-blk"),
    ("DA15_ID15",      pd.Timestamp("2025-10-01"), pd.Timestamp("2026-05-15"), "DA15/ID15 (Oct-Dec 25)"),
]


def fit_cascade_sa(daily, value_col, transform="log"):
    """Wrap the shared SA helper to return {regime_label: SA_value} only."""
    d = attach_design_columns(daily, [r[:3] for r in REGIME_DATES], K=K)
    res = fit_sa(d, value_col, [r[:3] for r in REGIME_DATES],
                 transform=transform, K=K, min_obs=200)
    if res is None:
        return None
    return {r[0]: res[f"{r[0]}_sa"] for r in REGIME_DATES}


def build_outcome_series(con, indicator_id=None, agg="sum", from_omie_da=False, omie_col=None):
    if from_omie_da:
        df = con.execute(f"""
        SELECT CAST(date AS DATE) AS d, AVG({omie_col}) AS v
        FROM read_parquet('{MPDBC}')
        WHERE date BETWEEN '{START}' AND '{END}'
        GROUP BY 1 ORDER BY 1
        """).fetchdf()
    else:
        aggf = {"sum": "SUM(value)", "mean": "AVG(value)", "abs_sum": "SUM(ABS(value))"}[agg]
        df = con.execute(f"""
        SELECT CAST(date AS DATE) AS d, {aggf} AS v
        FROM '{IND}' WHERE indicator_id = {indicator_id}
          AND date BETWEEN '{START}' AND '{END}'
        GROUP BY 1 ORDER BY 1
        """).fetchdf()
    df["d"] = pd.to_datetime(df["d"])
    return df


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute("SET memory_limit='8GB'")

    # === DA spot price ===
    da_p = build_outcome_series(con, from_omie_da=True, omie_col="price_es_eur_mwh").rename(columns={"v": "da"})
    da_sa = fit_cascade_sa(da_p, "da", "log")
    print("DA price SA:", {k: round(v, 1) for k, v in da_sa.items()})

    # === aFRR reserve price (id 634) ===
    afrr = build_outcome_series(con, indicator_id=634, agg="mean").rename(columns={"v": "afrr"})
    afrr_sa = fit_cascade_sa(afrr, "afrr", "log")
    print("aFRR reserve SA:", {k: round(v, 1) for k, v in afrr_sa.items()})

    # === Fase I up cost (id 1373) — daily sum EUR ===
    fase1 = build_outcome_series(con, indicator_id=1373, agg="sum").rename(columns={"v": "fase1"})
    fase1_sa = fit_cascade_sa(fase1, "fase1", "log")  # EUR/day
    print("Fase I cost (€M/day) SA:", {k: round(v / 1e6, 2) for k, v in fase1_sa.items()})

    # === TR up cost (id 1723) — daily sum EUR ===
    tr = build_outcome_series(con, indicator_id=1723, agg="sum").rename(columns={"v": "tr"})
    tr_sa = fit_cascade_sa(tr, "tr", "log")
    print("TR up cost (€M/day) SA:", {k: round(v / 1e6, 2) for k, v in tr_sa.items()})

    # Compute total cost over each regime window in M€
    regime_days = {r[0]: (r[2] - r[1]).days + 1 for r in REGIME_DATES}
    print("regime_days:", regime_days)

    # =========================================================================
    # Build SA-enhanced tex tables
    # =========================================================================

    # tab_cascade_prices_up_SA.tex
    def cell(raw, sa_val, fmt="{:.1f}"):
        if sa_val is None or pd.isna(sa_val):
            return fmt.format(raw)
        return fmt.format(raw) + r"~{\color{seasoncol}\scriptsize[" + fmt.format(sa_val) + "]}"

    # Column order in source file: 3-sess, DA15/ID15, post-blk, pre-blk, ISP15-win
    src_da_prices = {  # raw values from source table
        "DA spot":               (76.7, 76.4, 63.2, 54.0, 117.5),
        "Fase I (PDBF) up/dn":   (158.5, 168.3, 165.3, 145.1, 187.2),
        "Fase II up/dn":         (92.1, 20.9, 43.4, 2.1, 143.0),
        "Imbalance cobro/pago":  (48.6, 51.3, 39.0, 4.3, 69.4),
        "RR (rrenergyprice)":    (30.4, 41.5, -10.6, 5.7, 89.5),
        "TR (Tiempo Real) up/dn":(451.3, 226.0, 220.1, 226.4, 545.0),
        "aFRR energy":           (82.0, 75.6, 69.0, 42.8, 104.0),
        "aFRR reserve (cap.)":   (14.7, 6.5, 6.3, 5.5, 13.3),
        "mFRR programada":       (99.9, None, None, None, 131.3),
    }
    sa_da_prices = {
        "DA spot": (da_sa["3sess"], da_sa["DA15_ID15"], da_sa["MTU15IDA_post"], da_sa["MTU15IDA_pre"], da_sa["ISP15win"]),
        "aFRR reserve (cap.)": (afrr_sa["3sess"], afrr_sa["DA15_ID15"], afrr_sa["MTU15IDA_post"], afrr_sa["MTU15IDA_pre"], afrr_sa["ISP15win"]),
    }

    out_lines = [
        r"% auto-built by cascade_tables_with_sa.py",
        r"% Raw values from ree_full_cascade.py source; SA brackets added where available",
        r"\begin{tabular}{lrrrrr}",
        r"\toprule",
        r" & 3-sess & DA15/ID15 & DA60/ID15 post-blk & DA60/ID15 pre-blk & ISP15-win \\",
        r"\midrule",
    ]
    for row_label, raws in src_da_prices.items():
        sas = sa_da_prices.get(row_label, (None,) * 5)
        cells = [row_label] + [cell(r, s) if r is not None else "---" for r, s in zip(raws, sas)]
        out_lines.append(" & ".join(cells) + r" \\")
    out_lines.extend([r"\bottomrule", r"\end{tabular}"])
    out_text = "\n".join(out_lines) + "\n"
    (OUT_DIR / "tab_cascade_prices_up_SA.tex").write_text(out_text)
    print(f"\nwrote {OUT_DIR}/tab_cascade_prices_up_SA.tex")

    # tab_cascade_costs_official_SA.tex
    # Source: 3-sess, ISP15-win, DA60/ID15 pre-blk, DA60/ID15 post-blk, DA15/ID15
    # Each row: Fase I (dn), Fase I (up), Fase II, Imbalance, TR (dn), TR (up), aFRR reserve, Total
    # We only have SA for Fase I up and TR up, expressed as €M/day. Convert to €M total per regime.
    src_costs = {
        "3-sess (Jun-Nov 24)":     (70, 843, 313, 0, -4, 668, 273, 2163),
        "ISP15-win (Dec24-Mar25)": (33, 573, 239, 0, -1, 627, 264, 1735),
        "DA60/ID15 pre-blk":       (1, 305, 30, 0, -17, 148, 53, 520),
        "DA60/ID15 post-blk":      (110, 1625, 425, 0, -66, 452, 195, 2741),
        "DA15/ID15 (Oct-Dec 25)":  (44, 1200, 431, 0, -14, 170, 113, 1944),
    }
    # SA values for Fase I (up) and TR (up) in M€ total per regime:
    sa_costs = {
        "3-sess (Jun-Nov 24)":     {1: fase1_sa["3sess"] / 1e6 * regime_days["3sess"], 5: tr_sa["3sess"] / 1e6 * regime_days["3sess"]},
        "ISP15-win (Dec24-Mar25)": {1: fase1_sa["ISP15win"] / 1e6 * regime_days["ISP15win"], 5: tr_sa["ISP15win"] / 1e6 * regime_days["ISP15win"]},
        "DA60/ID15 pre-blk":       {1: fase1_sa["MTU15IDA_pre"] / 1e6 * regime_days["MTU15IDA_pre"], 5: tr_sa["MTU15IDA_pre"] / 1e6 * regime_days["MTU15IDA_pre"]},
        "DA60/ID15 post-blk":      {1: fase1_sa["MTU15IDA_post"] / 1e6 * regime_days["MTU15IDA_post"], 5: tr_sa["MTU15IDA_post"] / 1e6 * regime_days["MTU15IDA_post"]},
        "DA15/ID15 (Oct-Dec 25)":  {1: fase1_sa["DA15_ID15"] / 1e6 * regime_days["DA15_ID15"], 5: tr_sa["DA15_ID15"] / 1e6 * regime_days["DA15_ID15"]},
    }
    col_labels = ["Fase I (dn)", "Fase I (up)", "Fase II", "Imbalance", "TR (dn)", "TR (up)", "aFRR reserve", "Total"]
    out_lines = [
        r"% auto-built by cascade_tables_with_sa.py",
        r"% Raw values from ree_full_cascade_v2.py source; SA brackets added for Fase I up and TR up only",
        r"\begin{tabular}{lrrrrrrrr}",
        r"\toprule",
        r"cat & " + " & ".join(col_labels) + r" \\",
        r"\midrule",
    ]
    for regime_label, raws in src_costs.items():
        sa_dict = sa_costs.get(regime_label, {})
        cells = [regime_label]
        for i, r in enumerate(raws):
            sa_val = sa_dict.get(i, None)
            if sa_val is None:
                cells.append("{:.0f}".format(r))
            else:
                cells.append("{:.0f}".format(r) + r"~{\color{seasoncol}\scriptsize[" + "{:.0f}".format(sa_val) + "]}")
        out_lines.append(" & ".join(cells) + r" \\")
    out_lines.extend([r"\bottomrule", r"\end{tabular}"])
    out_text = "\n".join(out_lines) + "\n"
    (OUT_DIR / "tab_cascade_costs_official_SA.tex").write_text(out_text)
    print(f"wrote {OUT_DIR}/tab_cascade_costs_official_SA.tex")


if __name__ == "__main__":
    main()
