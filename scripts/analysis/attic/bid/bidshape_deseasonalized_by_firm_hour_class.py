# STATUS: ALIVE
# LAST-AUDIT: 2026-05-19
# CLAIM: Per-(firm, tech, hour-class) raw + SA in-band share tables. Sibling of
#        bidshape_deseasonalized_by_hour_class.py with FIRM added to the
#        grouping. Each (firm, tech, hour-class, day) is one observation; the
#        SA regression is fit per (firm, tech, hour-class).
#
# OUT:
#   results/regressions/bid/seasonality_adjusted/
#     tab_bidshape_DA_by_firm_hour_class_deseasonalized.tex
#     tab_bidshape_IDA_by_firm_hour_class_deseasonalized.tex

from __future__ import annotations
from pathlib import Path
import sys

import duckdb
import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
if str(REPO / "src") not in sys.path:
    sys.path.insert(0, str(REPO / "src"))
from mtu.analysis.sa_fwl import fit_sa, attach_design_columns  # noqa: E402

DET = REPO / "data/processed/omie/mercado_diario/ofertas/det_all.parquet"
CAB = REPO / "data/processed/omie/mercado_diario/ofertas/cab_all.parquet"
IDET = REPO / "data/processed/omie/mercado_intradiario_subastas/ofertas/idet_all.parquet"
ICAB = REPO / "data/processed/omie/mercado_intradiario_subastas/ofertas/icab_all.parquet"
MPDBC = REPO / "data/processed/omie/mercado_diario/precios/marginalpdbc_all.parquet"
MPIBC = REPO / "data/processed/omie/mercado_intradiario_subastas/precios/marginalpibc_all.parquet"
UNITS = REPO / "data/external/omie_reference/lista_unidades.csv"
OUT_DIR = REPO / "results/regressions/bid/seasonality_adjusted"

START = "2022-01-01"
END = "2026-05-15"
K_HARMONICS = 4
H = 50.0

REGIME_DATES = [
    ("3sess",         pd.Timestamp("2024-06-14"), pd.Timestamp("2024-11-30"), "3-sess"),
    ("ISP15win",      pd.Timestamp("2024-12-01"), pd.Timestamp("2025-03-18"), "ISP15-win"),
    ("MTU15IDA_pre",  pd.Timestamp("2025-03-19"), pd.Timestamp("2025-04-27"), "DA60/ID15 pre-blk"),
    ("MTU15IDA_post", pd.Timestamp("2025-04-28"), pd.Timestamp("2025-09-30"), "DA60/ID15 post-blk"),
    ("DA15_ID15",     pd.Timestamp("2025-10-01"), pd.Timestamp("2026-05-15"), "DA15/ID15"),
]

HOUR_CLASS = {
    "Critical": [5, 6, 7, 8, 16, 17, 18, 19, 20, 21, 22],
    "Flat":     [1, 2, 3],
    "Midday":   [11, 12, 13, 14],
}

TECHS = ["CCGT", "Hydro", "Hydro pump", "Nuclear", "Wind", "Solar PV", "Solar Thermal", "Cogen"]
TECH_KEY = {
    "CCGT": "CCGT", "Hydro": "Hydro", "Hydro pump": "Hydro_pump",
    "Nuclear": "Nuclear", "Wind": "Wind", "Solar PV": "Solar_PV",
    "Solar Thermal": "Solar_Thermal", "Cogen": "Cogen",
}

FIRMS_ORDER = ["IB", "GE", "GN", "HC", "REP", "OTH"]


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
    if "re mercado térmica no renovab" in t: return "Cogen"
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


def build_panel(market: str) -> pd.DataFrame:
    """Per (date, firm, tech, hour_class) daily in-band share."""
    con = duckdb.connect()
    con.execute("SET memory_limit='12GB'")
    con.execute("SET threads=4")
    units = pd.read_csv(UNITS)
    units["tech"] = units["technology"].apply(tech_bucket)
    units["firm"] = units["owner_agent"].apply(firm_bucket)
    con.register("u", units[["unit_code", "tech", "firm"]])

    if market == "DA":
        cab_p, det_p, mp_p = CAB, DET, MPDBC
        sess_col = sess_part = sess_join_cab = sess_join_mp = ""
    else:
        cab_p, det_p, mp_p = ICAB, IDET, MPIBC
        sess_col = ", session_number"
        sess_part = ", session_number"
        sess_join_cab = " AND dv.session_number = c.session_number"
        sess_join_mp = " AND mp.session_number = dv.session_number"

    sql = f"""
    WITH cab_last AS (
      SELECT CAST(date AS DATE) AS d, offer_code, unit_code{sess_col},
             ROW_NUMBER() OVER (PARTITION BY CAST(date AS DATE), offer_code, unit_code{sess_part}
                                ORDER BY version DESC) AS rn
      FROM read_parquet('{cab_p}')
      WHERE date >= '{START}' AND date <= '{END}' AND buy_sell='V'
    ),
    cab_l AS (SELECT d, offer_code, unit_code{sess_col} FROM cab_last WHERE rn=1),
    det AS (
      SELECT CAST(date AS DATE) AS d, offer_code, period{sess_col},
             price_eur_mwh AS p, quantity_mw AS q, COALESCE(mtu_minutes, 60) AS mtu
      FROM read_parquet('{det_p}')
      WHERE date >= '{START}' AND date <= '{END}' AND quantity_mw > 0
    ),
    mp AS (
      SELECT CAST(date AS DATE) AS d, period{sess_col},
             price_es_eur_mwh AS p_clear, COALESCE(mtu_minutes, 60) AS mtu_p
      FROM read_parquet('{mp_p}')
      WHERE date >= '{START}' AND date <= '{END}' AND price_es_eur_mwh IS NOT NULL
    ),
    joined AS (
      SELECT mp.d, mp.period, c.unit_code, dv.q, mp.p_clear,
             (dv.p BETWEEN mp.p_clear - {H} AND mp.p_clear + {H})::INT AS in_band,
             COALESCE(mp.mtu_p, dv.mtu) AS mtu_minutes,
             CASE WHEN COALESCE(mp.mtu_p, dv.mtu) = 60 THEN mp.period - 1
                  ELSE CAST(FLOOR((mp.period - 1) / 4.0) AS INT) END AS clock_hour
      FROM det dv JOIN cab_l c ON dv.d=c.d AND dv.offer_code=c.offer_code{sess_join_cab}
        JOIN mp ON mp.d=dv.d AND mp.period=dv.period{sess_join_mp}
    ),
    per_cell AS (
      SELECT d, clock_hour, period, unit_code,
             SUM(q * mtu_minutes/60.0) AS mw_total,
             SUM(q * mtu_minutes/60.0 * in_band) AS mw_in
      FROM joined GROUP BY 1, 2, 3, 4
    ),
    per_firm_tech_hour AS (
      SELECT pc.d, u.firm, u.tech, pc.clock_hour,
             SUM(pc.mw_total) AS mw_total,
             SUM(pc.mw_in) AS mw_in
      FROM per_cell pc JOIN u ON pc.unit_code = u.unit_code
      GROUP BY 1, 2, 3, 4
    )
    SELECT d, firm, tech, clock_hour, mw_in / NULLIF(mw_total, 0) AS in_band_share, mw_total
    FROM per_firm_tech_hour WHERE mw_total > 0
    """
    df = con.execute(sql).fetchdf()
    df["d"] = pd.to_datetime(df["d"])

    def assign_hc(h):
        for hc, hs in HOUR_CLASS.items():
            if int(h) in hs:
                return hc
        return None
    df["hour_class"] = df["clock_hour"].apply(assign_hc)
    df = df[df["hour_class"].notna()].copy()
    df["mw_in"] = df["in_band_share"] * df["mw_total"]
    agg = (df.groupby(["d", "firm", "tech", "hour_class"])
             .agg(mw_in=("mw_in", "sum"), mw_total=("mw_total", "sum"))
             .reset_index())
    agg["in_band_share"] = agg["mw_in"] / agg["mw_total"]
    return agg[["d", "firm", "tech", "hour_class", "in_band_share"]]


def add_covariates(df):
    return attach_design_columns(df, [r[:3] for r in REGIME_DATES], K=K_HARMONICS)


def fit_and_get_adjusted(df_t):
    res = fit_sa(df_t, "in_band_share", [r[:3] for r in REGIME_DATES],
                 transform="logit", K=K_HARMONICS, min_obs=80)
    if res is None:
        return None
    out = {"_base": res["baseline_sa"] * 100.0}
    for label, _, _, _ in REGIME_DATES:
        out[label] = res[f"{label}_sa"] * 100.0
    return out


def build_tex_table(market: str, panel: pd.DataFrame) -> str:
    panel = panel.copy()
    panel["regime"] = "preIDA"
    for label, lo, hi, _ in REGIME_DATES:
        m = (panel["d"] >= lo) & (panel["d"] <= hi)
        panel.loc[m, "regime"] = label
    raw = (panel.groupby(["firm", "tech", "hour_class", "regime"])["in_band_share"].mean()
                .reset_index()
                .pivot_table(index=["firm", "tech", "hour_class"],
                             columns="regime", values="in_band_share"))

    adj_rows = {}
    panel_cov = add_covariates(panel.copy())
    for tech in [TECH_KEY[t] for t in TECHS]:
        for firm in FIRMS_ORDER:
            for hc in HOUR_CLASS:
                sub = panel_cov[(panel_cov["tech"] == tech)
                                & (panel_cov["firm"] == firm)
                                & (panel_cov["hour_class"] == hc)]
                if len(sub) < 80:
                    continue
                res = fit_and_get_adjusted(sub)
                if res is None:
                    continue
                adj_rows[(tech, firm, hc)] = res

    lines = [
        f"% auto-built by bidshape_deseasonalized_by_firm_hour_class.py ({market})",
        r"% Cells: raw share / \textcolor{seasoncol}{Spec A FWL deseasonalized share}",
        r"\begin{tabular}{l l l r r r r r}",
        r"\toprule",
        r"Tech & Firm & Hour-cl & " + " & ".join(r[3] for r in REGIME_DATES) + r" \\",
        r"\midrule",
    ]
    last_tech, last_firm = None, None
    for tech_label in TECHS:
        tk = TECH_KEY[tech_label]
        for firm in FIRMS_ORDER:
            for hc in ["Critical", "Flat", "Midday"]:
                key = (tk, firm, hc)
                row_exists = (key in adj_rows) or (
                    (firm, tk, hc) in raw.index and not raw.loc[(firm, tk, hc)].isna().all()
                )
                if not row_exists:
                    continue
                tech_lbl = tech_label if tech_label != last_tech else ""
                firm_lbl = firm if (tech_label != last_tech or firm != last_firm) else ""
                if tech_label != last_tech and last_tech is not None:
                    lines.append(r"\addlinespace")
                last_tech, last_firm = tech_label, firm
                cells = [tech_lbl, firm_lbl, hc]
                for label, _, _, _ in REGIME_DATES:
                    try:
                        raw_v = raw.loc[(firm, tk, hc), label] * 100
                    except KeyError:
                        raw_v = float("nan")
                    adj_v = adj_rows.get(key, {}).get(label, float("nan"))
                    if pd.isna(raw_v) and pd.isna(adj_v):
                        cells.append("--")
                    elif pd.isna(adj_v):
                        cells.append(f"{raw_v:.1f}")
                    else:
                        cells.append(f"{raw_v:.1f}~{{\\color{{seasoncol}}\\scriptsize[{adj_v:.1f}]}}")
                lines.append(" & ".join(cells) + r" \\")
    lines.extend([r"\bottomrule", r"\end{tabular}"])
    return "\n".join(lines) + "\n"


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    for market, out_name in [
        ("DA", "tab_bidshape_DA_by_firm_hour_class_deseasonalized.tex"),
        ("IDA", "tab_bidshape_IDA_by_firm_hour_class_deseasonalized.tex"),
    ]:
        print(f"\n=== Market: {market} ===")
        panel = build_panel(market)
        print(f"  panel rows: {len(panel):,}, days: {panel['d'].nunique()}, "
              f"({panel['firm'].nunique()} firms × {panel['tech'].nunique()} techs × 3 hcs)")
        tex = build_tex_table(market, panel)
        out_path = OUT_DIR / out_name
        out_path.write_text(tex)
        print(f"  wrote {out_path}")


if __name__ == "__main__":
    main()
