# STATUS: ALIVE
# LAST-AUDIT: 2026-05-19
# CLAIM: Generate raw + seasonality-adjusted bid-shape tables matching the
#        format of tab_bidshape_DA_by_regime_normalized.tex and IDA equivalent,
#        with adjusted values in [seasoncol] color brackets per cell.
#
#        For each (market, tech, hour_class) cell, fit Spec A (seasonality
#        only) via the shared SA helper:
#          logit(share_t) = α + Σ_r β_r·D_regime
#                            + Σ_k Fourier_k(t)
#                            + Σ_j δ_j·1{dow(t)=j} + ε_t
#        Adjusted cell = Duan-smeared sigmoid of (α + β_r) at within-week DOW
#        mean, annual-mean Fourier. Pooled 2022-2026 daily-(tech, hour-class)
#        panel. No HAC — point-estimation device.
#
# Hour-class definition (matches §1 of descriptive_facts.tex):
#   Critical: clock hours 5,6,7,8,16,17,18,19,20,21,22
#   Flat:     clock hours 1,2,3
#   Midday:   clock hours 11,12,13,14
#
# OUT:
#   results/regressions/bid/seasonality_adjusted/tab_bidshape_DA_by_regime_deseasonalized.tex
#   results/regressions/bid/seasonality_adjusted/tab_bidshape_IDA_by_regime_deseasonalized.tex

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
DET = REPO / "data/processed/omie/mercado_diario/ofertas/det_all.parquet"
CAB = REPO / "data/processed/omie/mercado_diario/ofertas/cab_all.parquet"
IDET = REPO / "data/processed/omie/mercado_intradiario_subastas/ofertas/idet_all.parquet"
ICAB = REPO / "data/processed/omie/mercado_intradiario_subastas/ofertas/icab_all.parquet"
MPDBC = REPO / "data/processed/omie/mercado_diario/precios/marginalpdbc_all.parquet"
MPIBC = REPO / "data/processed/omie/mercado_intradiario_subastas/precios/marginalpibc_all.parquet"
UNITS = REPO / "data/external/omie_reference/lista_unidades.csv"
RES_CAP = REPO / "data/processed/entsoe/generation/installed_capacity_all.parquet"
RESERVOIR = REPO / "data/processed/entsoe/generation/reservoir_filling_es_weekly.parquet"
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
RES_GW_ANNUAL = {2022: 48.6, 2023: 56.3, 2024: 61.0, 2025: 65.5, 2026: 67.0}

# Hour-class clock-hour membership (per doc §1).
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


def build_panel(market: str) -> pd.DataFrame:
    """Per (date, tech, hour_class) daily in-band share. market ∈ {'DA', 'IDA'}."""
    con = duckdb.connect()
    con.execute("SET memory_limit='12GB'")
    con.execute("SET threads=4")
    units = pd.read_csv(UNITS)[["unit_code", "technology"]]
    units["tech"] = units["technology"].apply(tech_bucket)
    con.register("u", units[["unit_code", "tech"]])

    if market == "DA":
        cab_p, det_p, mp_p = CAB, DET, MPDBC
        sess_col = ""
        sess_part = ""
        sess_join_cab = ""
        sess_join_mp = ""
    else:  # IDA — same buy_sell filter; carry session_number through joins
        cab_p, det_p, mp_p = ICAB, IDET, MPIBC
        sess_col = ", session_number"
        sess_part = ", session_number"
        sess_join_cab = " AND dv.session_number = c.session_number"
        sess_join_mp = " AND mp.session_number = dv.session_number"
    buysell_filter = "AND buy_sell = 'V'"
    price_col = "price_es_eur_mwh"

    # Map clock-hour from period+mtu
    # MTU60: clock_hour = period - 1
    # MTU15: clock_hour = (period - 1) // 4
    sql = f"""
    WITH cab_last AS (
      SELECT CAST(date AS DATE) AS d, offer_code, unit_code{sess_col},
             ROW_NUMBER() OVER (PARTITION BY CAST(date AS DATE), offer_code, unit_code{sess_part}
                                ORDER BY version DESC) AS rn
      FROM read_parquet('{cab_p}')
      WHERE date >= '{START}' AND date <= '{END}' {buysell_filter}
    ),
    cab_l AS (SELECT d, offer_code, unit_code{sess_col} FROM cab_last WHERE rn=1),
    det AS (
      SELECT CAST(date AS DATE) AS d, offer_code, period{sess_col}, price_eur_mwh AS p,
             quantity_mw AS q, COALESCE(mtu_minutes, 60) AS mtu
      FROM read_parquet('{det_p}')
      WHERE date >= '{START}' AND date <= '{END}' AND quantity_mw > 0
    ),
    mp AS (
      SELECT CAST(date AS DATE) AS d, period{sess_col}, {price_col} AS p_clear,
             COALESCE(mtu_minutes, 60) AS mtu_p
      FROM read_parquet('{mp_p}')
      WHERE date >= '{START}' AND date <= '{END}' AND {price_col} IS NOT NULL
    ),
    joined AS (
      SELECT mp.d, mp.period, c.unit_code, dv.q, mp.p_clear,
             (dv.p BETWEEN mp.p_clear - {H} AND mp.p_clear + {H})::INT AS in_band,
             COALESCE(mp.mtu_p, dv.mtu) AS mtu_minutes,
             CASE WHEN COALESCE(mp.mtu_p, dv.mtu) = 60 THEN mp.period - 1
                  ELSE (mp.period - 1) / 4 END AS clock_hour
      FROM det dv JOIN cab_l c ON dv.d=c.d AND dv.offer_code=c.offer_code{sess_join_cab}
        JOIN mp ON mp.d=dv.d AND mp.period=dv.period{sess_join_mp}
    ),
    per_cell AS (
      SELECT d, clock_hour, period, unit_code,
             SUM(q * mtu_minutes/60.0) AS mw_total,
             SUM(q * mtu_minutes/60.0 * in_band) AS mw_in
      FROM joined GROUP BY 1, 2, 3, 4
    ),
    per_tech_hour AS (
      SELECT pc.d, u.tech, pc.clock_hour,
             SUM(pc.mw_total) AS mw_total, SUM(pc.mw_in) AS mw_in
      FROM per_cell pc JOIN u ON pc.unit_code=u.unit_code
      GROUP BY 1, 2, 3
    )
    SELECT d, tech, clock_hour, mw_in/NULLIF(mw_total,0) AS in_band_share, mw_total
    FROM per_tech_hour WHERE mw_total > 0
    ORDER BY d, tech, clock_hour
    """
    df = con.execute(sql).fetchdf()
    df["d"] = pd.to_datetime(df["d"])

    # Compute hour-class daily aggregate (weighted by MW total)
    def assign_hc(h):
        for hc, hs in HOUR_CLASS.items():
            if int(h) in hs:
                return hc
        return None
    df["hour_class"] = df["clock_hour"].apply(assign_hc)
    df = df[df["hour_class"].notna()].copy()

    # Weighted by mw_total to give the hour-class daily mean share
    df["mw_in"] = df["in_band_share"] * df["mw_total"]
    agg = df.groupby(["d", "tech", "hour_class"]).agg(
        mw_in=("mw_in", "sum"), mw_total=("mw_total", "sum")).reset_index()
    agg["in_band_share"] = agg["mw_in"] / agg["mw_total"]
    return agg[["d", "tech", "hour_class", "in_band_share"]]


def add_covariates(df):
    # Bid-shape SA: Spec A only — no exogenous controls required beyond regime
    # dummies, Fourier, and DOW (all attached by the shared helper).
    return attach_design_columns(df, [r[:3] for r in REGIME_DATES], K=K_HARMONICS)


def fit_and_get_adjusted(df_t):
    res = fit_sa(df_t, "in_band_share", [r[:3] for r in REGIME_DATES],
                 transform="logit", K=K_HARMONICS, min_obs=100)
    if res is None:
        return None
    out = {"_base": res["baseline_sa"] * 100.0}
    for label, _, _, _ in REGIME_DATES:
        out[label] = res[f"{label}_sa"] * 100.0
    return out


def build_tex_table(market: str, panel: pd.DataFrame) -> str:
    """Generate tex table matching the existing format but with adjusted values."""
    # Compute raw means per (tech, hour_class, regime)
    panel = panel.copy()
    panel["regime"] = "preIDA"
    for label, lo, hi, _ in REGIME_DATES:
        m = (panel["d"] >= lo) & (panel["d"] <= hi)
        panel.loc[m, "regime"] = label

    raw = (panel.groupby(["tech", "hour_class", "regime"])["in_band_share"].mean()
           .reset_index().pivot_table(index=["hour_class", "tech"], columns="regime", values="in_band_share"))

    # Compute adjusted values per (tech, hour_class)
    adj_rows = {}
    panel_cov = add_covariates(panel.copy())
    for tech in [TECH_KEY[t] for t in TECHS]:
        for hc in HOUR_CLASS:
            sub = panel_cov[(panel_cov["tech"] == tech) & (panel_cov["hour_class"] == hc)]
            if len(sub) < 200:
                continue
            res = fit_and_get_adjusted(sub)
            if res is None:
                continue
            adj_rows[(tech, hc)] = res

    # Build tex
    lines = [
        f"% auto-built by bidshape_deseasonalized_by_hour_class.py ({market})",
        r"% Cells: raw share / \textcolor{seasoncol}{Spec A FWL deseasonalized share}",
        r"\begin{tabular}{l r r r r r}",
        r"\toprule",
        " & " + " & ".join(r[3] for r in REGIME_DATES) + r" \\",
        r"\midrule",
    ]
    for hc in ["Critical", "Flat", "Midday"]:
        lines.append(r"\multicolumn{6}{l}{\textit{Hour-class: " + hc + r"}} \\")
        for tech_label in TECHS:
            tk = TECH_KEY[tech_label]
            cells = [tech_label]
            for label, _, _, _ in REGIME_DATES:
                try:
                    raw_v = raw.loc[(hc, tk), label] * 100
                except KeyError:
                    raw_v = float("nan")
                adj_v = adj_rows.get((tk, hc), {}).get(label, float("nan"))
                if pd.isna(raw_v) and pd.isna(adj_v):
                    cells.append("--")
                elif pd.isna(adj_v):
                    cells.append(f"{raw_v:.1f}")
                else:
                    cells.append(f"{raw_v:.1f}~{{\\color{{seasoncol}}\\scriptsize[{adj_v:.1f}]}}")
            lines.append(" & ".join(cells) + r" \\")
        if hc != "Midday":
            lines.append(r"\midrule")
    lines.extend([r"\bottomrule", r"\end{tabular}"])
    return "\n".join(lines) + "\n"


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    for market, out_name in [("DA", "tab_bidshape_DA_by_regime_deseasonalized.tex"),
                              ("IDA", "tab_bidshape_IDA_by_regime_deseasonalized.tex")]:
        print(f"\n=== Market: {market} ===")
        print("Building per-(tech, hour_class) panel...")
        panel = build_panel(market)
        print(f"  panel rows: {len(panel):,}, days: {panel['d'].nunique()}")
        print("Building tex table with adjusted values...")
        tex = build_tex_table(market, panel)
        out_path = OUT_DIR / out_name
        out_path.write_text(tex)
        print(f"wrote {out_path}")


if __name__ == "__main__":
    main()
