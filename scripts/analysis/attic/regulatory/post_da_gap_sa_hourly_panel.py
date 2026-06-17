# STATUS: ALIVE
# LAST-AUDIT: 2026-05-20
# CLAIM: Per-(firm, tech, date, clock_hour) post-DA gap panel with a SA
#        column. Hour-disaggregated companion to post_da_gap_sa_daily_panel.py
#        used by the per-tech hour-conditional 3D ridge figures.
#        Per-(firm, tech, clock_hour) FWL regression on gap_gwh with identity
#        link, K=4 annual Fourier, six DOW dummies, regime-pooled seasonality
#        (sa_fwl.py spec). Strips Fourier + DOW deviation, keeps regime
#        contribution + idiosyncratic residual.
#
# OUT: data/derived/panels/post_da_gap_sa_hourly.parquet
#      columns: d, clock_hour, firm, tech, gap_gwh, gap_gwh_sa

from pathlib import Path
import sys

import duckdb
import numpy as np
import pandas as pd
import statsmodels.api as sm

REPO = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO / "src"))
from mtu.analysis.sa_fwl import fourier_terms, dow_dummies, DEFAULT_K  # noqa: E402

PDBC = REPO / "data/processed/omie/mercado_diario/programas/pdbc_all.parquet"
PHF  = REPO / "data/processed/omie/mercado_intradiario_subastas/programas/phf_all.parquet"
UNITS_CSV = REPO / "data/external/omie_reference/lista_unidades.csv"
OUT_DIR = REPO / "data/derived/panels"
OUT_DIR.mkdir(parents=True, exist_ok=True)
OUT = OUT_DIR / "post_da_gap_sa_hourly.parquet"

START = "2024-06-14"
END = "2026-05-15"
K = DEFAULT_K
MIN_OBS = 80

REGIME_DATES = [
    ("3sess",         pd.Timestamp("2024-06-14"), pd.Timestamp("2024-11-30")),
    ("ISP15win",      pd.Timestamp("2024-12-01"), pd.Timestamp("2025-03-18")),
    ("MTU15IDA_pre",  pd.Timestamp("2025-03-19"), pd.Timestamp("2025-04-27")),
    ("MTU15IDA_post", pd.Timestamp("2025-04-28"), pd.Timestamp("2025-09-30")),
    ("DA15_ID15",     pd.Timestamp("2025-10-01"), pd.Timestamp("2026-05-15")),
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


def build_raw_panel():
    """Per-(date, firm, tech, clock_hour) daily post-DA gap (GWh)."""
    units = pd.read_csv(UNITS_CSV)
    units["tech"] = units["technology"].apply(tech_bucket)
    units["firm"] = units["owner_agent"].apply(firm_bucket)
    units = units[units["tech"].isin(TECHS) & units["firm"].isin(FIRMS)][
        ["unit_code", "firm", "tech"]
    ].drop_duplicates("unit_code")
    con = duckdb.connect()
    con.execute("SET memory_limit='12GB'")
    con.execute("SET threads=4")
    con.register("u", units)
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
             COALESCE(pdbc.mtu, phf.mtu, 60) AS mtu,
             CASE WHEN COALESCE(pdbc.mtu, phf.mtu, 60) = 60 THEN COALESCE(pdbc.period, phf.period) - 1
                  ELSE CAST(FLOOR((COALESCE(pdbc.period, phf.period) - 1) / 4.0) AS INT) END AS clock_hour
      FROM pdbc FULL OUTER JOIN phf
        ON pdbc.d=phf.d AND pdbc.period=phf.period AND pdbc.unit_code=phf.unit_code
    )
    SELECT j.d, j.clock_hour, u.firm, u.tech,
           SUM((j.phf_mw - j.pdbc_mw) * j.mtu/60.0) / 1000.0 AS gap_gwh
    FROM j JOIN u ON j.unit_code = u.unit_code
    GROUP BY 1, 2, 3, 4
    """
    df = con.execute(sql).fetchdf()
    df["d"] = pd.to_datetime(df["d"])
    return df


def attach_design(df):
    out = df.reset_index(drop=True).copy()
    doy = out["d"].dt.dayofyear.values
    fk = fourier_terms(doy, K)
    dw = dow_dummies(out["d"])
    out = pd.concat([out, fk.reset_index(drop=True), dw.reset_index(drop=True)], axis=1)
    for label, lo, hi in REGIME_DATES:
        out[f"D_{label}"] = ((out["d"] >= lo) & (out["d"] <= hi)).astype(float)
    return out


def per_firm_tech_hour_sa(group, fourier_cols, dow_cols, regime_cols):
    g = group.dropna(subset=["gap_gwh"]).copy()
    if len(g) < MIN_OBS:
        g["gap_gwh_sa"] = np.nan
        return g
    y = g["gap_gwh"].astype(float).values
    cols = regime_cols + fourier_cols + dow_cols
    X = sm.add_constant(g[cols].astype(float).values)
    try:
        fit = sm.OLS(y, X, hasconst=True).fit()
    except (np.linalg.LinAlgError, ValueError):
        g["gap_gwh_sa"] = np.nan
        return g
    params = fit.params
    n_regime = len(regime_cols)
    n_fourier = len(fourier_cols)
    eta_full = X @ params
    resid = y - eta_full
    fourier_idx = slice(1 + n_regime, 1 + n_regime + n_fourier)
    fourier_contrib = X[:, fourier_idx] @ params[fourier_idx]
    dow_idx = slice(1 + n_regime + n_fourier, X.shape[1])
    dow_contrib = X[:, dow_idx] @ params[dow_idx]
    dow_mean_within_week = float(np.sum(params[dow_idx])) / 7.0
    g["gap_gwh_sa"] = eta_full - fourier_contrib - dow_contrib + dow_mean_within_week + resid
    return g


def main():
    print("Building raw hourly post-DA gap panel...")
    df = build_raw_panel()
    print(f"  {len(df):,} (date, hour, firm, tech) cells")
    df = attach_design(df)

    fourier_cols = [f"{p}_{k}" for k in range(1, K + 1) for p in ("cos", "sin")]
    dow_cols = [f"dow_{i}" for i in range(1, 7)]
    regime_cols = [f"D_{label}" for label, _, _ in REGIME_DATES]

    print(f"Fitting per-(firm, tech, hour) FWL-SA (K={K}, MIN_OBS={MIN_OBS})...")
    pieces = []
    for _, g in df.groupby(["firm", "tech", "clock_hour"], sort=False):
        pieces.append(per_firm_tech_hour_sa(g, fourier_cols=fourier_cols,
                                            dow_cols=dow_cols, regime_cols=regime_cols))
    out = pd.concat(pieces, ignore_index=True)
    n_sa = out["gap_gwh_sa"].notna().sum()
    print(f"  {n_sa:,} / {len(out):,} cells with SA gap")

    out_cols = ["d", "clock_hour", "firm", "tech", "gap_gwh", "gap_gwh_sa"]
    out[out_cols].to_parquet(OUT, index=False)
    print(f"wrote {OUT}")


if __name__ == "__main__":
    main()
