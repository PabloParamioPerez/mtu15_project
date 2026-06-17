# STATUS: ALIVE
# LAST-AUDIT: 2026-05-19
# CLAIM: Build a per-(unit, date, clock_hour) panel of daily in-band MW share
#        with a SA column attached. Per-(unit, hour) FWL regression on
#        logit(share) ~ const + regime + Fourier(doy, K=4) + DOW; per-cell SA
#        strips the Fourier + DOW deviation and keeps regime + idiosyncratic
#        residual, then maps back through the inverse logit. Pooled seasonality
#        across regimes (sa_fwl.py constraint: the 40-day MTU15-IDA pre-blk
#        window cannot identify regime-specific seasonal coefficients).
#
# OUT: data/derived/panels/bidshape_sa_daily.parquet
#      columns: d, clock_hour, unit_code, tech, in_band_share,
#               in_band_share_sa, mw_total

from pathlib import Path
import sys

import duckdb
import numpy as np
import pandas as pd
import statsmodels.api as sm

REPO = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO / "src"))
from mtu.analysis.sa_fwl import fourier_terms, dow_dummies, DEFAULT_K  # noqa: E402

DET = REPO / "data/processed/omie/mercado_diario/ofertas/det_all.parquet"
CAB = REPO / "data/processed/omie/mercado_diario/ofertas/cab_all.parquet"
MPDBC = REPO / "data/processed/omie/mercado_diario/precios/marginalpdbc_all.parquet"
PDBC = REPO / "data/processed/omie/mercado_diario/programas/pdbc_all.parquet"
UNITS = REPO / "data/external/omie_reference/lista_unidades.csv"
OUT_DIR = REPO / "data/derived/panels"
OUT_DIR.mkdir(parents=True, exist_ok=True)
OUT = OUT_DIR / "bidshape_sa_daily.parquet"

H = 50.0
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


def tech_bucket(t):
    if t is None: return "Other"
    t = str(t).lower()
    if "ciclo combinado" in t: return "CCGT"
    if "hidráulica generación" in t: return "Hydro"
    if "bombeo" in t: return "Hydro_pump"
    return "Other"


def build_raw_panel():
    """Per-(unit, date, clock_hour) daily in-band MW share."""
    con = duckdb.connect()
    con.execute("SET memory_limit='12GB'")
    con.execute("SET threads=4")
    units = pd.read_csv(UNITS)
    units["tech"] = units["technology"].apply(tech_bucket)
    units = units[units["tech"].isin(TECHS)][["unit_code", "tech"]].drop_duplicates("unit_code")
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
    pdbc_daily AS (
      SELECT CAST(date AS DATE) AS d, unit_code,
             SUM(assigned_power_mw * COALESCE(mtu_minutes, 60)/60.0) AS daily_mwh
      FROM read_parquet('{PDBC}')
      WHERE date >= '{START}' AND date <= '{END}'
      GROUP BY 1, 2
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
    per_cell AS (
      SELECT d, clock_hour, period, unit_code,
             SUM(q * mtu_minutes/60.0) AS mw_total,
             SUM(q * mtu_minutes/60.0 * in_band) AS mw_in
      FROM joined GROUP BY 1, 2, 3, 4
    ),
    daily AS (
      SELECT d, clock_hour, unit_code,
             SUM(mw_in) / NULLIF(SUM(mw_total), 0) AS in_band_share,
             SUM(mw_total) AS mw_total
      FROM per_cell GROUP BY 1, 2, 3
    )
    SELECT dlymw.d, dlymw.clock_hour, dlymw.unit_code, u.tech,
           dlymw.in_band_share, dlymw.mw_total,
           CASE WHEN COALESCE(pd.daily_mwh, 0) > 0 THEN 1 ELSE 0 END AS mic_active
    FROM daily dlymw JOIN u ON dlymw.unit_code = u.unit_code
      LEFT JOIN pdbc_daily pd ON pd.d = dlymw.d AND pd.unit_code = dlymw.unit_code
    WHERE dlymw.in_band_share IS NOT NULL
    """
    df = con.execute(sql).fetchdf()
    df["d"] = pd.to_datetime(df["d"])
    return df


def attach_design(df):
    """Attach Fourier + DOW + regime dummies."""
    out = df.reset_index(drop=True).copy()
    doy = out["d"].dt.dayofyear.values
    fk = fourier_terms(doy, K)
    dw = dow_dummies(out["d"])
    out = pd.concat([out, fk.reset_index(drop=True), dw.reset_index(drop=True)], axis=1)
    for label, lo, hi in REGIME_DATES:
        out[f"D_{label}"] = ((out["d"] >= lo) & (out["d"] <= hi)).astype(float)
    return out


def per_unit_hour_sa(group, fourier_cols, dow_cols, regime_cols):
    """Fit FWL on logit(share); return group with in_band_share_sa column."""
    g = group.dropna(subset=["in_band_share"]).copy()
    if len(g) < MIN_OBS:
        g["in_band_share_sa"] = np.nan
        return g
    p = np.clip(g["in_band_share"].values, 0.001, 0.999)
    y = np.log(p / (1.0 - p))
    cols = regime_cols + fourier_cols + dow_cols
    X = sm.add_constant(g[cols].astype(float).values)
    try:
        fit = sm.OLS(y, X, hasconst=True).fit()
    except (np.linalg.LinAlgError, ValueError):
        g["in_band_share_sa"] = np.nan
        return g
    params = fit.params
    n_regime = len(regime_cols)
    n_fourier = len(fourier_cols)
    eta_full = X @ params
    resid = y - eta_full
    # Strip Fourier contribution (annual-mean Fourier = 0)
    fourier_idx = slice(1 + n_regime, 1 + n_regime + n_fourier)
    fourier_contrib = X[:, fourier_idx] @ params[fourier_idx]
    # Strip DOW deviation, keep within-week DOW mean
    dow_idx = slice(1 + n_regime + n_fourier, X.shape[1])
    dow_contrib = X[:, dow_idx] @ params[dow_idx]
    dow_mean_within_week = float(np.sum(params[dow_idx])) / 7.0  # Mon baseline=0 baked in
    eta_sa = eta_full - fourier_contrib - dow_contrib + dow_mean_within_week + resid
    p_sa = 1.0 / (1.0 + np.exp(-eta_sa))
    g["in_band_share_sa"] = np.clip(p_sa, 0.001, 0.999)
    return g


def main():
    print("Building raw daily share panel...")
    df = build_raw_panel()
    print(f"  {len(df):,} cells, {df['unit_code'].nunique()} units, {df['clock_hour'].nunique()} hours")
    df = attach_design(df)

    fourier_cols = [f"{p}_{k}" for k in range(1, K + 1) for p in ("cos", "sin")]
    dow_cols = [f"dow_{i}" for i in range(1, 7)]
    regime_cols = [f"D_{label}" for label, _, _ in REGIME_DATES]

    print(f"Fitting per-(unit, hour) FWL-SA (K={K}, MIN_OBS={MIN_OBS}, {df['unit_code'].nunique() * 24} potential series)...")
    pieces = []
    for _, g in df.groupby(["unit_code", "clock_hour"], sort=False):
        pieces.append(per_unit_hour_sa(g, fourier_cols=fourier_cols,
                                       dow_cols=dow_cols, regime_cols=regime_cols))
    out = pd.concat(pieces, ignore_index=True)
    n_sa = out["in_band_share_sa"].notna().sum()
    print(f"  {n_sa:,} / {len(out):,} cells with SA share ({(out['in_band_share_sa'].isna() & out['in_band_share'].notna()).sum():,} dropped below MIN_OBS)")

    out_cols = ["d", "clock_hour", "unit_code", "tech",
                "in_band_share", "in_band_share_sa", "mw_total", "mic_active"]
    out[out_cols].to_parquet(OUT, index=False)
    n_mic = int(out["mic_active"].sum())
    print(f"  {n_mic:,} / {len(out):,} cells MIC-active (unit cleared >0 MWh that day)")
    print(f"wrote {OUT}")


if __name__ == "__main__":
    main()
