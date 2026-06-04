# STATUS: ALIVE
# LAST-AUDIT: 2026-06-05
# FEEDS: Spec C bid-shape DiD on the DEMAND side (buy_sell = 'C'), per-curve.
#        Same OLS pipeline as build_per_curve_slope.py but with buy_sell='C',
#        tranches sorted DESCENDING by price (demand curves slope down), and
#        a distinct unit-type classification for demand-side agents
#        (Comercializador / Consumidor Directo / CUR / Distribuidor /
#        Portfolio / Bombeo).
#
# Per-curve outcome (one row per (unit, date, period[, session])):
#   slope     = OLS coefficient of bid price on cumulative buy MW (negative
#               by construction; more negative = steeper demand curve).
#   intercept = OLS y-intercept (price at Q=0; the top of the demand ladder).
#   gamma     = quadratic coefficient (gamma > 0 = convex, gamma < 0 = concave).
#   slope_n_tranche = K (need K>=2 for slope, K>=3 for gamma).
#
# CALL: build(window_lo, window_hi, market, h, out_path).

from pathlib import Path
import duckdb
import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
DET   = REPO / "data/processed/omie/mercado_diario/ofertas/det_all.parquet"
CAB   = REPO / "data/processed/omie/mercado_diario/ofertas/cab_all.parquet"
MPDBC = REPO / "data/processed/omie/mercado_diario/precios/marginalpdbc_all.parquet"
IDET  = REPO / "data/processed/omie/mercado_intradiario_subastas/ofertas/idet_all.parquet"
ICAB  = REPO / "data/processed/omie/mercado_intradiario_subastas/ofertas/icab_all.parquet"
MPIBC = REPO / "data/processed/omie/mercado_intradiario_subastas/precios/marginalpibc_all.parquet"
UNITS = REPO / "data/external/omie_reference/lista_unidades.csv"

CRITICAL = {5, 6, 7, 8, 16, 17, 18, 19, 20, 21, 22}
FLAT     = {1, 2, 3}
MIDDAY   = {11, 12, 13, 14}

DEMAND_TYPES = ["COMERCIALIZADOR", "CONSUMIDOR DIRECTO",
                "COMERCIALIZADOR ULTIMO RECURSO", "DISTRIBUIDOR",
                "PORFOLIO", "BOMBEO"]
# Top-4 incumbent groups by ownership keyword for retailers.
RETAIL_FIRMS = ["IB", "GE", "GN", "HC"]


def demand_class(t):
    """Map OMIE unit_type to a compact demand classification."""
    if t is None: return "Other"
    t = str(t).upper().strip()
    if "COMERCIALIZADOR ULTIMO RECURSO" in t: return "CUR"
    if "COMERCIALIZADOR" in t:                return "Retailer"
    if "CONSUMIDOR DIRECTO" in t:             return "DirectCons"
    if "DISTRIBUIDOR" in t:                   return "Distrib"
    if "PORFOLIO" in t:                       return "Portfolio"
    if "BOMBEO" in t:                         return "PumpBuy"
    return "Other"


def firm_bucket(o):
    if not isinstance(o, str): return "OTH"
    o = o.lower()
    if "iberdrola" in o:                       return "IB"
    if "endesa" in o:                          return "GE"
    if "naturgy" in o or "gas natural" in o:   return "GN"
    if "edp" in o or "hidroel" in o:           return "HC"
    if "repsol" in o:                          return "REP"
    return "OTH"


def hour_class(h):
    if h in CRITICAL: return "Critical"
    if h in FLAT:     return "Flat"
    if h in MIDDAY:   return "Midday"
    return "Other"


def units_table(con):
    units = pd.read_csv(UNITS)
    units["demand_class"] = units["unit_type"].apply(demand_class)
    units["firm"] = units["owner_agent"].apply(firm_bucket)
    keep = units[units["demand_class"] != "Other"][
        ["unit_code", "firm", "demand_class"]
    ].drop_duplicates("unit_code")
    con.register("u", keep)
    return keep


def _fetch_inband_da(con, lo, hi, h):
    sql = f"""
    WITH cab_l AS (
      SELECT d, offer_code, unit_code FROM (
        SELECT CAST(date AS DATE) d, offer_code, unit_code, version,
               ROW_NUMBER() OVER (PARTITION BY CAST(date AS DATE), offer_code, unit_code
                                  ORDER BY version DESC) AS rn
        FROM '{CAB}' WHERE date BETWEEN '{lo}' AND '{hi}' AND buy_sell='C'
      ) WHERE rn=1
    ),
    det AS (
      SELECT CAST(date AS DATE) d, offer_code, period,
             price_eur_mwh p, quantity_mw q, COALESCE(mtu_minutes, 60) mtu
      FROM '{DET}' WHERE date BETWEEN '{lo}' AND '{hi}' AND quantity_mw > 0
    ),
    mp AS (
      SELECT CAST(date AS DATE) d, period, price_es_eur_mwh p_clear,
             COALESCE(mtu_minutes, 60) mtu_p
      FROM '{MPDBC}' WHERE date BETWEEN '{lo}' AND '{hi}'
                      AND price_es_eur_mwh IS NOT NULL
    )
    SELECT mp.d, mp.period, c.unit_code, u.firm, u.demand_class,
           CASE WHEN COALESCE(mp.mtu_p, dv.mtu) = 60 THEN mp.period - 1
                ELSE CAST(FLOOR((mp.period - 1) / 4.0) AS INT) END AS clock_hour,
           dv.q, dv.p
    FROM det dv
      JOIN cab_l c ON dv.d=c.d AND dv.offer_code=c.offer_code
      JOIN mp ON mp.d=dv.d AND mp.period=dv.period
      JOIN u ON c.unit_code = u.unit_code
    WHERE dv.p BETWEEN mp.p_clear - {h} AND mp.p_clear + {h}
    """
    return con.execute(sql).fetchdf()


def _fetch_inband_ida(con, lo, hi, h):
    sql = f"""
    WITH icab_l AS (
      SELECT d, session_number, offer_code, version, unit_code FROM (
        SELECT CAST(date AS DATE) d, session_number, offer_code, version, unit_code,
               ROW_NUMBER() OVER (PARTITION BY CAST(date AS DATE), session_number,
                                                offer_code, unit_code
                                  ORDER BY version DESC) AS rn
        FROM '{ICAB}' WHERE date BETWEEN '{lo}' AND '{hi}' AND buy_sell='C'
      ) WHERE rn=1
    ),
    idet AS (
      SELECT CAST(date AS DATE) d, session_number, offer_code, version, unit_code,
             period, price_eur_mwh p, quantity_mw q, COALESCE(mtu_minutes, 60) mtu
      FROM '{IDET}' WHERE date BETWEEN '{lo}' AND '{hi}' AND quantity_mw > 0
    ),
    mp AS (
      SELECT CAST(date AS DATE) d, session_number, period,
             price_es_eur_mwh p_clear, COALESCE(mtu_minutes, 60) mtu_p
      FROM '{MPIBC}' WHERE date BETWEEN '{lo}' AND '{hi}'
                      AND price_es_eur_mwh IS NOT NULL
    )
    SELECT mp.d, mp.session_number, mp.period, c.unit_code, u.firm, u.demand_class,
           CASE WHEN COALESCE(mp.mtu_p, dv.mtu) = 60 THEN mp.period - 1
                ELSE CAST(FLOOR((mp.period - 1) / 4.0) AS INT) END AS clock_hour,
           dv.q, dv.p
    FROM idet dv
      JOIN icab_l c
        ON dv.d=c.d AND dv.session_number=c.session_number
       AND dv.offer_code=c.offer_code AND dv.version=c.version
       AND dv.unit_code=c.unit_code
      JOIN mp ON mp.d=dv.d AND mp.session_number=dv.session_number
              AND mp.period=dv.period
      JOIN u ON c.unit_code = u.unit_code
    WHERE dv.p BETWEEN mp.p_clear - {h} AND mp.p_clear + {h}
    """
    return con.execute(sql).fetchdf()


def _slope_per_curve_demand(group):
    """Unweighted OLS p_k = alpha + beta*Q_k + gamma*Q_k^2, where the buy
    curve is sorted DESCENDING by price. Q_k = sum_{j<=k} q_j is cumulative
    buy MW going from the highest-priced (top) demand tranche downward."""
    g = group.sort_values("p", ascending=False, kind="mergesort").reset_index(drop=True)
    p = g["p"].to_numpy(dtype=float)
    q = g["q"].to_numpy(dtype=float)
    K = len(p)
    out = {"slope": np.nan, "intercept": np.nan, "gamma": np.nan,
           "slope_n_tranche": K}
    if K < 2:
        return pd.Series(out)
    cum = np.cumsum(q)
    x_mean = cum.mean(); y_mean = p.mean()
    num = ((cum - x_mean) * (p - y_mean)).sum()
    den = ((cum - x_mean) ** 2).sum()
    if den <= 0:
        return pd.Series(out)
    out["slope"] = num / den
    out["intercept"] = y_mean - out["slope"] * x_mean
    if K < 3:
        return pd.Series(out)
    X = np.column_stack([np.ones(K), cum, cum ** 2])
    try:
        coefs, *_ = np.linalg.lstsq(X, p, rcond=None)
        out["gamma"] = coefs[2]
    except np.linalg.LinAlgError:
        pass
    return pd.Series(out)


def build(lo, hi, market, h, out_path):
    print(f"[demand:{market}] window {lo} -> {hi}, h={h}")
    con = duckdb.connect()
    con.execute("SET memory_limit='12GB'"); con.execute("SET threads=4")
    units_table(con)
    df = (_fetch_inband_da(con, lo, hi, h) if market == "da"
          else _fetch_inband_ida(con, lo, hi, h))
    df["d"] = pd.to_datetime(df["d"])
    keys = (["d", "period", "clock_hour", "unit_code", "firm", "demand_class"]
            if market == "da"
            else ["d", "session_number", "period", "clock_hour",
                  "unit_code", "firm", "demand_class"])
    slopes = df.groupby(keys, sort=False).apply(_slope_per_curve_demand).reset_index()
    slopes["hour_class"] = slopes["clock_hour"].apply(hour_class)
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    slopes.to_parquet(out_path, index=False)
    print(f"  -> {len(slopes):,} curves, wrote {out_path}")


CONFIG = [
    ("ID15_real_DA",  "da",   "2024-06-14", "2025-04-27", 50),
    ("ID15_real_IDA", "ida",  "2024-06-14", "2025-04-27", 62),
    ("DA15_real_DA",  "da",   "2025-04-28", "2026-02-26", 50),
    ("DA15_real_IDA", "ida",  "2025-04-28", "2026-02-26", 58),
]

OUT_DIR = REPO / "data/derived/panels/per_curve_slope_demand_windowed"


if __name__ == "__main__":
    for label, market, lo, hi, h in CONFIG:
        out = OUT_DIR / f"demand_{label}_h{h}.parquet"
        build(lo, hi, market, h, out)
