# STATUS: ALIVE
# LAST-AUDIT: 2026-05-28
# FEEDS: scripts/analysis/bid/run_spec_c_did_p90.py and the Spec C bid-shape
#        DiD on sigma_p / N_eff using a window-specific p90 bandwidth (Big-4
#        firms, dispatchable techs). Replaces per_curve_metrics.py's
#        hard-coded H=140 with per-(window, market) h.
#
# Per-curve outcome (one row per (unit, date, period[, session])):
#   sigma_p = MW-weighted SD of in-band tranche prices.
#   N_eff   = effective in-band tranche count (inverse-HHI of MW shares).
#
# CALL: build(window_lo, window_hi, market, h, out_path).

from pathlib import Path
import sys

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

TECHS = ["CCGT", "Hydro", "Hydro_pump", "Wind",
         "Cogen", "Coal", "Hybrid", "Biomass"]
FIRMS = None  # None => keep all firms (sets firm column via firm_bucket)


def tech_bucket(t):
    if t is None: return "Other"
    t = str(t).lower()
    if "ciclo combinado" in t: return "CCGT"
    if "hidráulica generación" in t: return "Hydro"
    if "bombeo" in t: return "Hydro_pump"
    if "eólica" in t: return "Wind"
    if "carbón" in t or "carbon" in t: return "Coal"
    if "híbrid" in t or "hibrid" in t: return "Hybrid"
    if "térmica renovable" in t or "termica renovable" in t: return "Biomass"
    if "térmica no renovab" in t or "termica no renovab" in t: return "Cogen"
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


def hour_class(h):
    if h in CRITICAL: return "Critical"
    if h in FLAT:     return "Flat"
    if h in MIDDAY:   return "Midday"
    return "Other"


def units_table(con):
    units = pd.read_csv(UNITS)
    units["tech"] = units["technology"].apply(tech_bucket)
    units["firm"] = units["owner_agent"].apply(firm_bucket)
    mask = units["tech"].isin(TECHS)
    if FIRMS is not None:
        mask &= units["firm"].isin(FIRMS)
    units = units[mask][
        ["unit_code", "firm", "tech"]
    ].drop_duplicates("unit_code")
    con.register("u", units)
    return units


def build_da(lo, hi, h):
    con = duckdb.connect()
    con.execute("SET memory_limit='12GB'"); con.execute("SET threads=4")
    units_table(con)
    sql = f"""
    WITH cab_l AS (
      SELECT d, offer_code, unit_code FROM (
        SELECT CAST(date AS DATE) d, offer_code, unit_code, version,
               ROW_NUMBER() OVER (PARTITION BY CAST(date AS DATE), offer_code, unit_code
                                  ORDER BY version DESC) AS rn
        FROM '{CAB}' WHERE date BETWEEN '{lo}' AND '{hi}' AND buy_sell='V'
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
    ),
    inband AS (
      SELECT mp.d, mp.period, c.unit_code, dv.q, dv.p,
             CASE WHEN COALESCE(mp.mtu_p, dv.mtu) = 60 THEN mp.period - 1
                  ELSE CAST(FLOOR((mp.period - 1) / 4.0) AS INT) END AS clock_hour
      FROM det dv
        JOIN cab_l c ON dv.d=c.d AND dv.offer_code=c.offer_code
        JOIN mp ON mp.d=dv.d AND mp.period=dv.period
      WHERE dv.p BETWEEN mp.p_clear - {h} AND mp.p_clear + {h}
    )
    SELECT i.d, i.period, i.clock_hour, i.unit_code, u.firm, u.tech,
           SUM(i.q) sum_w, SUM(i.q*i.p) sum_wp,
           SUM(i.q*i.p*i.p) sum_wp2, SUM(i.q*i.q) sum_w2,
           COUNT(*) n_tranche
    FROM inband i JOIN u ON i.unit_code = u.unit_code
    GROUP BY 1,2,3,4,5,6 HAVING SUM(i.q) > 0
    """
    return con.execute(sql).fetchdf()


def build_ida(lo, hi, h):
    con = duckdb.connect()
    con.execute("SET memory_limit='12GB'"); con.execute("SET threads=4")
    units_table(con)
    sql = f"""
    WITH icab_l AS (
      SELECT d, session_number, offer_code, version, unit_code FROM (
        SELECT CAST(date AS DATE) d, session_number, offer_code, version, unit_code,
               ROW_NUMBER() OVER (PARTITION BY CAST(date AS DATE), session_number,
                                                offer_code, unit_code
                                  ORDER BY version DESC) AS rn
        FROM '{ICAB}' WHERE date BETWEEN '{lo}' AND '{hi}' AND buy_sell='V'
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
    ),
    inband AS (
      SELECT mp.d, mp.session_number, mp.period, c.unit_code, dv.q, dv.p,
             CASE WHEN COALESCE(mp.mtu_p, dv.mtu) = 60 THEN mp.period - 1
                  ELSE CAST(FLOOR((mp.period - 1) / 4.0) AS INT) END AS clock_hour
      FROM idet dv
        JOIN icab_l c
          ON dv.d=c.d AND dv.session_number=c.session_number
         AND dv.offer_code=c.offer_code AND dv.version=c.version
         AND dv.unit_code=c.unit_code
        JOIN mp ON mp.d=dv.d AND mp.session_number=dv.session_number
                AND mp.period=dv.period
      WHERE dv.p BETWEEN mp.p_clear - {h} AND mp.p_clear + {h}
    )
    SELECT i.d, i.session_number, i.period, i.clock_hour, i.unit_code,
           u.firm, u.tech,
           SUM(i.q) sum_w, SUM(i.q*i.p) sum_wp,
           SUM(i.q*i.p*i.p) sum_wp2, SUM(i.q*i.q) sum_w2,
           COUNT(*) n_tranche
    FROM inband i JOIN u ON i.unit_code = u.unit_code
    GROUP BY 1,2,3,4,5,6,7 HAVING SUM(i.q) > 0
    """
    return con.execute(sql).fetchdf()


def finalize(df):
    df = df.copy()
    df["d"] = pd.to_datetime(df["d"])
    mean_p = df["sum_wp"] / df["sum_w"]
    var_p  = (df["sum_wp2"] / df["sum_w"]) - mean_p ** 2
    df["sigma_p"] = np.sqrt(np.clip(var_p, 0, None))
    df["n_eff"]   = (df["sum_w"] ** 2) / df["sum_w2"]
    df["mean_p"]  = mean_p
    df["hour_class"] = df["clock_hour"].apply(hour_class)
    return df


def build(lo, hi, market, h, out_path):
    print(f"[{market}] window {lo} -> {hi}, h={h}")
    df = build_da(lo, hi, h) if market == "da" else build_ida(lo, hi, h)
    df = finalize(df)
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out_path, index=False)
    print(f"  -> {len(df):,} curves, wrote {out_path}")


CONFIG = [
    # (label,         market, lo,           hi,           h)
    ("ID15_real_DA",  "da",   "2024-06-14", "2025-04-27", 50),
    ("ID15_real_IDA", "ida",  "2024-06-14", "2025-04-27", 62),
    ("DA15_real_DA",  "da",   "2025-04-28", "2025-11-09", 50),
    ("DA15_real_IDA", "ida",  "2025-04-28", "2025-11-09", 58),
]

OUT_DIR = REPO / "data/derived/panels/per_curve_windowed"


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    for label, market, lo, hi, h in CONFIG:
        out = OUT_DIR / f"per_curve_{label}_h{h}.parquet"
        build(lo, hi, market, h, out)


if __name__ == "__main__":
    main()
