# STATUS: ALIVE
# LAST-AUDIT: 2026-05-28
# FEEDS: scripts/analysis/bid/bsts_hour_class_p90.R -- 4 window-specific
#        BSTS panels for the Spec B per-hour-class analysis under the new
#        window-and-market-specific p90 bandwidth.
#
# For each (reform, real-or-placebo) window we build ONE panel with:
#   - DA-side bid outcomes computed at h_DA  for that window
#   - IDA-side bid outcomes computed at h_IDA for that window
#   - Covariates: wind_gwh, solar_gwh, gas_eur (from bsts_daily_panel)
#
# Outputs 4 parquet files in data/derived/panels/bsts_hour_class_p90/.

from pathlib import Path
import duckdb
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
DET   = REPO / "data/processed/omie/mercado_diario/ofertas/det_all.parquet"
CAB   = REPO / "data/processed/omie/mercado_diario/ofertas/cab_all.parquet"
MPDBC = REPO / "data/processed/omie/mercado_diario/precios/marginalpdbc_all.parquet"
IDET  = REPO / "data/processed/omie/mercado_intradiario_subastas/ofertas/idet_all.parquet"
ICAB  = REPO / "data/processed/omie/mercado_intradiario_subastas/ofertas/icab_all.parquet"
MPIBC = REPO / "data/processed/omie/mercado_intradiario_subastas/precios/marginalpibc_all.parquet"
UNIT_MAP = REPO / "data/derived/panels/bid_shape_critical_flat/_unit_map.parquet"
BSTS_BASE = REPO / "data/derived/panels/bsts_daily_panel.parquet"

OUT_DIR = REPO / "data/derived/panels/bsts_hour_class_p90"
OUT_DIR.mkdir(parents=True, exist_ok=True)

CRITICAL = {5, 6, 7, 8, 16, 17, 18, 19, 20, 21, 22}
MIDDAY   = {11, 12, 13, 14}
FLAT     = {1, 2, 3}
TECHS = ["CCGT", "Hydro", "Hydro_pump"]
HOUR_CLASSES = ["critical", "midday", "flat"]

# (label,            lo,           hi,           h_DA, h_IDA)
WINDOWS = [
    ("ID15_real",    "2024-06-14", "2025-04-27", 50,   62),
    ("ID15_placebo", "2023-06-14", "2024-04-27", 45,   46),
    ("DA15_real",    "2025-04-28", "2025-11-09", 50,   58),
    ("DA15_placebo", "2024-04-28", "2024-11-09", 45,   49),
]


def da_query(lo, hi, h):
    crit_set = ",".join(str(k) for k in CRITICAL)
    mid_set  = ",".join(str(k) for k in MIDDAY)
    flat_set = ",".join(str(k) for k in FLAT)
    return f"""
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
      SELECT mp.d, c.unit_code, dv.q, dv.p,
             CASE WHEN COALESCE(mp.mtu_p, dv.mtu) = 60 THEN mp.period - 1
                  ELSE CAST(FLOOR((mp.period - 1) / 4.0) AS INT) END AS clock_hour
      FROM det dv JOIN cab_l c ON dv.d=c.d AND dv.offer_code=c.offer_code
        JOIN mp ON mp.d=dv.d AND mp.period=dv.period
      WHERE dv.p BETWEEN mp.p_clear - {h} AND mp.p_clear + {h}
    )
    SELECT i.d,
           CASE WHEN i.clock_hour IN ({crit_set}) THEN 'critical'
                WHEN i.clock_hour IN ({mid_set})  THEN 'midday'
                WHEN i.clock_hour IN ({flat_set}) THEN 'flat' END AS hour_class,
           CASE WHEN u.tech_group = 'Hydro_pump' THEN 'Hydro_pump'
                ELSE u.tech_group END AS tech,
           SUM(i.q) sum_q, SUM(i.q*i.p) sum_qp
    FROM inband i JOIN '{UNIT_MAP}' u ON i.unit_code = u.unit_code
    WHERE u.tech_group IS NOT NULL
    GROUP BY 1, 2, 3
    """


def ida_query(lo, hi, h):
    crit_set = ",".join(str(k) for k in CRITICAL)
    mid_set  = ",".join(str(k) for k in MIDDAY)
    flat_set = ",".join(str(k) for k in FLAT)
    return f"""
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
      SELECT mp.d, c.unit_code, dv.q, dv.p,
             CASE WHEN COALESCE(mp.mtu_p, dv.mtu) = 60 THEN mp.period - 1
                  ELSE CAST(FLOOR((mp.period - 1) / 4.0) AS INT) END AS clock_hour
      FROM idet dv JOIN icab_l c
        ON dv.d=c.d AND dv.session_number=c.session_number
       AND dv.offer_code=c.offer_code AND dv.version=c.version
       AND dv.unit_code=c.unit_code
      JOIN mp ON mp.d=dv.d AND mp.session_number=dv.session_number
              AND mp.period=dv.period
      WHERE dv.p BETWEEN mp.p_clear - {h} AND mp.p_clear + {h}
    )
    SELECT i.d,
           CASE WHEN i.clock_hour IN ({crit_set}) THEN 'critical'
                WHEN i.clock_hour IN ({mid_set})  THEN 'midday'
                WHEN i.clock_hour IN ({flat_set}) THEN 'flat' END AS hour_class,
           CASE WHEN u.tech_group = 'Hydro_pump' THEN 'Hydro_pump'
                ELSE u.tech_group END AS tech,
           SUM(i.q) sum_q, SUM(i.q*i.p) sum_qp
    FROM inband i JOIN '{UNIT_MAP}' u ON i.unit_code = u.unit_code
    WHERE u.tech_group IS NOT NULL
    GROUP BY 1, 2, 3
    """


def aggregate(con, market, query):
    df = con.execute(query).fetchdf()
    df["d"] = pd.to_datetime(df["d"])
    df = df[df["hour_class"].isin(HOUR_CLASSES)]
    df = df[df["tech"].isin(TECHS)]
    df["mean_p"] = df["sum_qp"] / df["sum_q"]
    df["market"] = market
    return df


def pivot_wide(df):
    df = df.copy()
    df["tech_lower"] = df["tech"].str.lower()
    p = df.pivot_table(index="d", columns=["tech_lower", "market", "hour_class"],
                       values="mean_p", aggfunc="first").reset_index()
    q = df.pivot_table(index="d", columns=["tech_lower", "market", "hour_class"],
                       values="sum_q", aggfunc="first").reset_index()
    p.columns = ["d"] + [f"p_{t}_{m}_{hc}" for (t, m, hc) in p.columns[1:]]
    q.columns = ["d"] + [f"q_{t}_{m}_{hc}" for (t, m, hc) in q.columns[1:]]
    return p.merge(q, on="d", how="outer").sort_values("d").reset_index(drop=True)


def build_one(label, lo, hi, h_da, h_ida):
    print(f"\n=== {label}: {lo} -> {hi} (h_DA={h_da}, h_IDA={h_ida}) ===")
    con = duckdb.connect()
    con.execute("SET memory_limit='12GB'"); con.execute("SET threads=4")
    print("  Aggregating DA tranche-level in-band data...")
    da = aggregate(con, "da", da_query(lo, hi, h_da))
    print(f"    {len(da):,} rows")
    print("  Aggregating IDA tranche-level in-band data...")
    ida = aggregate(con, "ida", ida_query(lo, hi, h_ida))
    print(f"    {len(ida):,} rows")
    long = pd.concat([da, ida], ignore_index=True)
    wide = pivot_wide(long)
    base = pd.read_parquet(BSTS_BASE)
    base["d"] = pd.to_datetime(base["d"])
    out = wide.merge(base[["d", "wind_gwh", "solar_gwh", "gas_eur"]],
                      on="d", how="left")
    out = out.sort_values("d").reset_index(drop=True)
    fp = OUT_DIR / f"bsts_hour_class_{label}_hDA{h_da}_hIDA{h_ida}.parquet"
    out.to_parquet(fp, index=False)
    print(f"  Wrote {fp} ({len(out):,} dates x {len(out.columns)} cols)")


def main():
    for label, lo, hi, h_da, h_ida in WINDOWS:
        build_one(label, lo, hi, h_da, h_ida)


if __name__ == "__main__":
    main()
