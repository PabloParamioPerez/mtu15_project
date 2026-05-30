# STATUS: ALIVE
# LAST-AUDIT: 2026-05-29
# FEEDS: scripts/analysis/bid/bsts_daily_per_session.R -- per-IDA-session
#        BSTS daily panel. One row per date with separate columns for
#        each IDA session's mean clearing price.
#
# OUT: data/derived/panels/bsts_per_session_panel.parquet

from pathlib import Path

import duckdb

REPO = Path(__file__).resolve().parents[3]
OUT = REPO / "data/derived/panels/bsts_per_session_panel.parquet"
BASE = REPO / "data/derived/panels/bsts_quantities_panel.parquet"
IDA = REPO / "data/processed/omie/mercado_intradiario_subastas/precios/marginalpibc_all.parquet"


def main():
    con = duckdb.connect()
    q = f"""
    WITH ida_daily AS (
      SELECT
        date::DATE AS d,
        session_number,
        AVG(price_es_eur_mwh) AS p
      FROM read_parquet('{IDA}')
      WHERE price_es_eur_mwh IS NOT NULL
        AND session_number BETWEEN 1 AND 3
      GROUP BY date, session_number
    ),
    ida_wide AS (
      SELECT
        d,
        MAX(CASE WHEN session_number = 1 THEN p END) AS ida_price_eur_s1,
        MAX(CASE WHEN session_number = 2 THEN p END) AS ida_price_eur_s2,
        MAX(CASE WHEN session_number = 3 THEN p END) AS ida_price_eur_s3
      FROM ida_daily
      GROUP BY d
    ),
    base AS (
      SELECT d::DATE AS d, da_price_eur, ida_price_eur,
             wind_gwh, solar_gwh, gas_eur
      FROM read_parquet('{BASE}')
    )
    SELECT b.d, b.da_price_eur, b.ida_price_eur,
           iw.ida_price_eur_s1, iw.ida_price_eur_s2, iw.ida_price_eur_s3,
           b.wind_gwh, b.solar_gwh, b.gas_eur
    FROM base b
    LEFT JOIN ida_wide iw USING (d)
    ORDER BY b.d
    """
    df = con.execute(q).df()
    OUT.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(OUT, index=False)
    n_full = df[["ida_price_eur_s1", "ida_price_eur_s2", "ida_price_eur_s3"]].notna().all(axis=1).sum()
    print(f"Wrote {len(df)} rows ({n_full} with all 3 sessions) to {OUT.relative_to(REPO)}")
    print(df.tail(3))


if __name__ == "__main__":
    main()
