# STATUS: ALIVE
# LAST-AUDIT: 2026-06-02
# CLAIM: In dual-direction (buy + sell) IDA unit-sessions, how big is the
#        buy side relative to the sell side? If buy_mw << sell_mw, the
#        dual-direction signature is "small forecast-correction" rather
#        than symmetric position-taking; if buy_mw ~ sell_mw, the unit is
#        running closer to a paired-position arbitrage. RAM-careful via
#        duckdb streaming over the parquet files.
#
# IN:  data/processed/omie/mercado_intradiario_subastas/ofertas/idet_all.parquet
#      data/derived/panels/bid_shape_critical_flat/_unit_map.parquet
# OUT: results/regressions/firm/arbitrage_quantity_compare.csv

from pathlib import Path
import duckdb

REPO = Path(__file__).resolve().parents[3]
ICAB = REPO / "data/processed/omie/mercado_intradiario_subastas/ofertas/icab_all.parquet"
IDET = REPO / "data/processed/omie/mercado_intradiario_subastas/ofertas/idet_all.parquet"
UMAP = REPO / "data/derived/panels/bid_shape_critical_flat/_unit_map.parquet"
OUT = REPO / "results/regressions/firm/arbitrage_quantity_compare.csv"
OUT.parent.mkdir(parents=True, exist_ok=True)

START = "2018-01-01"
END = "2026-01-31"


def main():
    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'; SET threads=4")

    q = f"""
    WITH offers_summed AS (
      SELECT i.date::DATE AS d, i.session_number, i.unit_code, i.buy_sell,
             SUM(d.quantity_mw) AS offer_mw
      FROM '{ICAB}' i
      JOIN '{IDET}' d
        ON i.date = d.date
       AND i.session_number = d.session_number
       AND i.offer_code = d.offer_code
       AND i.unit_code = d.unit_code
      WHERE i.date BETWEEN '{START}' AND '{END}'
        AND i.unit_code IS NOT NULL
        AND d.quantity_mw IS NOT NULL
      GROUP BY 1, 2, 3, 4
    ),
    unit_session AS (
      SELECT d, session_number, unit_code,
             SUM(CASE WHEN buy_sell = 'V' THEN offer_mw ELSE 0 END) AS sell_mw,
             SUM(CASE WHEN buy_sell = 'C' THEN offer_mw ELSE 0 END) AS buy_mw,
             BOOL_OR(buy_sell = 'V') AS has_sell,
             BOOL_OR(buy_sell = 'C') AS has_buy
      FROM offers_summed GROUP BY 1, 2, 3
    ),
    joined AS (
      SELECT u.tech_group, us.sell_mw, us.buy_mw, us.has_sell, us.has_buy
      FROM unit_session us JOIN '{UMAP}' u USING (unit_code)
    )
    SELECT tech_group,
           COUNT(*)                                                    AS n_unit_sessions,
           SUM(CASE WHEN has_sell AND has_buy THEN 1 ELSE 0 END)      AS n_dual,
           100.0 * SUM(CASE WHEN has_sell AND has_buy THEN 1 ELSE 0 END) /
                 NULLIF(COUNT(*), 0)                                    AS pct_dual,
           AVG(CASE WHEN has_sell AND has_buy THEN sell_mw END)        AS mean_sell_mw_dual,
           AVG(CASE WHEN has_sell AND has_buy THEN buy_mw END)         AS mean_buy_mw_dual,
           100.0 * AVG(CASE WHEN has_sell AND has_buy THEN buy_mw END) /
                 NULLIF(AVG(CASE WHEN has_sell AND has_buy THEN sell_mw END), 0)
                                                                       AS buy_pct_of_sell
    FROM joined GROUP BY 1 ORDER BY n_unit_sessions DESC
    """
    df = con.execute(q).fetchdf()
    print(df.to_string(index=False, float_format=lambda x: f"{x:.2f}"))
    df.to_csv(OUT, index=False)
    print(f"\nWrote {OUT}")


if __name__ == "__main__":
    main()
