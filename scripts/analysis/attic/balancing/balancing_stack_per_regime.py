# STATUS: ALIVE
# LAST-AUDIT: 2026-05-31
# CLAIM: Balancing-stack metrics per regime, complementing the cost
#        cascade. Two panels:
#          - ESIOS aFRR offer curve (per ISP, per direction): mean
#            offered MW, number of price steps, price range.
#          - ENTSO-E aggregated_bids mFRR (per ISP, per direction): mean
#            offered MW. (Prices not in aggregated_bids; that's why
#            aFRR adds the price dimension.)
#
# IN:  data/processed/esios/reservas/curvas_ofertas_afrr_all.parquet
#      data/processed/entsoe/balancing/aggregated_bids_all.parquet
# OUT: results/regressions/balancing/balancing_stack_per_regime.csv

from pathlib import Path

import duckdb

REPO = Path(__file__).resolve().parents[3]
CO = REPO / "data/processed/esios/reservas/curvas_ofertas_afrr_all.parquet"
AB = REPO / "data/processed/entsoe/balancing/aggregated_bids_all.parquet"
OUT = REPO / "results/regressions/balancing/balancing_stack"
OUT.mkdir(parents=True, exist_ok=True)


def main():
    con = duckdb.connect()
    q = f"""
    WITH per_isp AS (
      SELECT date, isp, direction,
             SUM(mw) AS total_mw,
             COUNT(*) AS n_price_steps,
             MAX(price_eur_mw) - MIN(price_eur_mw) AS price_range
      FROM '{CO}' WHERE date BETWEEN DATE '2024-11-20' AND DATE '2026-04-30'
      GROUP BY 1, 2, 3
    )
    SELECT
      CASE WHEN date < DATE '2024-12-01' THEN '0.3-sess'
           WHEN date < DATE '2025-03-19' THEN '3.ISP15-win'
           WHEN date < DATE '2025-04-28' THEN '4.MTU15-IDA pre-blk'
           WHEN date < DATE '2025-10-01' THEN '5.MTU15-IDA post-blk'
           ELSE                              '6.DA15/ID15' END AS regime,
      direction,
      ROUND(AVG(total_mw))          AS avg_mw_offered,
      ROUND(AVG(n_price_steps))     AS avg_n_steps,
      ROUND(AVG(price_range), 1)    AS avg_price_range_eur_mwh
    FROM per_isp GROUP BY 1, 2 ORDER BY direction, regime
    """
    afrr = con.execute(q).fetchdf()
    afrr.to_csv(OUT / "afrr_offer_curve_per_regime.csv", index=False)
    print("=== aFRR offer curve per regime ===")
    print(afrr.to_string(index=False))

    q2 = f"""
    WITH per_isp AS (
      SELECT (isp_start_utc AT TIME ZONE 'Europe/Madrid')::DATE AS d,
             flow_direction, AVG(quantity_mw) AS qty_mw
      FROM '{AB}' WHERE isp_start_utc >= '2024-01-01'
      GROUP BY 1, 2
    )
    SELECT
      CASE WHEN d < DATE '2024-06-14' THEN '1.pre-IDA'
           WHEN d < DATE '2024-12-01' THEN '2.3-sess'
           WHEN d < DATE '2025-03-19' THEN '3.ISP15-win'
           WHEN d < DATE '2025-04-28' THEN '4.MTU15-IDA pre-blk'
           WHEN d < DATE '2025-10-01' THEN '5.MTU15-IDA post-blk'
           ELSE                          '6.DA15/ID15' END AS regime,
      flow_direction,
      ROUND(AVG(qty_mw))            AS avg_mw_offered
    FROM per_isp GROUP BY 1, 2 ORDER BY flow_direction, regime
    """
    mfrr = con.execute(q2).fetchdf()
    mfrr.to_csv(OUT / "mfrr_aggregated_bids_per_regime.csv", index=False)
    print("\n=== mFRR (ENTSO-E aggregated) MW offered per regime ===")
    print(mfrr.to_string(index=False))


if __name__ == "__main__":
    main()
