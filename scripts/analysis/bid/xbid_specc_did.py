# STATUS: ALIVE
# LAST-AUDIT: 2026-05-31
# CLAIM: Extends preliminary's Spec C bid-shape DiD to the XBID
#        continuous-intraday venue. Computes sigma_p (MW-weighted SD of
#        in-band order prices) and N_eff (inverse-Herfindahl of in-band
#        order MW shares) per (unit, delivery_date, period) sell-order
#        cluster, then runs critical/flat DiD on Big-4 CCGT around the
#        ID15 cutover (2025-03-19). Bandwidth h = 62 EUR/MWh, the
#        preliminary ID15-IDA p90-p50 of MCP; anchor price = same-period
#        IDA marginalpibc clearing price.
#
# RAM-safety: 162M-row XBID orders panel; we push the filters (Big-4
# CCGT units, sell-side, V window) into the DuckDB plan so only the
# relevant slice is materialised.
#
# IN:  data/processed/omie/.../orders_all.parquet
#      data/processed/omie/.../marginalpibc_all.parquet
#      data/derived/panels/bid_shape_critical_flat/_unit_map.parquet
# OUT: results/regressions/bid/xbid_specc/xbid_specc_did.csv

from pathlib import Path

import duckdb

REPO = Path(__file__).resolve().parents[3]
ORD = REPO / "data/processed/omie/mercado_intradiario_continuo/ofertas/orders_all.parquet"
MPIBC = REPO / "data/processed/omie/mercado_intradiario_subastas/precios/marginalpibc_all.parquet"
UMAP = REPO / "data/derived/panels/bid_shape_critical_flat/_unit_map.parquet"
OUT = REPO / "results/regressions/bid/xbid_specc"
OUT.mkdir(parents=True, exist_ok=True)

H = 62.0  # preliminary's ID15 IDA bandwidth


def main():
    con = duckdb.connect()
    con.execute("SET memory_limit='6GB'"); con.execute("SET threads=4")

    # MCP reference: per-period mean IDA clearing price across the 3 daily
    # sessions, for the same delivery period as the XBID order.
    q = f"""
    WITH ccgt_units AS (
      SELECT unit_code, firm_class FROM '{UMAP}'
      WHERE tech_group = 'CCGT' AND firm_class IN ('IB','GN','GE','HC')
    ),
    ida_mcp AS (
      SELECT date::DATE AS d, period, AVG(price_es_eur_mwh) AS mcp
      FROM '{MPIBC}'
      WHERE date BETWEEN '2024-12-19' AND '2025-04-27'
      GROUP BY 1, 2
    ),
    xbid_orders AS (
      SELECT
        CAST(o.delivery_date AS DATE) AS d,
        o.period,
        o.unit_code,
        u.firm_class,
        o.price_eur_mwh AS p,
        CAST(o.quantity_mw AS DOUBLE) AS q,
        o.mtu_minutes
      FROM '{ORD}' o
      JOIN ccgt_units u USING (unit_code)
      WHERE o.offer_type = 'V'
        AND o.quantity_mw IS NOT NULL AND o.quantity_mw > 0
        AND o.price_eur_mwh IS NOT NULL
        AND o.delivery_date BETWEEN '2024-12-19' AND '2025-04-27'
        AND o.exec_condition IN ('', 'ICE')  -- exclude IOC/FOK opportunistic flips
    ),
    joined AS (
      SELECT x.*, m.mcp,
             CAST(CEIL(x.period * (x.mtu_minutes / 60.0)) AS INT) AS clock_hour
      FROM xbid_orders x
      LEFT JOIN ida_mcp m USING (d, period)
      WHERE m.mcp IS NOT NULL AND ABS(x.p - m.mcp) <= {H}
    ),
    pc AS (
      SELECT d, period, unit_code, firm_class, clock_hour,
             SUM(q) AS sw, SUM(q*p) AS swp, SUM(q*p*p) AS swp2,
             SUM(q*q) AS sw2, COUNT(*) AS n_tr
      FROM joined
      GROUP BY d, period, unit_code, firm_class, clock_hour
    ),
    classified AS (
      SELECT
        firm_class, unit_code, d, period, clock_hour,
        CASE WHEN clock_hour IN (5,6,7,8,16,17,18,19,20,21,22) THEN 'Critical'
             WHEN clock_hour IN (1,2,3) THEN 'Flat' END AS hc,
        CASE WHEN d < DATE '2025-03-19' THEN 'pre' ELSE 'post' END AS win,
        n_tr,
        sqrt(GREATEST(swp2/sw - (swp/sw)*(swp/sw), 0)) AS sigma_p,
        (sw*sw) / NULLIF(sw2, 0) AS n_eff
      FROM pc
      WHERE clock_hour IN (1,2,3,5,6,7,8,16,17,18,19,20,21,22)
    )
    SELECT
      win, hc,
      COUNT(*) AS n_curves,
      COUNT(DISTINCT unit_code) AS n_units,
      ROUND(AVG(n_tr), 2)   AS mean_n_tr,
      ROUND(AVG(sigma_p), 3) AS sigma_p,
      ROUND(AVG(n_eff), 3)   AS n_eff
    FROM classified
    WHERE hc IN ('Critical', 'Flat')
    GROUP BY win, hc ORDER BY win, hc
    """
    df = con.execute(q).fetchdf()
    df.to_csv(OUT / "xbid_specc_pooled.csv", index=False)
    print("=== XBID Spec C: pooled CCGT, pre/post ID15, crit/flat, h=62 ===")
    print(df.to_string(index=False))

    # DiD
    if len(df) == 4:
        d = {(r.win, r.hc): r for r in df.itertuples()}
        sig_did = (d[('post','Critical')].sigma_p - d[('post','Flat')].sigma_p) \
                - (d[('pre','Critical')].sigma_p - d[('pre','Flat')].sigma_p)
        neff_did = (d[('post','Critical')].n_eff - d[('post','Flat')].n_eff) \
                 - (d[('pre','Critical')].n_eff - d[('pre','Flat')].n_eff)
        print(f"\nSigma_p DiD: {sig_did:+.3f}    N_eff DiD: {neff_did:+.3f}")


if __name__ == "__main__":
    main()
