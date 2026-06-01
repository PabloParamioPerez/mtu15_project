# STATUS: ALIVE
# LAST-AUDIT: 2026-06-01
# CLAIM: Do production units submit BOTH buy ('C') and sell ('V') offers
#        in OMIE auction headers? This is the signature of unit-level
#        arbitrage behaviour predicted by the model's arbitrageur agent.
#        Computes per-tech-group statistics of unit-days (DA) and unit-
#        sessions (IDA) with both directions. RAM-careful: duckdb streams
#        parquet files without loading them in full.
#
# IN:  data/processed/omie/mercado_diario/ofertas/cab_all.parquet
#      data/processed/omie/mercado_intradiario_subastas/ofertas/icab_all.parquet
#      data/derived/panels/bid_shape_critical_flat/_unit_map.parquet
# OUT: results/regressions/firm/arbitrage_units.csv

from pathlib import Path
import duckdb
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
CAB = REPO / "data/processed/omie/mercado_diario/ofertas/cab_all.parquet"
ICAB = REPO / "data/processed/omie/mercado_intradiario_subastas/ofertas/icab_all.parquet"
UMAP = REPO / "data/derived/panels/bid_shape_critical_flat/_unit_map.parquet"
OUT = REPO / "results/regressions/firm/arbitrage_units.csv"
OUT.parent.mkdir(parents=True, exist_ok=True)

START = "2018-01-01"
END = "2026-01-31"


def main():
    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'; SET threads=4")

    # ---- DA market ----
    q_da = f"""
    WITH offers AS (
      SELECT DISTINCT unit_code, date::DATE AS d, buy_sell
      FROM '{CAB}'
      WHERE date BETWEEN '{START}' AND '{END}' AND unit_code IS NOT NULL
    ),
    unit_day AS (
      SELECT unit_code, d,
             BOOL_OR(buy_sell = 'V') AS has_sell,
             BOOL_OR(buy_sell = 'C') AS has_buy
      FROM offers GROUP BY 1, 2
    ),
    joined AS (
      SELECT u.tech_group, ud.has_sell, ud.has_buy
      FROM unit_day ud JOIN '{UMAP}' u USING (unit_code)
    )
    SELECT tech_group,
           COUNT(*)                                       AS n_unit_days,
           SUM(CASE WHEN has_sell THEN 1 ELSE 0 END)      AS n_with_sell,
           SUM(CASE WHEN has_buy THEN 1 ELSE 0 END)       AS n_with_buy,
           SUM(CASE WHEN has_sell AND has_buy THEN 1 ELSE 0 END) AS n_with_both,
           100.0 * SUM(CASE WHEN has_sell AND has_buy THEN 1 ELSE 0 END) /
                 NULLIF(SUM(CASE WHEN has_sell THEN 1 ELSE 0 END), 0)    AS pct_both_of_sellers
    FROM joined GROUP BY 1 ORDER BY n_unit_days DESC
    """
    da = con.execute(q_da).fetchdf()
    da.insert(0, "market", "DA")
    print(f"\n--- DA market ({START} to {END}) ---")
    print(da.to_string(index=False, float_format=lambda x: f"{x:.2f}"))

    # ---- IDA auctions ----
    q_ida = f"""
    WITH offers AS (
      SELECT DISTINCT unit_code, date::DATE AS d, session_number, buy_sell
      FROM '{ICAB}'
      WHERE date BETWEEN '{START}' AND '{END}' AND unit_code IS NOT NULL
    ),
    unit_session AS (
      SELECT unit_code, d, session_number,
             BOOL_OR(buy_sell = 'V') AS has_sell,
             BOOL_OR(buy_sell = 'C') AS has_buy
      FROM offers GROUP BY 1, 2, 3
    ),
    joined AS (
      SELECT u.tech_group, us.has_sell, us.has_buy
      FROM unit_session us JOIN '{UMAP}' u USING (unit_code)
    )
    SELECT tech_group,
           COUNT(*)                                       AS n_unit_sessions,
           SUM(CASE WHEN has_sell THEN 1 ELSE 0 END)      AS n_with_sell,
           SUM(CASE WHEN has_buy THEN 1 ELSE 0 END)       AS n_with_buy,
           SUM(CASE WHEN has_sell AND has_buy THEN 1 ELSE 0 END) AS n_with_both,
           100.0 * SUM(CASE WHEN has_sell AND has_buy THEN 1 ELSE 0 END) /
                 NULLIF(SUM(CASE WHEN has_sell THEN 1 ELSE 0 END), 0)    AS pct_both_of_sellers
    FROM joined GROUP BY 1 ORDER BY n_unit_sessions DESC
    """
    ida = con.execute(q_ida).fetchdf()
    ida.insert(0, "market", "IDA")
    ida.rename(columns={"n_unit_sessions": "n_unit_days"}, inplace=True)
    print(f"\n--- IDA auctions ({START} to {END}) ---")
    print(ida.to_string(index=False, float_format=lambda x: f"{x:.2f}"))

    out = pd.concat([da, ida], ignore_index=True)
    out.to_csv(OUT, index=False)
    print(f"\nWrote {OUT}")


if __name__ == "__main__":
    main()
