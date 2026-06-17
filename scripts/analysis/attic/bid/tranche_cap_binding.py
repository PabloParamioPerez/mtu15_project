# STATUS: ALIVE
# LAST-AUDIT: 2026-06-13
# FEEDS: thesis.tex sec:results:6-1 -- the model-supporting fact that the
#        finite-tranche friction (OMIE simple-offer step cap) is real and
#        binds for the dispatchable price-setter.
#
# OMIE caps a SIMPLE offer at a fixed number of price-quantity steps per
# delivery period: 25 in the day-ahead auction (file spec v1.37 sec 5.1.4)
# and 5 in the intraday auctions (intraday operations doc sec 2.1). This
# script measures, per technology, how often offers sit AT that cap.
#
# Tranche count per offer-period:
#   - Day-ahead (det): simple offers are block_number = 0; the number of
#     tranches is the count of price-quantity rows per
#     (date, offer_code, version, period). offer_code -> unit_code (and
#     buy_sell) comes from the offer headers (cab); we keep sell offers.
#   - Intraday auctions (idet): block_number is itself the tranche index
#     (1..5, no segment_number column); the number of tranches is the count
#     of distinct block_number per (date, session, offer_code, version,
#     period). unit_code is carried on the file.
# Technology comes from the per-unit map (tech_group).
#
# OUT: results/regressions/bid/tranche_cap_binding.csv
#      one row per (market, tech_group): n_offers, pct_at_cap, mean_tr,
#      max_tr, cap, n_over_cap (sanity: must be 0).

from pathlib import Path

import duckdb

PROJECT = Path(__file__).resolve().parents[3]
DET = PROJECT / "data/processed/omie/mercado_diario/ofertas/det_all.parquet"
CAB = PROJECT / "data/processed/omie/mercado_diario/ofertas/cab_all.parquet"
IDET = PROJECT / "data/processed/omie/mercado_intradiario_subastas/ofertas/idet_all.parquet"
UMAP = PROJECT / "data/derived/panels/bid_shape_critical_flat/_unit_map.parquet"
OUT = PROJECT / "results/regressions/bid/tranche_cap_binding.csv"

DA_CAP = 25   # day-ahead simple-offer step cap (OMIE file spec sec 5.1.4)
IDA_CAP = 5   # intraday-auction simple-offer step cap (OMIE intraday doc sec 2.1)


def main() -> None:
    con = duckdb.connect()

    # --- Day-ahead: tranches per simple sell offer-period, by technology ---
    con.execute(
        f"""
        CREATE TEMP TABLE da AS
        WITH g AS (
            SELECT date, offer_code, version, period, COUNT(*) AS n_tr
            FROM read_parquet('{DET}')
            WHERE block_number = 0          -- simple offers (block offers are block_number >= 1)
            GROUP BY 1, 2, 3, 4
        )
        SELECT 'DA' AS market,
               COALESCE(u.tech_group, 'Unmapped') AS tech_group,
               g.n_tr
        FROM g
        JOIN read_parquet('{CAB}') c USING (date, offer_code, version)
        LEFT JOIN read_parquet('{UMAP}') u ON c.unit_code = u.unit_code
        WHERE c.buy_sell = 'V'              -- sell side: the supply bidder
        """
    )

    # --- Intraday auctions: distinct-tranche count per offer-period, by tech ---
    con.execute(
        f"""
        CREATE TEMP TABLE ida AS
        WITH g AS (
            SELECT date, session_number, offer_code, version, period, unit_code,
                   COUNT(DISTINCT block_number) AS n_tr   -- block_number is the tranche index 1..5
            FROM read_parquet('{IDET}')
            GROUP BY 1, 2, 3, 4, 5, 6
        )
        SELECT 'IDA' AS market,
               COALESCE(u.tech_group, 'Unmapped') AS tech_group,
               g.n_tr
        FROM g
        LEFT JOIN read_parquet('{UMAP}') u USING (unit_code)
        """
    )

    summary = con.sql(
        f"""
        SELECT market, tech_group,
               COUNT(*)                                                   AS n_offers,
               ROUND(100.0 * AVG(CASE WHEN n_tr = cap THEN 1 ELSE 0 END), 2) AS pct_at_cap,
               ROUND(AVG(n_tr), 2)                                        AS mean_tr,
               MAX(n_tr)                                                  AS max_tr,
               cap,
               SUM(CASE WHEN n_tr > cap THEN 1 ELSE 0 END)                AS n_over_cap
        FROM (
            SELECT *, {DA_CAP}  AS cap FROM da
            UNION ALL
            SELECT *, {IDA_CAP} AS cap FROM ida
        )
        GROUP BY market, tech_group, cap
        ORDER BY market, n_offers DESC
        """
    ).df()

    n_over = int(summary["n_over_cap"].sum())
    assert n_over == 0, f"{n_over} offer-periods exceed the institutional step cap"

    OUT.parent.mkdir(parents=True, exist_ok=True)
    summary.to_csv(OUT, index=False)

    print(summary.to_string(index=False))
    print(f"\nNo offer exceeds its cap ({DA_CAP} DA / {IDA_CAP} IDA). Wrote {OUT.relative_to(PROJECT)}")


if __name__ == "__main__":
    main()
