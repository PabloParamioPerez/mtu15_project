# STATUS: ALIVE
# LAST-AUDIT: 2026-06-01
# CLAIM: Builds a daily panel of arbitrage intensity per tech-group:
#        number of (unit, session) pairs in IDA auctions where the unit
#        submitted BOTH buy ('C') and sell ('V') offers on the same date
#        and session. This is the per-day analogue of the static stat
#        in additional_results.tex §13 and the BSTS outcome for the
#        next companion script (bsts_arbitrage_intensity.R).
#
#        Theory: the MTU15-IDA reform (2025-03-19) multiplies matched
#        spot products from per-hour to per-quarter, so arbitrage
#        opportunities per session increase. If the model's arbitrageur
#        is empirically active, dual-direction count should JUMP.
#
# IN:  data/processed/omie/mercado_intradiario_subastas/ofertas/icab_all.parquet
#      data/derived/panels/bid_shape_critical_flat/_unit_map.parquet
#      data/derived/panels/bsts_daily_panel.parquet  (for wind/solar/gas
#                                                     covariates)
# OUT: data/derived/panels/arbitrage_intensity_daily.parquet

from pathlib import Path
import duckdb

REPO = Path(__file__).resolve().parents[3]
ICAB = REPO / "data/processed/omie/mercado_intradiario_subastas/ofertas/icab_all.parquet"
UMAP = REPO / "data/derived/panels/bid_shape_critical_flat/_unit_map.parquet"
BSTS = REPO / "data/derived/panels/bsts_daily_panel.parquet"
OUT = REPO / "data/derived/panels/arbitrage_intensity_daily.parquet"
OUT.parent.mkdir(parents=True, exist_ok=True)

START = "2022-06-14"
END = "2026-02-28"


def main():
    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'; SET threads=4")

    q = f"""
    WITH offers AS (
      SELECT DISTINCT date::DATE AS d, session_number, unit_code, buy_sell
      FROM '{ICAB}'
      WHERE date BETWEEN '{START}' AND '{END}' AND unit_code IS NOT NULL
    ),
    unit_session AS (
      SELECT d, session_number, unit_code,
             BOOL_OR(buy_sell = 'V') AS has_sell,
             BOOL_OR(buy_sell = 'C') AS has_buy
      FROM offers GROUP BY 1, 2, 3
    ),
    tagged AS (
      SELECT us.d, us.session_number, u.tech_group, us.has_sell, us.has_buy
      FROM unit_session us JOIN '{UMAP}' u USING (unit_code)
    ),
    daily AS (
      SELECT d,
             tech_group,
             COUNT(*) AS n_unit_sessions,
             SUM(CASE WHEN has_sell AND has_buy THEN 1 ELSE 0 END) AS n_dual,
             SUM(CASE WHEN has_sell THEN 1 ELSE 0 END) AS n_with_sell
      FROM tagged GROUP BY 1, 2
    )
    SELECT d, tech_group, n_unit_sessions, n_dual, n_with_sell,
           100.0 * n_dual / NULLIF(n_with_sell, 0) AS pct_dual
    FROM daily ORDER BY d, tech_group
    """
    df = con.execute(q).fetchdf()
    print(f"Built {len(df)} (date, tech_group) rows.")

    # Attach BSTS covariates (wind, solar, gas) at daily level.
    cov_q = f"""
    SELECT d::DATE AS d, wind_gwh, solar_gwh, gas_eur
    FROM '{BSTS}'
    WHERE d BETWEEN '{START}' AND '{END}'
    """
    cov = con.execute(cov_q).fetchdf()
    out = df.merge(cov, on="d", how="left")
    out.to_parquet(OUT, index=False)
    print(f"Wrote {OUT}  ({out.shape[0]} rows, {out['tech_group'].nunique()} techs)")


if __name__ == "__main__":
    main()
