# STATUS: ALIVE
# LAST-AUDIT: 2026-05-29
# FEEDS: thesis/provisional/advisor_memo.tex sec 6.B (added in this session)
#        -- daily continuous-market panel for BSTS-style counterfactuals at
#        the three structural reform dates: ISP15 (2024-12-11), ID15
#        (2025-03-19), DA15 (2025-10-01).
#
# Outcomes: daily ES-leg GWh, daily volume-weighted mean price (EUR/MWh),
#           daily n_trades. ES leg = either seller_zone or buyer_zone is
#           10YES-REE------0.
#
# Covariates from bsts_daily_panel: wind_gwh, solar_gwh, gas_eur.
#
# OUT: data/derived/panels/continuous_daily_panel.parquet

from pathlib import Path
import duckdb
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
TR    = REPO / "data/processed/omie/mercado_intradiario_continuo/transacciones/trades_all.parquet"
COVAR = REPO / "data/derived/panels/bsts_daily_panel.parquet"
OUT   = REPO / "data/derived/panels/continuous_daily_panel.parquet"

LO, HI = "2022-01-01", "2026-02-13"


def main():
    con = duckdb.connect()
    print(f"Aggregating ES-leg trades {LO} -> {HI}...")
    df = con.execute(f"""
        SELECT trade_date AS d,
               COUNT(*) AS n_trades,
               SUM(quantity_mw * COALESCE(mtu_minutes,60)/60.0) AS mwh,
               SUM(quantity_mw * COALESCE(mtu_minutes,60)/60.0 * price_eur_mwh) /
                   NULLIF(SUM(quantity_mw * COALESCE(mtu_minutes,60)/60.0), 0) AS vw_price
        FROM '{TR}'
        WHERE trade_date BETWEEN '{LO}' AND '{HI}'
          AND (seller_zone LIKE '%REE%' OR buyer_zone LIKE '%REE%')
        GROUP BY 1 ORDER BY 1
    """).fetchdf()
    df["d"] = pd.to_datetime(df["d"])
    df["gwh"] = df["mwh"] / 1000.0
    df = df.drop(columns=["mwh"])
    print(f"  {len(df):,} days; gwh range {df['gwh'].min():.1f}–{df['gwh'].max():.1f}")

    print("Merging covariates from bsts_daily_panel...")
    cov = pd.read_parquet(COVAR)
    cov["d"] = pd.to_datetime(cov["d"])
    cov = cov[["d", "wind_gwh", "solar_gwh", "gas_eur"]]
    out = df.merge(cov, on="d", how="left").sort_values("d").reset_index(drop=True)
    print(f"  joined {out['wind_gwh'].notna().sum()}/{len(out)} days with covariates")

    out.to_parquet(OUT, index=False)
    print(f"Wrote {OUT}")


if __name__ == "__main__":
    main()
