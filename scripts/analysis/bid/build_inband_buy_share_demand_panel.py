# STATUS: ALIVE
# LAST-AUDIT: 2026-06-05
# FEEDS: Demand-side mirror of build_ida_inband_sell_share_panel.py.
#        Per (demand_class, market, day) computes in-band buy_mw and sell_mw,
#        and buy_share = buy_mw / (buy_mw + sell_mw). Captures whether
#        demand-side agents (retailers, direct consumers, distributors,
#        portfolios, pump-storage charging units) shift from net buyers
#        toward partial resellers across the MTU15 reform sequence.
#
# OUT: data/derived/panels/inband_buy_share_demand_daily.parquet
#      columns: d, market ('DA' or 'IDA'), demand_class,
#               buy_mw, sell_mw, buy_share
#
# In-band defined as |bid_price - MCP| <= H_BAND for each period. For IDA
# uses the LATEST session per (date, period) for the MCP reference.

from pathlib import Path
import duckdb
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
CAB   = REPO / "data/processed/omie/mercado_diario/ofertas/cab_all.parquet"
DET   = REPO / "data/processed/omie/mercado_diario/ofertas/det_all.parquet"
MPDBC = REPO / "data/processed/omie/mercado_diario/precios/marginalpdbc_all.parquet"
ICAB  = REPO / "data/processed/omie/mercado_intradiario_subastas/ofertas/icab_all.parquet"
IDET  = REPO / "data/processed/omie/mercado_intradiario_subastas/ofertas/idet_all.parquet"
MPIBC = REPO / "data/processed/omie/mercado_intradiario_subastas/precios/marginalpibc_all.parquet"
UNITS = REPO / "data/external/omie_reference/lista_unidades.csv"
OUT   = REPO / "data/derived/panels/inband_buy_share_demand_daily.parquet"

DATE_LO = "2023-01-01"
DATE_HI = "2026-04-30"
H_BAND  = 60.0  # uniform in-band bandwidth (matches sell-share analysis)


def demand_class(t):
    if t is None:
        return "Other"
    t = str(t).upper().strip()
    if "COMERCIALIZADOR ULTIMO RECURSO" in t: return "CUR"
    if "COMERCIALIZADOR" in t:                return "Retailer"
    if "CONSUMIDOR DIRECTO" in t:             return "DirectCons"
    if "DISTRIBUIDOR" in t:                   return "Distrib"
    if "PORFOLIO" in t:                       return "Portfolio"
    if "BOMBEO" in t:                         return "PumpBuy"
    return "Other"


def main() -> None:
    con = duckdb.connect()
    con.execute("SET threads=4; SET memory_limit='10GB'")

    units = pd.read_csv(UNITS)
    units["demand_class"] = units["unit_type"].apply(demand_class)
    keep = units[units["demand_class"] != "Other"][
        ["unit_code", "demand_class"]
    ].drop_duplicates("unit_code")
    con.register("u", keep)

    # DA leg ---------------------------------------------------------------
    q_da = f"""
    WITH mcp AS (
      SELECT CAST(date AS DATE) AS d, period, price_es_eur_mwh AS mcp
      FROM '{MPDBC}'
      WHERE date BETWEEN '{DATE_LO}' AND '{DATE_HI}' AND price_es_eur_mwh IS NOT NULL
    ),
    offers AS (
      SELECT CAST(c.date AS DATE) AS d, dd.period,
             c.unit_code, c.buy_sell, dd.price_eur_mwh AS p, dd.quantity_mw AS q
      FROM '{CAB}' c JOIN '{DET}' dd
        ON c.date = dd.date AND c.offer_code = dd.offer_code
      WHERE c.date BETWEEN '{DATE_LO}' AND '{DATE_HI}'
        AND dd.quantity_mw > 0 AND dd.price_eur_mwh IS NOT NULL
    ),
    inband AS (
      SELECT o.d, u.demand_class, o.buy_sell, o.q
      FROM offers o
        JOIN mcp m USING (d, period)
        JOIN u ON o.unit_code = u.unit_code
      WHERE ABS(o.p - m.mcp) <= {H_BAND}
    )
    SELECT d, 'DA' AS market, demand_class,
           SUM(CASE WHEN buy_sell='C' THEN q ELSE 0 END) AS buy_mw,
           SUM(CASE WHEN buy_sell='V' THEN q ELSE 0 END) AS sell_mw
    FROM inband GROUP BY 1, 2, 3
    """
    df_da = con.execute(q_da).fetchdf()

    # IDA leg (latest session per period) ---------------------------------
    q_ida = f"""
    WITH mcp_all AS (
      SELECT CAST(date AS DATE) AS d, session_number, period,
             price_es_eur_mwh AS mcp,
             ROW_NUMBER() OVER (PARTITION BY date::DATE, period
                                ORDER BY session_number DESC) AS rn
      FROM '{MPIBC}'
      WHERE date BETWEEN '{DATE_LO}' AND '{DATE_HI}' AND price_es_eur_mwh IS NOT NULL
    ),
    mcp AS (SELECT d, period, mcp FROM mcp_all WHERE rn = 1),
    offers AS (
      SELECT CAST(c.date AS DATE) AS d, dd.period,
             c.unit_code, c.buy_sell, dd.price_eur_mwh AS p, dd.quantity_mw AS q
      FROM '{ICAB}' c JOIN '{IDET}' dd
        ON c.date = dd.date AND c.offer_code = dd.offer_code AND c.version = dd.version
      WHERE c.date BETWEEN '{DATE_LO}' AND '{DATE_HI}'
        AND dd.quantity_mw > 0 AND dd.price_eur_mwh IS NOT NULL
    ),
    inband AS (
      SELECT o.d, u.demand_class, o.buy_sell, o.q
      FROM offers o
        JOIN mcp m USING (d, period)
        JOIN u ON o.unit_code = u.unit_code
      WHERE ABS(o.p - m.mcp) <= {H_BAND}
    )
    SELECT d, 'IDA' AS market, demand_class,
           SUM(CASE WHEN buy_sell='C' THEN q ELSE 0 END) AS buy_mw,
           SUM(CASE WHEN buy_sell='V' THEN q ELSE 0 END) AS sell_mw
    FROM inband GROUP BY 1, 2, 3
    """
    df_ida = con.execute(q_ida).fetchdf()

    df = pd.concat([df_da, df_ida], ignore_index=True)
    df["buy_share"] = df["buy_mw"] / (df["buy_mw"] + df["sell_mw"]).where(
        df["buy_mw"] + df["sell_mw"] > 0
    )
    df = df.sort_values(["market", "demand_class", "d"])
    OUT.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(OUT, index=False)
    print(f"Saved {OUT}: {len(df):,} rows")
    print(f"Date range: {df['d'].min()} -> {df['d'].max()}")
    print("\nMean in-band buy-share by (market, demand_class) over the full sample:")
    print(
        df.groupby(["market", "demand_class"])["buy_share"].mean().round(3).to_string()
    )

    # Pre/post snapshots
    df["d"] = pd.to_datetime(df["d"])
    for label, lo, hi in [
        ("ID15 real pre  (2024-06-14 to 2025-03-18)", "2024-06-14", "2025-03-18"),
        ("ID15 real post (2025-03-19 to 2025-04-27)", "2025-03-19", "2025-04-27"),
        ("DA15 real pre  (2025-04-28 to 2025-09-30)", "2025-04-28", "2025-09-30"),
        ("DA15 real post (2025-10-01 to 2026-03-06)", "2025-10-01", "2026-03-06"),
    ]:
        sub = df[(df["d"] >= lo) & (df["d"] <= hi)]
        print(f"\n{label}:")
        print(
            sub.groupby(["market", "demand_class"]).agg(
                n_days=("d", "nunique"),
                buy_share=("buy_share", "mean"),
                buy_mw=("buy_mw", "mean"),
                sell_mw=("sell_mw", "mean"),
            ).round(3).to_string()
        )


if __name__ == "__main__":
    main()
