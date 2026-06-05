# STATUS: ALIVE
# LAST-AUDIT: 2026-06-05
# FEEDS: Per-(date, period, market) aggregate-supply and aggregate-demand
#        slopes inside the in-band region [MCP - h, MCP + h], from which
#        we recover a system-level residual-demand-slope proxy
#                  b_residual ~= b_supply + b_demand
#        with b_supply = +d(price)/d(cum sell MW) (positive, steeper = larger)
#        and  b_demand = -d(price)/d(cum buy MW)  (positive after sign flip).
#
#        The model predicts that at the symmetric reform state (Q,Q), per-product
#        residual demand is thinner than per-pooled-hour residual demand
#        (sum_t b_{1t} < Q b_1 in the Cournot scale-up condition of
#        sec:theory:da15.iv). Empirically: post-MTU15 per-quarter slope
#        should be steeper than pre-MTU15 per-hour slope.
#
# OUT: data/derived/panels/residual_demand_slope_daily.parquet
#      columns: d, market, period, mcp, n_sell, n_buy,
#               b_supply, b_demand, b_residual

from pathlib import Path
import duckdb
import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
CAB   = REPO / "data/processed/omie/mercado_diario/ofertas/cab_all.parquet"
DET   = REPO / "data/processed/omie/mercado_diario/ofertas/det_all.parquet"
MPDBC = REPO / "data/processed/omie/mercado_diario/precios/marginalpdbc_all.parquet"
ICAB  = REPO / "data/processed/omie/mercado_intradiario_subastas/ofertas/icab_all.parquet"
IDET  = REPO / "data/processed/omie/mercado_intradiario_subastas/ofertas/idet_all.parquet"
MPIBC = REPO / "data/processed/omie/mercado_intradiario_subastas/precios/marginalpibc_all.parquet"
OUT   = REPO / "data/derived/panels/residual_demand_slope_daily.parquet"

DATE_LO = "2024-06-14"
DATE_HI = "2026-03-06"
H_BAND  = 60.0  # uniform bandwidth (close to window-specific p90 of 50-62)


def _ols_slope(df_sorted_by_price: pd.DataFrame, ascending: bool) -> float:
    """OLS slope of price on cumulative-MW (signed). For ascending=True (supply)
    the slope is positive; for ascending=False (demand sorted DESCending), the
    slope is negative."""
    p = df_sorted_by_price["p"].to_numpy(dtype=float)
    q = df_sorted_by_price["q"].to_numpy(dtype=float)
    if len(p) < 2:
        return float("nan")
    cum = np.cumsum(q)
    x_mean = cum.mean(); y_mean = p.mean()
    num = ((cum - x_mean) * (p - y_mean)).sum()
    den = ((cum - x_mean) ** 2).sum()
    if den <= 0:
        return float("nan")
    return float(num / den)


def _fetch_inband_da(con) -> pd.DataFrame:
    sql = f"""
    WITH mcp AS (
      SELECT CAST(date AS DATE) AS d, period, price_es_eur_mwh AS mcp,
             COALESCE(mtu_minutes, 60) AS mtu_p
      FROM '{MPDBC}'
      WHERE date BETWEEN '{DATE_LO}' AND '{DATE_HI}' AND price_es_eur_mwh IS NOT NULL
    ),
    offers AS (
      SELECT CAST(c.date AS DATE) AS d, dd.period,
             c.buy_sell, dd.price_eur_mwh AS p, dd.quantity_mw AS q
      FROM '{CAB}' c JOIN '{DET}' dd
        ON c.date = dd.date AND c.offer_code = dd.offer_code
      WHERE c.date BETWEEN '{DATE_LO}' AND '{DATE_HI}'
        AND dd.quantity_mw > 0 AND dd.price_eur_mwh IS NOT NULL
    )
    SELECT mcp.d, mcp.period, mcp.mcp, mcp.mtu_p AS mtu_minutes,
           o.buy_sell, o.p, o.q
    FROM offers o JOIN mcp USING (d, period)
    WHERE ABS(o.p - mcp.mcp) <= {H_BAND}
    """
    return con.execute(sql).fetchdf()


def _fetch_inband_ida(con) -> pd.DataFrame:
    sql = f"""
    WITH mcp_all AS (
      SELECT CAST(date AS DATE) AS d, session_number, period,
             price_es_eur_mwh AS mcp, COALESCE(mtu_minutes, 60) AS mtu_p,
             ROW_NUMBER() OVER (PARTITION BY date::DATE, period
                                ORDER BY session_number DESC) AS rn
      FROM '{MPIBC}'
      WHERE date BETWEEN '{DATE_LO}' AND '{DATE_HI}' AND price_es_eur_mwh IS NOT NULL
    ),
    mcp AS (SELECT d, period, mcp, mtu_p FROM mcp_all WHERE rn = 1),
    offers AS (
      SELECT CAST(c.date AS DATE) AS d, dd.period,
             c.buy_sell, dd.price_eur_mwh AS p, dd.quantity_mw AS q
      FROM '{ICAB}' c JOIN '{IDET}' dd
        ON c.date = dd.date AND c.offer_code = dd.offer_code
       AND c.version = dd.version AND c.unit_code = dd.unit_code
      WHERE c.date BETWEEN '{DATE_LO}' AND '{DATE_HI}'
        AND dd.quantity_mw > 0 AND dd.price_eur_mwh IS NOT NULL
    )
    SELECT mcp.d, mcp.period, mcp.mcp, mcp.mtu_p AS mtu_minutes,
           o.buy_sell, o.p, o.q
    FROM offers o JOIN mcp USING (d, period)
    WHERE ABS(o.p - mcp.mcp) <= {H_BAND}
    """
    return con.execute(sql).fetchdf()


def _per_curve_slopes(df: pd.DataFrame, market: str) -> pd.DataFrame:
    """For each (d, period) group, compute aggregate-supply and aggregate-demand
    slopes in the in-band region. Returns one row per (d, period)."""
    rows = []
    keys = ["d", "period", "mcp", "mtu_minutes"]
    for keyv, group in df.groupby(keys, sort=False):
        d, period, mcp, mtu = keyv
        sells = group[group["buy_sell"] == "V"].sort_values("p", kind="mergesort")
        buys  = group[group["buy_sell"] == "C"].sort_values("p", ascending=False, kind="mergesort")
        b_supply = _ols_slope(sells, ascending=True)
        b_demand = _ols_slope(buys, ascending=False)
        b_residual = (b_supply if not np.isnan(b_supply) else 0) + (
            abs(b_demand) if not np.isnan(b_demand) else 0
        )
        if np.isnan(b_supply) and np.isnan(b_demand):
            b_residual = float("nan")
        rows.append({
            "d": d, "period": period, "market": market, "mcp": mcp,
            "mtu_minutes": int(mtu), "n_sell": len(sells), "n_buy": len(buys),
            "b_supply": b_supply, "b_demand": b_demand, "b_residual": b_residual,
        })
    return pd.DataFrame(rows)


def main() -> None:
    con = duckdb.connect()
    con.execute("SET memory_limit='10GB'; SET threads=4")

    print("Fetching DA in-band offers...", flush=True)
    df_da = _fetch_inband_da(con)
    print(f"  {len(df_da):,} in-band DA tranches", flush=True)
    print("Aggregating DA per-period slopes...", flush=True)
    panel_da = _per_curve_slopes(df_da, "da")
    print(f"  {len(panel_da):,} DA (date, period) cells", flush=True)
    del df_da

    print("Fetching IDA in-band offers...", flush=True)
    df_ida = _fetch_inband_ida(con)
    print(f"  {len(df_ida):,} in-band IDA tranches", flush=True)
    print("Aggregating IDA per-period slopes...", flush=True)
    panel_ida = _per_curve_slopes(df_ida, "ida")
    print(f"  {len(panel_ida):,} IDA (date, period) cells", flush=True)
    del df_ida

    panel = pd.concat([panel_da, panel_ida], ignore_index=True)
    panel["d"] = pd.to_datetime(panel["d"])
    panel = panel.sort_values(["market", "d", "period"])
    OUT.parent.mkdir(parents=True, exist_ok=True)
    panel.to_parquet(OUT, index=False)
    print(f"\nWrote {OUT}: {len(panel):,} rows")

    # Descriptive splits: pre vs post each reform, by mtu_minutes (the natural
    # within-regime partition).
    print("\nMean slopes by (market, mtu_minutes):")
    print(
        panel.groupby(["market", "mtu_minutes"]).agg(
            n_cells=("b_residual", "size"),
            b_supply_mean=("b_supply", "mean"),
            b_demand_mean=("b_demand", "mean"),
            b_residual_mean=("b_residual", "mean"),
        ).round(4).to_string()
    )

    for label, lo, hi in [
        ("ID15 pre  (2024-06-14 to 2025-03-18, hourly IDA + DA)", "2024-06-14", "2025-03-18"),
        ("ID15 post (2025-03-19 to 2025-04-27, 15-min IDA, hourly DA)", "2025-03-19", "2025-04-27"),
        ("DA15 pre  (2025-04-28 to 2025-09-30, 15-min IDA, hourly DA)", "2025-04-28", "2025-09-30"),
        ("DA15 post (2025-10-01 to 2026-03-06, 15-min IDA + DA)", "2025-10-01", "2026-03-06"),
    ]:
        sub = panel[(panel["d"] >= lo) & (panel["d"] <= hi)]
        print(f"\n{label}:")
        print(
            sub.groupby(["market", "mtu_minutes"]).agg(
                n_days=("d", "nunique"),
                b_supply=("b_supply", "mean"),
                b_demand=("b_demand", "mean"),
                b_residual=("b_residual", "mean"),
            ).round(4).to_string()
        )


if __name__ == "__main__":
    main()
