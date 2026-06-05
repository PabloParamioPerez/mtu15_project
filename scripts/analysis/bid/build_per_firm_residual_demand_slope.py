# STATUS: ALIVE
# LAST-AUDIT: 2026-06-05
# FEEDS: Per-firm residual-demand slope b_{mt} at the market clearing price,
#        following Ito and Reguant (2016 AER) Section III.A and footnote 30.
#
#        Methodology (matching IR2016):
#          For each (date, period, market, focal_firm in {IB, GE, GN, HC}):
#            1. Build the residual demand curve faced by the focal firm =
#                 aggregate demand bids
#                 - supply bids of all OTHER firms (everyone except focal).
#               Both sides are step functions over price.
#            2. Sample the residual demand curve on a price grid around the
#               period's MCP (we use MCP +/- H_GRID EUR/MWh in 2 EUR steps).
#            3. Fit a quadratic to {(p_k, D_res(p_k))} in deviation-from-MCP
#               form: D_res(p) = a + b*(p - MCP) + c*(p - MCP)^2.
#            4. Report the slope b at MCP (= b coefficient of the quadratic).
#
#        Then aggregate per (firm, regime window, market).
#
# OUT: data/derived/panels/per_firm_residual_demand_slope.parquet
#      results/regressions/bid/mtu15_critical_flat/per_firm_residual_demand_slope_summary.csv

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
UNITS = REPO / "data/external/omie_reference/lista_unidades.csv"

OUT_PANEL = REPO / "data/derived/panels/per_firm_residual_demand_slope.parquet"
OUT_SUM   = REPO / "results/regressions/bid/mtu15_critical_flat/per_firm_residual_demand_slope_summary.csv"

H_GRID = 50.0   # half-width of the price grid around MCP (EUR/MWh)
GRID_STEP = 2.0  # step size of the price grid (EUR/MWh)

DOMINANT_FIRMS = ["IB", "GE", "GN", "HC"]

WINDOWS = [
    # (label, market, lo, hi, granularity_min)
    ("pre_ID15_IDA",   "IDA", "2024-06-14", "2025-03-18", 60),
    ("post_ID15_IDA",  "IDA", "2025-03-19", "2025-04-27", 15),
    ("pre_DA15_DA",    "DA",  "2024-06-14", "2025-09-30", 60),
    ("post_DA15_DA",   "DA",  "2025-10-01", "2026-03-06", 15),
    ("pre_DA15_IDA",   "IDA", "2025-04-28", "2025-09-30", 15),
    ("post_DA15_IDA",  "IDA", "2025-10-01", "2026-03-06", 15),
]


def firm_bucket(o):
    if not isinstance(o, str): return "OTH"
    o = o.lower()
    if "iberdrola" in o:                       return "IB"
    if "endesa" in o:                          return "GE"
    if "naturgy" in o or "gas natural" in o:   return "GN"
    if "edp" in o or "hidroel" in o:           return "HC"
    if "repsol" in o:                          return "REP"
    return "OTH"


def _fetch_offers_da(con, lo, hi):
    sql = f"""
    WITH cab_l AS (
      SELECT d, offer_code, unit_code, buy_sell FROM (
        SELECT CAST(date AS DATE) d, offer_code, unit_code, buy_sell, version,
               ROW_NUMBER() OVER (PARTITION BY CAST(date AS DATE), offer_code, unit_code
                                  ORDER BY version DESC) AS rn
        FROM '{CAB}' WHERE date BETWEEN '{lo}' AND '{hi}'
      ) WHERE rn=1
    ),
    det AS (
      SELECT CAST(date AS DATE) d, offer_code, period,
             price_eur_mwh p, quantity_mw q
      FROM '{DET}' WHERE date BETWEEN '{lo}' AND '{hi}' AND quantity_mw>0
    ),
    mp AS (
      SELECT CAST(date AS DATE) d, period, price_es_eur_mwh AS mcp
      FROM '{MPDBC}' WHERE date BETWEEN '{lo}' AND '{hi}' AND price_es_eur_mwh IS NOT NULL
    )
    SELECT mp.d, mp.period, mp.mcp, c.unit_code, c.buy_sell, dv.p, dv.q
    FROM det dv
      JOIN cab_l c ON dv.d=c.d AND dv.offer_code=c.offer_code
      JOIN mp ON mp.d=dv.d AND mp.period=dv.period
    """
    return con.execute(sql).fetchdf()


def _fetch_offers_ida(con, lo, hi):
    sql = f"""
    WITH icab_l AS (
      SELECT d, session_number, offer_code, version, unit_code, buy_sell FROM (
        SELECT CAST(date AS DATE) d, session_number, offer_code, version, unit_code, buy_sell,
               ROW_NUMBER() OVER (PARTITION BY CAST(date AS DATE), session_number,
                                                offer_code, unit_code
                                  ORDER BY version DESC) AS rn
        FROM '{ICAB}' WHERE date BETWEEN '{lo}' AND '{hi}'
      ) WHERE rn=1
    ),
    idet AS (
      SELECT CAST(date AS DATE) d, session_number, offer_code, version, unit_code,
             period, price_eur_mwh p, quantity_mw q
      FROM '{IDET}' WHERE date BETWEEN '{lo}' AND '{hi}' AND quantity_mw>0
    ),
    mp_all AS (
      SELECT CAST(date AS DATE) AS d, session_number, period,
             price_es_eur_mwh AS mcp,
             ROW_NUMBER() OVER (PARTITION BY date::DATE, period
                                ORDER BY session_number DESC) AS rn
      FROM '{MPIBC}' WHERE date BETWEEN '{lo}' AND '{hi}' AND price_es_eur_mwh IS NOT NULL
    ),
    mp AS (SELECT d, period, mcp FROM mp_all WHERE rn=1)
    SELECT mp.d, mp.period, mp.mcp, c.unit_code, c.buy_sell, dv.p, dv.q
    FROM idet dv
      JOIN icab_l c
        ON dv.d=c.d AND dv.session_number=c.session_number
       AND dv.offer_code=c.offer_code AND dv.version=c.version
       AND dv.unit_code=c.unit_code
      JOIN mp ON mp.d=dv.d AND mp.period=dv.period
    """
    return con.execute(sql).fetchdf()


def _cum_supply_at_prices(sells: pd.DataFrame, p_grid: np.ndarray) -> np.ndarray:
    """Given a sells DataFrame with columns p, q (sorted ascending by p),
    return cumulative MW supplied at each price in p_grid."""
    if sells.empty:
        return np.zeros_like(p_grid)
    p_arr = sells["p"].to_numpy()
    q_arr = sells["q"].to_numpy()
    # cum_q[i] = total MW with bid price <= p_arr[i]
    cum_q = np.cumsum(q_arr)
    # For each grid price p, find supply = sum of q with bid_price <= p
    idx = np.searchsorted(p_arr, p_grid, side="right") - 1
    out = np.where(idx >= 0, cum_q[np.clip(idx, 0, len(cum_q) - 1)], 0.0)
    return out


def _cum_demand_at_prices(buys: pd.DataFrame, p_grid: np.ndarray) -> np.ndarray:
    """Buys are sorted DESCENDING by p; cumulative buy MW at price p = sum of q
    with bid_price >= p."""
    if buys.empty:
        return np.zeros_like(p_grid)
    p_arr = buys["p"].to_numpy()  # descending
    q_arr = buys["q"].to_numpy()
    cum_q = np.cumsum(q_arr)
    # For each grid price p, find demand = sum of q with bid_price >= p
    # Since p_arr is descending, count = first index where p_arr[i] < p (then take cum_q[i-1])
    # Use a reversed-sort trick: convert to ascending order for searchsorted
    p_asc = p_arr[::-1]
    cum_asc = cum_q[::-1]  # cum_asc[i] = sum of q with bid_price >= p_asc[i]
    # Actually no, cum_q was on descending so reversing gives reverse cumsum
    # Let me redo more carefully:
    # buys_sorted_desc: row 0 has highest price, row N-1 has lowest
    # cum_q[i] = sum of q for rows 0..i = MW willing to pay >= p_arr[i]
    # For grid price p: demand = max cum_q[i] s.t. p_arr[i] >= p
    #                          = cum_q[last_i with p_arr[i] >= p]
    # Since p_arr is descending: find largest i with p_arr[i] >= p
    out = np.zeros_like(p_grid)
    for j, p in enumerate(p_grid):
        # binary search in descending array: find largest i with p_arr[i] >= p
        # equivalent: count how many p_arr entries are >= p
        n_above = np.searchsorted(-p_arr, -p, side="right")
        if n_above > 0:
            out[j] = cum_q[n_above - 1]
    return out


def _slope_at_mcp(p_grid: np.ndarray, d_res: np.ndarray, mcp: float) -> float:
    """Fit a quadratic D_res(p) = a + b*(p - mcp) + c*(p - mcp)^2 by OLS;
    return the slope b at MCP. Skip if too few non-zero points."""
    valid = ~np.isnan(d_res)
    if valid.sum() < 5:
        return np.nan
    x = p_grid[valid] - mcp
    y = d_res[valid]
    X = np.column_stack([np.ones_like(x), x, x**2])
    try:
        coefs, *_ = np.linalg.lstsq(X, y, rcond=None)
    except np.linalg.LinAlgError:
        return np.nan
    return float(coefs[1])


def process_window(label: str, market: str, lo: str, hi: str, units_df: pd.DataFrame):
    print(f"[{label}] window {lo} -> {hi}, market={market}", flush=True)
    con = duckdb.connect()
    con.execute("SET memory_limit='10GB'; SET threads=4")
    fetch = _fetch_offers_da if market == "DA" else _fetch_offers_ida
    df = fetch(con, lo, hi)
    if df.empty:
        print(f"  no data; skip"); return []
    df["d"] = pd.to_datetime(df["d"])
    df = df.merge(units_df[["unit_code", "firm"]], on="unit_code", how="left")
    df["firm"] = df["firm"].fillna("OTH")
    print(f"  total tranches: {len(df):,}", flush=True)

    rows = []
    group_cols = ["d", "period", "mcp"]
    for (d_v, period_v, mcp_v), g in df.groupby(group_cols, sort=False):
        if not np.isfinite(mcp_v):
            continue
        sells_all = g[g["buy_sell"] == "V"].sort_values("p", kind="mergesort")
        buys_all  = g[g["buy_sell"] == "C"].sort_values("p", ascending=False, kind="mergesort")
        if sells_all.empty or buys_all.empty:
            continue
        # Restrict to in-band for efficiency (no point computing residual far from MCP)
        sells_all = sells_all[(sells_all["p"] >= mcp_v - H_GRID - 20) &
                              (sells_all["p"] <= mcp_v + H_GRID + 20)]
        buys_all  = buys_all[(buys_all["p"]  >= mcp_v - H_GRID - 20) &
                              (buys_all["p"]  <= mcp_v + H_GRID + 20)]
        if len(sells_all) < 5 or len(buys_all) < 5:
            continue
        p_grid = np.arange(mcp_v - H_GRID, mcp_v + H_GRID + GRID_STEP, GRID_STEP)
        # Aggregate demand on the grid (constant per period across focal firms)
        D_p = _cum_demand_at_prices(buys_all, p_grid)
        for focal in DOMINANT_FIRMS:
            sells_others = sells_all[sells_all["firm"] != focal]
            if sells_others.empty:
                continue
            S_p = _cum_supply_at_prices(sells_others, p_grid)
            D_res = D_p - S_p
            b = _slope_at_mcp(p_grid, D_res, mcp_v)
            if np.isnan(b):
                continue
            # Ito-Reguant convention: b is in MW per (EUR/MWh) -- since D is in MW
            # and p is in EUR/MWh. Slope of D w.r.t. p; for a downward-sloping
            # residual demand we expect b < 0; report |b| for the model's b_mt
            # primitive (which is in the form D = A - b*p, so b > 0 in the model).
            rows.append({
                "d": d_v, "period": period_v, "market": market,
                "focal_firm": focal, "mcp": mcp_v,
                "b_residual_mw_per_eur": -b,   # sign-flip so positive = thinner
                "n_sell": len(sells_others), "n_buy": len(buys_all),
                "window": label,
            })
    print(f"  -> {len(rows):,} firm-period slopes", flush=True)
    return rows


def main():
    units = pd.read_csv(UNITS)
    units["firm"] = units["owner_agent"].apply(firm_bucket)
    units = units[["unit_code", "firm"]].drop_duplicates("unit_code")
    print(f"unit map: {len(units):,} unit codes (firm distribution: {units['firm'].value_counts().to_dict()})")

    all_rows = []
    for label, market, lo, hi, _gran in WINDOWS:
        all_rows.extend(process_window(label, market, lo, hi, units))

    df = pd.DataFrame(all_rows)
    OUT_PANEL.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(OUT_PANEL, index=False)
    print(f"\nWrote {OUT_PANEL}: {len(df):,} rows")

    # Aggregate per (window, focal_firm)
    summary = (df.groupby(["window", "focal_firm"])
                  .agg(n_periods=("b_residual_mw_per_eur", "size"),
                       b_mean=("b_residual_mw_per_eur", "mean"),
                       b_median=("b_residual_mw_per_eur", "median"),
                       b_sd=("b_residual_mw_per_eur", "std"))
                  .round(2).reset_index())
    OUT_SUM.parent.mkdir(parents=True, exist_ok=True)
    summary.to_csv(OUT_SUM, index=False)
    print(f"\nSummary by (window, firm):")
    print(summary.to_string(index=False))

    # Compact pre/post comparison per firm + market
    print("\n=== Pre vs post (Ito-Reguant convention, MW per EUR/MWh) ===")
    for firm in DOMINANT_FIRMS:
        for pair in [("ID15 IDA", "pre_ID15_IDA", "post_ID15_IDA"),
                     ("DA15 DA",  "pre_DA15_DA",  "post_DA15_DA"),
                     ("DA15 IDA", "pre_DA15_IDA", "post_DA15_IDA")]:
            name, prelbl, postlbl = pair
            pre  = summary[(summary["window"]==prelbl)  & (summary["focal_firm"]==firm)]
            post = summary[(summary["window"]==postlbl) & (summary["focal_firm"]==firm)]
            if pre.empty or post.empty: continue
            pre_b  = float(pre["b_mean"].iloc[0])
            post_b = float(post["b_mean"].iloc[0])
            ratio = post_b / pre_b if pre_b > 0 else float("nan")
            print(f"  {firm} {name}: pre {pre_b:.1f}  post {post_b:.1f}  ratio {ratio:.2f}")


if __name__ == "__main__":
    main()
