# STATUS: ALIVE
# LAST-AUDIT: 2026-06-10
# Per-firm strategic markup proxy following Ito-Reguant + accounting for
# renewables as infra-marginal. Computes for each (d, period, market, focal_firm):
#
#   b_f         = slope of residual demand at MCP (as in
#                 build_per_firm_residual_demand_slope.py, same method)
#   q_f^total   = focal firm's cleared sells at MCP (cumulative MW with p <= MCP)
#   q_f^renew   = same, restricted to renewable units (RE Mercado families)
#   q_f^strat   = q_f^total - q_f^renew
#   markup_f    = q_f^strat / b_f   (EUR/MWh units; bigger = more pricing power)
#
# OUT: data/derived/panels/per_firm_strategic_markup.parquet
#      results/regressions/bid/mtu15_critical_flat/per_firm_strategic_markup_summary.csv

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

OUT_PANEL = REPO / "data/derived/panels/per_firm_strategic_markup.parquet"
OUT_SUM   = REPO / "results/regressions/bid/mtu15_critical_flat/per_firm_strategic_markup_summary.csv"

H_GRID = 50.0
GRID_STEP = 2.0
DOMINANT_FIRMS = ["IB", "GE", "GN", "HC"]

# Renewable units = the "RE Mercado" families that bid at marginal cost ~0
# We INCLUDE wind/solar PV/run-of-river hydro/CSP/renewable thermal.
# We EXCLUDE "RE Mercado Térmica no Renovab." which is fossil CHP.
RENEWABLE_TECHS = {
    "RE Mercado Solar Fotovoltáica",
    "RE Mercado Eólica",
    "RE Mercado Hidráulica",
    "RE Mercado Térmica Renovable",
    "RE Mercado Solar Térmica",
    "RE Tarifa CUR (uof)",
}

WINDOWS = [
    ("pre_ID15_IDA",   "IDA", "2024-06-14", "2025-03-18"),
    ("post_ID15_IDA",  "IDA", "2025-03-19", "2025-04-27"),
    ("pre_ID15_DA",    "DA",  "2024-06-14", "2025-03-18"),
    ("post_ID15_DA",   "DA",  "2025-03-19", "2025-04-27"),
    ("pre_DA15_DA",    "DA",  "2024-06-14", "2025-09-30"),
    ("post_DA15_DA",   "DA",  "2025-10-01", "2026-03-06"),
    ("pre_DA15_IDA",   "IDA", "2025-04-28", "2025-09-30"),
    ("post_DA15_IDA",  "IDA", "2025-10-01", "2026-03-06"),
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


def _cum_supply_at_prices(p, q, p_grid):
    if len(p) == 0:
        return np.zeros_like(p_grid)
    cum_q = np.cumsum(q)
    idx = np.searchsorted(p, p_grid, side="right") - 1
    return np.where(idx >= 0, cum_q[np.clip(idx, 0, len(cum_q) - 1)], 0.0)


def _cum_demand_at_prices(p, q, p_grid):
    if len(p) == 0:
        return np.zeros_like(p_grid)
    cum_q = np.cumsum(q)
    out = np.zeros_like(p_grid)
    neg_p = -p
    for j, pp in enumerate(p_grid):
        n_above = np.searchsorted(neg_p, -pp, side="right")
        if n_above > 0:
            out[j] = cum_q[n_above - 1]
    return out


def _slope_at_mcp(p_grid, d_res, mcp):
    valid = ~np.isnan(d_res)
    if valid.sum() < 5: return np.nan
    x = p_grid[valid] - mcp
    y = d_res[valid]
    X = np.column_stack([np.ones_like(x), x, x**2])
    try:
        coefs, *_ = np.linalg.lstsq(X, y, rcond=None)
    except np.linalg.LinAlgError:
        return np.nan
    return float(coefs[1])


def _q_at_mcp(p_arr, q_arr, mcp):
    """Cumulative cleared MW for sells with bid price <= MCP."""
    if len(p_arr) == 0: return 0.0
    cum_q = np.cumsum(q_arr)
    idx = np.searchsorted(p_arr, mcp, side="right") - 1
    return float(cum_q[idx]) if idx >= 0 else 0.0


def process_window(label, market, lo, hi, units_df):
    print(f"[{label}] {lo} -> {hi}, market={market}", flush=True)
    con = duckdb.connect()
    con.execute("SET memory_limit='10GB'; SET threads=4")
    fetch = _fetch_offers_da if market == "DA" else _fetch_offers_ida
    df = fetch(con, lo, hi)
    if df.empty: return []
    df["d"] = pd.to_datetime(df["d"])
    df = df.merge(units_df[["unit_code", "firm", "is_renew"]], on="unit_code", how="left")
    df["firm"] = df["firm"].fillna("OTH")
    df["is_renew"] = df["is_renew"].fillna(False)
    print(f"  total tranches: {len(df):,}", flush=True)

    rows = []
    for (d_v, period_v, mcp_v), g in df.groupby(["d", "period", "mcp"], sort=False):
        if not np.isfinite(mcp_v): continue
        sells_all = g[g["buy_sell"] == "V"].sort_values("p", kind="mergesort")
        buys_all  = g[g["buy_sell"] == "C"].sort_values("p", ascending=False, kind="mergesort")
        if sells_all.empty or buys_all.empty: continue
        sells_all = sells_all[(sells_all["p"] >= mcp_v - H_GRID - 20) &
                              (sells_all["p"] <= mcp_v + H_GRID + 20)]
        buys_all  = buys_all[(buys_all["p"]  >= mcp_v - H_GRID - 20) &
                             (buys_all["p"]  <= mcp_v + H_GRID + 20)]
        if len(sells_all) < 5 or len(buys_all) < 5: continue
        p_grid = np.arange(mcp_v - H_GRID, mcp_v + H_GRID + GRID_STEP, GRID_STEP)
        D_p = _cum_demand_at_prices(buys_all["p"].to_numpy(),
                                     buys_all["q"].to_numpy(), p_grid)
        for focal in DOMINANT_FIRMS:
            sells_others = sells_all[sells_all["firm"] != focal]
            sells_focal  = sells_all[sells_all["firm"] == focal]
            if sells_others.empty or sells_focal.empty: continue
            S_p = _cum_supply_at_prices(sells_others["p"].to_numpy(),
                                         sells_others["q"].to_numpy(), p_grid)
            D_res = D_p - S_p
            b = _slope_at_mcp(p_grid, D_res, mcp_v)
            if np.isnan(b): continue
            sf = sells_focal.sort_values("p")
            q_total = _q_at_mcp(sf["p"].to_numpy(), sf["q"].to_numpy(), mcp_v)
            sf_renew = sf[sf["is_renew"]]
            q_renew = _q_at_mcp(sf_renew["p"].to_numpy(),
                                sf_renew["q"].to_numpy(), mcp_v)
            q_strat = q_total - q_renew
            b_pos = -b   # sign-flip so positive = thinner residual demand
            markup = q_strat / b_pos if b_pos > 1e-6 else np.nan
            rows.append({
                "d": d_v, "period": period_v, "market": market,
                "focal_firm": focal, "mcp": mcp_v,
                "b_residual_mw_per_eur": b_pos,
                "q_total_mw": q_total, "q_renew_mw": q_renew, "q_strat_mw": q_strat,
                "markup_eur_per_mwh": markup,
                "window": label,
            })
    print(f"  -> {len(rows):,} firm-period rows", flush=True)
    return rows


def main():
    units = pd.read_csv(UNITS)
    units["firm"] = units["owner_agent"].apply(firm_bucket)
    units["is_renew"] = units["technology"].isin(RENEWABLE_TECHS)
    print(f"unit map: {len(units):,} unit codes; renewable units: {units['is_renew'].sum():,}")
    print("renewable techs in map:")
    for t in sorted(RENEWABLE_TECHS):
        n = (units["technology"] == t).sum()
        print(f"  {t:<35s}  n={n:>4d}")

    units = units[["unit_code", "firm", "is_renew"]].drop_duplicates("unit_code")

    all_rows = []
    for label, market, lo, hi in WINDOWS:
        all_rows.extend(process_window(label, market, lo, hi, units))

    df = pd.DataFrame(all_rows)
    OUT_PANEL.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(OUT_PANEL, index=False)
    print(f"\nWrote {OUT_PANEL}: {len(df):,} rows")

    summary = (df.groupby(["window", "focal_firm"])
                  .agg(n=("b_residual_mw_per_eur", "size"),
                       b_mean=("b_residual_mw_per_eur", "mean"),
                       q_total_mean=("q_total_mw", "mean"),
                       q_renew_mean=("q_renew_mw", "mean"),
                       q_strat_mean=("q_strat_mw", "mean"),
                       markup_mean=("markup_eur_per_mwh", "mean"),
                       markup_median=("markup_eur_per_mwh", "median"))
                  .round(2).reset_index())
    OUT_SUM.parent.mkdir(parents=True, exist_ok=True)
    summary.to_csv(OUT_SUM, index=False)
    print("\nSummary by (window, firm):")
    print(summary.to_string(index=False))

    print("\n=== Pre vs post % change in b and in markup ===")
    for firm in DOMINANT_FIRMS:
        for pair in [("ID15 IDA", "pre_ID15_IDA", "post_ID15_IDA"),
                     ("ID15 DA",  "pre_ID15_DA",  "post_ID15_DA"),
                     ("DA15 DA",  "pre_DA15_DA",  "post_DA15_DA"),
                     ("DA15 IDA", "pre_DA15_IDA", "post_DA15_IDA")]:
            name, prelbl, postlbl = pair
            pre  = summary[(summary["window"]==prelbl)  & (summary["focal_firm"]==firm)]
            post = summary[(summary["window"]==postlbl) & (summary["focal_firm"]==firm)]
            if pre.empty or post.empty: continue
            pb, pm = float(pre["b_mean"].iloc[0]),  float(pre["markup_mean"].iloc[0])
            ob, om = float(post["b_mean"].iloc[0]), float(post["markup_mean"].iloc[0])
            pqr = float(pre["q_renew_mean"].iloc[0]); oqr = float(post["q_renew_mean"].iloc[0])
            pqs = float(pre["q_strat_mean"].iloc[0]); oqs = float(post["q_strat_mean"].iloc[0])
            print(f"  {firm} {name:8s}: "
                  f"b {pb:5.0f}->{ob:5.0f} ({(ob/pb-1)*100:+5.1f}%)  "
                  f"q_strat {pqs:5.0f}->{oqs:5.0f} ({(oqs/pqs-1)*100:+5.1f}%)  "
                  f"q_renew {pqr:5.0f}->{oqr:5.0f}  "
                  f"markup {pm:5.2f}->{om:5.2f} ({(om/pm-1)*100:+5.1f}%)")


if __name__ == "__main__":
    main()
