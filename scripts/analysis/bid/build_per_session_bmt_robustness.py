"""Per-session b_mt at multiple bandwidths for robustness."""
import sys
from pathlib import Path
REPO = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO / "src"))
from mtu.classification.units import firm_unit_panel
import duckdb, numpy as np, pandas as pd

ICAB = REPO/"data/processed/omie/mercado_intradiario_subastas/ofertas/icab_all.parquet"
IDET = REPO/"data/processed/omie/mercado_intradiario_subastas/ofertas/idet_all.parquet"
MPIBC = REPO/"data/processed/omie/mercado_intradiario_subastas/precios/marginalpibc_all.parquet"
UNITS = REPO/"data/external/omie_reference/lista_unidades.csv"

STEP = 2.0
FIRMS = ["IB","GE","GN","HC"]
BANDS = [50.0, 70.0, 90.0]   # robustness: 50 (baseline), 70, 90 EUR/MWh

WINDOWS = [
    ("pre_ID15",  "2024-06-14", "2025-03-18"),
    ("post_ID15", "2025-03-19", "2025-04-27"),
]

def fetch(con, lo, hi):
    q = f"""
    WITH icab_l AS (
      SELECT d, session_number, offer_code, version, unit_code, buy_sell FROM (
        SELECT CAST(date AS DATE) d, session_number, offer_code, version, unit_code, buy_sell,
               ROW_NUMBER() OVER (PARTITION BY CAST(date AS DATE), session_number, offer_code, unit_code
                                  ORDER BY version DESC) AS rn
        FROM '{ICAB}' WHERE date BETWEEN '{lo}' AND '{hi}'
      ) WHERE rn=1
    ),
    idet AS (
      SELECT CAST(date AS DATE) d, session_number, offer_code, version, unit_code,
             period, price_eur_mwh p, quantity_mw q
      FROM '{IDET}' WHERE date BETWEEN '{lo}' AND '{hi}' AND quantity_mw>0
    ),
    mp AS (
      SELECT CAST(date AS DATE) d, session_number, period, price_es_eur_mwh AS mcp
      FROM '{MPIBC}' WHERE date BETWEEN '{lo}' AND '{hi}' AND price_es_eur_mwh IS NOT NULL
    )
    SELECT mp.d, mp.session_number, mp.period, mp.mcp, c.unit_code, c.buy_sell, dv.p, dv.q
    FROM idet dv
      JOIN icab_l c ON dv.d=c.d AND dv.session_number=c.session_number
        AND dv.offer_code=c.offer_code AND dv.version=c.version AND dv.unit_code=c.unit_code
      JOIN mp ON mp.d=dv.d AND mp.period=dv.period AND mp.session_number=dv.session_number
    """
    return con.execute(q).df()

def cum_supply(p_arr, q_arr, p_grid):
    cum_q = np.cumsum(q_arr)
    idx = np.searchsorted(p_arr, p_grid, side="right") - 1
    return np.where(idx >= 0, cum_q[np.clip(idx, 0, len(cum_q)-1)], 0.0)

def cum_demand(p_arr_desc, q_arr_desc, p_grid):
    cum_q = np.cumsum(q_arr_desc)
    out = np.zeros_like(p_grid)
    neg = -p_arr_desc
    for j, p in enumerate(p_grid):
        n_above = np.searchsorted(neg, -p, side="right")
        if n_above > 0: out[j] = cum_q[n_above - 1]
    return out

def slope_at_mcp(p_grid, d_res, mcp):
    v = ~np.isnan(d_res)
    if v.sum() < 5: return np.nan
    x = p_grid[v] - mcp; y = d_res[v]
    X = np.column_stack([np.ones_like(x), x, x**2])
    try:
        c, *_ = np.linalg.lstsq(X, y, rcond=None); return float(c[1])
    except: return np.nan

def process(label, lo, hi, units):
    print(f"[{label}] {lo} -> {hi}", flush=True)
    con = duckdb.connect()
    con.execute("SET memory_limit='10GB'; SET threads=4")
    df = fetch(con, lo, hi)
    if df.empty: print("  empty"); return pd.DataFrame()
    df["d"] = pd.to_datetime(df["d"])
    df = df.merge(units, on="unit_code", how="left")
    df["firm"] = df["parent"].fillna("OTH")
    print(f"  tranches: {len(df):,}", flush=True)

    rows = []
    keys = df.groupby(["d","period","session_number","mcp"], sort=False)
    n_groups = len(keys); cnt = 0
    for (d, period, sess, mcp), g in keys:
        cnt += 1
        if cnt % 20000 == 0:
            print(f"    {cnt}/{n_groups}", flush=True)
        if not np.isfinite(mcp): continue
        buys = g[g["buy_sell"] == "C"].sort_values("p", ascending=False)
        sells_all = g[g["buy_sell"] == "V"].sort_values("p")
        for H in BANDS:
            p_grid = np.arange(mcp - H, mcp + H + STEP, STEP)
            if buys.empty:
                d_total = np.zeros_like(p_grid)
            else:
                d_total = cum_demand(buys["p"].to_numpy(), buys["q"].to_numpy(), p_grid)
            for focal in FIRMS:
                sells_other = sells_all[sells_all["firm"] != focal]
                if sells_other.empty:
                    s_other = np.zeros_like(p_grid)
                else:
                    s_other = cum_supply(sells_other["p"].to_numpy(), sells_other["q"].to_numpy(), p_grid)
                d_res = d_total - s_other
                b = slope_at_mcp(p_grid, d_res, mcp)
                rows.append((d, period, sess, focal, mcp, b, H))
    out = pd.DataFrame(rows, columns=["d","period","session","firm","mcp","b","bandwidth"])
    out["window"] = label
    return out

units = firm_unit_panel(csv_path=str(UNITS), scheme="short", mode="primary_owner")[["unit_code","parent"]]

all_out = []
for label, lo, hi in WINDOWS:
    df = process(label, lo, hi, units)
    if not df.empty: all_out.append(df)

out = pd.concat(all_out, ignore_index=True)
out["abs_b"] = -out["b"]   # convert to IR convention: positive = thicker = more elastic
out.to_parquet("/tmp/per_session_bmt_robust.parquet")

print("\n=== Mean |b| by (bandwidth, window, session), MW per EUR/MWh ===")
piv = (out.dropna(subset=["abs_b"]).groupby(["bandwidth","window","session"])["abs_b"]
       .mean().unstack(["window","session"]).round(1))
print(piv)

print("\n=== Pre/post ratios by session, per bandwidth ===")
for H in BANDS:
    sub = out[out["bandwidth"]==H].dropna(subset=["abs_b"])
    pre = sub[sub["window"]=="pre_ID15"].groupby("session")["abs_b"].mean()
    post = sub[sub["window"]=="post_ID15"].groupby("session")["abs_b"].mean()
    print(f"\n  H={H:.0f}:")
    for s in sorted(pre.index):
        if s in post.index:
            print(f"    S{int(s)}: pre={pre[s]:.1f}, post={post[s]:.1f}, ratio={post[s]/pre[s]:.2f}")
