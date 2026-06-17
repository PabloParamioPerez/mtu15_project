# STATUS: ALIVE
# LAST-AUDIT: 2026-05-16
# FEEDS: provisional.tex §price-vs-qty decomposition
# CLAIM: Decompose the within-hour quarter W1 dissimilarity into a
#        price component and a quantity component. For each pair of
#        quarter bid curves, define:
#          Δp = mean over the common quantile range of |p_i(u) - p_j(u)|
#               where p(u) is the inverse cumulative bid curve (EUR/MWh)
#          Δq = |M_i - M_j|, where M_i = total quantity offered (MWh)
#        Both are computed within the kernel band [c - h, c + h] around
#        the hour-average clearing price. The cell-level Δp / Δq is the
#        max over the 6 quarter-pairs. Aggregate by (firm, tech, hour
#        class).

from __future__ import annotations

import sys
from itertools import combinations
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO / "src"))
from mtu.classification.units import firm_unit_panel  # noqa: E402

DET = REPO / "data" / "processed" / "omie" / "mercado_diario" / "ofertas" / "det_all.parquet"
CAB = REPO / "data" / "processed" / "omie" / "mercado_diario" / "ofertas" / "cab_all.parquet"
MARGINALPDBC = REPO / "data" / "processed" / "omie" / "mercado_diario" / "precios" / "marginalpdbc_all.parquet"
UNITS_CSV = REPO / "data" / "external" / "omie_reference" / "lista_unidades.csv"
OUTDIR = REPO / "results" / "regressions" / "bid" / "w1_decomposition"
OUTDIR.mkdir(parents=True, exist_ok=True)

CRIT = (5, 6, 7, 8, 16, 17, 18, 19, 20, 21, 22)
FLAT = (1, 2, 3)
MID  = (11, 12, 13, 14)
PIVOTAL = ("IB", "GE", "GN", "HC")
KERNEL_BANDWIDTH = 50.0  # EUR/MWh band around clearing for the price-restricted decomposition

WINDOW = ("2025-10-01", "2026-01-01")


def hour_class(h):
    if h in CRIT: return "critical"
    if h in FLAT: return "flat"
    if h in MID:  return "midday"
    return "other"


def decompose_pair(p_i, q_i, p_j, q_j, clearing, h):
    """Restrict to band [clearing - h, clearing + h] (in price) and return
    (Δp, Δq) for one pair of quarter curves.

    Δq = |M_i^band - M_j^band|  where M^band is the sum of quantities
          offered within the band.

    Δp = mean over u ∈ [0, min(M_i^band, M_j^band)] of |p_i(u) - p_j(u)|,
          where p(u) is the inverse of the cumulative bid curve restricted
          to the band — "average price shift at matched cumulative
          quantity within the band".
    """
    if clearing is None or not np.isfinite(clearing):
        return np.nan, np.nan
    lo = clearing - h; hi = clearing + h
    pi = np.asarray(p_i, float); qi = np.asarray(q_i, float)
    pj = np.asarray(p_j, float); qj = np.asarray(q_j, float)
    # Restrict to band
    mi = (pi >= lo) & (pi <= hi); pi, qi = pi[mi], qi[mi]
    mj = (pj >= lo) & (pj <= hi); pj, qj = pj[mj], qj[mj]
    # Sort
    oi = np.argsort(pi); pi, qi = pi[oi], qi[oi]
    oj = np.argsort(pj); pj, qj = pj[oj], qj[oj]
    Mi = float(qi.sum()); Mj = float(qj.sum())
    dq = abs(Mi - Mj)

    if Mi <= 1e-9 or Mj <= 1e-9:
        # Cannot define a price shift on common mass; only quantity diff.
        return 0.0, dq

    # Inverse-cumulative function: at cumulative quantity u, price is p_k
    # where k is the smallest index with cumsum(q)[k] >= u.
    cum_i = np.cumsum(qi); cum_j = np.cumsum(qj)

    def p_at(u, cum, prices):
        k = np.searchsorted(cum, u, side="left")
        k = min(k, len(prices) - 1)
        return prices[k]

    M_common = min(Mi, Mj)
    # Sample 64 equally spaced u points on (0, M_common]
    U = np.linspace(M_common / 64, M_common, 64)
    dp = float(np.mean([abs(p_at(u, cum_i, pi) - p_at(u, cum_j, pj)) for u in U]))
    return dp, dq


def cell_decompose(q2t, clearing, h):
    """Across 6 pairs of quarters return (median Δp, median Δq, max Δp, max Δq)."""
    if len(q2t) != 4:
        return (np.nan,)*4
    dps, dqs = [], []
    for qi, qj in combinations((1, 2, 3, 4), 2):
        dp, dq = decompose_pair(q2t[qi]["p"].values, q2t[qi]["q"].values,
                                 q2t[qj]["p"].values, q2t[qj]["q"].values,
                                 clearing, h)
        if np.isfinite(dp): dps.append(dp)
        if np.isfinite(dq): dqs.append(dq)
    if not dps: return (np.nan,)*4
    return float(np.median(dps)), float(np.median(dqs)), float(np.max(dps)), float(np.max(dqs))


def main():
    units = firm_unit_panel(csv_path=str(UNITS_CSV), scheme="short", mode="primary_owner")
    keep = units[units["parent"].isin(PIVOTAL) & (units["tech_group"] == "CCGT")][
        ["unit_code", "parent"]].rename(columns={"parent": "firm"})
    con = duckdb.connect(); con.execute("PRAGMA threads=4"); con.execute("SET memory_limit='6GB'")
    con.register("uft", keep)
    q = f"""
    WITH cab AS (
        SELECT date::DATE AS d, offer_code, version, unit_code,
               ROW_NUMBER() OVER (PARTITION BY date::DATE, offer_code, unit_code
                                  ORDER BY version DESC) AS rn
        FROM '{CAB}'
        WHERE buy_sell = 'V' AND date::DATE >= DATE '{WINDOW[0]}' AND date::DATE < DATE '{WINDOW[1]}'
    ),
    cab_l AS (SELECT * FROM cab WHERE rn = 1),
    det AS (
        SELECT date::DATE AS d, offer_code, version, period,
               price_eur_mwh AS p, quantity_mw AS q
        FROM '{DET}'
        WHERE date::DATE >= DATE '{WINDOW[0]}' AND date::DATE < DATE '{WINDOW[1]}'
          AND price_eur_mwh IS NOT NULL AND quantity_mw > 0
    )
    SELECT d.d AS date, c.unit_code, u.firm,
           ((d.period - 1) // 4)::INT AS hour,
           (((d.period - 1) % 4) + 1)::INT AS quarter,
           d.p, d.q
    FROM det d
      JOIN cab_l c USING (d, offer_code, version)
      JOIN uft   u ON c.unit_code = u.unit_code
    """
    df = con.execute(q).df()
    cl = con.execute(f"""
        SELECT date::DATE AS date,
               ((period - 1) / 4)::INT AS hour,
               AVG(price_es_eur_mwh) AS p_clear
        FROM '{MARGINALPDBC}'
        WHERE date::DATE >= DATE '{WINDOW[0]}' AND date::DATE < DATE '{WINDOW[1]}'
          AND price_es_eur_mwh IS NOT NULL
        GROUP BY 1, 2
    """).df()
    cl_map = {(row.date, row.hour): row.p_clear for row in cl.itertuples()}

    rows = []
    grp = df.groupby(["firm", "unit_code", "date", "hour"], sort=False)
    for (firm, unit, date, hour), g in grp:
        by_q = {}
        for q_ in (1, 2, 3, 4):
            qq = g[g["quarter"] == q_]
            if qq.empty: continue
            agg = qq.groupby("p", as_index=False)["q"].sum()
            by_q[q_] = agg
        clearing = cl_map.get((date, hour))
        dp_med, dq_med, dp_max, dq_max = cell_decompose(by_q, clearing, KERNEL_BANDWIDTH)
        rows.append({
            "firm": firm, "unit_code": unit, "date": date, "hour": hour,
            "hour_class": hour_class(hour),
            "dp_med_eur_mwh": dp_med, "dq_med_mwh": dq_med,
            "dp_max_eur_mwh": dp_max, "dq_max_mwh": dq_max,
            "p_clear": clearing,
        })
    cells = pd.DataFrame(rows)
    cells.to_csv(OUTDIR / "cells_decomposed.csv", index=False)

    print("=== DA Oct-Dec 2025: W1 decomposition into Δprice (EUR/MWh) and Δquantity (MWh) ===")
    print("    band: |p - clearing| ≤ 50 EUR/MWh; cell statistic = MAX over 6 quarter-pairs")
    print()
    summary = cells.groupby(["firm", "hour_class"], observed=True).agg(
        n=("dp_max_eur_mwh", "size"),
        share_dp_active=("dp_max_eur_mwh", lambda v: (v > 1e-6).mean() * 100),
        share_dq_active=("dq_max_mwh",     lambda v: (v > 1e-6).mean() * 100),
        med_dp_if_active=("dp_max_eur_mwh", lambda v: np.median(v[v > 1e-6]) if (v > 1e-6).any() else np.nan),
        med_dq_if_active=("dq_max_mwh",     lambda v: np.median(v[v > 1e-6]) if (v > 1e-6).any() else np.nan),
    ).round(2).reset_index()
    print(summary.to_string(index=False))
    summary.to_csv(OUTDIR / "summary_by_firm.csv", index=False)


if __name__ == "__main__":
    main()
