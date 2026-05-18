# STATUS: ALIVE
# LAST-AUDIT: 2026-05-16
# FEEDS: thesis paper.tex §4 (IDA blackout-split bid-shape test)
# CLAIM: Within-hour quarter dissimilarity of bid curves for the IDA
#        market (ICAB + IDET, pooled across the 3 IDA sessions per day),
#        evaluated at the kernel-weighted price band centred on the
#        hour-average IDA clearing price. Test design: compare
#        pre-blackout (post-MTU15-IDA, 2025-03-19 → 2025-04-27) vs
#        post-blackout DA60/ID15 (2025-04-28 → 2025-09-30). Under the
#        granularity-exploitation hypothesis with REE post-clearing
#        intervention removing the leverage, pre > post near-clearing
#        dispersion in pivotal-firm CCGT bids.

from __future__ import annotations

import sys
from pathlib import Path
from itertools import combinations

import duckdb
import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO / "src"))
from mtu.classification.units import firm_unit_panel  # noqa: E402

ICAB = REPO / "data" / "processed" / "omie" / "mercado_intradiario_subastas" / "ofertas" / "icab_all.parquet"
IDET = REPO / "data" / "processed" / "omie" / "mercado_intradiario_subastas" / "ofertas" / "idet_all.parquet"
MARGINALPIBC = REPO / "data" / "processed" / "omie" / "mercado_intradiario_subastas" / "precios" / "marginalpibc_all.parquet"
UNITS_CSV = REPO / "data" / "external" / "omie_reference" / "lista_unidades.csv"
OUTDIR = REPO / "results" / "regressions" / "bid" / "quarter_dissimilarity_ida"
OUTDIR.mkdir(parents=True, exist_ok=True)
FIGDIR = REPO / "figures" / "thesis"

KERNEL_BANDWIDTH = 50.0

CRIT = (5, 6, 7, 8, 16, 17, 18, 19, 20, 21, 22)
FLAT = (1, 2, 3)
MID  = (11, 12, 13, 14)

PIVOTAL = ("IB", "GE", "GN", "HC")
TECHS = ("CCGT", "Hydro", "Hydro_pump", "Nuclear")


def _epa_int(p, c, h):
    u = np.clip((p - c) / h, -1.0, 1.0)
    return h * (u - u ** 3 / 3.0)


def l1_between(prices_i, qtys_i, prices_j, qtys_j, kernel_center=None, kernel_h=KERNEL_BANDWIDTH):
    pi = np.asarray(prices_i, float); qi = np.asarray(qtys_i, float)
    pj = np.asarray(prices_j, float); qj = np.asarray(qtys_j, float)
    oi = np.argsort(pi); pi, qi = pi[oi], qi[oi]
    oj = np.argsort(pj); pj, qj = pj[oj], qj[oj]
    cum_i = np.cumsum(qi); cum_j = np.cumsum(qj)
    grid = np.unique(np.concatenate([pi, pj]))
    if len(grid) < 2:
        return abs(cum_i[-1] - cum_j[-1]) if (len(cum_i) and len(cum_j)) else 0.0
    integral = 0.0
    for k in range(len(grid) - 1):
        p_lo = grid[k]; p_hi = grid[k + 1]
        ki = np.searchsorted(pi, p_lo, side="right") - 1
        kj = np.searchsorted(pj, p_lo, side="right") - 1
        ci = cum_i[ki] if ki >= 0 else 0.0
        cj = cum_j[kj] if kj >= 0 else 0.0
        gap = abs(ci - cj)
        if gap == 0:
            continue
        if kernel_center is None:
            integral += gap * (p_hi - p_lo)
        else:
            w = _epa_int(p_hi, kernel_center, kernel_h) - _epa_int(p_lo, kernel_center, kernel_h)
            integral += gap * w
    return integral


def cell_dissim(q2t, kernel_center=None):
    if len(q2t) != 4:
        return np.nan, np.nan
    d = [l1_between(q2t[qi]["p"].values, q2t[qi]["q"].values,
                    q2t[qj]["p"].values, q2t[qj]["q"].values,
                    kernel_center=kernel_center)
         for qi, qj in combinations((1, 2, 3, 4), 2)]
    return float(np.max(d)), float(np.mean(d))


def hour_class(h):
    if h in CRIT: return "critical"
    if h in FLAT: return "flat"
    if h in MID:  return "midday"
    return "other"


def load_ida_tranches(window):
    """Pull IDA sell-side tranches for pivotal firms, all sessions pooled."""
    units = firm_unit_panel(csv_path=str(UNITS_CSV), scheme="short", mode="primary_owner")
    keep = units[units["parent"].isin(PIVOTAL) & units["tech_group"].isin(TECHS)][
        ["unit_code", "parent", "tech_group"]
    ].rename(columns={"parent": "firm"})
    con = duckdb.connect(); con.execute("PRAGMA threads=4"); con.execute("SET memory_limit='10GB'")
    con.register("uft", keep)
    # For each (date, session, offer_code, unit_code), take latest version.
    # Each tranche is identified by (date, session, period, unit_code) and we
    # pool tranches across sessions for the same delivery (date, period).
    q = f"""
    WITH icab AS (
        SELECT date::DATE AS d, session_number, offer_code, version, unit_code,
               ROW_NUMBER() OVER (PARTITION BY date::DATE, session_number, offer_code, unit_code
                                  ORDER BY version DESC) AS rn
        FROM '{ICAB}'
        WHERE buy_sell = 'V'
          AND date::DATE >= DATE '{window[0]}' AND date::DATE < DATE '{window[1]}'
    ),
    icab_l AS (SELECT * FROM icab WHERE rn = 1),
    idet AS (
        SELECT date::DATE AS d, session_number, offer_code, version, period,
               price_eur_mwh AS p, quantity_mw AS q
        FROM '{IDET}'
        WHERE date::DATE >= DATE '{window[0]}' AND date::DATE < DATE '{window[1]}'
          AND price_eur_mwh IS NOT NULL AND quantity_mw > 0
    )
    SELECT d.d AS date, c.unit_code, u.firm, u.tech_group,
           d.session_number, d.period,
           ((d.period - 1) // 4)::INT AS hour,
           (((d.period - 1) % 4) + 1)::INT AS quarter,
           d.p, d.q
    FROM idet d
      JOIN icab_l c USING (d, session_number, offer_code, version)
      JOIN uft u ON c.unit_code = u.unit_code
    """
    return con.execute(q).df()


def load_ida_hourly_clearing(window):
    """Per (date, hour): mean IDA clearing price across sessions and quarters."""
    df = duckdb.execute(f"""
        SELECT date::DATE AS date,
               ((period - 1) / 4)::INT AS hour,
               AVG(price_es_eur_mwh) AS p_clear,
               STDDEV_SAMP(price_es_eur_mwh) AS p_clear_std
        FROM '{MARGINALPIBC}'
        WHERE date::DATE >= DATE '{window[0]}' AND date::DATE < DATE '{window[1]}'
          AND price_es_eur_mwh IS NOT NULL
        GROUP BY 1, 2
    """).df()
    return df


def compute_dissim(df, clearing):
    """Per (firm, unit, date, hour) cell, pool IDA tranches across sessions,
    compute D and D_w. Returns one row per cell."""
    cl_map = {(row.date, row.hour): (row.p_clear, row.p_clear_std)
              for row in clearing.itertuples()}
    df = df.copy()
    df["hour_class"] = df["hour"].apply(hour_class)
    rows = []
    grp = df.groupby(["firm", "unit_code", "tech_group", "date", "hour"], sort=False)
    for (firm, unit, tech, date, hour), g in grp:
        # Pool tranches across sessions for the same (date, period)
        by_q = {}
        for q in (1, 2, 3, 4):
            qq = g[g["quarter"] == q]
            if qq.empty:
                continue
            # Aggregate (price, qty) — sum qty within same price within (session pooled)
            agg = qq.groupby("p", as_index=False)["q"].sum()
            by_q[q] = agg
        if len(by_q) != 4:
            continue
        d_max, d_mean = cell_dissim(by_q, kernel_center=None)
        center, std = cl_map.get((date, hour), (None, None))
        d_max_w, d_mean_w = cell_dissim(by_q, kernel_center=center) if center is not None else (np.nan, np.nan)
        rows.append({
            "firm": firm, "unit_code": unit, "tech_group": tech,
            "date": date, "hour": hour, "hour_class": hour_class(hour),
            "d_max": d_max, "d_mean": d_mean,
            "d_max_w": d_max_w, "d_mean_w": d_mean_w,
            "p_clear": center, "p_clear_std": std,
        })
    return pd.DataFrame(rows)


def summary_table(cells, label):
    """Per (firm, tech, hour_class), report % flagged on D_w near clearing."""
    rows = []
    for tech in TECHS:
        for firm in PIVOTAL:
            for hc in ("critical", "flat"):
                sub = cells[(cells["firm"] == firm) & (cells["tech_group"] == tech)
                            & (cells["hour_class"] == hc)]
                if sub.empty:
                    continue
                rows.append({
                    "regime": label,
                    "tech": tech, "firm": firm, "hour_class": hc,
                    "n_cells": len(sub),
                    "pct_flagged_d":  100 * (sub["d_max"]   > 1e-6).mean(),
                    "pct_flagged_dw": 100 * (sub["d_max_w"] > 1e-6).mean(),
                    "median_dw": sub["d_max_w"].median(),
                    "p99_dw": sub["d_max_w"].quantile(0.99),
                })
    return pd.DataFrame(rows)


def main():
    windows = {
        "pre_blackout":  ("2025-03-19", "2025-04-28"),
        "post_blackout": ("2025-04-28", "2025-10-01"),
    }
    summaries = []
    for label, w in windows.items():
        print(f"\n=== {label}  window={w} ===")
        print("  loading IDA tranches...")
        df = load_ida_tranches(w)
        print(f"  {len(df):,} tranche rows")
        print("  loading hourly clearing centers...")
        cl = load_ida_hourly_clearing(w)
        print(f"  {len(cl):,} (date, hour) cells")
        print("  computing per-cell dissimilarity...")
        cells = compute_dissim(df, cl)
        print(f"  {len(cells):,} cells with all 4 quarters present")
        cells.to_csv(OUTDIR / f"cells_{label}.csv", index=False)
        s = summary_table(cells, label)
        s.to_csv(OUTDIR / f"summary_{label}.csv", index=False)
        summaries.append(s)

    merged = pd.concat(summaries, ignore_index=True)
    merged.to_csv(OUTDIR / "summary_pre_vs_post.csv", index=False)
    print("\n=== CCGT critical-hour comparison (% flagged near clearing, D_w > 0) ===")
    ccgt_crit = merged[(merged["tech"] == "CCGT") & (merged["hour_class"] == "critical")]
    pivoted = ccgt_crit.pivot(index="firm", columns="regime",
                              values="pct_flagged_dw").round(1)
    pivoted["diff_pre_minus_post"] = (pivoted["pre_blackout"] -
                                        pivoted["post_blackout"]).round(1)
    print(pivoted.to_string())
    pivoted.to_csv(OUTDIR / "ccgt_critical_pre_vs_post.csv")
    print(f"\nAll outputs in {OUTDIR}")


if __name__ == "__main__":
    main()
