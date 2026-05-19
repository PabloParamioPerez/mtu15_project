# STATUS: ALIVE
# LAST-AUDIT: 2026-05-17
# FEEDS: descriptive_facts.tex §2 (price-setter / strategic bidding by tech and firm)
# CLAIM: Within-hour Δp / Δq decomposition (the same kernel-weighted
#        Wasserstein-1 framework used for CCGT) applied to CCGT, Wind,
#        Hydro, and Hydro_pump units, and to a broader firm set including
#        independent RES aggregators (Gesternova, AXPO, NEXUS, Shell,
#        Engie). Window: DA15/ID15 (2025-10-01 -> 2025-12-31) — the only
#        regime where DA bids have 4 quarters per hour.
#
# Outputs:
#   results/regressions/bid/w1_decomposition/multitech_summary.csv
#   results/regressions/bid/w1_decomposition/multitech_cells.parquet

from __future__ import annotations

import sys
from itertools import combinations
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
DET = REPO / "data" / "processed" / "omie" / "mercado_diario" / "ofertas" / "det_all.parquet"
CAB = REPO / "data" / "processed" / "omie" / "mercado_diario" / "ofertas" / "cab_all.parquet"
MARGINALPDBC = REPO / "data" / "processed" / "omie" / "mercado_diario" / "precios" / "marginalpdbc_all.parquet"
UNITS_CSV = REPO / "data" / "external" / "omie_reference" / "lista_unidades.csv"
OUTDIR = REPO / "results" / "regressions" / "bid" / "w1_decomposition"
OUTDIR.mkdir(parents=True, exist_ok=True)

CRIT = (5, 6, 7, 8, 16, 17, 18, 19, 20, 21, 22)
FLAT = (1, 2, 3)
MID  = (11, 12, 13, 14)
KERNEL_BANDWIDTH = 50.0   # EUR/MWh band around clearing
WINDOW = ("2025-10-01", "2025-12-31")  # DA15/ID15 only


def hour_class(h):
    if h in CRIT: return "critical"
    if h in FLAT: return "flat"
    if h in MID:  return "midday"
    return "dropped"


def map_tech(s):
    if not isinstance(s, str): return "Other"
    t = s.lower()
    if "ciclo combinado"   in t: return "CCGT"
    if "bombeo mixto" in t or "consumo bombeo" in t: return "Pump_load"
    if "bombeo puro" in t or ("bombeo" in t and "turb" in t): return "Hydro_pump"
    if "hidráulica generación" in t: return "Hydro"
    if "re mercado hidráulica" in t: return "Hydro_RES"
    if "re mercado eólica" in t: return "Wind"
    if "re mercado solar fotovolt" in t: return "Solar_PV"
    if "nuclear" in t: return "Nuclear"
    return "Other"


def map_firm(owner):
    if not isinstance(owner, str): return "OTHER"
    s = owner.upper()
    if "IBERDROLA" in s: return "IB"
    if "ENDESA" in s: return "GE"
    if "NATURGY" in s or "GAS NATURAL" in s: return "GN"
    if "HIDROCANTABRICO" in s or " EDP" in s or s.startswith("EDP "): return "HC"
    if "REPSOL" in s: return "REP"
    if "ACCIONA" in s: return "ACC"
    if "GESTERNOVA" in s: return "GST"
    if "AXPO" in s: return "AXPO"
    if "SHELL" in s: return "SHELL"
    if "ENGIE" in s: return "ENGIE"
    if "NEXUS" in s: return "NEXUS"
    if "IGNIS" in s: return "IGNIS"
    return "OTHER"


def decompose_pair(p_i, q_i, p_j, q_j, clearing, h):
    """Restrict to band [clearing - h, clearing + h] and return (Δp, Δq) for
    one pair of quarter curves. Same metric as w1_decomposition.py."""
    if clearing is None or not np.isfinite(clearing):
        return np.nan, np.nan
    lo = clearing - h; hi = clearing + h
    pi = np.asarray(p_i, float); qi = np.asarray(q_i, float)
    pj = np.asarray(p_j, float); qj = np.asarray(q_j, float)
    mi = (pi >= lo) & (pi <= hi); pi, qi = pi[mi], qi[mi]
    mj = (pj >= lo) & (pj <= hi); pj, qj = pj[mj], qj[mj]
    oi = np.argsort(pi); pi, qi = pi[oi], qi[oi]
    oj = np.argsort(pj); pj, qj = pj[oj], qj[oj]
    Mi = float(qi.sum()); Mj = float(qj.sum())
    dq = abs(Mi - Mj)
    if Mi <= 1e-9 or Mj <= 1e-9:
        return 0.0, dq
    cum_i = np.cumsum(qi); cum_j = np.cumsum(qj)
    def p_at(u, cum, prices):
        k = np.searchsorted(cum, u, side="left")
        k = min(k, len(prices) - 1)
        return prices[k]
    M_common = min(Mi, Mj)
    U = np.linspace(M_common / 64, M_common, 64)
    dp = float(np.mean([abs(p_at(u, cum_i, pi) - p_at(u, cum_j, pj)) for u in U]))
    return dp, dq


def cell_decompose(q2t, clearing, h):
    """Return median Δp, median Δq, max Δp, max Δq across 6 quarter-pairs."""
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
    return (float(np.median(dps)), float(np.median(dqs)),
            float(np.max(dps)),    float(np.max(dqs)))


def main():
    print("Loading unit reference + tech / firm maps ...")
    units = pd.read_csv(UNITS_CSV)
    units["tech_group"] = units["technology"].apply(map_tech)
    units["firm"]       = units["owner_agent"].apply(map_firm)
    techs_of_interest = ["CCGT", "Wind", "Hydro", "Hydro_pump"]
    keep = units[(units["zone"] == "ZONA ESPAÑOLA")
                 & (units["tech_group"].isin(techs_of_interest))][
        ["unit_code", "firm", "tech_group"]
    ].drop_duplicates("unit_code")
    print(f"  {len(keep)} units kept (Spanish, in {techs_of_interest})")
    print("  by tech:", keep["tech_group"].value_counts().to_dict())

    con = duckdb.connect()
    con.execute("PRAGMA threads=4"); con.execute("SET memory_limit='10GB'")
    con.register("uft", keep)

    print(f"\nPulling tranches for {WINDOW} ...")
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
    SELECT d.d AS date, c.unit_code, u.firm, u.tech_group,
           ((d.period - 1) // 4)::INT AS hour,
           (((d.period - 1) % 4) + 1)::INT AS quarter,
           d.p, d.q
    FROM det d
      JOIN cab_l c USING (d, offer_code, version)
      JOIN uft   u USING (unit_code)
    """
    df = con.execute(q).df()
    print(f"  loaded {len(df):,} tranches")

    cl = con.execute(f"""
        SELECT date::DATE AS date,
               ((period - 1) // 4)::INT AS hour,
               AVG(price_es_eur_mwh) AS p_clear
        FROM '{MARGINALPDBC}'
        WHERE date::DATE >= DATE '{WINDOW[0]}' AND date::DATE < DATE '{WINDOW[1]}'
          AND price_es_eur_mwh IS NOT NULL AND mtu_minutes = 15
        GROUP BY 1, 2
    """).df()
    cl_map = {(row.date, row.hour): row.p_clear for row in cl.itertuples()}
    print(f"  {len(cl_map):,} (date, hour) clearing prices loaded")

    print("\nDecomposing each (firm, unit, date, hour) cell ...")
    rows = []
    grp = df.groupby(["firm", "tech_group", "unit_code", "date", "hour"], sort=False)
    n_groups = len(grp)
    for i, ((firm, tech, unit, date, hour), g) in enumerate(grp):
        if i % 50000 == 0 and i > 0:
            print(f"    {i:,}/{n_groups:,} ({100*i/n_groups:.0f}%)")
        by_q = {}
        for q_ in (1, 2, 3, 4):
            qq = g[g["quarter"] == q_]
            if qq.empty: continue
            agg = qq.groupby("p", as_index=False)["q"].sum()
            by_q[q_] = agg
        clearing = cl_map.get((date, hour))
        dp_med, dq_med, dp_max, dq_max = cell_decompose(by_q, clearing, KERNEL_BANDWIDTH)
        rows.append({
            "firm": firm, "tech": tech, "unit_code": unit, "date": date, "hour": hour,
            "hour_class": hour_class(hour),
            "dp_med": dp_med, "dq_med": dq_med, "dp_max": dp_max, "dq_max": dq_max,
            "p_clear": clearing,
        })
    cells = pd.DataFrame(rows)
    cells.to_parquet(OUTDIR / "multitech_cells.parquet", index=False)
    print(f"  decomposed {len(cells):,} cells")

    print("\n=== Summary by (firm, tech, hour_class), DA15/ID15 ===")
    # Restrict to critical + flat
    summ = (cells[cells["hour_class"].isin(["critical", "flat"])]
            .groupby(["firm", "tech", "hour_class"], observed=True).agg(
        n=("dp_max", "size"),
        share_dp_active=("dp_max", lambda v: (v > 1e-6).mean() * 100),
        share_dq_active=("dq_max", lambda v: (v > 1e-6).mean() * 100),
        med_dp_active=("dp_max", lambda v: np.median(v[v > 1e-6]) if (v > 1e-6).any() else np.nan),
        med_dq_active=("dq_max", lambda v: np.median(v[v > 1e-6]) if (v > 1e-6).any() else np.nan),
    ).round(2).reset_index())
    summ.to_csv(OUTDIR / "multitech_summary.csv", index=False)
    # Pivot and print
    for tech in ["CCGT", "Wind", "Hydro", "Hydro_pump"]:
        sub = summ[summ["tech"] == tech].sort_values(["hour_class", "share_dq_active"], ascending=[True, False])
        if len(sub) == 0: continue
        print(f"\n[{tech}]")
        print(sub.to_string(index=False))


if __name__ == "__main__":
    main()
