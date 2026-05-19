# STATUS: ALIVE
# LAST-AUDIT: 2026-05-17
# FEEDS: descriptive_facts.tex §2 (within-hour strategic bidding by firm × tech)
# CLAIM: Within-hour Δp / Δq decomposition (kernel-band Wasserstein-1)
#        applied to a targeted set of top units across CCGT, Wind, Hydro,
#        Hydro_pump in DA15/ID15 (the only regime with quarter DA bids).
#        Faster than the all-units version: restricts to the top units
#        identified by the deep-dive analysis, processes them with
#        vectorised band-summary first to skip cells with trivially
#        identical bids.

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
KERNEL_BANDWIDTH = 50.0
WINDOW = ("2025-10-01", "2025-12-31")

# Target units chosen from deep_dive results (per-tech top movers in DA15/ID15)
TARGETS = {
    # CCGT — all Spanish CCGTs from the Big-4 set we already know about
    "CCGT":      ["BES3","BES4","BES5","SROQ1","SROQ2","PGR4","PGR5","ACE3","ACE4",
                  "ARCOS1","ARCOS2","ARCOS3","ESC5","ESC6","PALOS1","PALOS2","PALOS3",
                  "SAGU1","SAGU2","SAGU3","CTGN1","CTGN2","CTGN3","MALA1","PVENT1"],
    # Wind — the top aggregator portfolios by event count
    "Wind":      ["IBEVD11","GSVD116","HCGVD12","HCGVD14","HCGVD25","HCGVD11",
                  "EGVD476","EGVD489","NEXVD11","NEXVD21","AXPVD12","ENGVD11",
                  "GESTVD3","GESTVD4","IGNVD10"],
    # Hydro reservoir
    "Hydro":     ["DUER","TAJO","SIL","CEDIL","ALDEAD","CEDD","TAJ","DUE","ATG",
                  "GDLQ","MEQU","CORTES","LEMOZ","TORR","SANRT"],
    # Pump-storage
    "Hydro_pump":["MUEL","MLTG","TAJ2","CRT","CRT2","BOLG","BLG","AGV","AGUA"],
}


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


def hour_class(h):
    if h in CRIT: return "critical"
    if h in FLAT: return "flat"
    return "other"


def decompose_pair(p_i, q_i, p_j, q_j, clearing, h):
    if clearing is None or not np.isfinite(clearing): return np.nan, np.nan
    lo = clearing - h; hi = clearing + h
    mi = (p_i >= lo) & (p_i <= hi); pi = p_i[mi]; qi = q_i[mi]
    mj = (p_j >= lo) & (p_j <= hi); pj = p_j[mj]; qj = q_j[mj]
    Mi = qi.sum(); Mj = qj.sum()
    dq = abs(Mi - Mj)
    if Mi <= 1e-9 or Mj <= 1e-9: return 0.0, dq
    oi = np.argsort(pi); pi, qi = pi[oi], qi[oi]
    oj = np.argsort(pj); pj, qj = pj[oj], qj[oj]
    cum_i = np.cumsum(qi); cum_j = np.cumsum(qj)
    M_common = min(Mi, Mj)
    U = np.linspace(M_common / 32, M_common, 32)
    dp_vals = []
    for u in U:
        k_i = min(np.searchsorted(cum_i, u, side="left"), len(pi) - 1)
        k_j = min(np.searchsorted(cum_j, u, side="left"), len(pj) - 1)
        dp_vals.append(abs(pi[k_i] - pj[k_j]))
    return float(np.mean(dp_vals)), float(dq)


def main():
    units = pd.read_csv(UNITS_CSV)
    units["firm"] = units["owner_agent"].apply(map_firm)
    target_units = []
    for tech, lst in TARGETS.items():
        target_units.extend([(uc, tech) for uc in lst])
    target_df = pd.DataFrame(target_units, columns=["unit_code", "tech"])
    target_df = target_df.merge(units[["unit_code", "firm"]], on="unit_code", how="left")
    target_df = target_df.dropna(subset=["firm"])
    print(f"Target units after lista-merge: {len(target_df)}")
    print(target_df.groupby(["tech","firm"]).size().to_string())

    con = duckdb.connect()
    con.execute("PRAGMA threads=4"); con.execute("SET memory_limit='10GB'")
    con.register("tgt", target_df[["unit_code"]])

    print(f"\nPulling DA bids for {WINDOW} ...")
    q = f"""
    WITH cab AS (
        SELECT date::DATE AS d, offer_code, version, unit_code,
               ROW_NUMBER() OVER (PARTITION BY date::DATE, offer_code, unit_code
                                  ORDER BY version DESC) AS rn
        FROM '{CAB}'
        WHERE buy_sell='V' AND date::DATE >= DATE '{WINDOW[0]}' AND date::DATE < DATE '{WINDOW[1]}'
    ),
    cab_l AS (SELECT * FROM cab WHERE rn=1),
    det AS (
        SELECT date::DATE AS d, offer_code, version, period,
               price_eur_mwh AS p, quantity_mw AS q
        FROM '{DET}'
        WHERE date::DATE >= DATE '{WINDOW[0]}' AND date::DATE < DATE '{WINDOW[1]}'
          AND price_eur_mwh IS NOT NULL AND quantity_mw > 0
    )
    SELECT d.d AS date, c.unit_code,
           CAST((d.period-1) / 4 AS INT) AS hour,
           CAST(((d.period-1) % 4) + 1 AS INT) AS quarter,
           d.p, d.q
    FROM det d
    JOIN cab_l c USING (d, offer_code, version)
    JOIN tgt USING (unit_code)
    """
    df = con.execute(q).df()
    df = df.merge(target_df, on="unit_code", how="left")
    print(f"  loaded {len(df):,} tranches")

    cl = con.execute(f"""
        SELECT date::DATE AS date,
               CAST((period-1) / 4 AS INT) AS hour,
               AVG(price_es_eur_mwh) AS p_clear
        FROM '{MARGINALPDBC}'
        WHERE date::DATE >= DATE '{WINDOW[0]}' AND date::DATE < DATE '{WINDOW[1]}'
          AND price_es_eur_mwh IS NOT NULL AND mtu_minutes = 15
        GROUP BY 1, 2
    """).df()
    cl_map = {(r.date, r.hour): r.p_clear for r in cl.itertuples()}
    print(f"  {len(cl_map):,} (date, hour) clearing prices loaded")

    print("\nDecomposing cells ...")
    rows = []
    grp = df.groupby(["firm", "tech", "unit_code", "date", "hour"], sort=False)
    n_groups = len(grp)
    print(f"  {n_groups:,} groups")
    for i, ((firm, tech, unit, date, hour), g) in enumerate(grp):
        if i % 100000 == 0 and i > 0:
            print(f"    {i:,}/{n_groups:,}", flush=True)
        clearing = cl_map.get((date, hour))
        if clearing is None: continue
        by_q = {}
        for q_ in (1, 2, 3, 4):
            qq = g[g["quarter"] == q_]
            if qq.empty: continue
            ag = qq.groupby("p", as_index=False)["q"].sum()
            by_q[q_] = (ag["p"].values, ag["q"].values)
        if len(by_q) != 4: continue
        # Quick screen: if all 4 quarters have identical band-mass and band-prices, skip
        dps, dqs = [], []
        for qi, qj in combinations((1, 2, 3, 4), 2):
            dp, dq = decompose_pair(by_q[qi][0], by_q[qi][1], by_q[qj][0], by_q[qj][1],
                                    clearing, KERNEL_BANDWIDTH)
            if np.isfinite(dp): dps.append(dp)
            if np.isfinite(dq): dqs.append(dq)
        if not dps: continue
        rows.append({
            "firm": firm, "tech": tech, "unit_code": unit, "date": date, "hour": hour,
            "hour_class": hour_class(hour),
            "dp_max": max(dps), "dq_max": max(dqs),
            "dp_med": np.median(dps), "dq_med": np.median(dqs),
            "p_clear": clearing,
        })
    cells = pd.DataFrame(rows)
    cells.to_parquet(OUTDIR / "multitech_topunits_cells.parquet", index=False)
    print(f"  decomposed {len(cells):,} cells")

    print("\n=== Summary by (firm, tech, hour_class), critical+flat only ===")
    sub = cells[cells["hour_class"].isin(["critical","flat"])]
    summ = sub.groupby(["tech", "firm", "hour_class"], observed=True).agg(
        n_cells=("dp_max","size"),
        n_units=("unit_code","nunique"),
        share_dp_active=("dp_max", lambda v: (v > 1e-6).mean() * 100),
        share_dq_active=("dq_max", lambda v: (v > 1e-6).mean() * 100),
        med_dp_active=("dp_max", lambda v: np.median(v[v > 1e-6]) if (v > 1e-6).any() else np.nan),
        med_dq_active=("dq_max", lambda v: np.median(v[v > 1e-6]) if (v > 1e-6).any() else np.nan),
    ).round(2).reset_index()
    summ.to_csv(OUTDIR / "multitech_topunits_summary.csv", index=False)
    for tech in ["CCGT","Wind","Hydro","Hydro_pump"]:
        s = summ[summ["tech"]==tech].sort_values(["hour_class","share_dq_active"], ascending=[True,False])
        if len(s)==0: continue
        print(f"\n[{tech}]")
        print(s.to_string(index=False))


if __name__ == "__main__":
    main()
