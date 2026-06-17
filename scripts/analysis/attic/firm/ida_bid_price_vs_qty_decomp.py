# STATUS: ALIVE
# LAST-AUDIT: 2026-05-11
# FEEDS: §4 mechanism evidence — IDA mirror of price-vs-quantity decomposition
# CLAIM: Test whether dominant firms exploit MTU15 granularity in the intraday
#        auctions (IDA1/IDA2/IDA3) via price-side variation, mirror of the DA
#        bid_price_vs_qty_quarter_decomp.py analysis.

from __future__ import annotations

from pathlib import Path
import sys

import duckdb
import numpy as np
import pandas as pd

PROJECT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT / "src"))
from mtu.classification.units import firm_unit_panel  # noqa: E402

ICAB = PROJECT / "data" / "processed" / "omie" / "mercado_intradiario_subastas" / "ofertas" / "icab_all.parquet"
IDET = PROJECT / "data" / "processed" / "omie" / "mercado_intradiario_subastas" / "ofertas" / "idet_all.parquet"
UNITS = PROJECT / "data" / "external" / "omie_reference" / "lista_unidades.csv"

OUTDIR = PROJECT / "results" / "regressions" / "bid"
OUTDIR.mkdir(parents=True, exist_ok=True)

CRITICAL = [5, 6, 7, 8, 16, 17, 18, 19, 20, 21, 22]
FLAT = [1, 2, 3]
MIDDAY = [11, 12, 13, 14]
CV_THRESHOLD = 0.01

THESIS_FIRMS = ["IB", "GE", "GN", "HC", "EDP-PT", "Repsol", "Engie", "TotalEnergies", "Moeve"]


def tech_bucket(t):
    if t is None: return "unknown"
    t = str(t)
    if "Solar Fotov" in t or "Solar Térm" in t: return "solar"
    if "Eólica" in t: return "wind"
    if "Hidráulica" in t or "Hidraulic" in t: return "hydro"
    if "Ciclo Combinado" in t: return "CCGT"
    if "Nuclear" in t: return "nuclear"
    if "Térmica no Renovab" in t: return "thermal_nonRE"
    return "other"


def main():
    con = duckdb.connect()
    con.execute("SET threads=4")
    con.execute("SET memory_limit='12GB'")

    units = firm_unit_panel(csv_path=str(UNITS), scheme="short", mode="primary_owner")
    units["tech"] = units["technology"].apply(tech_bucket)
    uft = units[units["parent"].isin(THESIS_FIRMS)][["unit_code", "parent", "tech"]].rename(
        columns={"parent": "firm"})
    con.register("uft", uft)
    print(f"{len(uft):,} thesis-firm unit-codes mapped")

    out_rows = []
    for sess in [1, 2, 3]:
        print(f"\n[Session IDA{sess}] Pulling per-period IDA bid aggregates (post-MTU15-DA Oct-Dec 2025)…")
        per_period = con.execute(f"""
            WITH icab AS (
                SELECT CAST(date AS DATE) AS d, session_number, offer_code, version, unit_code,
                       ROW_NUMBER() OVER (PARTITION BY CAST(date AS DATE), session_number, offer_code, unit_code
                                          ORDER BY version DESC) AS rn
                FROM '{ICAB}'
                WHERE buy_sell = 'V'
                  AND session_number = {sess}
                  AND CAST(date AS DATE) >= DATE '2025-10-01'
                  AND CAST(date AS DATE) <  DATE '2026-01-01'
            ),
            icab_l AS (SELECT * FROM icab WHERE rn = 1),
            idet AS (
                SELECT CAST(date AS DATE) AS d, session_number, offer_code, version, period,
                       price_eur_mwh AS price, quantity_mw AS qty
                FROM '{IDET}'
                WHERE session_number = {sess}
                  AND CAST(date AS DATE) >= DATE '2025-10-01'
                  AND CAST(date AS DATE) <  DATE '2026-01-01'
                  AND period BETWEEN 1 AND 96
                  AND quantity_mw IS NOT NULL AND quantity_mw > 0
                  AND price_eur_mwh IS NOT NULL
            )
            SELECT d.d AS d, d.period,
                   (d.period - 1) // 4 AS hour,
                   ((d.period - 1) % 4) AS quarter,
                   c.unit_code, u.firm, u.tech,
                   SUM(d.price * d.qty) / NULLIF(SUM(d.qty), 0) AS p_avg,
                   SUM(d.qty) AS q_total
            FROM idet d
              JOIN icab_l c USING (d, session_number, offer_code, version)
              JOIN uft u ON c.unit_code = u.unit_code
            GROUP BY 1,2,3,4,5,6,7
        """).df()
        print(f"   {len(per_period):,} per-period rows")

        if len(per_period) == 0: continue

        g = per_period.groupby(["d", "hour", "unit_code", "firm", "tech"])
        agg = g.agg(
            n_q=("p_avg", "count"),
            p_avg_mean=("p_avg", "mean"),
            p_avg_std=("p_avg", "std"),
            q_total_mean=("q_total", "mean"),
            q_total_std=("q_total", "std"),
        ).reset_index()
        agg = agg[agg["n_q"] == 4].copy()
        agg["cv_p"] = agg["p_avg_std"] / agg["p_avg_mean"].abs().clip(lower=1e-3)
        agg["cv_q"] = agg["q_total_std"] / agg["q_total_mean"].abs().clip(lower=1e-3)
        agg["price_var"] = (agg["cv_p"] > CV_THRESHOLD).astype(int)
        agg["qty_var"] = (agg["cv_q"] > CV_THRESHOLD).astype(int)
        agg["flag"] = np.where(agg["price_var"] & agg["qty_var"], "both",
                      np.where(agg["price_var"], "price-only",
                      np.where(agg["qty_var"], "qty-only", "neither")))

        def hour_class(h):
            if h in CRITICAL: return "critical"
            if h in FLAT:     return "flat"
            if h in MIDDAY:   return "midday"
            return "transitional"
        agg["hour_class"] = agg["hour"].apply(hour_class)
        agg = agg[agg["hour_class"].isin(["critical", "flat", "midday"])]
        print(f"   {len(agg):,} unit-hours with 4 complete quarters and in scope")

        for (firm, tech, hc), sub in agg.groupby(["firm", "tech", "hour_class"]):
            n = len(sub)
            if n < 50: continue
            row = {
                "session": sess, "firm": firm, "tech": tech, "hour_class": hc, "n": n,
                "share_price_only": (sub["flag"]=="price-only").mean(),
                "share_qty_only":   (sub["flag"]=="qty-only").mean(),
                "share_both":       (sub["flag"]=="both").mean(),
                "share_neither":    (sub["flag"]=="neither").mean(),
                "mean_cv_p":        sub["cv_p"].mean(),
                "mean_cv_q":        sub["cv_q"].mean(),
            }
            out_rows.append(row)

    out_df = pd.DataFrame(out_rows)
    csv_out = OUTDIR / "ida_bid_price_vs_qty_quarter_decomp.csv"
    out_df.to_csv(csv_out, index=False)
    print(f"\nWrote {csv_out}")

    # CCGT focus: across sessions
    print("\n=== IDA CCGT — share of unit-hours that vary across quarters (per session) ===")
    print(f"{'session':>8} {'firm':14s} {'hour':>10}  {'n':>6}  {'price-only':>11}  "
          f"{'qty-only':>9}  {'both':>6}  {'neither':>8}  {'cv_p':>7}  {'cv_q':>7}")
    for sess in [1, 2, 3]:
        ccgt = out_df[(out_df.session==sess) & (out_df.tech=='CCGT')]
        for firm in THESIS_FIRMS:
            for hc in ['flat','midday','critical']:
                r = ccgt[(ccgt.firm==firm) & (ccgt.hour_class==hc)]
                if r.empty: continue
                r = r.iloc[0]
                print(f"IDA{sess:>5}  {firm:14s} {hc:>10}  {int(r.n):6d}  "
                      f"{r.share_price_only:11.1%}  {r.share_qty_only:9.1%}  "
                      f"{r.share_both:6.1%}  {r.share_neither:8.1%}  "
                      f"{r.mean_cv_p:7.3f}  {r.mean_cv_q:7.3f}")


if __name__ == "__main__":
    main()
