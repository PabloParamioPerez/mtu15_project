# STATUS: ALIVE
# LAST-AUDIT: 2026-06-06
# FEEDS: thesis/paper/thesis.tex --- tests Claim D from NeuroDATE_II
#        (3 Dec 2025): "estamos sacando con las comercializadoras más
#        de 2 EUR/MWh por este arbitraje". User's reading: re-reforzada
#        displaces TR-up (real-time) into Fase I (pre-IDA), so the
#        system arrives at IDA1 with MORE unresolved imbalance, which
#        retailers/producers arbitrage. Test in IDA bids.
#
# Expected signatures, per session:
#   (a) more in-band tranches per (date, session, unit) curve post-DA15
#   (b) higher offered in-band volume (sum of MW) per session-week
#   (c) higher total offered tranches per session-week
#
# WINDOWS: pre-DA15 (DA60+ID15 era) 2025-04-28 to 2025-09-30, post-DA15
#          2025-10-01 to 2025-12-31. Pre-DA15 starts from blackout to
#          hold reforzada constant; re-reforzada (Nov 2025) sits inside
#          the post window.
#
# OUT: results/regressions/bid/claim_D_ida_activity.csv
#      Console summary.

from pathlib import Path
import duckdb
import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
ICAB = REPO / "data/processed/omie/mercado_intradiario_subastas/ofertas/icab_all.parquet"
IDET = REPO / "data/processed/omie/mercado_intradiario_subastas/ofertas/idet_all.parquet"
MPIBC = REPO / "data/processed/omie/mercado_intradiario_subastas/precios/marginalpibc_all.parquet"
UMAP = REPO / "data/derived/panels/bid_shape_critical_flat/_unit_map.parquet"
OUT = REPO / "results/regressions/bid/claim_D_ida_activity.csv"

H_BAND = 150.0
PRE_LO, PRE_HI = "2025-04-28", "2025-09-30"
POST_LO, POST_HI = "2025-10-01", "2025-12-31"


def fetch(con, sess, side, tech):
    """In-band sells/buys for (session, side, tech). Returns per-curve aggregates."""
    buy_sell = "V" if side == "supply" else "C"
    tech_filter = "AND u.tech_group = '"+tech+"'" if tech != "ALL" else ""
    umap_join = "JOIN '"+str(UMAP)+"' u ON c.unit_code = u.unit_code" if tech != "ALL" else ""
    q = f"""
    WITH mcp_raw AS (
      SELECT CAST(date AS DATE) AS d, session_number, period,
             price_es_eur_mwh AS mcp,
             ROW_NUMBER() OVER (PARTITION BY date::DATE, session_number, period
                                 ORDER BY mtu_minutes ASC) AS rn
      FROM '{MPIBC}'
      WHERE session_number = {sess}
        AND price_es_eur_mwh IS NOT NULL
        AND CAST(date AS DATE) BETWEEN '{PRE_LO}' AND '{POST_HI}'
    ),
    mcp AS (SELECT d, session_number, period, mcp FROM mcp_raw WHERE rn=1),
    banded AS (
      SELECT CAST(c.date AS DATE) AS d, c.session_number AS sess, c.offer_code,
             c.unit_code, d.period, d.price_eur_mwh AS p, d.quantity_mw AS q
      FROM '{ICAB}' c JOIN '{IDET}' d
        ON c.date = d.date AND c.session_number = d.session_number
       AND c.offer_code = d.offer_code AND c.version = d.version
      {umap_join}
      JOIN mcp m ON CAST(c.date AS DATE) = m.d AND c.session_number = m.session_number
                AND d.period = m.period
      WHERE c.buy_sell = '{buy_sell}'
        AND c.session_number = {sess}
        AND c.block_order_avg_price_eur IS NULL
        AND d.price_eur_mwh IS NOT NULL AND d.quantity_mw > 0
        AND ABS(d.price_eur_mwh - m.mcp) <= {H_BAND}
        {tech_filter}
        AND c.date BETWEEN '{PRE_LO}' AND '{POST_HI}'
    )
    SELECT d, sess, unit_code, period,
           COUNT(*) AS n_tranche, SUM(q) AS in_band_mw
    FROM banded GROUP BY 1, 2, 3, 4
    """
    return con.execute(q).df()


def main():
    con = duckdb.connect()
    con.execute("SET threads=4; SET memory_limit='6GB'")

    rows = []
    cells = [
        ("supply", "CCGT"),
        ("supply", "ALL"),
        ("demand", "ALL"),
    ]
    for side, tech in cells:
        for sess in (1, 2, 3):
            df = fetch(con, sess, side, tech)
            if len(df) == 0:
                continue
            df["d"] = pd.to_datetime(df["d"])
            df["regime"] = np.where(df["d"] < pd.to_datetime(POST_LO), "pre_DA15", "post_DA15")
            # Per-curve mean tranche count and in-band MW.
            for reg in ("pre_DA15", "post_DA15"):
                sub = df[df["regime"] == reg]
                if len(sub) == 0:
                    continue
                n_curves = len(sub)
                n_days = sub["d"].nunique()
                mean_n_tranche = sub["n_tranche"].mean()
                mean_in_band_mw = sub["in_band_mw"].mean()
                # Per-day totals (system-wide activity).
                daily_curves = sub.groupby("d").size()
                daily_mw = sub.groupby("d")["in_band_mw"].sum()
                rows.append({
                    "side": side, "tech": tech, "sess": sess, "regime": reg,
                    "n_curves": n_curves, "n_days": n_days,
                    "curves_per_day": daily_curves.mean(),
                    "mean_n_tranche": mean_n_tranche,
                    "mean_in_band_mw_per_curve": mean_in_band_mw,
                    "total_in_band_mw_per_day": daily_mw.mean(),
                })
    con.close()

    out_df = pd.DataFrame(rows)
    OUT.parent.mkdir(parents=True, exist_ok=True)
    out_df.to_csv(OUT, index=False)
    print(f"Wrote {OUT} with {len(out_df)} rows.\n")

    # Pretty table: pre vs post for each (side, tech, sess).
    for side, tech in cells:
        print(f"\n=== {side.upper()} side, {tech} ===")
        sub = out_df[(out_df["side"] == side) & (out_df["tech"] == tech)]
        if sub.empty:
            print("  (no data)"); continue
        print(f"{'sess':>5s} {'regime':>10s}  "
              f"{'curves/day':>11s}  {'tranches/curve':>15s}  "
              f"{'MW/curve':>9s}  {'MW/day (sum)':>13s}")
        for sess in (1, 2, 3):
            for reg in ("pre_DA15", "post_DA15"):
                r = sub[(sub["sess"] == sess) & (sub["regime"] == reg)]
                if r.empty:
                    continue
                r = r.iloc[0]
                print(f"  IDA{sess:1d} {reg:>10s}  "
                      f"{r['curves_per_day']:>11,.0f}  "
                      f"{r['mean_n_tranche']:>15.2f}  "
                      f"{r['mean_in_band_mw_per_curve']:>9.2f}  "
                      f"{r['total_in_band_mw_per_day']:>13,.0f}")
        # % change
        print(f"\n  % change post vs pre:")
        for sess in (1, 2, 3):
            pre  = sub[(sub["sess"] == sess) & (sub["regime"] == "pre_DA15")]
            post = sub[(sub["sess"] == sess) & (sub["regime"] == "post_DA15")]
            if pre.empty or post.empty: continue
            pre = pre.iloc[0]; post = post.iloc[0]
            d_curves = 100*(post["curves_per_day"]-pre["curves_per_day"])/pre["curves_per_day"]
            d_tranches = 100*(post["mean_n_tranche"]-pre["mean_n_tranche"])/pre["mean_n_tranche"]
            d_mw_curve = 100*(post["mean_in_band_mw_per_curve"]-pre["mean_in_band_mw_per_curve"])/pre["mean_in_band_mw_per_curve"]
            d_mw_day = 100*(post["total_in_band_mw_per_day"]-pre["total_in_band_mw_per_day"])/pre["total_in_band_mw_per_day"]
            print(f"  IDA{sess:1d}: curves/day {d_curves:+6.1f}%  "
                  f"tranches/curve {d_tranches:+6.1f}%  "
                  f"MW/curve {d_mw_curve:+6.1f}%  "
                  f"MW/day {d_mw_day:+6.1f}%")


if __name__ == "__main__":
    main()
