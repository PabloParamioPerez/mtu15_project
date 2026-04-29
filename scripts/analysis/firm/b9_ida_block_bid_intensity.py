# STATUS: ALIVE
# LAST-AUDIT: 2026-04-29
# FEEDS: B9 IDA bid composition — mechanism diagnostic
# CLAIM: If asymmetric-clock IDA (DA60/ID15) raises the strategic cost of
#        gross repositioning trades (model_v2.tex §5.3 B^ID > B^DA), Big-4
#        firms may shift their IDA bid composition toward block bids
#        (cheaper per-MWh strategic cost of spanning multiple ISPs) and
#        away from simple bids (per-ISP price-impact intensive). This
#        script measures the cleared-volume share of block bids by firm
#        and regime in the IDA market.
"""B9 IDA block-bid intensity test.

Reads PIBCIE for cleared volume by firm by offer_type per regime, and
classifies offer types into:
  - "simple" (offer_type 1, 6, 7, 8): single-ISP bids
  - "block"  (offer_type 3, 9): multi-ISP block bids
  - "RE"     (offer_type 10): RE special regime
  - "other"  (other codes)

Reports % of cleared MWh in block bids per Big-4 firm per regime. The
mechanism prediction is:
  - If asymmetric clocks raise B^ID, firms increase block-bid share to
    span ISPs more cheaply.
  - If clock structure is symmetric, simple bids per-ISP suffice.

Output: firm × regime block-bid share.
"""
from __future__ import annotations
from pathlib import Path
import time
import duckdb
import pandas as pd

PROJECT  = Path(__file__).resolve().parents[3]
PIBCIE   = PROJECT / "data" / "processed" / "omie" / "mercado_intradiario_subastas" / "programas" / "pibcie_all.parquet"
OUTDIR   = PROJECT / "results" / "regressions" / "b9_block_bid_intensity"
OUTDIR.mkdir(parents=True, exist_ok=True)

REGIMES = ["pre-IDA", "3-sess", "ISP15-win", "DA60/ID15", "DA15/ID15"]
BIG4 = ["GE", "IB", "GN", "HC"]


def main() -> None:
    t0 = time.time()
    print(f"[{time.strftime('%H:%M:%S')}] Starting block-bid intensity test…", flush=True)

    con = duckdb.connect()
    con.execute("SET memory_limit='6GB'")
    con.execute("SET threads=4")
    con.execute("SET preserve_insertion_order=false")

    big4_sql = "(" + ",".join(f"'{f}'" for f in BIG4) + ")"
    REGIME_CASE = """
        CASE WHEN date < '2024-06-14' THEN 'pre-IDA'
             WHEN date < '2024-12-01' THEN '3-sess'
             WHEN date < '2025-03-19' THEN 'ISP15-win'
             WHEN date < '2025-10-01' THEN 'DA60/ID15'
             ELSE 'DA15/ID15' END
    """

    # ============================================================
    # Volume by firm × regime × offer_type, ABSOLUTE values
    # (we want to know how MUCH was cleared by type, regardless of sign)
    # ============================================================
    print("[1/2] Aggregating |cleared volume| by firm × regime × offer_type…", flush=True)
    df = con.execute(f"""
        SELECT grupo_empresarial AS firm,
               {REGIME_CASE} AS regime,
               offer_type,
               SUM(ABS(assigned_power_mw) * mtu_minutes / 60.0) AS abs_mwh,
               SUM(assigned_power_mw * mtu_minutes / 60.0)      AS net_mwh,
               COUNT(*)                                         AS n_records
        FROM '{PIBCIE}'
        WHERE assigned_power_mw IS NOT NULL
          AND grupo_empresarial IN {big4_sql}
        GROUP BY 1, 2, 3
    """).df()
    df["regime"] = pd.Categorical(df["regime"], categories=REGIMES, ordered=True)
    print(f"   firm × regime × offer_type rows: {len(df):,}", flush=True)
    print(f"   offer_type distribution (Big-4 only):", flush=True)
    print(df.groupby("offer_type")["abs_mwh"].sum().round(0).sort_values(ascending=False).to_string(), flush=True)
    print()

    # Classify offer_types into simple / block / RE / other
    SIMPLE_TYPES = [1, 6, 7, 8]
    BLOCK_TYPES  = [3, 9]
    RE_TYPES     = [10]

    def classify(ot):
        if ot in SIMPLE_TYPES: return "simple"
        if ot in BLOCK_TYPES:  return "block"
        if ot in RE_TYPES:     return "RE"
        return "other"
    df["bid_class"] = df["offer_type"].apply(classify)

    composition = (df.groupby(["firm", "regime", "bid_class"], observed=True)["abs_mwh"]
                       .sum()
                       .reset_index())
    composition_pv = (composition.pivot_table(index=["firm", "regime"],
                                              columns="bid_class",
                                              values="abs_mwh", fill_value=0,
                                              observed=True))
    composition_pv["total"] = composition_pv.sum(axis=1)
    for cls in ["simple", "block", "RE", "other"]:
        if cls in composition_pv.columns:
            composition_pv[f"{cls}_pct"] = 100 * composition_pv[cls] / composition_pv["total"].clip(lower=1)
    composition_pv.to_csv(OUTDIR / "bid_composition_by_firm_regime.csv")
    print("Bid composition by firm × regime (% of |cleared MWh|):", flush=True)
    pct_cols = [c for c in ["simple_pct", "block_pct", "RE_pct", "other_pct"] if c in composition_pv.columns]
    print(composition_pv[pct_cols].round(1).to_string(), flush=True)
    print()

    # ============================================================
    # Block-bid share by firm × regime, transposed for easy reading
    # ============================================================
    print("[2/2] Block-bid share trajectory by firm:", flush=True)
    if "block_pct" in composition_pv.columns:
        block_share = composition_pv["block_pct"].unstack("regime").reindex(BIG4)[REGIMES]
        print(block_share.round(1).to_string(), flush=True)
        block_share.to_csv(OUTDIR / "block_share_pct_by_firm_regime.csv")
    print()

    # Also: simple-bid share trajectory
    if "simple_pct" in composition_pv.columns:
        simple_share = composition_pv["simple_pct"].unstack("regime").reindex(BIG4)[REGIMES]
        print("Simple-bid share by firm × regime (% of |cleared MWh|):", flush=True)
        print(simple_share.round(1).to_string(), flush=True)
        simple_share.to_csv(OUTDIR / "simple_share_pct_by_firm_regime.csv")
    print()

    print("Reading: if asymmetric-window regimes (3-sess, ISP15-win, DA60/ID15) show", flush=True)
    print("a higher block-bid share than symmetric (pre-IDA, DA15/ID15), this is", flush=True)
    print("evidence the strategic-cost asymmetry B^ID > B^DA pushes firms toward", flush=True)
    print("block bidding when the IDA clock is fine.", flush=True)
    print(flush=True)
    print("Caveat: pre-IDA had 6 IDA sessions; 3-sess onward has 3 sessions. The", flush=True)
    print("session-architecture change (June 2024 SIDC) also affected bid format", flush=True)
    print("availability — the 'icab block_order' fields appear post-2024-06-14.", flush=True)

    print(f"\nTotal runtime: {(time.time() - t0)/60:.1f} min", flush=True)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        import traceback
        print(f"ERROR: {type(e).__name__}: {e}", flush=True)
        print(traceback.format_exc(), flush=True)
        raise
