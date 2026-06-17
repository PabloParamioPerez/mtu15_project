# STATUS: ALIVE
# LAST-AUDIT: 2026-04-26
# FEEDS: B8 (refinement to disentangle within-unit vs participation-spreading)
# CLAIM: Within-unit tranches-per-period (avg per unit then avg across units), separate from participation spreading
"""B8 refinement: separate within-unit complexification from participation spreading.

The firm-aggregate `bid_complexity_panel.py` metric mixes two effects:
  (a) within-unit tranches-per-period (does each unit bid more tranches per
      ISP in which it participates?)
  (b) participation pattern (how many of a firm's units bid in any given ISP?)

This script isolates (a). For each (unit, month), compute tranches-per-
active-period within that unit. Then average across units belonging to
the same firm.

If (a) is up but firm-aggregate is down, the firm-aggregate decline
reflects participation-spreading (fewer units per ISP), not bid
simplification.
"""
from __future__ import annotations

from pathlib import Path
import duckdb
import pandas as pd

PROJECT = Path(__file__).resolve().parents[3]
DET = PROJECT / "data" / "processed" / "omie" / "mercado_diario" / "ofertas" / "det_all.parquet"
CAB = PROJECT / "data" / "processed" / "omie" / "mercado_diario" / "ofertas" / "cab_all.parquet"
PDBCE = PROJECT / "data" / "processed" / "omie" / "mercado_diario" / "programas" / "pdbce_all.parquet"
EXIT_CSV = PROJECT / "results" / "regressions" / "ccgt_extensive_margin_exit.csv"


def main() -> None:
    exit_df = pd.read_csv(EXIT_CSV)
    fringe_survivors = exit_df[
        (exit_df["firm_group"] == "Fringe")
        & (~exit_df["exited"].astype(bool))
        & (exit_df["pre_active_obs"] > 0)
        & (exit_df["post_active_obs"] > 0)
    ]["unit_code"].tolist()

    con = duckdb.connect()
    con.execute("SET memory_limit='1500MB'")
    con.execute("SET threads=4")

    print("[1/3] unit -> firm map...")
    con.execute(f"""
        CREATE TEMP TABLE unit_firm AS
        WITH cnts AS (
            SELECT unit_code, grupo_empresarial, COUNT(*) n
            FROM '{PDBCE}'
            WHERE offer_type = 1 AND grupo_empresarial IS NOT NULL
            GROUP BY 1, 2
        ),
        ranked AS (
            SELECT unit_code, grupo_empresarial,
                   ROW_NUMBER() OVER (PARTITION BY unit_code ORDER BY n DESC) rk
            FROM cnts
        )
        SELECT unit_code, grupo_empresarial AS firm
        FROM ranked WHERE rk = 1
    """)

    print("[2/3] cab_small with firm group...")
    survivor_list = ",".join(f"'{u}'" for u in fringe_survivors) if fringe_survivors else "''"
    con.execute(f"""
        CREATE TEMP TABLE cab_small AS
        SELECT c.date, c.offer_code, c.version, c.unit_code,
               CASE WHEN uf.firm IN ('GE','IB','GN','HC') THEN uf.firm
                    WHEN c.unit_code IN ({survivor_list}) THEN 'Fringe-surv'
                    ELSE 'Fringe-other' END AS firm_group
        FROM '{CAB}' c
        LEFT JOIN unit_firm uf USING (unit_code)
        WHERE c.buy_sell = 'V'
    """)

    print("[3/3] per-unit per-month tranches-per-active-period; then avg across units...")
    df = con.sql(f"""
        WITH joined AS (
            SELECT c.firm_group, c.unit_code,
                   d.date, d.period, d.quantity_mw
            FROM '{DET}' d
            JOIN cab_small c
              ON c.date = d.date
             AND c.offer_code = d.offer_code
             AND c.version = d.version
            WHERE d.quantity_mw > 0
        ),
        per_unit_month AS (
            SELECT firm_group, unit_code,
                   DATE_TRUNC('month', CAST(date AS DATE)) AS month,
                   COUNT(*) AS u_total_rows,
                   COUNT(DISTINCT (date, period)) AS u_active_cells,
                   COUNT(*) * 1.0 / NULLIF(COUNT(DISTINCT (date, period)), 0)
                     AS u_tpp
            FROM joined
            WHERE firm_group != 'Fringe-other'
            GROUP BY firm_group, unit_code, month
        )
        SELECT firm_group, month,
               COUNT(*) AS units_in_month,
               AVG(u_tpp) AS within_unit_tpp_unweighted,
               -- Weighted by unit's active cells (gives more weight to units that bid frequently)
               SUM(u_tpp * u_active_cells) / NULLIF(SUM(u_active_cells), 0) AS within_unit_tpp_weighted,
               SUM(u_total_rows) AS firm_total_rows,
               -- Unique (date, period) cells where the firm has ANY unit bidding
               -- (we cannot reconstruct this here without re-aggregating the joined CTE,
               --  so the firm-aggregate metric stays in the sister script.)
               SUM(u_active_cells) AS sum_unit_active_cells
        FROM per_unit_month
        GROUP BY firm_group, month
        ORDER BY firm_group, month
    """).df()
    df["month"] = pd.to_datetime(df["month"])

    print()
    print("=" * 100)
    print("Within-unit tranches-per-period (averaged across firm's units)")
    print("=" * 100)

    # Headline ratio
    print()
    print(f"{'firm':<13}  {'pre tpp':>10}  {'post tpp':>10}  {'ratio':>8}  {'verdict'}")
    rows_out = []
    for fg in ["GE", "IB", "GN", "HC", "Fringe-surv"]:
        sub = df[df["firm_group"] == fg].copy()
        if len(sub) == 0:
            continue
        sub["era"] = sub["month"].apply(
            lambda d: "post" if pd.Timestamp(d) >= pd.Timestamp("2025-03-19") else "pre"
        )
        # Use weighted within-unit tpp, weighted further by sum_unit_active_cells across months
        for era in ["pre", "post"]:
            cell = sub[sub["era"] == era]
            if len(cell) == 0:
                continue
            w = cell["sum_unit_active_cells"]
            tpp = (cell["within_unit_tpp_weighted"] * w).sum() / w.sum() if w.sum() > 0 else float("nan")
            rows_out.append({"firm_group": fg, "era": era, "within_unit_tpp": tpp})
        out_pre = next((r for r in rows_out if r["firm_group"] == fg and r["era"] == "pre"), None)
        out_post = next((r for r in rows_out if r["firm_group"] == fg and r["era"] == "post"), None)
        if not (out_pre and out_post):
            continue
        ratio = out_post["within_unit_tpp"] / out_pre["within_unit_tpp"]
        if ratio < 0.7:
            v = "SIMPLIFIED"
        elif ratio < 0.95:
            v = "modest decline"
        elif ratio < 1.10:
            v = "stable"
        elif ratio < 1.50:
            v = "complexified"
        else:
            v = "STRONGLY COMPLEXIFIED"
        print(f"{fg:<13}  {out_pre['within_unit_tpp']:>10.2f}  {out_post['within_unit_tpp']:>10.2f}  {ratio:>8.3f}  {v}")

    # Comparison: firm-aggregate vs within-unit
    print()
    print("Reconciliation with firm-aggregate metric (`bid_complexity_panel.py`):")
    print("  firm        firm-agg ratio   within-unit ratio   difference reflects:")
    fa_ratios = {"GE": 0.673, "IB": 1.515, "GN": 0.486, "HC": 0.584, "Fringe-surv": 1.288}
    for fg in ["GE", "IB", "GN", "HC", "Fringe-surv"]:
        out_pre = next((r for r in rows_out if r["firm_group"] == fg and r["era"] == "pre"), None)
        out_post = next((r for r in rows_out if r["firm_group"] == fg and r["era"] == "post"), None)
        if not (out_pre and out_post):
            continue
        wu_ratio = out_post["within_unit_tpp"] / out_pre["within_unit_tpp"]
        fa = fa_ratios.get(fg, float("nan"))
        if wu_ratio > 1 and fa < 1:
            diff = "PARTICIPATION SPREADING (within-unit up, firm-agg down: fewer units per ISP)"
        elif abs(wu_ratio - fa) < 0.10:
            diff = "consistent (no participation effect)"
        else:
            diff = "mixed"
        print(f"  {fg:<11}  {fa:>13.3f}  {wu_ratio:>17.3f}    {diff}")

    out = PROJECT / "results" / "regressions" / "bid_complexity_unit_level.csv"
    df.to_csv(out, index=False)
    print(f"\nwrote {out}")


if __name__ == "__main__":
    main()
