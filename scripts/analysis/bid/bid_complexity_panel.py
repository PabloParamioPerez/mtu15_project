# STATUS: ALIVE
# LAST-AUDIT: 2026-04-26
# FEEDS: B8 (robustness)
# CLAIM: Per-firm-group monthly bid-complexity panel; tests (a) generality, (b) timing, (c) Big-4-vs-Fringe heterogeneity
"""B8 robustness check: per-firm-group monthly bid-complexity panel.

Three questions on top of the named-unit verification:

(a) Does the per-ISP bid complexification generalise beyond the 4 named
    complex-bidders (TAPOWER/SRI4R/ARCOS1/CTN4)?
(b) When exactly does it happen — abrupt at MTU15-IDA (2025-03-19),
    or only at MTU15-DA (2025-10-01), or gradual?
(c) Does Fringe behave the same way, or is it Big-4-specific?
    (If Fringe also complexifies, the effect is mechanical/format;
    if only Big-4, it's strategic.)

Spec. Aggregate det_all + cab_all per (firm_group, year_month):
    total_tranche_rows, active (date,period) cells,
    tranches_per_period = total / active_cells.
Restrict to sell-side. Firm groups: GE, IB, GN, HC, Fringe-survivor.
'Fringe-survivor' = Fringe units active in BOTH pre and post regimes
(per ccgt_extensive_margin_exit.py output) — excludes the 26 exiters
to avoid contaminating with extensive-margin effects.

Memory-conscious: DuckDB streams; only the small per-(firm-group,
month) panel comes back to Python.
"""
from __future__ import annotations

from pathlib import Path
import duckdb
import pandas as pd

PROJECT = Path(__file__).resolve().parents[3]
DET = PROJECT / "data" / "processed" / "omie" / "mercado_diario" / "ofertas" / "det_all.parquet"
CAB = PROJECT / "data" / "processed" / "omie" / "mercado_diario" / "ofertas" / "cab_all.parquet"
PDBCE = PROJECT / "data" / "processed" / "omie" / "mercado_diario" / "programas" / "pdbce_all.parquet"
EXIT_CSV = PROJECT / "data" / "derived" / "results" / "ccgt_extensive_margin_exit.csv"


def assign_regime(d):
    d = pd.Timestamp(d)
    if d < pd.Timestamp("2024-06-14"):
        return "pre-IDA"
    if d < pd.Timestamp("2024-12-01"):
        return "3-sess"
    if d < pd.Timestamp("2025-03-19"):
        return "ISP15 win"
    if d < pd.Timestamp("2025-10-01"):
        return "DA60/ID15"
    return "DA15/ID15"


def main() -> None:
    # Identify Fringe-survivor units (Fringe + active both pre and post)
    exit_df = pd.read_csv(EXIT_CSV)
    fringe_survivors = exit_df[
        (exit_df["firm_group"] == "Fringe")
        & (~exit_df["exited"].astype(bool))
        & (exit_df["pre_active_obs"] > 0)
        & (exit_df["post_active_obs"] > 0)
    ]["unit_code"].tolist()
    print(f"Fringe-survivor units: {len(fringe_survivors):,}")

    con = duckdb.connect()
    con.execute("SET memory_limit='1500MB'")
    con.execute("SET threads=4")

    # Build unit -> firm map from pdbce (mode firm per unit)
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

    # Build cab_small (sell-side, with firm)
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

    # Aggregate det_all join cab_small per (firm_group, year_month)
    print("[3/3] tranches per (firm_group, month)...")
    df = con.sql(f"""
        WITH joined AS (
            SELECT c.firm_group,
                   d.date,
                   d.period,
                   d.quantity_mw
            FROM '{DET}' d
            JOIN cab_small c
              ON c.date = d.date
             AND c.offer_code = d.offer_code
             AND c.version = d.version
            WHERE d.quantity_mw > 0
        )
        SELECT firm_group,
               DATE_TRUNC('month', CAST(date AS DATE)) AS month,
               COUNT(*) AS total_tranche_rows,
               COUNT(DISTINCT (date, period)) AS active_period_cells,
               COUNT(*) * 1.0 / NULLIF(COUNT(DISTINCT (date, period)), 0)
                 AS tranches_per_period
        FROM joined
        WHERE firm_group != 'Fringe-other'
        GROUP BY firm_group, month
        ORDER BY firm_group, month
    """).df()
    df["month"] = pd.to_datetime(df["month"])
    df["regime"] = df["month"].apply(assign_regime)

    # Restrict to 2024+ for cleaner pre/post-MTU15-IDA comparison
    print()
    print("=" * 100)
    print("BID COMPLEXITY PANEL — tranches-per-period by firm group and month")
    print("=" * 100)

    # Per-firm regime means
    print()
    print("Mean tranches-per-period by firm group and regime:")
    print(f"{'firm':<13}  {'pre-IDA':>10}  {'3-sess':>10}  {'ISP15 win':>11}  {'DA60/ID15':>11}  {'DA15/ID15':>11}")
    for fg in ["GE", "IB", "GN", "HC", "Fringe-surv"]:
        sub = df[df["firm_group"] == fg]
        if len(sub) == 0:
            continue
        row = f"{fg:<13}  "
        for r in ["pre-IDA", "3-sess", "ISP15 win", "DA60/ID15", "DA15/ID15"]:
            cell = sub[sub["regime"] == r]
            if len(cell) == 0:
                row += f"{'—':>11}  "
            else:
                # Weighted mean (weight by active_period_cells)
                w = cell["active_period_cells"]
                wm = (cell["tranches_per_period"] * w).sum() / w.sum() if w.sum() > 0 else float("nan")
                row += f"{wm:>11.2f}  "
        print(row)

    # Headline: ratio post-MTU15-IDA / pre-MTU15-IDA
    print()
    print("Headline test: tranches-per-period ratio (post-MTU15-IDA / pre-MTU15-IDA)")
    print("  post = DA60/ID15 + DA15/ID15 pooled; pre = pre-IDA + 3-sess + ISP15 win pooled")
    print(f"{'firm':<13}  {'pre tpp':>9}  {'post tpp':>10}  {'ratio':>8}  {'verdict'}")
    for fg in ["GE", "IB", "GN", "HC", "Fringe-surv"]:
        sub = df[df["firm_group"] == fg].copy()
        if len(sub) == 0:
            continue
        sub["era"] = sub["month"].apply(
            lambda d: "post" if pd.Timestamp(d) >= pd.Timestamp("2025-03-19") else "pre"
        )
        rows = {}
        for era in ["pre", "post"]:
            cell = sub[sub["era"] == era]
            if len(cell) == 0:
                rows[era] = float("nan")
                continue
            w = cell["active_period_cells"]
            rows[era] = (cell["tranches_per_period"] * w).sum() / w.sum() if w.sum() > 0 else float("nan")
        ratio = rows["post"] / rows["pre"] if rows["pre"] > 0 else float("nan")
        if ratio < 0.7:
            verdict = "SIMPLIFIED"
        elif ratio < 0.95:
            verdict = "modest decline"
        elif ratio < 1.10:
            verdict = "stable"
        elif ratio < 1.50:
            verdict = "complexified"
        else:
            verdict = "STRONGLY COMPLEXIFIED"
        print(f"{fg:<13}  {rows['pre']:>9.2f}  {rows['post']:>10.2f}  {ratio:>8.3f}  {verdict}")

    # Timing: monthly trajectory around MTU15-IDA and MTU15-DA
    print()
    print("Monthly trajectory (Big-4 only, recent months):")
    print(f"{'month':<10}  " + "  ".join(f"{fg:>11}" for fg in ["GE","IB","GN","HC","Fringe-surv"]))
    recent = df[df["month"] >= pd.Timestamp("2024-09-01")].copy()
    months = sorted(recent["month"].unique())
    for m in months:
        cells = []
        for fg in ["GE","IB","GN","HC","Fringe-surv"]:
            x = recent[(recent["month"] == m) & (recent["firm_group"] == fg)]
            if len(x) == 0:
                cells.append(f"{'—':>11}")
            else:
                cells.append(f"{x['tranches_per_period'].iloc[0]:>11.2f}")
        print(f"{pd.Timestamp(m).strftime('%Y-%m'):<10}  " + "  ".join(cells))

    out = PROJECT / "data" / "derived" / "results" / "bid_complexity_monthly.csv"
    df.to_csv(out, index=False)
    print(f"\nwrote {out}")


if __name__ == "__main__":
    main()
