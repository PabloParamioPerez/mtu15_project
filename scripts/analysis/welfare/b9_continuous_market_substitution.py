# STATUS: ALIVE
# LAST-AUDIT: 2026-04-29
# FEEDS: B9 continuous-market substitution test — model limitation check
# CLAIM: The structural model (model_v2.tex) treats IDA as the only voluntary
#        post-DA repositioning channel. If asymmetric clocks push some
#        strategic conduct from IDA auctions to the continuous market (XBID),
#        the B9 measure understates strategic compression. This script tests
#        whether Big-4 continuous-market voluntary repositioning q^CI moves
#        in the OPPOSITE direction to q₂_IDA across regimes.
"""B9 continuous-market substitution check.

Compute q^CI = SUM(PIBCICE.assigned_power_mw × mtu/60) per firm-period at
native granularity post-MTU15-IDA, replicated to MTU15 grain otherwise.

Compare to q₂_IDA trajectory:
  - If q^CI rises in asymmetric regimes while q₂_IDA falls → substitution
  - If q^CI moves in the SAME direction as q₂_IDA → no substitution; the
    asymmetric clock genuinely compresses strategic conduct, not just
    relocates it
  - The model's parsimony assumption (no CI substitution) is supported by
    the second pattern.
"""
from __future__ import annotations
from pathlib import Path
import time
import duckdb
import numpy as np
import pandas as pd

PROJECT  = Path(__file__).resolve().parents[3]
PIBCICE  = PROJECT / "data" / "processed" / "omie" / "mercado_intradiario_continuo" / "programas" / "pibcice_all.parquet"
PDBCE    = PROJECT / "data" / "processed" / "omie" / "mercado_diario" / "programas" / "pdbce_all.parquet"
PIBCIE   = PROJECT / "data" / "processed" / "omie" / "mercado_intradiario_subastas" / "programas" / "pibcie_all.parquet"
OUTDIR   = PROJECT / "data" / "derived" / "results" / "b9_continuous_market"
OUTDIR.mkdir(parents=True, exist_ok=True)

REGIMES = ["pre-IDA", "3-sess", "ISP15-win", "DA60/ID15", "DA15/ID15"]
BIG4 = ["GE", "IB", "GN", "HC"]


def assign_regime(d) -> str:
    d = pd.Timestamp(d)
    if d < pd.Timestamp("2024-06-14"): return "pre-IDA"
    if d < pd.Timestamp("2024-12-01"): return "3-sess"
    if d < pd.Timestamp("2025-03-19"): return "ISP15-win"
    if d < pd.Timestamp("2025-10-01"): return "DA60/ID15"
    return "DA15/ID15"


def main() -> None:
    t0 = time.time()
    print(f"[{time.strftime('%H:%M:%S')}] Starting continuous-market substitution test…", flush=True)

    con = duckdb.connect()
    con.execute("SET memory_limit='6GB'")
    con.execute("SET threads=4")
    con.execute("SET preserve_insertion_order=false")

    big4_sql = "(" + ",".join(f"'{f}'" for f in BIG4) + ")"

    # Inspect schema first — does pibcice have grupo_empresarial?
    cols = con.execute(f"DESCRIBE SELECT * FROM '{PIBCICE}' LIMIT 0").df()["column_name"].tolist()
    has_firm = "grupo_empresarial" in cols
    print(f"PIBCICE columns: {cols}", flush=True)
    print(f"Has firm column: {has_firm}", flush=True)

    if has_firm:
        # PIBCICE uses grupo_short for the short codes (GE/IB/GN/HC) — PIBCIE uses grupo_empresarial.
        # Switch to grupo_short for Big-4 filter in continuous-market data.
        ci = con.execute(f"""
            SELECT date, round_number, period, mtu_minutes,
                   COALESCE(grupo_short, 'NA') AS firm,
                   SUM(assigned_power_mw * mtu_minutes / 60.0) AS qci_mwh
            FROM '{PIBCICE}'
            WHERE assigned_power_mw IS NOT NULL
              AND grupo_short IN {big4_sql}
            GROUP BY 1, 2, 3, 4, 5
        """).df()
    else:
        # Need unit→firm map
        print("PIBCICE doesn't carry firm directly — using unit→firm map from PDBCE…", flush=True)
        con.execute(f"""
            CREATE TABLE unit_firm AS
            WITH counts AS (
                SELECT unit_code, COALESCE(grupo_empresarial,'NA') AS firm, COUNT(*) AS n
                FROM '{PDBCE}' WHERE unit_code IS NOT NULL GROUP BY 1, 2
            ),
            ranked AS (
                SELECT unit_code, firm,
                       ROW_NUMBER() OVER (PARTITION BY unit_code ORDER BY n DESC) rk
                FROM counts
            )
            SELECT unit_code, firm FROM ranked WHERE rk = 1 AND firm IN {big4_sql}
        """)
        # PIBCIC is the per-unit version; PIBCICE may exist or not
        PIBCIC = PROJECT / "data" / "processed" / "omie" / "mercado_intradiario_continuo" / "programas" / "pibcic_all.parquet"
        ci = con.execute(f"""
            SELECT a.date, a.round_number, a.period, a.mtu_minutes,
                   uf.firm,
                   SUM(a.assigned_power_mw * a.mtu_minutes / 60.0) AS qci_mwh
            FROM '{PIBCIC}' AS a
            JOIN unit_firm uf ON a.unit_code = uf.unit_code
            WHERE a.assigned_power_mw IS NOT NULL
            GROUP BY 1, 2, 3, 4, 5
        """).df()

    ci["date"] = pd.to_datetime(ci["date"])
    print(f"   continuous-market firm-period rows: {len(ci):,}", flush=True)
    print(f"   date range: {ci['date'].min().date()} → {ci['date'].max().date()}", flush=True)
    print(f"   MTU dist: {dict(ci['mtu_minutes'].value_counts())}", flush=True)

    # Aggregate to firm-day for trajectory comparison (no replication needed at firm-day)
    ci["regime"] = ci["date"].apply(assign_regime)
    fd_ci = (ci.groupby(["firm", "date", "regime"], observed=True)["qci_mwh"]
                .sum().reset_index())
    big4_traj = (fd_ci.groupby(["firm", "regime"], observed=True)["qci_mwh"]
                       .agg(["mean", "median", "count"])
                       .reset_index())
    big4_traj["regime"] = pd.Categorical(big4_traj["regime"], categories=REGIMES, ordered=True)
    pv_ci = (big4_traj.pivot(index="firm", columns="regime", values="mean")
                       .reindex(BIG4).reindex(REGIMES, axis=1))
    print()
    print("Big-4 q^CI (continuous-market) firm-day MEAN trajectory (MWh per firm-day):", flush=True)
    print(pv_ci.round(0).to_string(), flush=True)
    print()

    pv_ci.to_csv(OUTDIR / "big4_qci_perfirm_perregime.csv")
    big4_traj.to_csv(OUTDIR / "big4_qci_summary.csv", index=False)

    # Compare to IDA q₂ trajectory (from previous result)
    Q2_PERFIRM = PROJECT / "data" / "derived" / "results" / "b9_replicated_isp_grain_perfirm.csv"
    if Q2_PERFIRM.exists():
        # Note: that file is in MWh per firm-ISP at ISP grain.
        # Convert to MWh per firm-day for comparable trajectory test:
        # firm-ISP × 96 ISPs/day = firm-day MWh
        q2 = pd.read_csv(Q2_PERFIRM, index_col=0).reindex(BIG4)
        q2_per_day = q2 * 96  # rough conversion
        print("Big-4 q₂_IDA (firm-ISP × 96 ≈ firm-day) MWh:", flush=True)
        print(q2_per_day.round(0).to_string(), flush=True)
        print()
        # Compute % change pre-IDA → ISP15-win for both
        change_ci = (pv_ci["ISP15-win"] - pv_ci["pre-IDA"])
        change_q2 = (q2_per_day["ISP15-win"] - q2_per_day["pre-IDA"])
        print("Change pre-IDA → ISP15-win (MWh per firm-day):", flush=True)
        print(pd.DataFrame({"q^CI change": change_ci, "q₂_IDA change": change_q2}).round(0).to_string(), flush=True)
        print()
        print("Substitution test:", flush=True)
        print("  - If q^CI rises (positive change) while q₂_IDA falls (negative), CI substitutes for IDA.", flush=True)
        print("  - If both fall, the asymmetric clock genuinely compresses both channels.", flush=True)

    # Aggregate to system level: total Big-4 q^CI per regime per month
    fd_ci["month"] = fd_ci["date"].dt.to_period("M").dt.to_timestamp()
    monthly = (fd_ci.groupby(["month", "regime"], observed=True)["qci_mwh"]
                     .sum().reset_index())
    monthly["regime"] = pd.Categorical(monthly["regime"], categories=REGIMES, ordered=True)
    monthly.to_csv(OUTDIR / "big4_qci_monthly.csv", index=False)
    sysm = monthly.groupby("regime", observed=True)["qci_mwh"].agg(["mean", "median", "count"]).reindex(REGIMES)
    print("System-level Big-4 q^CI by regime (sum over Big-4 firms, MWh per month):", flush=True)
    print(sysm.round(0).to_string(), flush=True)
    print()

    print(f"Total runtime: {(time.time() - t0)/60:.1f} min", flush=True)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        import traceback
        print(f"ERROR: {type(e).__name__}: {e}", flush=True)
        print(traceback.format_exc(), flush=True)
        raise
