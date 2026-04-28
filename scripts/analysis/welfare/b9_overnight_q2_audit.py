# STATUS: ALIVE
# LAST-AUDIT: 2026-04-29
# FEEDS: B9 overnight verification — corrected q₂ formula at multiple granularities
# CLAIM: Under the IR-cleanest q₂ = SUM(PIBCIE × mtu/60), the B9 progressive
#        Big-4 collapse trajectory holds at (a) firm-day, (b) firm-hour,
#        (c) firm-ISP (post-MTU15-IDA), (d) same-calendar-month sub-sample
#        (Apr-Sep restriction across regimes), (e) per-IDA-session decomposition.
"""B9 overnight q₂ audit — memory-bounded summary suite.

Memory discipline (16GB system, 6GB DuckDB cap):
   - All aggregation done in DuckDB; only summary-sized tables hit pandas
   - One PIBCIE pass per granularity (no full-row pandas materialization)
   - Each step writes its CSV then frees its DuckDB table

Outputs:
    data/derived/results/b9_overnight_q2_audit/
        ├── 01_big4_firmday.csv            # Big-4 firm-day q₂ by regime
        ├── 02_big4_firmhour.csv           # Big-4 firm-hour q₂ by regime
        ├── 03_big4_firmISP_postMTU15.csv  # Big-4 firm-ISP, post-MTU15-IDA only
        ├── 04_perfirm_perregime.csv       # firm × regime matrix
        ├── 05_per_session.csv             # per-IDA-session × regime matrix
        ├── 05_per_session_perfirm.csv     # firm × regime × session
        ├── 06_samecalmonth_apr_sep.csv    # Apr-Sep sub-sample trajectory
        ├── 06_samecalmonth_perfirm.csv    # Apr-Sep per-firm trajectory
        ├── 06_full_vs_aprsep_compare.csv  # full vs Apr-Sep ratio
        ├── 07_signed_vs_abs.csv           # signed q₂ vs |q₂|
        ├── 08_big4_vs_fringe_means.csv    # Big-4 vs Fringe descriptive gap
        └── REPORT.txt                     # human-readable summary

Runtime expectation: ~30-90 minutes.  No regressions; means + medians + 95% CIs
via SEM only (cluster-robust regression deferred to a separate small script
once we confirm the descriptives lock down the trajectory).
"""
from __future__ import annotations
from pathlib import Path
import time
import duckdb
import pandas as pd
import numpy as np

PROJECT  = Path(__file__).resolve().parents[3]
PIBCIE   = PROJECT / "data" / "processed" / "omie" / "mercado_intradiario_subastas" / "programas" / "pibcie_all.parquet"
OUTDIR   = PROJECT / "data" / "derived" / "results" / "b9_overnight_q2_audit"
OUTDIR.mkdir(parents=True, exist_ok=True)
REPORT   = OUTDIR / "REPORT.txt"

REGIMES = ["pre-IDA", "3-sess", "ISP15-win", "DA60/ID15", "DA15/ID15"]
BIG4 = ["GE", "IB", "GN", "HC"]

REGIME_CASE = """
    CASE WHEN date < '2024-06-14' THEN 'pre-IDA'
         WHEN date < '2024-12-01' THEN '3-sess'
         WHEN date < '2025-03-19' THEN 'ISP15-win'
         WHEN date < '2025-10-01' THEN 'DA60/ID15'
         ELSE 'DA15/ID15' END
"""


report_lines: list[str] = []


def log(msg: str = "") -> None:
    line = f"[{time.strftime('%H:%M:%S')}] {msg}" if msg else ""
    print(line, flush=True)
    report_lines.append(line)


def section(title: str) -> None:
    bar = "=" * 90
    log(); log(bar); log(f"  {title}"); log(bar); log()


def write_report() -> None:
    REPORT.write_text("\n".join(report_lines))


def with_ci(df: pd.DataFrame, mean_col: str, std_col: str, n_col: str) -> pd.DataFrame:
    df = df.copy()
    df["sem"]   = df[std_col] / np.sqrt(df[n_col].clip(lower=1))
    df["ci_lo"] = df[mean_col] - 1.96 * df["sem"]
    df["ci_hi"] = df[mean_col] + 1.96 * df["sem"]
    return df


def main() -> None:
    t0 = time.time()
    log(f"Starting B9 overnight q₂ audit at {time.strftime('%Y-%m-%d %H:%M:%S')}")
    log(f"Output dir: {OUTDIR}")
    log(f"PIBCIE source: {PIBCIE}")
    log()

    con = duckdb.connect()
    con.execute("SET memory_limit='6GB'")
    con.execute("SET threads=4")
    con.execute("SET preserve_insertion_order=false")

    big4_sql = "(" + ",".join(f"'{f}'" for f in BIG4) + ")"

    # ============================================================
    # STEP 1: Big-4 firm-day q₂ trajectory
    # ============================================================
    section("STEP 1 — Big-4 firm-day q₂ trajectory")

    log("Aggregating firm-day q₂, Big-4 only…")
    con.execute(f"""
        CREATE OR REPLACE TABLE big4_fd AS
        SELECT date, grupo_empresarial AS firm,
               {REGIME_CASE} AS regime,
               SUM(assigned_power_mw * mtu_minutes / 60.0) AS q2_mwh
        FROM '{PIBCIE}'
        WHERE assigned_power_mw IS NOT NULL
          AND grupo_empresarial IN {big4_sql}
        GROUP BY 1, 2, 3
    """)
    n = con.execute("SELECT COUNT(*) FROM big4_fd").fetchone()[0]
    log(f"  Big-4 firm-day rows: {n:,}")

    s_fd = con.execute("""
        SELECT regime,
               AVG(q2_mwh) AS mean,
               MEDIAN(q2_mwh) AS median,
               STDDEV(q2_mwh) AS std,
               COUNT(*) AS count
        FROM big4_fd GROUP BY regime
    """).df()
    s_fd["regime"] = pd.Categorical(s_fd["regime"], categories=REGIMES, ordered=True)
    s_fd = s_fd.sort_values("regime").set_index("regime")
    s_fd = with_ci(s_fd, "mean", "std", "count")
    log("Big-4 firm-day q₂ (mean MWh per firm-day):")
    log(s_fd[["mean", "median", "ci_lo", "ci_hi", "count"]].round(1).to_string())
    s_fd.to_csv(OUTDIR / "01_big4_firmday.csv")
    log()

    # Per-firm × regime
    log("Per-firm × regime matrix…")
    pf = con.execute("""
        SELECT firm, regime, AVG(q2_mwh) AS mean
        FROM big4_fd GROUP BY 1, 2
    """).df()
    pf["regime"] = pd.Categorical(pf["regime"], categories=REGIMES, ordered=True)
    pm = pf.pivot(index="firm", columns="regime", values="mean").reindex(BIG4).reindex(REGIMES, axis=1)
    log(pm.round(0).to_string())
    pm.to_csv(OUTDIR / "04_perfirm_perregime.csv")
    log()

    # ============================================================
    # STEP 2: Apr-Sep (same-cal-month) restriction at firm-day
    # ============================================================
    section("STEP 2 — Same-calendar-month robustness (Apr-Sep)")

    log("Restricting Big-4 firm-day to Apr-Sep months across regimes…")
    s_ap = con.execute("""
        SELECT regime,
               AVG(q2_mwh) AS mean,
               MEDIAN(q2_mwh) AS median,
               STDDEV(q2_mwh) AS std,
               COUNT(*) AS count
        FROM big4_fd
        WHERE EXTRACT('month' FROM CAST(date AS DATE)) BETWEEN 4 AND 9
        GROUP BY regime
    """).df()
    s_ap["regime"] = pd.Categorical(s_ap["regime"], categories=REGIMES, ordered=True)
    s_ap = s_ap.sort_values("regime").set_index("regime")
    s_ap = with_ci(s_ap, "mean", "std", "count")
    log("Big-4 firm-day q₂, Apr-Sep ONLY:")
    log(s_ap[["mean", "median", "ci_lo", "ci_hi", "count"]].round(1).to_string())
    s_ap.to_csv(OUTDIR / "06_samecalmonth_apr_sep.csv")
    log()

    pf_ap = con.execute("""
        SELECT firm, regime, AVG(q2_mwh) AS mean
        FROM big4_fd
        WHERE EXTRACT('month' FROM CAST(date AS DATE)) BETWEEN 4 AND 9
        GROUP BY 1, 2
    """).df()
    pf_ap["regime"] = pd.Categorical(pf_ap["regime"], categories=REGIMES, ordered=True)
    pm_ap = pf_ap.pivot(index="firm", columns="regime", values="mean").reindex(BIG4).reindex(REGIMES, axis=1)
    log("Per-firm Apr-Sep trajectory:")
    log(pm_ap.round(0).to_string())
    pm_ap.to_csv(OUTDIR / "06_samecalmonth_perfirm.csv")
    log()

    cmp = pd.DataFrame({
        "full_sample": s_fd["mean"],
        "apr_sep_only": s_ap["mean"],
    })
    cmp["ratio_apr_sep_to_full"] = cmp["apr_sep_only"] / cmp["full_sample"]
    cmp["pct_change_vs_full"] = (cmp["apr_sep_only"] - cmp["full_sample"]) / cmp["full_sample"].abs() * 100
    log("Full-sample vs Apr-Sep comparison:")
    log(cmp.round(2).to_string())
    cmp.to_csv(OUTDIR / "06_full_vs_aprsep_compare.csv")
    log()

    # Free firm-day table — done with it
    con.execute("DROP TABLE big4_fd")

    # ============================================================
    # STEP 3: Big-4 firm-hour q₂ trajectory (collapse 15-min to hours)
    # ============================================================
    section("STEP 3 — Big-4 firm-hour q₂ trajectory")

    log("Aggregating firm-hour q₂ (Big-4 only)…")
    con.execute(f"""
        CREATE OR REPLACE TABLE big4_fh AS
        SELECT date,
               CASE WHEN mtu_minutes = 15 THEN CEIL(period / 4.0)::INT
                    ELSE period END AS hour,
               grupo_empresarial AS firm,
               {REGIME_CASE} AS regime,
               SUM(assigned_power_mw * mtu_minutes / 60.0) AS q2_mwh
        FROM '{PIBCIE}'
        WHERE assigned_power_mw IS NOT NULL
          AND grupo_empresarial IN {big4_sql}
        GROUP BY 1, 2, 3, 4
    """)
    n = con.execute("SELECT COUNT(*) FROM big4_fh").fetchone()[0]
    log(f"  Big-4 firm-hour rows: {n:,}")

    s_fh = con.execute("""
        SELECT regime,
               AVG(q2_mwh) AS mean,
               MEDIAN(q2_mwh) AS median,
               STDDEV(q2_mwh) AS std,
               COUNT(*) AS count,
               AVG(ABS(q2_mwh)) AS abs_mean
        FROM big4_fh GROUP BY regime
    """).df()
    s_fh["regime"] = pd.Categorical(s_fh["regime"], categories=REGIMES, ordered=True)
    s_fh = s_fh.sort_values("regime").set_index("regime")
    s_fh = with_ci(s_fh, "mean", "std", "count")
    s_fh["asymmetry_ratio"] = s_fh["mean"] / s_fh["abs_mean"]
    log("Big-4 firm-hour q₂ (mean MWh per firm-hour):")
    log("(asymmetry_ratio = signed_mean / |mean|; close to 1 = mostly one-direction)")
    log(s_fh[["mean", "abs_mean", "asymmetry_ratio", "ci_lo", "ci_hi", "count"]].round(2).to_string())
    s_fh.to_csv(OUTDIR / "02_big4_firmhour.csv")
    s_fh[["mean", "abs_mean", "asymmetry_ratio"]].to_csv(OUTDIR / "07_signed_vs_abs.csv")
    log()

    con.execute("DROP TABLE big4_fh")

    # ============================================================
    # STEP 4: Big-4 firm-ISP q₂ (post-MTU15-IDA only, native 15-min)
    # ============================================================
    section("STEP 4 — Big-4 firm-ISP q₂ (post-MTU15-IDA only)")

    log("Aggregating firm-ISP q₂, mtu_minutes=15, date >= 2025-03-19…")
    con.execute(f"""
        CREATE OR REPLACE TABLE big4_fi AS
        SELECT date, period,
               grupo_empresarial AS firm,
               {REGIME_CASE} AS regime,
               SUM(assigned_power_mw) * 0.25 AS q2_mwh
        FROM '{PIBCIE}'
        WHERE assigned_power_mw IS NOT NULL
          AND mtu_minutes = 15
          AND CAST(date AS DATE) >= DATE '2025-03-19'
          AND grupo_empresarial IN {big4_sql}
        GROUP BY 1, 2, 3, 4
    """)
    n = con.execute("SELECT COUNT(*) FROM big4_fi").fetchone()[0]
    log(f"  Big-4 firm-ISP rows: {n:,}")

    s_fi = con.execute("""
        SELECT regime,
               AVG(q2_mwh) AS mean,
               MEDIAN(q2_mwh) AS median,
               STDDEV(q2_mwh) AS std,
               COUNT(*) AS count
        FROM big4_fi GROUP BY regime
    """).df()
    s_fi["regime"] = pd.Categorical(s_fi["regime"], categories=REGIMES, ordered=True)
    s_fi = s_fi.sort_values("regime").set_index("regime")
    s_fi = with_ci(s_fi, "mean", "std", "count")
    log("Big-4 firm-ISP q₂ (post-MTU15-IDA only):")
    log(s_fi[["mean", "median", "ci_lo", "ci_hi", "count"]].round(2).to_string())
    s_fi.to_csv(OUTDIR / "03_big4_firmISP_postMTU15.csv")
    log()

    con.execute("DROP TABLE big4_fi")

    # ============================================================
    # STEP 5: Per-IDA-session decomposition
    # ============================================================
    section("STEP 5 — Per-IDA-session q₂ decomposition")

    log("Aggregating firm-day-session q₂, Big-4 only…")
    con.execute(f"""
        CREATE OR REPLACE TABLE big4_sess AS
        SELECT date, grupo_empresarial AS firm,
               session_number,
               {REGIME_CASE} AS regime,
               SUM(assigned_power_mw * mtu_minutes / 60.0) AS q2_mwh
        FROM '{PIBCIE}'
        WHERE assigned_power_mw IS NOT NULL
          AND grupo_empresarial IN {big4_sql}
        GROUP BY 1, 2, 3, 4
    """)
    n = con.execute("SELECT COUNT(*) FROM big4_sess").fetchone()[0]
    log(f"  Big-4 firm-day-session rows: {n:,}")

    pv = con.execute("""
        SELECT regime, session_number, AVG(q2_mwh) AS mean
        FROM big4_sess GROUP BY 1, 2
    """).df()
    pv["regime"] = pd.Categorical(pv["regime"], categories=REGIMES, ordered=True)
    pv_t = pv.pivot(index="regime", columns="session_number", values="mean").reindex(REGIMES)
    log("Big-4 q₂ by regime × IDA session (mean MWh per firm-day-session):")
    log(pv_t.round(1).to_string())
    pv_t.to_csv(OUTDIR / "05_per_session.csv")
    log()

    pf_sess = con.execute("""
        SELECT firm, regime, session_number, AVG(q2_mwh) AS mean
        FROM big4_sess GROUP BY 1, 2, 3
    """).df()
    pf_sess.to_csv(OUTDIR / "05_per_session_perfirm.csv", index=False)
    log("Per-firm × regime × session matrix saved.")
    log()

    con.execute("DROP TABLE big4_sess")

    # ============================================================
    # STEP 6: Big-4 vs Fringe descriptive gap (no regression — pure means)
    # ============================================================
    section("STEP 6 — Big-4 vs Fringe descriptive gap (firm-hour means)")

    log("Aggregating firm-hour q₂ for Big-4 + Fringe (regression deferred)…")
    con.execute(f"""
        CREATE OR REPLACE TABLE all_fh AS
        SELECT date,
               CASE WHEN mtu_minutes = 15 THEN CEIL(period / 4.0)::INT
                    ELSE period END AS hour,
               COALESCE(grupo_empresarial, 'NA') AS firm,
               {REGIME_CASE} AS regime,
               SUM(assigned_power_mw * mtu_minutes / 60.0) AS q2_mwh
        FROM '{PIBCIE}'
        WHERE assigned_power_mw IS NOT NULL
        GROUP BY 1, 2, 3, 4
    """)
    n = con.execute("SELECT COUNT(*) FROM all_fh").fetchone()[0]
    log(f"  All firm-hour rows: {n:,}")

    big4_fringe = con.execute(f"""
        SELECT regime,
               CASE WHEN firm IN {big4_sql} THEN 'Big-4' ELSE 'Fringe' END AS group_,
               AVG(q2_mwh) AS mean,
               STDDEV(q2_mwh) AS std,
               COUNT(*) AS count
        FROM all_fh GROUP BY 1, 2
    """).df()
    big4_fringe["regime"] = pd.Categorical(big4_fringe["regime"], categories=REGIMES, ordered=True)
    big4_fringe = big4_fringe.sort_values(["regime", "group_"])
    pv_bf = big4_fringe.pivot(index="regime", columns="group_", values="mean").reindex(REGIMES)
    pv_bf["gap_b4_minus_fringe"] = pv_bf.get("Big-4", 0) - pv_bf.get("Fringe", 0)
    log("Big-4 vs Fringe firm-hour q₂ means by regime (no regression):")
    log(pv_bf.round(2).to_string())
    pv_bf.to_csv(OUTDIR / "08_big4_vs_fringe_means.csv")
    log()

    con.execute("DROP TABLE all_fh")

    # ============================================================
    # FINAL: write report and exit
    # ============================================================
    section("FINAL")
    elapsed = time.time() - t0
    log(f"Total runtime: {elapsed/60:.1f} minutes")
    log(f"All CSVs in: {OUTDIR}")
    log()

    log("KEY RESULTS SUMMARY:")
    log("  See {OUTDIR}/01_big4_firmday.csv for Big-4 firm-day q₂ trajectory")
    log("  See {OUTDIR}/06_samecalmonth_apr_sep.csv for Apr-Sep robustness")
    log("  See {OUTDIR}/06_full_vs_aprsep_compare.csv for the seasonal-discount ratio")
    log("  See {OUTDIR}/04_perfirm_perregime.csv for per-firm trajectory")
    log("  See {OUTDIR}/05_per_session.csv for IDA-session decomposition")
    log("  See {OUTDIR}/08_big4_vs_fringe_means.csv for Big-4 vs Fringe gap")
    log()
    log("Next steps to run separately if descriptives confirm trajectory:")
    log("  1. Cluster-robust regression at firm-hour (Stata reghdfe or fixest)")
    log("  2. Same-cal-month regression with VRE control")
    log("  3. Per-firm session-level econometric test")

    write_report()
    print(f"\nREPORT written to {REPORT}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log(f"FATAL ERROR: {type(e).__name__}: {e}")
        import traceback
        log(traceback.format_exc())
        write_report()
        raise
