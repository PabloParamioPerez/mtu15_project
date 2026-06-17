# STATUS: ALIVE
# LAST-AUDIT: 2026-04-29
# FEEDS: q₂ correctness verification — alternative spot-quantity definitions
# CLAIM: For Big-4 firms × regime, multiple q₂ definitions are computed and
#        compared. Per OMIE spec v1.37 §5.2.2.x:
#          - PIBCIE = INCREMENTAL IDA cleared per firm (signed, MW)
#          - PIBCA  = ACCUMULATED IDA cleared per unit (level, signed, MW;
#                    flag_redespacho always 0 → no technical restrictions)
#          - PHF    = FINAL post-IDA program per unit (level, signed, MW;
#                    INCLUDES technical restrictions RT2 + rebalance)
#        Therefore q₂_IDA_voluntary = SUM(PIBCIE × mtu/60) is the cleanest
#        Ito-Reguant-style "strategic spot quantity" — pure firm voluntary
#        IDA repositioning, EXCLUDES bilaterals and ALL REE restrictions.
#        PHF − PIBCA isolates the RT2 effect.
"""q₂ DEFINITION COMPARISON — most-important verification for IR test.

Five definitions per firm-day, ordered cleanest-IR → most-inclusive:

  (1) q₂_IDA_volunt = SUM(PIBCIE.assigned × mtu/60)
      Pure voluntary IDA repositioning. Per OMIE spec, PIBCIE is the
      INCREMENTAL clearing per IDA session, signed natively.
      = Firm's strategic IDA bidding outcome. Closest to IR's q₂.

  (2) q₂_PIBCA_level = SUM(PIBCA_lastsession.assigned × mtu/60) − SUM(PDBC × mtu/60)
      Post-IDA accumulated level minus DA cleared, aggregated to firm-day
      via unit→firm map. Should ≈ (1) + (PDBF−PDBC) = (1) + bilaterals.
      No RT (PIBCA flag_redespacho is always 0).

  (3) q₂_PHF_level = SUM(PHF_lastsession × mtu/60) − SUM(PDBC × mtu/60)
      Final scheduled level after IDA + RT2 + rebalance, minus DA cleared.
      = (2) + RT2.

  (4) q₂_RT2_only = SUM(PHF_lastsession × mtu/60) − SUM(PIBCA_lastsession × mtu/60)
      Pure RT2 effect, isolated.

  (5) q₂_IDA_volunt_session_split = SUM(PIBCIE × mtu/60), broken by IDA session number
      Tracks per-session strategic conduct. Useful for the 6→3 transition.

Output:
  - Console: Big-4 firm × regime × definition matrix
  - results/regressions/q2_definitions_compare.csv
"""
from __future__ import annotations
from pathlib import Path
import duckdb
import pandas as pd
import numpy as np

PROJECT  = Path(__file__).resolve().parents[3]
PDBC     = PROJECT / "data" / "processed" / "omie" / "mercado_diario" / "programas" / "pdbc_all.parquet"
PDBCE    = PROJECT / "data" / "processed" / "omie" / "mercado_diario" / "programas" / "pdbce_all.parquet"
PIBCIE   = PROJECT / "data" / "processed" / "omie" / "mercado_intradiario_subastas" / "programas" / "pibcie_all.parquet"
PIBCA    = PROJECT / "data" / "processed" / "omie" / "mercado_intradiario_subastas" / "programas" / "pibca_all.parquet"
PHF      = PROJECT / "data" / "processed" / "omie" / "mercado_intradiario_subastas" / "programas" / "phf_all.parquet"

OUT      = PROJECT / "results" / "regressions" / "q2_definitions_compare.csv"

REGIMES = ["pre-IDA", "3-sess", "ISP15-win", "DA60/ID15", "DA15/ID15"]
BIG4 = ["GE", "IB", "GN", "HC"]


REGIME_CASE = """
    CASE WHEN date < '2024-06-14' THEN 'pre-IDA'
         WHEN date < '2024-12-01' THEN '3-sess'
         WHEN date < '2025-03-19' THEN 'ISP15-win'
         WHEN date < '2025-10-01' THEN 'DA60/ID15'
         ELSE 'DA15/ID15' END
"""


def main() -> None:
    con = duckdb.connect()
    con.execute("SET memory_limit='10GB'")
    con.execute("SET threads=4")
    con.execute("SET preserve_insertion_order=false")

    # ============================================================
    # Build unit → firm map from PDBCE (mode firm per unit)
    # ============================================================
    print("→ Building unit→firm map from PDBCE…")
    con.execute(f"""
        CREATE TABLE unit_firm AS
        WITH counts AS (
            SELECT unit_code,
                   COALESCE(grupo_empresarial, 'NA') AS firm,
                   COUNT(*) AS n
            FROM '{PDBCE}'
            WHERE unit_code IS NOT NULL
            GROUP BY 1, 2
        ),
        ranked AS (
            SELECT unit_code, firm,
                   ROW_NUMBER() OVER (PARTITION BY unit_code ORDER BY n DESC) AS rk
            FROM counts
        )
        SELECT unit_code, firm FROM ranked WHERE rk = 1
    """)
    print(f"  {con.execute('SELECT COUNT(*) FROM unit_firm').fetchone()[0]:,} units mapped")

    # Big-4-only unit list — used to slim PIBCA/PHF window operations
    con.execute(f"""
        CREATE TABLE big4_units AS
        SELECT unit_code FROM unit_firm
        WHERE firm IN ({', '.join("'" + f + "'" for f in BIG4)})
    """)
    n_big4_units = con.execute("SELECT COUNT(*) FROM big4_units").fetchone()[0]
    print(f"  {n_big4_units:,} Big-4 units (subset for PIBCA/PHF aggregation)\n")

    # ============================================================
    # DEFINITION 1: q₂_IDA_volunt = SUM PIBCIE × mtu/60 per firm-day
    # ============================================================
    print("→ (1) q₂_IDA_volunt = SUM(PIBCIE × mtu/60) per firm-day")
    con.execute(f"""
        CREATE TABLE q2_d1 AS
        SELECT date,
               COALESCE(grupo_empresarial, 'NA') AS firm,
               {REGIME_CASE} AS regime,
               SUM(assigned_power_mw * mtu_minutes / 60.0) AS q2_ida_volunt_mwh
        FROM '{PIBCIE}'
        WHERE assigned_power_mw IS NOT NULL
        GROUP BY 1, 2, 3
    """)
    n1 = con.execute("SELECT COUNT(*) FROM q2_d1").fetchone()[0]
    print(f"  rows: {n1:,}\n")

    # ============================================================
    # DEFINITION 5: q₂_IDA_volunt by session (firm-day-session)
    # ============================================================
    print("→ (5) q₂ split by IDA session_number, firm-day")
    con.execute(f"""
        CREATE TABLE q2_d5 AS
        SELECT date,
               COALESCE(grupo_empresarial, 'NA') AS firm,
               session_number,
               SUM(assigned_power_mw * mtu_minutes / 60.0) AS q2_session_mwh
        FROM '{PIBCIE}'
        WHERE assigned_power_mw IS NOT NULL
        GROUP BY 1, 2, 3
    """)

    # ============================================================
    # PDBC at firm-day via unit→firm map (q_DA in firm-day MWh)
    # ============================================================
    # NOTE: All level→firm-day aggregations restrict to Big-4 units to keep memory
    #       footprint manageable. The output table will only have Big-4 firm-day rows
    #       on definitions (2)/(3)/(4); definition (1) covers ALL firms.
    print("→ Computing q_DA (PDBC) per firm-day via unit→firm map (Big-4 units)…")
    con.execute(f"""
        CREATE TABLE pdbc_firm AS
        SELECT a.date,
               uf.firm,
               SUM(a.assigned_power_mw * a.mtu_minutes / 60.0) AS q_da_mwh
        FROM '{PDBC}' AS a
        JOIN unit_firm AS uf ON a.unit_code = uf.unit_code
        WHERE a.assigned_power_mw IS NOT NULL
          AND a.unit_code IN (SELECT unit_code FROM big4_units)
        GROUP BY 1, 2
    """)

    # ============================================================
    # DEFINITION 2: PIBCA last-session at firm-day, then minus PDBC
    # Window function pre-filtered to Big-4 units to control memory.
    # ============================================================
    print("→ (2) PIBCA last-session level → firm-day (Big-4 units only)")
    con.execute(f"""
        CREATE TABLE pibca_firm AS
        WITH big4 AS (
            SELECT date, unit_code, period, mtu_minutes, assigned_power_mw, session_number
            FROM '{PIBCA}'
            WHERE assigned_power_mw IS NOT NULL
              AND unit_code IN (SELECT unit_code FROM big4_units)
        ),
        last_per_period AS (
            SELECT date, unit_code, period, mtu_minutes, assigned_power_mw,
                   ROW_NUMBER() OVER (PARTITION BY date, unit_code, period
                                      ORDER BY session_number DESC) AS rk
            FROM big4
        )
        SELECT a.date,
               uf.firm,
               SUM(a.assigned_power_mw * a.mtu_minutes / 60.0) AS q_pibca_mwh
        FROM last_per_period AS a
        JOIN unit_firm AS uf ON a.unit_code = uf.unit_code
        WHERE a.rk = 1
        GROUP BY 1, 2
    """)

    # ============================================================
    # DEFINITION 3: PHF last-session at firm-day, then minus PDBC
    # ============================================================
    print("→ (3) PHF last-session level → firm-day (Big-4 units only)")
    con.execute(f"""
        CREATE TABLE phf_firm AS
        WITH big4 AS (
            SELECT date, unit_code, period, mtu_minutes, assigned_power_mw, session_number
            FROM '{PHF}'
            WHERE assigned_power_mw IS NOT NULL
              AND unit_code IN (SELECT unit_code FROM big4_units)
        ),
        last_per_period AS (
            SELECT date, unit_code, period, mtu_minutes, assigned_power_mw,
                   ROW_NUMBER() OVER (PARTITION BY date, unit_code, period
                                      ORDER BY session_number DESC) AS rk
            FROM big4
        )
        SELECT a.date,
               uf.firm,
               SUM(a.assigned_power_mw * a.mtu_minutes / 60.0) AS q_phf_mwh
        FROM last_per_period AS a
        JOIN unit_firm AS uf ON a.unit_code = uf.unit_code
        WHERE a.rk = 1
        GROUP BY 1, 2
    """)

    # ============================================================
    # MERGE all
    # ============================================================
    print("→ Merging…")
    df = con.execute("""
        SELECT d1.date, d1.firm, d1.regime, d1.q2_ida_volunt_mwh,
               pdbc.q_da_mwh,
               pibca.q_pibca_mwh,
               phf.q_phf_mwh,
               (COALESCE(pibca.q_pibca_mwh, 0) - COALESCE(pdbc.q_da_mwh, 0)) AS q2_pibca_minus_pdbc_mwh,
               (COALESCE(phf.q_phf_mwh, 0)   - COALESCE(pdbc.q_da_mwh, 0))   AS q2_phf_minus_pdbc_mwh,
               (COALESCE(phf.q_phf_mwh, 0)   - COALESCE(pibca.q_pibca_mwh, 0)) AS q2_rt2_only_mwh
        FROM q2_d1 AS d1
        LEFT JOIN pdbc_firm  AS pdbc  ON d1.date = pdbc.date  AND d1.firm = pdbc.firm
        LEFT JOIN pibca_firm AS pibca ON d1.date = pibca.date AND d1.firm = pibca.firm
        LEFT JOIN phf_firm   AS phf   ON d1.date = phf.date   AND d1.firm = phf.firm
    """).df()
    df["date"] = pd.to_datetime(df["date"])
    print(f"  Final merged: {len(df):,} firm-day rows")
    print(f"  date range:   {df['date'].min().date()} → {df['date'].max().date()}\n")

    # ============================================================
    # BIG-4 × REGIME aggregation
    # ============================================================
    big4 = df[df["firm"].isin(BIG4)].copy()
    big4["regime"] = pd.Categorical(big4["regime"], categories=REGIMES, ordered=True)

    print("=" * 90)
    print("BIG-4 q₂ DEFINITIONS — mean MWh per firm-day, by regime")
    print("=" * 90)
    cols = ["q2_ida_volunt_mwh", "q2_pibca_minus_pdbc_mwh",
            "q2_phf_minus_pdbc_mwh", "q2_rt2_only_mwh"]
    summary = (big4.groupby("regime", observed=True)[cols]
                  .mean()
                  .reindex(REGIMES))
    summary.columns = ["q₂_IDA_voluntary", "q₂_PIBCA−PDBC", "q₂_PHF−PDBC", "q₂_RT2_only"]
    print(summary.round(0).to_string())
    print()
    print("  Reading: q₂_IDA_voluntary is the IR-cleanest (pure firm voluntary IDA).")
    print("           q₂_PIBCA−PDBC = q₂_IDA_voluntary + bilaterals (PDBF − PDBC).")
    print("           q₂_PHF−PDBC   = q₂_PIBCA−PDBC + RT2 effect.")
    print("           q₂_RT2_only   = system-operator reschedule (PHF − PIBCA), excludes")
    print("                           voluntary IDA conduct entirely.")
    print()
    print("  IR strategic prediction: dominant firms net-SELL in the spot market post-DA")
    print("                           (q₂ > 0 = consistent with under-commitment in DA).")
    print()

    # ============================================================
    # PER-FIRM × REGIME breakdown — q₂_IDA_volunt (the cleanest)
    # ============================================================
    print("=" * 90)
    print("PER-FIRM q₂_IDA_voluntary by regime (mean MWh per firm-day)")
    print("       (the IR-cleanest definition; sign and trajectory are the load-bearing claim)")
    print("=" * 90)
    pv = (big4.groupby(["firm", "regime"], observed=True)["q2_ida_volunt_mwh"]
              .mean().unstack("regime").reindex(BIG4)[REGIMES])
    print(pv.round(0).to_string())
    print()

    # ============================================================
    # PER-FIRM × REGIME breakdown — q₂_PHF_minus_PDBC (most inclusive)
    # ============================================================
    print("=" * 90)
    print("PER-FIRM q₂_PHF−PDBC by regime (mean MWh per firm-day)")
    print("       (includes bilaterals + RT2; mostly to gauge how much RT distorts)")
    print("=" * 90)
    pv2 = (big4.groupby(["firm", "regime"], observed=True)["q2_phf_minus_pdbc_mwh"]
                .mean().unstack("regime").reindex(BIG4)[REGIMES])
    print(pv2.round(0).to_string())
    print()

    # ============================================================
    # PER-SESSION breakdown of q₂_IDA_voluntary for Big-4 (definition 5)
    # ============================================================
    print("=" * 90)
    print("PER-IDA-SESSION q₂_IDA_voluntary, Big-4 only, MWh per firm-day")
    print("       Tracks 6→3 sessions transition (2024-06-14): how does each session shift?")
    print("=" * 90)
    d5 = con.execute(f"""
        SELECT date, firm, session_number,
               q2_session_mwh,
               {REGIME_CASE.replace('regime', 'regime').strip()} AS regime
        FROM q2_d5
    """).df()
    d5["date"] = pd.to_datetime(d5["date"])
    d5_big4 = d5[d5["firm"].isin(BIG4)].copy()
    d5_big4["regime"] = pd.Categorical(d5_big4["regime"], categories=REGIMES, ordered=True)
    pv3 = (d5_big4.groupby(["regime", "session_number"], observed=True)
                 ["q2_session_mwh"].mean().unstack("session_number").reindex(REGIMES))
    print(pv3.round(0).to_string())
    print()

    # ============================================================
    # SAVE
    # ============================================================
    OUT.parent.mkdir(parents=True, exist_ok=True)
    big4.to_csv(OUT, index=False)
    print(f"wrote {OUT}")
    print()
    print("=" * 90)
    print("CONCLUSION: q₂_IDA_voluntary = SUM(PIBCIE × mtu/60) is the right q₂ for IR.")
    print("=" * 90)


if __name__ == "__main__":
    main()
