# STATUS: ALIVE
# LAST-AUDIT: 2026-04-29
# FEEDS: RT2 post-blackout regulatory cascade quantification (Part IV anchor)
# CLAIM: After the April 28 2025 Iberian blackout, REE's "operación reforzada"
#        forces large mandatory dispatch increments via Phase-2 technical
#        restrictions (RT2). This is regulatory, not strategic, but it
#        dominates the Big-4 final-scheduled-program movement in DA15/ID15
#        (Oct 2025 onward) by ~4× the voluntary q₂. Decomposed by firm,
#        technology, hour-of-day, and per-unit ranking.
"""RT2 post-blackout regulatory cascade — quantification.

RT2 = PHF − PIBCA per (date, period, unit), where:
   PHF   = final scheduled program after IDA + RT2 + rebalance (per OMIE §5.2.2.4)
   PIBCA = post-IDA accumulated program, RT-free (flag_redespacho=0 always)

So RT2 cleanly isolates the System Operator (REE) Phase-2 technical
restriction increment per unit-period.  This script:

   1. Builds per-unit per-period RT2 (last session) at MTU15 grain post-MTU15-IDA
   2. Aggregates per-firm × regime
   3. Splits by technology (CCGT / hydro / nuclear / thermal-other / RE)
   4. Hour-of-day profile in DA15/ID15
   5. Top-20 units ranked by RT2-up activation MWh

Outputs:
   data/derived/results/rt2_post_blackout/
       ├── 01_perfirm_perregime.csv
       ├── 02_pertechnology.csv
       ├── 03_hourofday_DA15ID15.csv
       └── 04_top20_units_DA15ID15.csv
"""
from __future__ import annotations
from pathlib import Path
import time
import duckdb
import pandas as pd

PROJECT = Path(__file__).resolve().parents[3]
PIBCA   = PROJECT / "data" / "processed" / "omie" / "mercado_intradiario_subastas" / "programas" / "pibca_all.parquet"
PHF     = PROJECT / "data" / "processed" / "omie" / "mercado_intradiario_subastas" / "programas" / "phf_all.parquet"
PDBCE   = PROJECT / "data" / "processed" / "omie" / "mercado_diario" / "programas" / "pdbce_all.parquet"
LISTA   = PROJECT / "data" / "external" / "omie_reference" / "lista_unidades.csv"

OUTDIR  = PROJECT / "data" / "derived" / "results" / "rt2_post_blackout"
OUTDIR.mkdir(parents=True, exist_ok=True)

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
    t0 = time.time()
    print(f"[{time.strftime('%H:%M:%S')}] Starting RT2 post-blackout deep-dive…", flush=True)

    con = duckdb.connect()
    con.execute("SET memory_limit='6GB'")
    con.execute("SET threads=4")
    con.execute("SET preserve_insertion_order=false")

    big4_sql = "(" + ",".join(f"'{f}'" for f in BIG4) + ")"

    # ============================================================
    # Build unit → firm map and unit → tech map
    # ============================================================
    print("[1/4] Building unit→firm map (mode firm per unit, all firms)…", flush=True)
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
    n_total = con.execute("SELECT COUNT(*) FROM unit_firm").fetchone()[0]
    n_big4 = con.execute(f"SELECT COUNT(*) FROM unit_firm WHERE firm IN {big4_sql}").fetchone()[0]
    print(f"   total units: {n_total:,};  Big-4 units: {n_big4:,}", flush=True)

    print("   Loading lista_unidades.csv for technology classification…", flush=True)
    lista = pd.read_csv(LISTA, encoding="latin1")
    lista["tech_simple"] = (
        lista["technology"].fillna("").str.lower()
        .map(lambda s: "CCGT" if "ciclo combinado" in s
                       else "Hydro" if "hidr" in s
                       else "Nuclear" if "nuclear" in s
                       else "Coal" if ("carbón" in s or "carbon" in s)
                       else "Thermal_other" if ("termic" in s or "fuel" in s or "gas" in s)
                       else "RE_solar" if "solar" in s or "fotovolt" in s
                       else "RE_wind" if "eólic" in s or "eolic" in s
                       else "RE_other" if "régimen especial" in s or "renewable" in s
                       else "Other")
    )
    con.register("unit_tech_pd", lista[["unit_code", "tech_simple"]])
    con.execute("CREATE TABLE unit_tech AS SELECT unit_code, tech_simple FROM unit_tech_pd")
    print("   Tech distribution (Big-4 units):", flush=True)
    tech_dist = con.execute(f"""
        SELECT ut.tech_simple, COUNT(*) AS n
        FROM unit_firm uf
        JOIN unit_tech ut USING (unit_code)
        WHERE uf.firm IN {big4_sql}
        GROUP BY 1 ORDER BY 2 DESC
    """).df()
    print(tech_dist.to_string(index=False), flush=True)
    print()

    # ============================================================
    # PIBCA last-session per (date, period, unit) — Big-4 units only for memory
    # PHF  last-session per (date, period, unit) — Big-4 units only
    # RT2 = PHF − PIBCA
    # ============================================================
    print("[2/4] Computing per-unit RT2 = PHF_last − PIBCA_last (Big-4 units only)…", flush=True)
    con.execute(f"""
        CREATE TABLE pibca_last AS
        WITH big4_units AS (
            SELECT unit_code FROM unit_firm WHERE firm IN {big4_sql}
        ),
        flt AS (
            SELECT date, unit_code, period, mtu_minutes, assigned_power_mw, session_number
            FROM '{PIBCA}'
            WHERE assigned_power_mw IS NOT NULL
              AND unit_code IN (SELECT unit_code FROM big4_units)
        ),
        ranked AS (
            SELECT date, unit_code, period, mtu_minutes, assigned_power_mw,
                   ROW_NUMBER() OVER (PARTITION BY date, unit_code, period
                                      ORDER BY session_number DESC) AS rk
            FROM flt
        )
        SELECT date, unit_code, period, mtu_minutes,
               assigned_power_mw AS pibca_mw
        FROM ranked WHERE rk = 1
    """)
    n_pibca = con.execute("SELECT COUNT(*) FROM pibca_last").fetchone()[0]
    print(f"   PIBCA last-session rows (Big-4 units): {n_pibca:,}", flush=True)

    con.execute(f"""
        CREATE TABLE phf_last AS
        WITH big4_units AS (
            SELECT unit_code FROM unit_firm WHERE firm IN {big4_sql}
        ),
        flt AS (
            SELECT date, unit_code, period, mtu_minutes, assigned_power_mw, session_number
            FROM '{PHF}'
            WHERE assigned_power_mw IS NOT NULL
              AND unit_code IN (SELECT unit_code FROM big4_units)
        ),
        ranked AS (
            SELECT date, unit_code, period, mtu_minutes, assigned_power_mw,
                   ROW_NUMBER() OVER (PARTITION BY date, unit_code, period
                                      ORDER BY session_number DESC) AS rk
            FROM flt
        )
        SELECT date, unit_code, period, mtu_minutes,
               assigned_power_mw AS phf_mw
        FROM ranked WHERE rk = 1
    """)
    n_phf = con.execute("SELECT COUNT(*) FROM phf_last").fetchone()[0]
    print(f"   PHF last-session rows (Big-4 units): {n_phf:,}", flush=True)

    # Build RT2 panel
    con.execute("""
        CREATE TABLE rt2 AS
        SELECT a.date, a.unit_code, a.period, a.mtu_minutes,
               a.phf_mw, COALESCE(b.pibca_mw, 0) AS pibca_mw,
               (a.phf_mw - COALESCE(b.pibca_mw, 0)) AS rt2_mw,
               (a.phf_mw - COALESCE(b.pibca_mw, 0)) * a.mtu_minutes / 60.0 AS rt2_mwh
        FROM phf_last AS a
        LEFT JOIN pibca_last AS b
          ON a.date = b.date AND a.unit_code = b.unit_code AND a.period = b.period
    """)
    n_rt2 = con.execute("SELECT COUNT(*) FROM rt2").fetchone()[0]
    print(f"   RT2 panel rows: {n_rt2:,}\n", flush=True)

    # ============================================================
    # 1. Per-firm × regime: total RT2 MWh and per-firm-day average
    # ============================================================
    print("[3/4] Aggregating per-firm × regime…", flush=True)
    con.execute(f"""
        CREATE TABLE rt2_perfirm AS
        SELECT uf.firm,
               {REGIME_CASE.replace('date', 'r.date')} AS regime,
               SUM(r.rt2_mwh) AS rt2_total_mwh,
               SUM(CASE WHEN r.rt2_mwh > 0 THEN r.rt2_mwh ELSE 0 END) AS rt2_up_mwh,
               SUM(CASE WHEN r.rt2_mwh < 0 THEN r.rt2_mwh ELSE 0 END) AS rt2_down_mwh,
               COUNT(DISTINCT r.date) AS n_days
        FROM rt2 r
        JOIN unit_firm uf USING (unit_code)
        WHERE uf.firm IN {big4_sql}
        GROUP BY 1, 2
    """)

    rt2_pf = con.execute("""
        SELECT firm, regime, rt2_total_mwh, rt2_up_mwh, rt2_down_mwh, n_days,
               rt2_total_mwh / GREATEST(n_days, 1) AS rt2_per_firm_day,
               rt2_up_mwh    / GREATEST(n_days, 1) AS rt2_up_per_firm_day,
               rt2_down_mwh  / GREATEST(n_days, 1) AS rt2_down_per_firm_day
        FROM rt2_perfirm
    """).df()
    rt2_pf["regime"] = pd.Categorical(rt2_pf["regime"], categories=REGIMES, ordered=True)
    rt2_pf = rt2_pf.sort_values(["firm", "regime"])

    print("\nPer-firm × regime RT2 NET (MWh per firm-day):", flush=True)
    pv_net = (rt2_pf.pivot(index="firm", columns="regime", values="rt2_per_firm_day")
                    .reindex(BIG4).reindex(REGIMES, axis=1))
    print(pv_net.round(0).to_string(), flush=True)
    print()

    print("Per-firm × regime RT2 UP-only (MWh per firm-day) — REE forcing dispatch UP:", flush=True)
    pv_up = (rt2_pf.pivot(index="firm", columns="regime", values="rt2_up_per_firm_day")
                   .reindex(BIG4).reindex(REGIMES, axis=1))
    print(pv_up.round(0).to_string(), flush=True)
    print()

    print("Per-firm × regime RT2 DOWN-only (MWh per firm-day) — REE forcing dispatch DOWN:", flush=True)
    pv_dn = (rt2_pf.pivot(index="firm", columns="regime", values="rt2_down_per_firm_day")
                   .reindex(BIG4).reindex(REGIMES, axis=1))
    print(pv_dn.round(0).to_string(), flush=True)
    print()

    rt2_pf.to_csv(OUTDIR / "01_perfirm_perregime.csv", index=False)
    print(f"   wrote {OUTDIR/'01_perfirm_perregime.csv'}\n", flush=True)

    # ============================================================
    # 2. By technology × regime
    # ============================================================
    print("[4/4] Tech split + DA15/ID15 hour-of-day + top-20 units…", flush=True)
    rt2_tech = con.execute(f"""
        SELECT ut.tech_simple AS tech,
               {REGIME_CASE.replace('date', 'r.date')} AS regime,
               SUM(r.rt2_mwh) AS rt2_total_mwh,
               SUM(CASE WHEN r.rt2_mwh > 0 THEN r.rt2_mwh ELSE 0 END) AS rt2_up_mwh,
               SUM(CASE WHEN r.rt2_mwh < 0 THEN r.rt2_mwh ELSE 0 END) AS rt2_down_mwh,
               COUNT(DISTINCT r.date) AS n_days
        FROM rt2 r
        JOIN unit_firm uf USING (unit_code)
        JOIN unit_tech ut USING (unit_code)
        WHERE uf.firm IN {big4_sql}
        GROUP BY 1, 2
    """).df()
    rt2_tech["regime"] = pd.Categorical(rt2_tech["regime"], categories=REGIMES, ordered=True)
    rt2_tech["rt2_up_per_day"] = rt2_tech["rt2_up_mwh"] / rt2_tech["n_days"].clip(lower=1)
    rt2_tech.to_csv(OUTDIR / "02_pertechnology.csv", index=False)

    print("\nBig-4 RT2 UP MWh per day, by technology × regime:", flush=True)
    pv_t = (rt2_tech.pivot(index="tech", columns="regime", values="rt2_up_per_day")
                    .reindex(REGIMES, axis=1))
    pv_t = pv_t.fillna(0).sort_values("DA15/ID15", ascending=False)
    print(pv_t.round(0).to_string(), flush=True)
    print()

    # ============================================================
    # 3. DA15/ID15 hour-of-day profile
    # ============================================================
    rt2_hod = con.execute(f"""
        SELECT (CASE WHEN r.mtu_minutes = 15
                     THEN CEIL(r.period / 4.0)::INT
                     ELSE r.period END) AS hour,
               AVG(r.rt2_mwh) AS rt2_mean_mwh,
               SUM(CASE WHEN r.rt2_mwh > 0 THEN r.rt2_mwh ELSE 0 END)
                 / COUNT(DISTINCT r.date) AS rt2_up_per_day,
               COUNT(DISTINCT r.date) AS n_days
        FROM rt2 r
        JOIN unit_firm uf USING (unit_code)
        WHERE uf.firm IN {big4_sql}
          AND r.date >= '2025-10-01'
        GROUP BY 1
    """).df()
    rt2_hod = rt2_hod.sort_values("hour")
    rt2_hod.to_csv(OUTDIR / "03_hourofday_DA15ID15.csv", index=False)
    print("\nDA15/ID15 RT2-up profile by hour-of-day (MWh per day, Big-4 sum):", flush=True)
    print(rt2_hod[["hour", "rt2_up_per_day", "rt2_mean_mwh"]].round(1).to_string(index=False), flush=True)
    print()

    # ============================================================
    # 4. Top-20 units by DA15/ID15 RT2 up activation
    # ============================================================
    top_units = con.execute(f"""
        SELECT r.unit_code, uf.firm, ut.tech_simple AS tech,
               SUM(CASE WHEN r.rt2_mwh > 0 THEN r.rt2_mwh ELSE 0 END) AS rt2_up_mwh,
               SUM(CASE WHEN r.rt2_mwh < 0 THEN r.rt2_mwh ELSE 0 END) AS rt2_down_mwh,
               SUM(r.rt2_mwh) AS rt2_net_mwh
        FROM rt2 r
        JOIN unit_firm uf USING (unit_code)
        LEFT JOIN unit_tech ut USING (unit_code)
        WHERE r.date >= '2025-10-01'
          AND uf.firm IN {big4_sql}
        GROUP BY 1, 2, 3
        ORDER BY rt2_up_mwh DESC
        LIMIT 20
    """).df()
    top_units.to_csv(OUTDIR / "04_top20_units_DA15ID15.csv", index=False)
    print("\nTop-20 Big-4 units by RT2-up MWh in DA15/ID15:", flush=True)
    print(top_units.round(0).to_string(index=False), flush=True)
    print()

    # ============================================================
    # Headline summary
    # ============================================================
    print("=" * 80, flush=True)
    print("HEADLINE SUMMARY — RT2 post-blackout regulatory cascade", flush=True)
    print("=" * 80, flush=True)
    big4_da15 = rt2_pf[(rt2_pf.firm.isin(BIG4)) & (rt2_pf.regime == "DA15/ID15")]
    total_up_per_day = big4_da15["rt2_up_per_firm_day"].sum()
    total_net_per_day = big4_da15["rt2_per_firm_day"].sum()
    print(f"\nBig-4 sum RT2-up in DA15/ID15:  {total_up_per_day:>+10.0f} MWh per FOUR-firm-day", flush=True)
    print(f"Big-4 sum RT2 net in DA15/ID15: {total_net_per_day:>+10.0f} MWh per FOUR-firm-day", flush=True)
    print(f"Big-4 average RT2 net:           {total_net_per_day/4:>+10.0f} MWh per single-firm-day", flush=True)
    print()
    print(f"Compare: voluntary q₂_IDA Big-4 in DA15/ID15 ≈ +3,969 MWh/firm-day", flush=True)
    print(f"         RT2 / voluntary q₂ ratio: {total_net_per_day / (4 * 3969):.1f}× larger", flush=True)
    print()
    print(f"Total runtime: {(time.time()-t0)/60:.1f} min", flush=True)


if __name__ == "__main__":
    main()
