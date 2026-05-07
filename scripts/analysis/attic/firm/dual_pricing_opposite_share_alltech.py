# STATUS: ALIVE
# LAST-AUDIT: 2026-05-02
# FEEDS: Dual-pricing imbalance-settlement option-value mechanism — extension to CCGT + hydro + nuclear
# CLAIM: User flagged that the original CCGT-only test missed IB's main strategic
#        asset (hydro = 64% of IB's price-setting transfer per F7). This script
#        extends the test to all dispatchable technologies — CCGT (B04) +
#        reservoir hydro (B12) + pumped hydro (B10) + nuclear (B14). If the
#        dual-pricing predictability mechanism operates through hydro positioning,
#        we should see a strong cross-regime lift pattern that the CCGT-only
#        test missed.
"""Dual-pricing opposite-share — CCGT + hydro + nuclear panel.

Method: same as `dual_pricing_opposite_share.py` but extended to all
dispatchable technologies. Maps A73 EIC → OMIE unit code → Big-4 firm
for B04 (CCGT), B10+B12 (hydro), B14 (nuclear).

Mapping construction:
  - CCGT: existing `data/external/omie_reference/ccgt_eic_to_omie.csv` (50 plants)
  - Hydro + Nuclear: extracted from A73 EICs (regex 18W<CODE>-...), looked up
    in pdbce.grupo_empresarial; pump units (offer_type=3, not in pdbce) hardcoded
    by ownership.

Output:
  results/regressions/dual_pricing_opposite_share_alltech.csv
"""
from __future__ import annotations

from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

PROJECT = Path(__file__).resolve().parents[3]
PHF     = PROJECT / "data" / "processed" / "omie" / "mercado_intradiario_subastas" / "programas" / "phf_all.parquet"
A73     = PROJECT / "data" / "processed" / "entsoe" / "generation" / "a73_per_unit_all.parquet"
IV      = PROJECT / "data" / "processed" / "entsoe" / "balancing" / "imbalance_volumes_all.parquet"
PDBC    = PROJECT / "data" / "processed" / "omie" / "mercado_diario" / "programas" / "pdbc_all.parquet"
PDBCE   = PROJECT / "data" / "processed" / "omie" / "mercado_diario" / "programas" / "pdbce_all.parquet"
CCGT    = PROJECT / "data" / "external" / "omie_reference" / "ccgt_eic_to_omie.csv"
OUT     = PROJECT / "results" / "regressions" / "dual_pricing_opposite_share_alltech.csv"

REGIMES = ["pre-IDA", "3-sess", "ISP15-win", "DA60/ID15", "DA15/ID15"]
BIG4    = ["IB", "GE", "GN", "HC"]

# Pump units (offer_type=3 in OMIE, not in pdbce.grupo_empresarial). Hardcoded:
PUMP_FIRM = {
    "DUEB": "IB",   # Iberdrola Duero pump
    "MUEB": "IB",   # Iberdrola Muela pump
    "TJEB": "IB",   # Iberdrola Tajo pump (OMIE code TAJB)
    "SLTB": "GE",   # Endesa Sallente pump
}


def assign_regime(d) -> str:
    d = pd.Timestamp(d)
    if d < pd.Timestamp("2024-06-14"): return "pre-IDA"
    if d < pd.Timestamp("2024-12-01"): return "3-sess"
    if d < pd.Timestamp("2025-03-19"): return "ISP15-win"
    if d < pd.Timestamp("2025-10-01"): return "DA60/ID15"
    return "DA15/ID15"


def build_eic_firm_mapping(con) -> pd.DataFrame:
    """Build a unified EIC→firm mapping for B04 (CCGT) + B10/B12 (hydro) + B14 (nuclear)."""
    # CCGT
    ccgt = pd.read_csv(CCGT)[["entsoe_eic", "omie_code", "firm"]]
    ccgt["psr_type"] = "B04"
    ccgt = ccgt.rename(columns={"entsoe_eic": "unit_eic"})
    # Hydro + Nuclear from A73
    a73 = con.execute(f"""
        SELECT DISTINCT psr_type, unit_eic
        FROM '{A73}'
        WHERE psr_type IN ('B10', 'B12', 'B14')
          AND unit_eic <> 'UNKNOWN'
    """).df()
    a73["omie_code"] = a73["unit_eic"].str.extract(r"^18W([A-Z0-9]+)-")[0]
    # Look up firm from pdbce
    firms = con.execute(f"""
        SELECT unit_code, grupo_empresarial AS firm
        FROM (
          SELECT unit_code, grupo_empresarial,
                 ROW_NUMBER() OVER (PARTITION BY unit_code ORDER BY date DESC) AS rn
          FROM '{PDBCE}'
          WHERE grupo_empresarial IS NOT NULL
        ) WHERE rn = 1
    """).df()
    a73 = a73.merge(firms, left_on="omie_code", right_on="unit_code", how="left").drop(columns=["unit_code"])
    # Fill pumps from hardcoded
    a73["firm"] = a73["firm"].fillna(a73["omie_code"].map(PUMP_FIRM))
    a73["firm"] = a73["firm"].fillna("OTHER")

    mapping = pd.concat([
        ccgt[["unit_eic", "omie_code", "firm", "psr_type"]],
        a73[["unit_eic", "omie_code", "firm", "psr_type"]],
    ], ignore_index=True)
    mapping = mapping.drop_duplicates(subset=["unit_eic"])  # one row per EIC
    return mapping


def main() -> None:
    con = duckdb.connect()
    con.execute("SET memory_limit='8GB'")
    con.execute("SET threads=4")
    con.execute("SET preserve_insertion_order=false")

    # ------------------------------------------------------------------
    # 1. Unified EIC→firm mapping
    # ------------------------------------------------------------------
    print("[1/6] Building EIC→firm mapping (CCGT + hydro + nuclear)…", flush=True)
    mapping = build_eic_firm_mapping(con)
    print(f"   total mapped units: {len(mapping)}")
    print(f"   firm × tech matrix:")
    pivot = mapping.pivot_table(index="firm", columns="psr_type", values="omie_code", aggfunc="count", fill_value=0)
    print(pivot)
    print()

    eic_to_firm = dict(zip(mapping.unit_eic, mapping.firm))
    omie_to_firm = dict(zip(mapping.omie_code, mapping.firm))

    # ------------------------------------------------------------------
    # 2. Firm hourly ACTUAL (A73, B04+B10+B12+B14, hourly)
    # ------------------------------------------------------------------
    print("[2/6] Building firm hourly actual generation (A73, all-tech)…", flush=True)
    eic_str = ",".join(f"'{e}'" for e in mapping.unit_eic)
    actual = con.execute(f"""
        SELECT  date_trunc('hour', isp_start_utc) AS hour_utc,
                unit_eic,
                psr_type,
                SUM(quantity_mw * mtu_minutes / 60.0) AS actual_mwh
        FROM '{A73}'
        WHERE psr_type IN ('B04', 'B10', 'B12', 'B14')
          AND unit_eic IN ({eic_str})
        GROUP BY 1, 2, 3
    """).df()
    actual["firm"] = actual["unit_eic"].map(eic_to_firm).fillna("OTHER")
    actual_firm = actual.groupby(["hour_utc", "firm"], as_index=False)["actual_mwh"].sum()
    print(f"   firm-hour actual rows: {len(actual_firm):,}", flush=True)

    # ------------------------------------------------------------------
    # 3. Firm hourly SCHEDULED (PHF max-session, all-tech via OMIE codes)
    # ------------------------------------------------------------------
    print("[3/6] Building firm hourly scheduled (PHF max-session, all-tech)…", flush=True)
    omie_str = ",".join(f"'{u}'" for u in mapping.omie_code)
    sched = con.execute(f"""
        WITH max_session AS (
            SELECT date, period, unit_code, MAX(session_number) AS max_s
            FROM '{PHF}'
            WHERE unit_code IN ({omie_str})
            GROUP BY 1, 2, 3
        ),
        last_phf AS (
            SELECT phf.date, phf.period, phf.unit_code,
                   phf.assigned_power_mw, phf.mtu_minutes
            FROM '{PHF}' phf
            JOIN max_session ms USING (date, period, unit_code)
            WHERE phf.session_number = ms.max_s
        )
        SELECT date,
               CASE WHEN mtu_minutes = 60 THEN period
                    WHEN mtu_minutes = 15 THEN ((period - 1) / 4) + 1
                    ELSE NULL END AS hour_local,
               unit_code,
               SUM(assigned_power_mw * mtu_minutes / 60.0) AS scheduled_mwh
        FROM last_phf
        WHERE mtu_minutes IN (15, 60)
        GROUP BY 1, 2, 3
    """).df()
    sched = sched.dropna(subset=["hour_local"])
    sched["hour_local"] = sched["hour_local"].astype(int)
    sched["hour_utc"] = pd.to_datetime(sched["date"]) + pd.to_timedelta(sched["hour_local"] - 1, unit="h")
    sched["firm"] = sched["unit_code"].map(omie_to_firm).fillna("OTHER")
    sched_firm = sched.groupby(["hour_utc", "firm"], as_index=False)["scheduled_mwh"].sum()
    print(f"   firm-hour scheduled rows: {len(sched_firm):,}", flush=True)

    # ------------------------------------------------------------------
    # 4. System imbalance (A19 net, hourly)
    # ------------------------------------------------------------------
    print("[4/6] Building system imbalance per hour…", flush=True)
    sys_imb = con.execute(f"""
        SELECT date_trunc('hour', isp_start_utc) AS hour_utc,
               SUM(CASE WHEN flow_direction = 'A01'
                        THEN volume_mwh * mtu_minutes / 60.0
                        WHEN flow_direction = 'A02'
                        THEN -volume_mwh * mtu_minutes / 60.0
                        ELSE 0 END) AS sys_imb_mwh
        FROM '{IV}'
        WHERE business_type = 'A19'
        GROUP BY 1
    """).df()
    print(f"   sys_imb rows: {len(sys_imb):,}", flush=True)

    # ------------------------------------------------------------------
    # 5. Merge → firm imbalance, regime, sign-match panel
    # ------------------------------------------------------------------
    print("[5/6] Merging firm imbalance with system imbalance…", flush=True)
    panel = sched_firm.merge(actual_firm, on=["hour_utc", "firm"], how="inner")
    panel["firm_imb_mwh"] = panel["actual_mwh"] - panel["scheduled_mwh"]
    panel = panel.merge(sys_imb, on="hour_utc", how="inner")
    panel["date"] = panel["hour_utc"].dt.normalize()
    panel["regime"] = panel["date"].apply(assign_regime)
    panel["sign_firm"] = np.sign(panel["firm_imb_mwh"])
    panel["sign_sys"]  = np.sign(panel["sys_imb_mwh"])
    print(f"   joined panel: {len(panel):,} firm-hour rows", flush=True)

    # ------------------------------------------------------------------
    # 6. Aggregate by firm × regime, lift, z-vs-independence
    # ------------------------------------------------------------------
    print("[6/6] Aggregating opposite-share by firm × regime…", flush=True)
    valid = panel[(panel.sign_firm != 0) & (panel.sign_sys != 0)].copy()
    valid["opposite"] = (valid.sign_firm != valid.sign_sys).astype(int)
    rows = []
    for firm in BIG4 + ["OTHER"]:
        for regime in REGIMES:
            sub = valid[(valid.firm == firm) & (valid.regime == regime)]
            if len(sub) < 30: continue
            sf_long = float((sub.sign_firm > 0).mean())
            ss_long = float((sub.sign_sys > 0).mean())
            indep_opp = sf_long * (1 - ss_long) + (1 - sf_long) * ss_long
            obs_opp   = float(sub.opposite.mean())
            n = len(sub)
            se_indep = (indep_opp * (1 - indep_opp) / n) ** 0.5 if n > 0 else float("nan")
            z = (obs_opp - indep_opp) / se_indep if se_indep > 0 else float("nan")
            rows.append({
                "firm": firm, "regime": regime, "n_hours": int(n),
                "opposite_share": obs_opp, "indep_baseline": indep_opp,
                "lift": obs_opp / indep_opp if indep_opp > 0 else float("nan"),
                "z_vs_indep": z,
                "share_firm_long": sf_long, "share_sys_long": ss_long,
                "mean_firm_imb": float(sub.firm_imb_mwh.mean()),
                "mean_abs_firm_imb": float(sub.firm_imb_mwh.abs().mean()),
            })
    out = pd.DataFrame(rows)

    print()
    print("=" * 110)
    print("DUAL-PRICING OPPOSITE-SHARE — ALL-TECH (CCGT + hydro + nuclear)")
    print("=" * 110)
    print(f"  Panel: {len(valid):,} firm-hours; coverage CCGT B04 + hydro B10/B12 + nuclear B14")
    print(f"  Big-4 firm × tech composition (mapped units):")
    print("  " + str(pivot).replace("\n", "\n  "))
    print()
    fmt = "{:<6}  {:<11}  {:>8}  {:>8}  {:>10}  {:>6}  {:>9}  {:>10}  {:>11}"
    print(fmt.format("firm", "regime", "n_hours",
                     "opp_sh", "indep_base",
                     "lift", "z_vs_ind",
                     "mean_imb", "|mean_imb|"))
    print("-" * 110)
    for _, r in out.iterrows():
        print(fmt.format(
            r["firm"], r["regime"],
            f"{r['n_hours']:,}",
            f"{r['opposite_share']:.3f}",
            f"{r['indep_baseline']:.3f}",
            f"{r['lift']:.3f}",
            f"{r['z_vs_indep']:+.2f}",
            f"{r['mean_firm_imb']:+.0f}",
            f"{r['mean_abs_firm_imb']:.0f}",
        ))
    print()
    print("Per-firm LIFT trajectory across regimes (>1 = anti-system positioning above chance):")
    pv = out.pivot(index="firm", columns="regime", values="lift")[REGIMES]
    print(pv.to_string(float_format=lambda x: f"{x:.3f}"))
    print()

    OUT.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUT, index=False)
    print(f"wrote {OUT}")


if __name__ == "__main__":
    main()
