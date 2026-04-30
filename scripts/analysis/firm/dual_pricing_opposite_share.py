# STATUS: ALIVE
# LAST-AUDIT: 2026-04-30
# FEEDS: New mechanism — dual-pricing imbalance-settlement option value
# CLAIM: Under EBGL Article 52 dual-pricing settlement, BRPs whose ISP
#        imbalance has the OPPOSITE sign to the system imbalance are
#        settled at a more favourable price (often DA price). This creates
#        an ex-ante incentive to take positions opposite to the
#        predicted system sign — independent of market power. Test:
#        per Big-4 firm × regime, opposite-share = fraction of hours where
#        sign(firm imbalance) != sign(system imbalance). If load-bearing,
#        Big-4 should show high opposite-share pre-IDA (when MTU60 system
#        sign was predictable from MTU60 DA position), drop in ISP15-win
#        (DA still MTU60 but settlement now MTU15 — predictability
#        collapses), partially recover at DA15/ID15.
"""Dual-pricing opposite-share by firm × regime — CCGT panel.

CCGT-only first cut. CCGTs are the most strategic dispatchable asset for
positional bets and have a clean unit-EIC → OMIE → firm mapping (50 plants
in `data/external/omie_reference/ccgt_eic_to_omie.csv`).

Construction:
  Firm CCGT scheduled per hour
    = sum over firm's CCGT units of PHF.assigned_power_mw * mtu_min/60
      at max(session_number) per (unit, period)
  Firm CCGT actual per hour
    = sum over firm's CCGT units of A73.quantity_mw * mtu_min/60 (psr_type=B04)
  Firm CCGT imbalance per hour
    = actual − scheduled
  System imbalance per hour
    = (A01 up-volume) − (A02 down-volume)  from imbalance_volumes_all
      (ENTSO-E business_type A19; sign convention verified below)
  Opposite-share per (firm, regime)
    = mean over hours of  1{sign(firm_imb) != sign(sys_imb)}
      conditional on both signs nonzero

Output:
  results/regressions/dual_pricing_opposite_share.csv
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
CCGT    = PROJECT / "data" / "external" / "omie_reference" / "ccgt_eic_to_omie.csv"
OUT     = PROJECT / "results" / "regressions" / "dual_pricing_opposite_share.csv"

REGIMES = ["pre-IDA", "3-sess", "ISP15-win", "DA60/ID15", "DA15/ID15"]
BIG4    = ["IB", "GE", "GN", "HC"]


def assign_regime(d) -> str:
    d = pd.Timestamp(d)
    if d < pd.Timestamp("2024-06-14"): return "pre-IDA"
    if d < pd.Timestamp("2024-12-01"): return "3-sess"
    if d < pd.Timestamp("2025-03-19"): return "ISP15-win"
    if d < pd.Timestamp("2025-10-01"): return "DA60/ID15"
    return "DA15/ID15"


def main() -> None:
    print("[0/6] Loading CCGT EIC→OMIE→firm mapping…", flush=True)
    ccgt = pd.read_csv(CCGT)
    print(f"   n_plants={len(ccgt)}; firm counts: {dict(ccgt.firm.value_counts())}", flush=True)
    eic_to_firm = dict(zip(ccgt.entsoe_eic, ccgt.firm))
    omie_to_firm = dict(zip(ccgt.omie_code, ccgt.firm))

    con = duckdb.connect()
    con.execute("SET memory_limit='6GB'")
    con.execute("SET threads=4")

    # ============================================================
    # 1. Firm CCGT actual (A73 B04 per unit, hourly)
    # ============================================================
    print("[1/6] Building firm CCGT actual (A73 B04, hourly)…", flush=True)
    eic_list_str = ",".join(f"'{e}'" for e in ccgt.entsoe_eic)
    actual = con.execute(f"""
        SELECT  date_trunc('hour', isp_start_utc) AS hour_utc,
                unit_eic,
                SUM(quantity_mw * mtu_minutes / 60.0) AS actual_mwh
        FROM '{A73}'
        WHERE psr_type = 'B04'
          AND unit_eic IN ({eic_list_str})
        GROUP BY 1, 2
    """).df()
    actual["firm"] = actual["unit_eic"].map(eic_to_firm).fillna("OTHER")
    actual_firm = actual.groupby(["hour_utc", "firm"], as_index=False)["actual_mwh"].sum()
    print(f"   actual rows after firm aggregation: {len(actual_firm):,}", flush=True)

    # ============================================================
    # 2. Firm CCGT scheduled (PHF max-session per unit, hourly)
    # ============================================================
    print("[2/6] Building firm CCGT scheduled (PHF max-session, hourly)…", flush=True)
    omie_list_str = ",".join(f"'{u}'" for u in ccgt.omie_code)
    sched = con.execute(f"""
        WITH max_session AS (
            SELECT date, period, unit_code, MAX(session_number) AS max_s
            FROM '{PHF}'
            WHERE unit_code IN ({omie_list_str})
            GROUP BY 1, 2, 3
        ),
        last_phf AS (
            SELECT phf.date, phf.period, phf.unit_code, phf.assigned_power_mw, phf.mtu_minutes
            FROM '{PHF}' phf
            JOIN max_session ms
              ON phf.date = ms.date AND phf.period = ms.period
             AND phf.unit_code = ms.unit_code AND phf.session_number = ms.max_s
        )
        SELECT date,
               -- Map each period (1..24 MTU60 or 1..96 MTU15) to the hour of the day (1..24)
               CASE WHEN mtu_minutes = 60 THEN period
                    WHEN mtu_minutes = 15 THEN ((period - 1) / 4) + 1
                    ELSE NULL END                                        AS hour_local,
               unit_code,
               SUM(assigned_power_mw * mtu_minutes / 60.0)               AS scheduled_mwh
        FROM last_phf
        WHERE mtu_minutes IN (15, 60)
        GROUP BY 1, 2, 3
    """).df()
    sched = sched.dropna(subset=["hour_local"])
    sched["hour_local"] = sched["hour_local"].astype(int)
    # Build hour_utc from local date+hour. Spain is CET/CEST; for sign-match purposes
    # we treat OMIE local hour as UTC+1 throughout. This is the same convention used
    # across the project's q₂ panels — see notebooks/memos/_modelling_track.md.
    sched["hour_utc"] = pd.to_datetime(sched["date"]) + pd.to_timedelta(sched["hour_local"] - 1, unit="h")
    sched["firm"] = sched["unit_code"].map(omie_to_firm).fillna("OTHER")
    sched_firm = sched.groupby(["hour_utc", "firm"], as_index=False)["scheduled_mwh"].sum()
    print(f"   scheduled rows after firm aggregation: {len(sched_firm):,}", flush=True)

    # ============================================================
    # 3. System imbalance per hour
    # ============================================================
    print("[3/6] Building system imbalance per hour (A19 business type)…", flush=True)
    sys_imb = con.execute(f"""
        SELECT  date_trunc('hour', isp_start_utc) AS hour_utc,
                SUM(CASE WHEN flow_direction = 'A01'
                         THEN volume_mwh * mtu_minutes / 60.0
                         WHEN flow_direction = 'A02'
                         THEN -volume_mwh * mtu_minutes / 60.0
                         ELSE 0 END)              AS sys_imb_mwh
        FROM '{IV}'
        WHERE business_type = 'A19'
        GROUP BY 1
    """).df()
    print(f"   sys_imb rows: {len(sys_imb):,}; sample mean = {sys_imb.sys_imb_mwh.mean():.1f} MWh/hr", flush=True)

    # ============================================================
    # 4. Merge firm imbalance with system imbalance
    # ============================================================
    print("[4/6] Merging firm imbalance with system imbalance…", flush=True)
    panel = sched_firm.merge(actual_firm, on=["hour_utc", "firm"], how="inner")
    panel["firm_imb_mwh"] = panel["actual_mwh"] - panel["scheduled_mwh"]
    panel = panel.merge(sys_imb, on="hour_utc", how="inner")
    panel["date"] = panel["hour_utc"].dt.normalize()
    panel["regime"] = panel["date"].apply(assign_regime)
    panel["sign_firm"] = np.sign(panel["firm_imb_mwh"])
    panel["sign_sys"]  = np.sign(panel["sys_imb_mwh"])
    print(f"   joined panel: {len(panel):,} (firm,hour) rows", flush=True)

    # ============================================================
    # 5. Aggregate opposite-share by firm × regime
    # ============================================================
    print("[5/6] Aggregating opposite-share by firm × regime…", flush=True)
    # Drop rows with zero sign on either side (sign-match undefined)
    valid = panel[(panel.sign_firm != 0) & (panel.sign_sys != 0)].copy()
    valid["opposite"] = (valid.sign_firm != valid.sign_sys).astype(int)
    rows = []
    for firm in BIG4 + ["OTHER"]:
        for regime in REGIMES:
            sub = valid[(valid.firm == firm) & (valid.regime == regime)]
            if len(sub) < 30:
                continue
            sf_long = float((sub.sign_firm > 0).mean())
            ss_long = float((sub.sign_sys > 0).mean())
            indep_opp = sf_long * (1 - ss_long) + (1 - sf_long) * ss_long
            obs_opp   = float(sub.opposite.mean())
            # Two-sided binomial test for opp != indep_opp (large-N → normal approx)
            n = len(sub)
            se_indep = (indep_opp * (1 - indep_opp) / n) ** 0.5 if n > 0 else float("nan")
            z = (obs_opp - indep_opp) / se_indep if se_indep > 0 else float("nan")
            rows.append({
                "firm":              firm,
                "regime":            regime,
                "n_hours":           int(n),
                "opposite_share":    obs_opp,
                "indep_baseline":    indep_opp,
                "lift":              obs_opp / indep_opp if indep_opp > 0 else float("nan"),
                "z_vs_indep":        z,
                "mean_firm_imb":     float(sub.firm_imb_mwh.mean()),
                "mean_abs_firm_imb": float(sub.firm_imb_mwh.abs().mean()),
                "mean_sys_imb":      float(sub.sys_imb_mwh.mean()),
                "share_firm_long":   sf_long,
                "share_sys_long":    ss_long,
            })
    out = pd.DataFrame(rows)

    # ============================================================
    # 6. Print + save
    # ============================================================
    print()
    print("=" * 100)
    print("DUAL-PRICING OPPOSITE-SHARE — CCGT panel")
    print("=" * 100)
    print(f"  Outcome: P(sign(firm CCGT imbalance) != sign(system imbalance))")
    print(f"  Per-firm hourly aggregation across the 50 mapped CCGT plants.")
    print(f"  Joint observations: {len(valid):,} firm-hours.")
    print()
    print("Predicted dual-pricing pattern: opposite-share HIGH pre-IDA,")
    print("DROPS in ISP15-win (predictability of MTU15 system sign collapses),")
    print("PARTIAL recovery DA15/ID15 (MTU15 alignment restored).")
    print("Fringe (OTHER) should be FLAT under the strict dual-pricing reading.")
    print()
    fmt = "{:<6}  {:<12}  {:>8}  {:>10}  {:>10}  {:>6}  {:>9}  {:>13}  {:>13}"
    print(fmt.format("firm", "regime", "n_hours",
                     "opp_sh", "indep_base",
                     "lift", "z_vs_indep",
                     "share_firm>0", "share_sys>0"))
    print("-" * 110)
    for _, r in out.iterrows():
        print(fmt.format(
            r["firm"], r["regime"],
            f"{r['n_hours']:,}",
            f"{r['opposite_share']:.3f}",
            f"{r['indep_baseline']:.3f}",
            f"{r['lift']:.3f}",
            f"{r['z_vs_indep']:+.2f}",
            f"{r['share_firm_long']:.3f}",
            f"{r['share_sys_long']:.3f}",
        ))
    print()

    print("Per-firm LIFT trajectory across regimes (>1 = positioning anti-system more than chance):")
    pivot = out.pivot(index="firm", columns="regime", values="lift")
    pivot = pivot[REGIMES]
    print(pivot.to_string(float_format=lambda x: f"{x:.3f}"))
    print()

    OUT.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUT, index=False)
    print(f"wrote {OUT}")


if __name__ == "__main__":
    main()
