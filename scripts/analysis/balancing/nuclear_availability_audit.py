# STATUS: ALIVE
# LAST-AUDIT: 2026-04-27
# FEEDS: CNMC Article 64.37 hypothesis — chronic unauthorized capacity reduction by Spanish nuclear
# CLAIM: Test whether Spanish nuclear capacity factor declined 2023-2025, and whether the decline is concentrated in IB-attributable plants and/or in low-price hours.

"""Spanish nuclear availability factor audit.

CNMC opened "very serious" expedientes on 2026-04-23 against Iberdrola
Generación Nuclear (Cofrentes) and C.N. Almaraz-Trillo A.I.E. for
Article 64.37 LSE — unauthorized reduction of production / repeated
failure to meet availability obligations. Investigation window covers
2023+ ("periodos prolongados de tiempo").

Reasoning before running:
  - Strategic-reduction hypothesis predicts: CF lower in 2023-2025 vs
    2018-2022; concentrated in IB-only plants (Cofrentes 100% IB) more
    than jointly-owned plants; NOT concentrated in low-price hours
    (else economic not strategic).
  - Age-only hypothesis predicts: smooth monotonic decline across all
    units regardless of ownership.
  - Economic hypothesis predicts: reductions concentrated in low-price
    hours (mid-day) but not over time.

  OVB: refueling outages typically scheduled in spring/fall — adding
  cal-month FE controls. Comparing IB-only Cofrentes to jointly-owned
  Almaraz/Trillo provides a within-firm placebo.

  Magnitude check: Spanish nuclear ~7.3 GW × 8760h = 64 TWh nameplate
  per year. Historical CF ~85% = 54 TWh actual. A 5pp decline = -3.2 TWh
  annually = ~€100-300M revenue loss at €30-100/MWh.

Output: results/regressions/nuclear_availability_audit.csv
"""
from __future__ import annotations

from pathlib import Path
import duckdb
import numpy as np
import pandas as pd

PROJECT = Path(__file__).resolve().parents[3]
PDBCE = PROJECT / "data" / "processed" / "omie" / "mercado_diario" / "programas" / "pdbce_all.parquet"
PRICE = PROJECT / "data" / "processed" / "omie" / "mercado_diario" / "precios" / "marginalpdbc_all.parquet"
OUT = PROJECT / "results" / "regressions" / "nuclear_availability_audit.csv"

# Nuclear units with nameplate (MW) and IB ownership share
NUCLEAR = {
    "ALZ1":   {"nameplate": 1011, "ib_share": 0.53, "name": "Almaraz I"},
    "ALZ2":   {"nameplate": 1006, "ib_share": 0.53, "name": "Almaraz II"},
    "ASC1":   {"nameplate": 1032, "ib_share": 0.00, "name": "Ascó I"},
    "ASC2":   {"nameplate": 1027, "ib_share": 0.15, "name": "Ascó II"},
    "COF":    {"nameplate": 1064, "ib_share": 1.00, "name": "Cofrentes"},
    "TRL1":   {"nameplate": 1066, "ib_share": 0.48, "name": "Trillo"},
    "VAN2":   {"nameplate": 1087, "ib_share": 0.28, "name": "Vandellós II"},
}


def main() -> None:
    print("[1/4] Pull cleared MW per (nuclear unit, day, hour) since 2018...")
    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")
    con.execute("SET threads=4")

    units_sql = ",".join(repr(u) for u in NUCLEAR.keys())
    df = con.sql(f"""
        SELECT CAST(date AS DATE) AS date,
               period, mtu_minutes, unit_code,
               assigned_power_mw,
               assigned_power_mw * mtu_minutes / 60.0 AS mwh
        FROM '{PDBCE}'
        WHERE unit_code IN ({units_sql})
          AND offer_type = 1
          AND assigned_power_mw IS NOT NULL
          AND CAST(date AS DATE) >= DATE '2018-01-01'
    """).df()
    df["date"] = pd.to_datetime(df["date"])
    df["year"] = df["date"].dt.year
    df["month"] = df["date"].dt.month
    print(f"   panel: {len(df):,} unit-period rows; range {df.date.min().date()} → {df.date.max().date()}")
    print(f"   units present: {sorted(df.unit_code.unique())}")

    # Daily aggregate per unit
    daily = df.groupby(["date", "unit_code", "year", "month"], as_index=False)["mwh"].sum()
    daily["nameplate_mwh"] = daily["unit_code"].map(lambda u: NUCLEAR[u]["nameplate"] * 24)
    daily["cf"] = daily["mwh"] / daily["nameplate_mwh"]
    # Daily CF capped at 1.05 to handle minor data noise
    daily["cf"] = daily["cf"].clip(0, 1.05)
    print(f"   daily panel: {len(daily):,} unit-days")

    print()
    print("[2/4] Annual capacity factor by unit (% of nameplate × 24h × days):")
    annual = daily.groupby(["unit_code", "year"]).agg(
        cf=("cf", "mean"),
        n_days=("date", "size"),
    ).reset_index()
    annual["cf_pct"] = annual["cf"] * 100
    pivot = annual.pivot(index="unit_code", columns="year", values="cf_pct").round(1)
    # Add IB-share for context
    pivot.insert(0, "IB%", pd.Series(NUCLEAR).apply(lambda u: u["ib_share"] * 100).round(0).astype(int))
    pivot.insert(1, "Name", pd.Series(NUCLEAR).apply(lambda u: u["name"]))
    pivot = pivot.sort_values("IB%", ascending=False)
    print(pivot.to_string())
    print()

    print("[3/4] System-aggregate annual CF (all 7 reactors weighted by nameplate):")
    # Weighted CF: total cleared MWh / total nameplate MWh
    daily["nameplate_mwh_unit"] = daily["unit_code"].map(lambda u: NUCLEAR[u]["nameplate"] * 24)
    sys_annual = daily.groupby("year").agg(
        total_cleared=("mwh", "sum"),
        total_nameplate=("nameplate_mwh_unit", "sum"),
        n_days=("date", "size"),
    ).reset_index()
    sys_annual["cf_pct"] = sys_annual["total_cleared"] / sys_annual["total_nameplate"] * 100
    sys_annual["delta_pct_vs_2018"] = sys_annual["cf_pct"] - sys_annual.loc[sys_annual["year"] == 2018, "cf_pct"].values[0]
    print(sys_annual[["year", "n_days", "cf_pct", "delta_pct_vs_2018"]].round(1).to_string(index=False))
    print()

    # IB-attributable nuclear (weighted by IB share)
    print("IB-attributable nuclear (capacity-share weighted):")
    daily["ib_share"] = daily["unit_code"].map(lambda u: NUCLEAR[u]["ib_share"])
    daily["ib_cleared"] = daily["mwh"] * daily["ib_share"]
    daily["ib_nameplate"] = daily["nameplate_mwh_unit"] * daily["ib_share"]
    ib_annual = daily.groupby("year").agg(
        ib_cleared=("ib_cleared", "sum"),
        ib_nameplate=("ib_nameplate", "sum"),
    ).reset_index()
    ib_annual["ib_cf_pct"] = ib_annual["ib_cleared"] / ib_annual["ib_nameplate"] * 100
    print(ib_annual[["year", "ib_cf_pct"]].round(1).to_string(index=False))

    # Cofrentes (100% IB) vs jointly-owned average
    print()
    print("Cofrentes (100% IB) vs jointly-owned average annual CF:")
    cof = annual[annual["unit_code"] == "COF"][["year", "cf_pct"]].rename(columns={"cf_pct": "Cofrentes"})
    joint_avg = annual[annual["unit_code"].isin(["ALZ1", "ALZ2", "TRL1", "ASC1", "ASC2", "VAN2"])].groupby("year")["cf_pct"].mean().rename("JointAvg")
    comp = cof.set_index("year").join(joint_avg).round(1)
    comp["delta"] = (comp["Cofrentes"] - comp["JointAvg"]).round(1)
    print(comp.to_string())

    print()
    print("[4/4] Same-calendar-month comparison: 2018-2022 baseline vs 2023-2025:")
    # Per (unit, month), mean cf across 2018-2022 vs 2023-2025
    daily["era"] = np.where(daily["year"] <= 2022, "1.pre-2023", "2.2023+")
    era = daily.groupby(["unit_code", "era"]).agg(cf=("cf", "mean")).reset_index()
    era_piv = era.pivot(index="unit_code", columns="era", values="cf") * 100
    era_piv["delta_pp"] = (era_piv["2.2023+"] - era_piv["1.pre-2023"]).round(1)
    era_piv["IB%"] = pd.Series(NUCLEAR).apply(lambda u: u["ib_share"] * 100).round(0).astype(int)
    era_piv = era_piv.round(1).sort_values("IB%", ascending=False)
    print(era_piv.to_string())

    OUT.parent.mkdir(parents=True, exist_ok=True)
    out_rows = pd.concat([
        annual.assign(_table="annual_unit"),
        sys_annual.assign(_table="annual_system"),
        ib_annual.assign(_table="ib_attributable_annual"),
        comp.reset_index().assign(_table="cofrentes_vs_joint"),
        era_piv.reset_index().assign(_table="era_comparison"),
    ], ignore_index=True, sort=False)
    out_rows.to_csv(OUT, index=False)
    print(f"\nwrote {OUT}")


if __name__ == "__main__":
    main()
