# STATUS: ALIVE
# LAST-AUDIT: 2026-04-27
# FEEDS: New economic angle — IB pumped-storage arbitrage on duck-curve spread
# CLAIM: IB monetizes the solar-induced mid-day-vs-evening price spread (S9 cannibalization) via pumped-storage arbitrage; this is a second revenue channel beyond F7 DA-spot rent.

"""F12 — IB pumped-storage arbitrage on the duck-curve spread.

S9 documents severe solar cannibalization (capture ratio 0.61) — solar
production peaks at h12-h14 when DA prices are €27-32/MWh and drops to
0.3 GW at h20-h22 when prices spike to €91-99/MWh. The €60/MWh mid-day
vs evening spread is a textbook arbitrage signal for storage.

IB owns the largest pumped-storage fleet in Spain:
  - MUEL (La Muela Turbinación) — pure pumped storage (1295 MW)
  - MUEB (La Muela Bombeo) — pumping side
  - DUER/DUEB, SIL/SILB, TAJO/TAJB, TAMEGA/TAMEGAB — mixed pump-hydro
    (river inflow + pumped water); pump arbitrage harder to isolate

In OMIE pdbce, pumping units appear with offer_type=3 and negative
assigned_power_mw (consumption); generating units appear with
offer_type=1 and positive assigned_power_mw.

Test:
  - Compute pump-weighted average price (when each B-unit consumes)
  - Compute gen-weighted average price (when corresponding gen unit
    produces)
  - Spread = mean(gen_price) - mean(pump_price)
  - Total arbitrage gross revenue gap = sum(gen_p × gen_mwh) -
    sum(pump_p × pump_mwh)
  - Compare across regimes — widening spread post-reform corroborates
    the duck-curve story

Output: results/regressions/f12_pumped_storage_arbitrage.csv
"""
from __future__ import annotations

from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

PROJECT = Path(__file__).resolve().parents[3]
PRICE = PROJECT / "data" / "processed" / "omie" / "mercado_diario" / "precios" / "marginalpdbc_all.parquet"
PDBCE = PROJECT / "data" / "processed" / "omie" / "mercado_diario" / "programas" / "pdbce_all.parquet"
OUT = PROJECT / "results" / "regressions" / "f12_pumped_storage_arbitrage.csv"

# IB pumped-storage portfolio
PUMP_UNITS = ["DUEB", "MUEB", "SILB", "TAJB", "TAMEGAB"]  # B-suffix = pumping (consumption)
GEN_UNITS = ["DUER", "MUEL", "SIL", "TAJO", "TAMEGA"]      # corresponding generation
PURE_GEN = ["MUEL"]
PURE_PUMP = ["MUEB"]


def assign_regime(d) -> str:
    if d < pd.Timestamp("2024-06-14"):
        return "1.pre-IDA"
    if d < pd.Timestamp("2024-12-01"):
        return "2.3-sess"
    if d < pd.Timestamp("2025-03-19"):
        return "3.ISP15-win"
    if d < pd.Timestamp("2025-10-01"):
        return "4.DA60/ID15"
    return "5.DA15/ID15"


def main() -> None:
    print("[1/5] Hourly Spain DA price...")
    con = duckdb.connect()
    con.execute("SET memory_limit='2GB'")
    px = con.sql(f"""
        SELECT date, period, AVG(price_es_eur_mwh) AS p_da, MAX(mtu_minutes) AS mtu_minutes
        FROM '{PRICE}'
        WHERE price_es_eur_mwh IS NOT NULL
          AND CAST(date AS DATE) >= DATE '2018-01-01'
        GROUP BY 1, 2
    """).df()
    px["date"] = pd.to_datetime(px["date"])
    print(f"   price panel: {len(px):,} ISPs")

    print("[2/5] IB pump + gen volumes from pdbce...")
    units_sql = ",".join(repr(u) for u in PUMP_UNITS + GEN_UNITS)
    con.execute(f"""
        CREATE TEMP TABLE pumps AS
        SELECT unit_code, offer_type, date, period, mtu_minutes, assigned_power_mw
        FROM '{PDBCE}'
        WHERE unit_code IN ({units_sql})
          AND assigned_power_mw IS NOT NULL
          AND CAST(date AS DATE) >= DATE '2018-01-01'
    """)
    pumps = con.sql("SELECT * FROM pumps").df()
    pumps["date"] = pd.to_datetime(pumps["date"])
    pumps["mwh"] = pumps["assigned_power_mw"] * np.where(pumps["mtu_minutes"] == 15, 0.25, 1.0)
    print(f"   pump+gen panel: {len(pumps):,} rows")

    pumps["side"] = np.where(pumps["unit_code"].isin(PUMP_UNITS), "pump", "gen")

    # Verify all pumping is offer_type=3 with negative MWh
    assert (pumps[pumps["side"] == "pump"]["mwh"] <= 0).all(), "Pump unit with positive MWh detected"
    assert (pumps[pumps["side"] == "gen"]["mwh"] >= 0).all(), "Gen unit with negative MWh detected"

    print("[3/5] Merge with hourly price + regime...")
    pumps = pumps.merge(px[["date", "period", "p_da"]], on=["date", "period"], how="inner")
    pumps["regime"] = pumps["date"].apply(assign_regime)
    print(f"   joined: {len(pumps):,} rows post-merge")

    print("[4/5] Pump-weighted vs Gen-weighted price by regime...")

    rows = []
    for regime, sub in pumps.groupby("regime"):
        # All IB units combined
        gen = sub[sub["side"] == "gen"]
        pump = sub[sub["side"] == "pump"]
        gen_mwh = gen["mwh"].sum()
        pump_mwh = -pump["mwh"].sum()  # consumption is negative; sign-flip to magnitude
        if pump_mwh == 0 or gen_mwh == 0:
            continue
        gen_revenue = (gen["p_da"] * gen["mwh"]).sum()
        pump_cost = (-pump["p_da"] * pump["mwh"]).sum()  # cost = price × |consumption|
        wavg_gen_price = gen_revenue / gen_mwh
        wavg_pump_price = pump_cost / pump_mwh
        spread = wavg_gen_price - wavg_pump_price
        gross_arb = gen_revenue - pump_cost  # gross trading profit (excluding O&M and round-trip loss)
        round_trip_eff = gen_mwh / pump_mwh
        # Net arbitrage assuming round-trip eff (gen ÷ pump):
        # Imagine you pay pump_cost, lose (1-eff) of energy, receive gen_revenue. The unit-by-unit accounting is
        # what's already shown in gross_arb. The eff captures whether the unit is also receiving free river inflow.
        rows.append({
            "regime": regime,
            "n_isps": len(sub),
            "gen_twh": gen_mwh / 1e6,
            "pump_twh": pump_mwh / 1e6,
            "round_trip_pct": round_trip_eff * 100,
            "wavg_gen_price": wavg_gen_price,
            "wavg_pump_price": wavg_pump_price,
            "spread_eur_mwh": spread,
            "gen_revenue_M": gen_revenue / 1e6,
            "pump_cost_M": pump_cost / 1e6,
            "gross_arbitrage_M": gross_arb / 1e6,
        })

    df = pd.DataFrame(rows).sort_values("regime")
    print()
    print("Combined IB pumped-storage portfolio (all 5 cascades):")
    print(df.to_string(index=False, float_format=lambda x: f"{x:.2f}"))

    print()
    print("[5/5] Pure-pump-storage isolation (MUEL/MUEB only — clean test):")
    rows2 = []
    for regime, sub in pumps.groupby("regime"):
        gen = sub[sub["unit_code"].isin(PURE_GEN)]
        pump = sub[sub["unit_code"].isin(PURE_PUMP)]
        gen_mwh = gen["mwh"].sum()
        pump_mwh = -pump["mwh"].sum()
        if pump_mwh == 0 or gen_mwh == 0:
            continue
        gen_revenue = (gen["p_da"] * gen["mwh"]).sum()
        pump_cost = (-pump["p_da"] * pump["mwh"]).sum()
        wavg_gen_price = gen_revenue / gen_mwh
        wavg_pump_price = pump_cost / pump_mwh
        spread = wavg_gen_price - wavg_pump_price
        gross_arb = gen_revenue - pump_cost
        round_trip_eff = gen_mwh / pump_mwh
        rows2.append({
            "regime": regime,
            "gen_twh": gen_mwh / 1e6,
            "pump_twh": pump_mwh / 1e6,
            "round_trip_pct": round_trip_eff * 100,
            "wavg_gen_price": wavg_gen_price,
            "wavg_pump_price": wavg_pump_price,
            "spread_eur_mwh": spread,
            "gross_arbitrage_M": gross_arb / 1e6,
        })

    df2 = pd.DataFrame(rows2).sort_values("regime")
    print(df2.to_string(index=False, float_format=lambda x: f"{x:.2f}"))

    # Same-period DA price spread (mid-day vs evening) for context — hour-mapped to handle DA15/ID15
    print()
    print("Reference: mid-day (h12-14) vs evening peak (h19-22) DA price spread by regime, hour-mapped:")
    px2 = px.copy()
    px2["hour"] = np.where(px2["mtu_minutes"] == 15, np.ceil(px2["period"] / 4.0).astype(int), px2["period"])
    px2["regime"] = px2["date"].apply(assign_regime)
    pmid = px2[px2["hour"].between(12, 14)]
    peve = px2[px2["hour"].between(19, 22)]
    sp = pd.DataFrame({
        "midday_avg": pmid.groupby("regime")["p_da"].mean(),
        "evening_avg": peve.groupby("regime")["p_da"].mean(),
    })
    sp["price_spread"] = sp["evening_avg"] - sp["midday_avg"]
    print(sp.round(2).to_string())

    OUT.parent.mkdir(parents=True, exist_ok=True)
    df["_table"] = "all_ib_pumped"
    df2["_table"] = "muel_pure"
    out = pd.concat([df, df2], ignore_index=True, sort=False)
    out.to_csv(OUT, index=False)
    print(f"\nwrote {OUT}")


if __name__ == "__main__":
    main()
