# STATUS: ALIVE
# LAST-AUDIT: 2026-04-27
# FEEDS: F7+F10 follow-up — IB transfer hour-of-day decomposition by technology
# CLAIM: Hydro carries IB's evening-peak rent; CCGT runs concentrated in evening but contributes less than hydro per hour.

"""IB transfer by hour-of-day, by technology.

Quick approximation: for each post-MTU15-IDA ISP, IB transfer is
mp_IB × q_IB. Allocate to tech by IB's cleared-MWh share of each tech
at that ISP. This is an approximate decomposition (joint substitution
≠ sum of unit substitutions), but informative for timing.

Output:
  results/regressions/ib_transfer_by_hour_tech.csv
"""
from __future__ import annotations

from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

PROJECT = Path(__file__).resolve().parents[3]
F7_ISP = PROJECT / "results" / "regressions" / "synthetic_firm_per_firm_isp.csv"
PDBCE = PROJECT / "data" / "processed" / "omie" / "mercado_diario" / "programas" / "pdbce_all.parquet"
REF = PROJECT / "data" / "external" / "omie_reference" / "lista_unidades.csv"
OUT = PROJECT / "results" / "regressions" / "ib_transfer_by_hour_tech.csv"


def main() -> None:
    print("[1/4] IB unit→tech mapping from lista_unidades...")
    ref = pd.read_csv(REF, encoding="latin1")
    ref["tech_low"] = ref["technology"].fillna("").astype(str).str.lower()
    def bucket(t: str) -> str:
        if "ciclo combinado" in t: return "CCGT"
        if "nuclear" in t: return "Nuclear"
        if "hidr" in t and "bombeo" not in t and "consumo" not in t: return "Hydro"
        return "Other"
    ref["tech"] = ref["tech_low"].apply(bucket)

    print("[2/4] IB cleared per-unit-ISP from pdbce, joined to F7 hourly mp_IB...")
    con = duckdb.connect()
    con.execute("SET memory_limit='2GB'")
    ib_q = con.sql(f"""
        SELECT date, period, unit_code,
               assigned_power_mw / CASE WHEN mtu_minutes = 15 THEN 4.0 ELSE 1.0 END AS q_mwh
        FROM '{PDBCE}'
        WHERE grupo_empresarial = 'IB' AND offer_type = 1
          AND assigned_power_mw IS NOT NULL AND assigned_power_mw > 0
          AND CAST(date AS DATE) >= DATE '2025-03-19'
    """).df()
    ib_q["date"] = pd.to_datetime(ib_q["date"])
    ib_q = ib_q.merge(ref[["unit_code", "tech"]], on="unit_code", how="left")
    ib_q["tech"] = ib_q["tech"].fillna("Other")

    iso = pd.read_csv(F7_ISP)
    iso["date"] = pd.to_datetime(iso["date"])
    iso = iso[(iso["regime"].isin(["DA60/ID15", "DA15/ID15"])) & (iso["p_actual"] > 0)].copy()
    iso["hour_of_day"] = np.where(iso["regime"] == "DA60/ID15",
                                   iso["period"],
                                   np.ceil(iso["period"] / 4.0).astype(int))

    # Sum IB cleared by tech per ISP
    ib_tech = ib_q.groupby(["date", "period", "tech"], as_index=False)["q_mwh"].sum()
    ib_total = ib_q.groupby(["date", "period"], as_index=False)["q_mwh"].sum().rename(columns={"q_mwh": "q_total"})
    ib_tech = ib_tech.merge(ib_total, on=["date", "period"])
    ib_tech["share"] = ib_tech["q_mwh"] / ib_tech["q_total"]

    # Join to mp_IB
    panel = ib_tech.merge(iso[["date", "period", "mp_IB", "hour_of_day"]], on=["date", "period"], how="inner")
    # Tech-attributed transfer at each ISP (note: approximation — tech-share allocation, not unit-substitution rerun)
    panel["transfer_eur"] = panel["mp_IB"] * panel["q_mwh"]

    print(f"   panel: {len(panel):,} rows ({panel.tech.nunique()} techs); ISPs covered: {panel[['date','period']].drop_duplicates().shape[0]:,}")

    print()
    print("[3/4] Hour-of-day decomposition (sum transfer in M€):")
    by_hour_tech = panel.groupby(["hour_of_day", "tech"])["transfer_eur"].sum().unstack(fill_value=0) / 1e6
    by_hour_tech["Total"] = by_hour_tech.sum(axis=1)
    print(by_hour_tech.round(2).to_string())

    print()
    print("[4/4] Tech share of total transfer by hour bucket:")
    total = panel["transfer_eur"].sum() / 1e6
    print(f"Total panel transfer: €{total:.1f}M")
    print()
    panel["bucket"] = pd.cut(panel["hour_of_day"], bins=[0, 6, 10, 16, 22, 25],
                             labels=["1.night (h1-6)", "2.morning (h7-10)", "3.midday (h11-16)", "4.evening (h17-22)", "5.late (h23-24)"])
    summary = panel.groupby(["bucket", "tech"], observed=True)["transfer_eur"].sum().unstack(fill_value=0) / 1e6
    summary["Total"] = summary.sum(axis=1)
    print("Transfer by bucket × tech (M€):")
    print(summary.round(2).to_string())
    print()
    pct = summary.div(summary["Total"], axis=0) * 100
    pct = pct.drop(columns="Total")
    print("Tech share within each bucket (%):")
    print(pct.round(1).to_string())

    OUT.parent.mkdir(parents=True, exist_ok=True)
    out = by_hour_tech.reset_index()
    out["_table"] = "by_hour_of_day"
    summary_out = summary.reset_index()
    summary_out["_table"] = "by_bucket"
    pd.concat([out, summary_out], ignore_index=True, sort=False).to_csv(OUT, index=False)
    print(f"\nwrote {OUT}")


if __name__ == "__main__":
    main()
