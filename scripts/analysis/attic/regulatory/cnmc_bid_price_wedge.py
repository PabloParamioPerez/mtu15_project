# STATUS: ALIVE
# LAST-AUDIT: 2026-04-28
# FEEDS: F21 (extends to bid-price wedge — strong CNMC SBO3 replication)
# CLAIM: Per-plant DA bid price by zone-pivotality situation; restriction-system price wedge
"""Bid-price-wedge replication of CNMC SBO3 (2023) framework.

Extends F21 (volume-side pivotality test) to the BID-PRICE side. For each
Big-4 CCGT plant, compute the volume-weighted mean DA bid price per hour
in each pivotality situation (Sit 1 competitive, Sit 2 sole pivot, Sit 3
joint pivot). If the same plant bids systematically HIGHER in Sit 2 hours
than Sit 1 hours, that is the within-firm SBO3 pricing pattern.

Also compute the restriction-system price per hour (from
totalrp48preccierre system aggregate) and the implied per-plant per-MWh
wedge = restriction_system_price − own DA bid price.

Data sources (all post-2025-03-19, the period for which det_all carries
non-zero bid prices per memory note `det_pre_reform_prices`):
  - data/processed/omie/mercado_diario/ofertas/det_all.parquet  (bid tranches)
  - data/processed/omie/mercado_diario/ofertas/cab_all.parquet  (offer headers - unit_code)
  - data/processed/omie/mercado_diario/programas/pdbc_all.parquet  (DA cleared)
  - data/processed/entsoe/generation/ccgt_per_firm_panel.parquet  (A73 actual + firm map)
  - data/processed/esios/restricciones/totalrp48preccierre_all.parquet (system restriction prices)

Output: results/regressions/cnmc_bid_price_wedge.csv
"""
from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd

PROJECT = Path(__file__).resolve().parents[3]

# Same plant-zone map as F21
PLANT_ZONE = {
    "SBO3": "Galicia", "PGR5": "Galicia", "MEI1": "Galicia",
    "BES3": "Cataluña", "BES4": "Cataluña", "BES5": "Cataluña",
    "PVENT1": "Cataluña", "PVENT2": "Cataluña",
    "CTGN1": "Murcia", "CTGN2": "Murcia", "CTGN3": "Murcia", "ESC6": "Murcia",
    "ESCCC1": "Murcia", "ESCCC2": "Murcia", "ESCCC3": "Murcia",
    "ARCOS1": "Andalucía", "ARCOS2": "Andalucía", "ARCOS3": "Andalucía",
    "PALOS1": "Andalucía", "PALOS2": "Andalucía", "PALOS3": "Andalucía",
    "MALA1": "Andalucía", "ALG3": "Andalucía", "COL4": "Andalucía",
    "CAMGI10": "Andalucía", "CAMG20R": "Andalucía",
    "SROQ1": "Andalucía", "SROQ2": "Andalucía",
    "CTN3": "Valencia", "CTN4": "Valencia",
    "CTJON1R": "Valencia", "CTJON2": "Valencia", "CTJON3R": "Valencia",
    "SAGU1": "Valencia", "SAGU2": "Valencia", "SAGU3": "Valencia",
    "ECT3": "Aragón", "CTNU": "Aragón",
    "ARRU1R": "Aragón", "ARRU2R": "Aragón",
    "SRI4R": "Asturias", "SRI5R": "Asturias",
    "STC4": "Vasconia", "BAHIAB": "Vasconia",
    "ACE3": "CLM", "ACE4": "CLM", "TAPOWER": "CLM",
    "AMBIETA": "Vasconia",
}


def main() -> None:
    proc = PROJECT / "data/processed"
    con = duckdb.connect()

    # 1. Per-plant per-hour volume-weighted DA bid price (post-2025-03-19, sell offers)
    print("Loading det_all + cab_all (sell offers post-2025-03-19)...")
    bids = con.execute(f"""
    SELECT cab.unit_code, det.date, det.period,
           SUM(det.price_eur_mwh * det.quantity_mw) /
             NULLIF(SUM(det.quantity_mw), 0)        AS qty_wmean_da_bid,
           MAX(det.price_eur_mwh)                   AS max_da_bid,
           SUM(det.quantity_mw)                     AS total_offered_mw
    FROM '{proc}/omie/mercado_diario/ofertas/det_all.parquet' det
    JOIN '{proc}/omie/mercado_diario/ofertas/cab_all.parquet' cab
      ON cab.offer_code = det.offer_code AND cab.date = det.date
    WHERE det.date >= '2025-03-19'
      AND cab.buy_sell = 'V'
    GROUP BY cab.unit_code, det.date, det.period
    """).df()
    bids["unit_code"] = bids["unit_code"].astype(str)
    print(f"  {len(bids):,} plant-hour DA bids")

    # 2. Per-plant DA cleared MW
    pdbc = con.execute(f"""
    SELECT date, unit_code, period, assigned_power_mw AS pdbc_mw
    FROM '{proc}/omie/mercado_diario/programas/pdbc_all.parquet'
    WHERE date >= '2025-03-19' AND offer_type = 1
    """).df()
    pdbc["unit_code"] = pdbc["unit_code"].astype(str)

    # 3. A73 actual generation (15-min resolution) → hourly mean per plant
    print("Loading A73 hourly (post-2025-03-19)...")
    a73 = pd.read_parquet(proc / "entsoe/generation/ccgt_per_firm_panel.parquet")
    a73["ts"] = pd.to_datetime(a73["isp_start_utc"])
    a73 = a73[a73.ts >= "2025-03-19"]
    a73["hour_start"] = a73["ts"].dt.floor("h")
    a73["zone"] = a73["omie_code"].map(PLANT_ZONE).fillna("Other")
    a73h = a73.groupby(["hour_start", "omie_code", "firm", "zone"]).agg(
        a73_mw=("quantity_mw", "mean"),
    ).reset_index()
    a73h["date"] = a73h["hour_start"].dt.date.astype(str)
    a73h["period"] = a73h["hour_start"].dt.hour + 1
    print(f"  A73 hourly: {len(a73h):,} rows")

    # 4. Three-situation classification (same logic as F21)
    a73h["available"] = (a73h["a73_mw"] > 1).astype(int)
    zone_avail = a73h.groupby(["hour_start", "zone"])["available"].sum().reset_index()
    zone_avail.columns = ["hour_start", "zone", "n_available_in_zone"]
    a73h = a73h.merge(zone_avail, on=["hour_start", "zone"], how="left")
    zone_size = {z: sum(1 for k, v in PLANT_ZONE.items() if v == z) for z in set(PLANT_ZONE.values())}
    a73h["zone_size"] = a73h["zone"].map(zone_size).fillna(1)

    def classify(row):
        if row["available"] == 0:
            return "Off"
        n_avail = row["n_available_in_zone"]
        n_total = row["zone_size"]
        if n_total <= 1:
            return "Sit_solo"
        if n_avail == 1:
            return "Sit2_sole_pivot"
        if n_avail >= max(2, int(0.5 * n_total)):
            return "Sit1_competitive"
        return "Sit3_joint_pivot"
    a73h["situation"] = a73h.apply(classify, axis=1)

    # 5. Join everything
    mg = a73h.merge(bids, left_on=["date", "omie_code", "period"],
                    right_on=["date", "unit_code", "period"], how="left")
    mg = mg.merge(pdbc, left_on=["date", "omie_code", "period"],
                  right_on=["date", "unit_code", "period"], how="left",
                  suffixes=("", "_pdbc"))
    mg["pdbc_mw"] = mg["pdbc_mw"].fillna(0)
    mg["restriction_redisp_mw"] = mg["a73_mw"] - mg["pdbc_mw"]

    # 6. System restriction price per hour (from totalrp48preccierre, mean of price_up across non-null)
    print("Loading totalrp48preccierre (system restriction price up)...")
    rp = con.execute(f"""
    SELECT date_trunc('hour', period_start_utc) AS hour_start_utc,
           AVG(price_up_eur)   AS rt_price_up_eur,
           AVG(price_down_eur) AS rt_price_down_eur,
           SUM(qty_up_mwh)     AS rt_qty_up_mwh,
           SUM(qty_down_mwh)   AS rt_qty_down_mwh
    FROM '{proc}/esios/restricciones/totalrp48preccierre_all.parquet'
    WHERE period_start_utc >= '2025-03-19'
    GROUP BY 1
    """).df()
    rp["hour_start_utc"] = pd.to_datetime(rp["hour_start_utc"]).dt.tz_localize(None)
    print(f"  RT price hourly rows: {len(rp):,}")

    mg["hour_start_utc"] = mg["hour_start"].dt.tz_localize(None) if mg["hour_start"].dt.tz else mg["hour_start"]
    mg = mg.merge(rp, on="hour_start_utc", how="left")
    mg["bid_to_rt_wedge"] = mg["rt_price_up_eur"] - mg["qty_wmean_da_bid"]

    # 7. Aggregate per (firm, zone, plant, situation)
    big4 = mg[mg.firm.isin(["IB", "GE", "GN", "HC"])].copy()
    summary = big4.groupby(["zone", "firm", "omie_code", "situation"]).agg(
        n_hours=("hour_start", "count"),
        mean_da_bid=("qty_wmean_da_bid", "mean"),
        mean_max_da_bid=("max_da_bid", "mean"),
        mean_pdbc_mw=("pdbc_mw", "mean"),
        mean_a73_mw=("a73_mw", "mean"),
        mean_redisp_mw=("restriction_redisp_mw", "mean"),
        mean_rt_price_up=("rt_price_up_eur", "mean"),
        mean_bid_to_rt_wedge=("bid_to_rt_wedge", "mean"),
    ).round(2).reset_index()

    summary = summary[summary["n_hours"] > 50]
    summary = summary.sort_values(["zone", "firm", "omie_code", "situation"])

    # 8. Print zone-by-zone
    print()
    print("=" * 88)
    print("Per-plant DA bid price + restriction-system price wedge by situation (post-2025-03-19)")
    print("Hypothesis (CNMC SBO3): mean_da_bid in Sit2 > mean_da_bid in Sit1 = pivotal pricing")
    print("=" * 88)
    cols = ["firm", "omie_code", "situation", "n_hours", "mean_da_bid",
            "mean_max_da_bid", "mean_redisp_mw", "mean_rt_price_up", "mean_bid_to_rt_wedge"]
    for zone in sorted(summary["zone"].unique()):
        zd = summary[summary.zone == zone]
        if len(zd) == 0:
            continue
        print(f"\n--- Zone: {zone} ---")
        print(zd[cols].to_string(index=False))

    # 9. Cross-plant: which plants raise their DA bid in Sit2 vs Sit1?
    print()
    print("=" * 88)
    print("Plants where Sit2 DA bid > Sit1 DA bid (within-firm SBO3 signature)")
    print("=" * 88)
    rows = []
    for (zone, firm, plant), g in summary.groupby(["zone", "firm", "omie_code"]):
        s1 = g[g.situation == "Sit1_competitive"]
        s2 = g[g.situation == "Sit2_sole_pivot"]
        if len(s1) and len(s2):
            b1 = s1["mean_da_bid"].iloc[0]
            b2 = s2["mean_da_bid"].iloc[0]
            r1 = s1["mean_redisp_mw"].iloc[0]
            r2 = s2["mean_redisp_mw"].iloc[0]
            n1 = s1["n_hours"].iloc[0]
            n2 = s2["n_hours"].iloc[0]
            rows.append({
                "zone": zone, "firm": firm, "plant": plant,
                "n_sit1": n1, "n_sit2": n2,
                "DA_bid_sit1_eur_mwh": b1, "DA_bid_sit2_eur_mwh": b2,
                "delta_DA_bid_eur_mwh": round(b2 - b1, 2) if pd.notna(b1) and pd.notna(b2) else None,
                "delta_DA_bid_pct": round(100 * (b2 / b1 - 1), 1) if pd.notna(b1) and b1 > 0 and pd.notna(b2) else None,
                "redisp_sit1_MW": r1, "redisp_sit2_MW": r2,
            })
    cmp = pd.DataFrame(rows).sort_values("delta_DA_bid_eur_mwh", ascending=False, na_position="last")
    print(cmp.to_string(index=False))

    out = PROJECT / "results/regressions/cnmc_bid_price_wedge.csv"
    out.parent.mkdir(parents=True, exist_ok=True)
    summary.to_csv(out, index=False)
    cmp.to_csv(out.with_name("cnmc_bid_price_wedge_summary.csv"), index=False)
    print(f"\nwrote {out}")
    print(f"wrote {out.with_name('cnmc_bid_price_wedge_summary.csv')}")


if __name__ == "__main__":
    main()
