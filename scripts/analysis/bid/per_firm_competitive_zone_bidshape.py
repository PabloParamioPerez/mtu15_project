# STATUS: ALIVE
# LAST-AUDIT: 2026-05-07
# FEEDS: B14 refinement (competitive-zone bidding)
# CLAIM: Re-do per-firm CCGT bid-shape excluding tranches priced above the
# observed clearing-price tail. Firm-specific "ceilings" (GN 1000, GE 2350,
# HC 700, IB 350) are reputational, not binding — clearing maxed at 240 €/MWh
# in Oct-Dec 2025 and 193 in Oct-Dec 2024. So tranches above ~250 are
# functionally unavailable supply, not competitive bidding.
#
# Restrict to tranches with price_eur_mwh ≤ 250 €/MWh (covers all clearing
# in both windows). Recompute n_tranches, p_min, p_med, p_max per firm × hour.

from __future__ import annotations

from pathlib import Path
import sys

import duckdb
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO / "src"))
from mtu.classification.units import firm_unit_panel  # noqa: E402

OUTDIR = REPO / "results" / "regressions" / "bid"
OUTDIR.mkdir(parents=True, exist_ok=True)

DET = REPO / "data" / "processed" / "omie" / "mercado_diario" / "ofertas" / "det_all.parquet"
CAB = REPO / "data" / "processed" / "omie" / "mercado_diario" / "ofertas" / "cab_all.parquet"
UNITS_CSV = REPO / "data" / "external" / "omie_reference" / "lista_unidades.csv"

# Competitive-zone cutoff: above this, tranches are effectively reputational/
# unavailable supply rather than competitive bids. Choice: 250 €/MWh covers
# all observed clearing in Oct-Dec 2024 (max 193) and Oct-Dec 2025 (max 240).
COMP_ZONE_CUTOFF = 250.0

WINDOWS = {
    "PRE_2024_MTU60": ("2024-10-01", "2025-01-01"),
    "POST_2025_MTU15": ("2025-10-01", "2026-01-01"),
}

CRITICAL_PRICE_PEAK = (18, 19, 20, 21, 22)


def hour_class(h: int) -> str:
    if h in CRITICAL_PRICE_PEAK:
        return "critical_h18_22"
    if h in (3, 4, 5):
        return "flat_h3_5"
    return "other"


def build_perperiod(con, start: str, end: str, cutoff: float) -> pd.DataFrame:
    return con.execute(
        f"""
        WITH cab_sell AS (
            SELECT date, offer_code, version, unit_code
            FROM '{CAB}'
            WHERE buy_sell = 'V'
              AND date::DATE >= DATE '{start}' AND date::DATE < DATE '{end}'
        ),
        det_filtered AS (
            SELECT d.date, d.offer_code, d.version, d.period, d.mtu_minutes,
                   d.segment_number, d.price_eur_mwh, d.quantity_mw,
                   c.unit_code
            FROM '{DET}' d
            JOIN cab_sell c
              ON c.date = d.date AND c.offer_code = d.offer_code AND c.version = d.version
            WHERE d.date::DATE >= DATE '{start}' AND d.date::DATE < DATE '{end}'
              AND d.quantity_mw > 0
              AND d.price_eur_mwh IS NOT NULL
              AND d.price_eur_mwh <= {cutoff}    -- competitive zone
        ),
        joined AS (
            SELECT df.*, cls.firm_class
            FROM det_filtered df
            JOIN ccgt_classification cls USING (unit_code)
        )
        SELECT date::DATE AS d, unit_code, firm_class, period, mtu_minutes,
               COUNT(*) AS n_tranches_comp,
               MIN(price_eur_mwh) AS p_min,
               quantile_cont(price_eur_mwh, 0.5) AS p_med,
               MAX(price_eur_mwh) AS p_max,
               -- offered MW in competitive zone
               SUM(quantity_mw) AS offered_mw_comp
        FROM joined
        GROUP BY 1,2,3,4,5
        """
    ).df()


def main() -> None:
    # Centralized firm classification (see _firm_classification_audit.md).
    units = firm_unit_panel(csv_path=str(UNITS_CSV), scheme="short",
                              mode="primary_owner").rename(columns={"parent": "firm_class"})
    ccgt = units.loc[units["tech_group"] == "CCGT", ["unit_code", "firm_class"]]

    con = duckdb.connect()
    con.execute("PRAGMA threads = 6")
    con.register("ccgt_classification", ccgt)

    out = []
    for window_label, (start, end) in WINDOWS.items():
        print(f"\n=== {window_label}: {start} to {end} (price ≤ {COMP_ZONE_CUTOFF}) ===")
        pp = build_perperiod(con, start, end, COMP_ZONE_CUTOFF)
        pp["window"] = window_label
        print(f"  per-period rows: {len(pp):,}")
        out.append(pp)

    perperiod = pd.concat(out, ignore_index=True)
    perperiod["hour"] = (
        (perperiod["period"] - 1) // (60 // perperiod["mtu_minutes"].clip(lower=1))
    ).astype(int)
    perperiod = perperiod.loc[perperiod["hour"].between(0, 23)].copy()
    perperiod["hour_class"] = perperiod["hour"].map(hour_class)

    # Per firm × window × hour_class
    summary = (
        perperiod.groupby(["firm_class", "window", "hour_class"])
        .agg(
            n_obs=("d", "count"),
            mean_n_tranches_comp=("n_tranches_comp", "mean"),
            mean_p_min=("p_min", "mean"),
            mean_p_med=("p_med", "mean"),
            mean_p_max=("p_max", "mean"),
            mean_offered_mw_comp=("offered_mw_comp", "mean"),
        )
        .reset_index()
    )
    print("\n--- Per-firm × hour_class × window (competitive zone, ≤250 €/MWh) ---")
    print(summary.to_string(index=False))
    summary.to_csv(OUTDIR / "perfirm_competitive_zone_pre_vs_post.csv", index=False)

    # Pivots focused on critical hours
    crit = summary[summary["hour_class"] == "critical_h18_22"].copy()
    print("\n--- Critical-hour competitive-zone n_tranches: pre vs post ---")
    p1 = crit.pivot(index="firm_class", columns="window", values="mean_n_tranches_comp").round(2)
    print(p1.to_string())

    print("\n--- Critical-hour competitive-zone p_max (top of bid in clearing-relevant zone) ---")
    p2 = crit.pivot(index="firm_class", columns="window", values="mean_p_max").round(1)
    p2["delta"] = (p2["POST_2025_MTU15"] - p2["PRE_2024_MTU60"]).round(1)
    print(p2.to_string())

    print("\n--- Critical-hour competitive-zone p_med ---")
    p3 = crit.pivot(index="firm_class", columns="window", values="mean_p_med").round(1)
    p3["delta"] = (p3["POST_2025_MTU15"] - p3["PRE_2024_MTU60"]).round(1)
    print(p3.to_string())

    print("\n--- Critical-hour competitive-zone offered MW per period ---")
    p4 = crit.pivot(index="firm_class", columns="window", values="mean_offered_mw_comp").round(1)
    p4["delta"] = (p4["POST_2025_MTU15"] - p4["PRE_2024_MTU60"]).round(1)
    print(p4.to_string())
    p4.to_csv(OUTDIR / "perfirm_competitive_zone_offered_mw_pre_vs_post.csv")

    # Bonus: what fraction of tranches sit IN the competitive zone vs above it?
    # Need to compute total tranches (with no filter) and divide.
    print("\n--- Computing competitive-zone share for each firm ---")
    for window_label, (start, end) in WINDOWS.items():
        total = con.execute(
            f"""
            SELECT cls.firm_class,
                   COUNT(*) AS n_tranches_total,
                   SUM(CASE WHEN d.price_eur_mwh <= {COMP_ZONE_CUTOFF} THEN 1 ELSE 0 END) AS n_in_comp_zone,
                   SUM(d.quantity_mw) AS qty_total,
                   SUM(CASE WHEN d.price_eur_mwh <= {COMP_ZONE_CUTOFF} THEN d.quantity_mw ELSE 0 END) AS qty_in_comp_zone
            FROM '{DET}' d
            JOIN '{CAB}' c
              ON c.date = d.date AND c.offer_code = d.offer_code AND c.version = d.version
            JOIN ccgt_classification cls
              ON cls.unit_code = c.unit_code
            WHERE c.buy_sell = 'V'
              AND d.date::DATE >= DATE '{start}' AND d.date::DATE < DATE '{end}'
              AND d.quantity_mw > 0
              AND d.price_eur_mwh IS NOT NULL
            GROUP BY 1
            """
        ).df()
        total["pct_tranches_in_comp"] = (total["n_in_comp_zone"] / total["n_tranches_total"] * 100).round(1)
        total["pct_qty_in_comp"] = (total["qty_in_comp_zone"] / total["qty_total"] * 100).round(1)
        total["window"] = window_label
        print(f"\n{window_label}:")
        print(total[["firm_class", "n_tranches_total", "pct_tranches_in_comp",
                     "qty_total", "pct_qty_in_comp"]].to_string(index=False))
        total.to_csv(OUTDIR / f"perfirm_comp_zone_share_{window_label}.csv", index=False)


if __name__ == "__main__":
    main()
