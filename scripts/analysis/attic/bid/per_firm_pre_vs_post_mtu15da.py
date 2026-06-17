# STATUS: ALIVE
# LAST-AUDIT: 2026-05-07
# FEEDS: B14 (per-firm CCGT decomposition), pre-vs-post comparison with prices
# CLAIM: Are firm-specific CCGT bid ceilings (IB ~350, GN 1000, HC 700, GE
# ~2350) pre-existing or did they emerge at the MTU15-DA reform?
#
# Compares Oct-Dec 2024 (MTU60, pre-MTU15-DA) vs Oct-Dec 2025 (MTU15,
# post-MTU15-DA). Same calendar months — controls for seasonality.
#
# Uses parser-fixed det_all.parquet (post-rebuild 2026-05-07). Pre-reform
# prices are now correctly populated.
#
# Dimension discipline: counts and quantiles only — no quantity sums across
# different MTU regimes (MW × period_length comparison would need explicit
# energy conversion).

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

WINDOWS = {
    "PRE_2024_MTU60": ("2024-10-01", "2025-01-01"),
    "POST_2025_MTU15": ("2025-10-01", "2026-01-01"),
}


def build_perperiod(con, start: str, end: str) -> pd.DataFrame:
    """Per-(unit, date, period) bid stats for CCGT sell-side, given window."""
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
        ),
        joined AS (
            SELECT df.*, cls.firm_class
            FROM det_filtered df
            JOIN ccgt_classification cls USING (unit_code)
        )
        SELECT date::DATE AS d, unit_code, firm_class, period, mtu_minutes,
               COUNT(*) AS n_tranches,
               MIN(price_eur_mwh) AS p_min,
               quantile_cont(price_eur_mwh, 0.5) AS p_med,
               MAX(price_eur_mwh) AS p_max,
               AVG(quantity_mw) AS mean_qty_mw,
               STRING_AGG(
                 ROUND(price_eur_mwh, 4)::VARCHAR || ':' || ROUND(quantity_mw, 4)::VARCHAR,
                 '|' ORDER BY segment_number
               ) AS bid_hash
        FROM joined
        GROUP BY 1,2,3,4,5
        """
    ).df()


def aggregate_to_hourly(perperiod: pd.DataFrame) -> pd.DataFrame:
    """Per-(unit, date, hour) aggregation. mech_strict applies only when
    multiple periods per hour exist (post-MTU15)."""
    perperiod = perperiod.copy()
    perperiod["hour"] = (
        (perperiod["period"] - 1) // (60 // perperiod["mtu_minutes"].clip(lower=1))
    ).astype(int)
    perperiod = perperiod.loc[perperiod["hour"].between(0, 23)]
    hourly = (
        perperiod.groupby(["d", "unit_code", "firm_class", "hour"])
        .agg(
            n_periods_in_hour=("period", "count"),
            n_unique_hashes=("bid_hash", "nunique"),
            n_tranches=("n_tranches", "mean"),
            p_min=("p_min", "mean"),
            p_med=("p_med", "mean"),
            p_max=("p_max", "mean"),
            mean_qty_mw=("mean_qty_mw", "mean"),
        )
        .reset_index()
    )
    # Mechanical-repeat is only meaningful when there are multiple periods per hour.
    hourly["mech_strict"] = (hourly["n_unique_hashes"] == 1).astype(int)
    return hourly


CRITICAL_PRICE_PEAK = (18, 19, 20, 21, 22)


def hour_class(h: int) -> str:
    if h in CRITICAL_PRICE_PEAK:
        return "critical_h18_22"
    if h in (3, 4, 5):
        return "flat_h3_5"
    return "other"


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
        print(f"\n=== Building {window_label}: {start} to {end} ===")
        perperiod = build_perperiod(con, start, end)
        print(f"  per-period rows: {len(perperiod):,}")
        hourly = aggregate_to_hourly(perperiod)
        hourly["window"] = window_label
        hourly["hour_class"] = hourly["hour"].map(hour_class)
        out.append(hourly)

    full = pd.concat(out, ignore_index=True)
    print(f"\nTotal hourly rows: {len(full):,}")

    # Headline 1: per-firm × window — mean p_max, p_med, n_tranches.
    print("\n--- Per-firm × window summary (24h pooled) ---")
    s1 = (
        full.groupby(["firm_class", "window"])
        .agg(
            n_obs=("d", "count"),
            n_units=("unit_code", "nunique"),
            mean_n_tranches=("n_tranches", "mean"),
            mean_p_min=("p_min", "mean"),
            mean_p_med=("p_med", "mean"),
            mean_p_max=("p_max", "mean"),
            mean_qty_mw=("mean_qty_mw", "mean"),
        )
        .reset_index()
    )
    print(s1.to_string(index=False))
    s1.to_csv(OUTDIR / "perfirm_pre_vs_post_mtu15da_pooled.csv", index=False)

    # Headline 2: per-firm × window × hour_class — disaggregated.
    print("\n--- Per-firm × hour_class × window ---")
    s2 = (
        full.groupby(["firm_class", "hour_class", "window"])
        .agg(
            n_obs=("d", "count"),
            mean_n_tranches=("n_tranches", "mean"),
            mean_p_min=("p_min", "mean"),
            mean_p_med=("p_med", "mean"),
            mean_p_max=("p_max", "mean"),
        )
        .reset_index()
    )
    print(s2.to_string(index=False))
    s2.to_csv(OUTDIR / "perfirm_pre_vs_post_by_hour_class.csv", index=False)

    # Pivot: p_max and n_tranches by firm × window, focused on critical hours.
    crit = s2[s2["hour_class"] == "critical_h18_22"].copy()
    print("\n--- p_max ceiling stability: critical hours, pre vs post ---")
    pivot_pmax = crit.pivot(index="firm_class", columns="window", values="mean_p_max").round(1)
    pivot_pmax["delta_pct"] = (
        (pivot_pmax["POST_2025_MTU15"] - pivot_pmax["PRE_2024_MTU60"])
        / pivot_pmax["PRE_2024_MTU60"] * 100
    ).round(1)
    print(pivot_pmax.to_string())
    pivot_pmax.to_csv(OUTDIR / "perfirm_pmax_critical_pre_vs_post.csv")

    print("\n--- n_tranches per period: critical hours, pre vs post ---")
    pivot_ntr = crit.pivot(index="firm_class", columns="window", values="mean_n_tranches").round(2)
    print(pivot_ntr.to_string())
    pivot_ntr.to_csv(OUTDIR / "perfirm_ntranches_critical_pre_vs_post.csv")

    # Pivot: p_med (median tranche price)
    print("\n--- p_med (median tranche price): critical hours, pre vs post ---")
    pivot_pmed = crit.pivot(index="firm_class", columns="window", values="mean_p_med").round(1)
    print(pivot_pmed.to_string())
    pivot_pmed.to_csv(OUTDIR / "perfirm_pmed_critical_pre_vs_post.csv")

    print(f"\nOutputs: {OUTDIR}")


if __name__ == "__main__":
    main()
