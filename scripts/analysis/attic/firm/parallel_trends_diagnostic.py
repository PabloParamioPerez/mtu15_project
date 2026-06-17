# STATUS: ALIVE
# LAST-AUDIT: 2026-05-08
# FEEDS: identification (parallel-trends visual diagnostic)
# CLAIM: Visualize the (critical_h18_22 − flat_h3_5) differential in DA cleared
# MWh by firm × month, across the MTU15-DA reform. Tests whether parallel-trends
# is plausible BEFORE any DiD specification.
#
# Outcome: DA-cleared MWh per firm × month × hour_class, normalized by
# energy-equivalent-per-hour to be comparable across MTU60 (1h periods) and
# MTU15 (0.25h periods).
#
# This is a DIAGNOSTIC — no DiD coefficients, just visual inspection.

from __future__ import annotations

from pathlib import Path
import sys

import duckdb
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO / "src"))
from mtu.classification.units import firm_unit_panel  # noqa: E402

OUTDIR = REPO / "results" / "regressions" / "firm" / "parallel_trends"
OUTDIR.mkdir(parents=True, exist_ok=True)
FIGDIR = REPO / "figures" / "working"
FIGDIR.mkdir(parents=True, exist_ok=True)

PDBCE = REPO / "data" / "processed" / "omie" / "mercado_diario" / "programas" / "pdbce_all.parquet"
UNITS_CSV = REPO / "data" / "external" / "omie_reference" / "lista_unidades.csv"

# Window: Jan 2024 to Dec 2025 (covers pre-MTU15-DA and post-MTU15-DA
# with overlap of multiple seasons).
START = "2024-01-01"
END = "2026-01-01"

CRITICAL_PRICE_PEAK = (18, 19, 20, 21, 22)
FLAT_HOURS = (3, 4, 5)

# Pivotality-based partition (per _pivotality_by_firm_critical_hours.md).
# "treatment_set" = expected to react; "placebo_set" = empirically non-pivotal.
PARENT_GROUPS = {
    "IB": "treatment",
    "GE": "treatment",
    "GN": "treatment",
    "HC": "treatment",
    "EDP-PT": "treatment",
    "Other-fringe-CCGT": "treatment_weak",
    "Repsol": "placebo",
    "Engie": "placebo",
    "TotalEnergies": "placebo",
    "Moeve": "placebo",
}


def main() -> None:
    # Centralized firm classification (see _firm_classification_audit.md).
    units = firm_unit_panel(csv_path=str(UNITS_CSV), scheme="short",
                              mode="primary_owner")
    # Restrict to Spanish-zone CCGT (the strategically-relevant subset)
    ccgt_es = units[(units["tech_group"] == "CCGT") & (units["zone"] == "ZONA ESPAÑOLA")][
        ["unit_code", "parent"]
    ]
    # Add EDP-PT (Portuguese zone) explicitly
    edp_pt = units[(units["tech_group"] == "CCGT") & (units["zone"] == "ZONA PORTUGUESA") &
                   (units["parent"] == "EDP-PT")][["unit_code", "parent"]]
    ccgt_pool = pd.concat([ccgt_es, edp_pt], ignore_index=True)
    print(f"CCGT pool ({len(ccgt_pool)} units):")
    print(ccgt_pool["parent"].value_counts().to_string())

    con = duckdb.connect()
    con.execute("PRAGMA threads = 6")
    con.register("ccgt_pool", ccgt_pool)

    crit_str = ",".join(map(str, CRITICAL_PRICE_PEAK))
    flat_str = ",".join(map(str, FLAT_HOURS))

    print("\n--- Building monthly DA cleared MWh by (firm, hour_class) ---")
    panel = con.execute(
        f"""
        WITH p AS (
            SELECT date::DATE AS d, period, mtu_minutes, unit_code, assigned_power_mw,
                   CASE WHEN mtu_minutes = 60 THEN period - 1
                        WHEN mtu_minutes = 15 THEN (period - 1) // 4
                        ELSE NULL END AS hour
            FROM '{PDBCE}'
            WHERE date::DATE >= DATE '{START}' AND date::DATE < DATE '{END}'
              AND assigned_power_mw > 0
        ),
        joined AS (
            SELECT p.*, c.parent
            FROM p JOIN ccgt_pool c USING (unit_code)
        )
        SELECT EXTRACT(YEAR FROM d) AS y,
               EXTRACT(MONTH FROM d) AS m,
               parent,
               CASE WHEN hour IN ({crit_str}) THEN 'critical_h18_22'
                    WHEN hour IN ({flat_str}) THEN 'flat_h3_5'
                    ELSE 'other' END AS hour_class,
               -- ENERGY in MWh: MW × period_length_hours
               SUM(assigned_power_mw * mtu_minutes / 60.0) AS energy_mwh,
               COUNT(DISTINCT d) AS n_days,
               -- normalize: energy per clock-hour-equivalent
               -- (n_clock_hours_in_class = days × hours_in_class)
               -- For comparison across regimes, we want MWh/hour of clock time
               COUNT(*) AS n_period_obs
        FROM joined
        WHERE hour IS NOT NULL
        GROUP BY 1,2,3,4
        """
    ).df()
    panel["year_month"] = pd.to_datetime(
        dict(year=panel["y"].astype(int), month=panel["m"].astype(int), day=1)
    )
    print(f"panel rows: {len(panel)}")

    # Critical is 5 hours/day, Flat is 3 hours/day. Normalize to MWh per
    # clock-hour-of-class-per-day so they're directly comparable.
    HOURS_IN_CLASS = {"critical_h18_22": 5, "flat_h3_5": 3, "other": 16}
    panel["hours_per_day"] = panel["hour_class"].map(HOURS_IN_CLASS)
    panel["mwh_per_clock_hour_per_day"] = panel["energy_mwh"] / (panel["n_days"] * panel["hours_per_day"])

    # Pivot for the (critical - flat) differential by firm × month
    pivot = (
        panel.pivot_table(
            index=["parent", "year_month"],
            columns="hour_class",
            values="mwh_per_clock_hour_per_day",
            aggfunc="sum",
        )
        .reset_index()
    )
    pivot["crit_minus_flat"] = pivot["critical_h18_22"] - pivot["flat_h3_5"]
    pivot.to_csv(OUTDIR / "monthly_critical_minus_flat_DA_cleared.csv", index=False)
    print(f"\nSaved pivot: {OUTDIR / 'monthly_critical_minus_flat_DA_cleared.csv'}")

    # Show by firm
    for parent in ["IB", "GE", "GN", "HC", "EDP-PT", "Repsol", "Engie", "TotalEnergies", "Moeve", "Other-fringe-CCGT"]:
        sub = pivot[pivot["parent"] == parent].sort_values("year_month")
        if len(sub) == 0:
            continue
        print(f"\n--- {parent} (treatment_role={PARENT_GROUPS.get(parent, '?')}) ---")
        print(sub[["year_month", "critical_h18_22", "flat_h3_5", "crit_minus_flat"]].to_string(index=False))

    # Plot parallel trends
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt

        fig, axes = plt.subplots(2, 5, figsize=(18, 7), sharex=True)
        focus = ["IB", "GE", "GN", "HC", "EDP-PT",
                "Other-fringe-CCGT", "Repsol", "Engie", "TotalEnergies", "Moeve"]
        for ax, parent in zip(axes.flat, focus):
            sub = pivot[pivot["parent"] == parent].sort_values("year_month")
            if len(sub) == 0:
                ax.set_title(f"{parent}: no data")
                continue
            ax.plot(sub["year_month"], sub["crit_minus_flat"], marker="o", linewidth=1.5)
            ax.axvline(pd.Timestamp("2025-10-01"), color="red", linestyle="--", linewidth=0.8, label="MTU15-DA")
            ax.axvline(pd.Timestamp("2025-03-19"), color="orange", linestyle=":", linewidth=0.6, label="MTU15-IDA")
            ax.set_title(f"{parent}\n({PARENT_GROUPS.get(parent, '?')})")
            ax.set_xlabel("month")
            ax.set_ylabel("crit−flat MWh/clock-h")
            ax.tick_params(axis="x", rotation=45, labelsize=7)
            ax.grid(alpha=0.3)
            if parent == "IB":
                ax.legend(fontsize=7, loc="upper left")
        fig.suptitle("Parallel-trends diagnostic: (critical h{18-22} − flat h{3-5}) DA cleared MWh/clock-hour\nCCGT only, by parent firm", fontsize=11)
        fig.tight_layout()
        out_png = FIGDIR / "parallel_trends_DA_cleared_per_firm.png"
        out_pdf = FIGDIR / "parallel_trends_DA_cleared_per_firm.pdf"
        fig.savefig(out_png, dpi=110, bbox_inches="tight")
        fig.savefig(out_pdf, bbox_inches="tight")
        plt.close(fig)
        print(f"\nFigure: {out_png}")
        print(f"Figure: {out_pdf}")
    except Exception as e:
        print(f"Plot failed: {e}")


if __name__ == "__main__":
    main()
