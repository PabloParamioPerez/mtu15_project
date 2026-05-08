# STATUS: ALIVE
# LAST-AUDIT: 2026-05-08
# FEEDS: identification (testing whether ID15 or DA15 was the binding reform)
# CLAIM: q_2 (Ito-Reguant net IDA upward adjustment) trajectory by month
# 2024-01 to 2025-12, by CCGT firm × hour-class. Tests whether the
# structural break is at March 2025 (MTU15-IDA) or October 2025 (MTU15-DA).
#
# Mechanism: dominant firms strategically WITHHOLD in DA and adjust
# UPWARDS in IDA. q_2 = sum(pibci across sessions) per (firm, hour, day).
# Positive q_2 = firm sold more in IDA than DA committed.
#
# Window: 2024-01-01 to 2026-01-01.

from __future__ import annotations

from pathlib import Path
import sys

import duckdb
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO / "src"))
from mtu.classification.units import classify_units  # noqa: E402

OUTDIR = REPO / "results" / "regressions" / "firm" / "parallel_trends"
FIGDIR = REPO / "figures" / "working"
FIGDIR.mkdir(parents=True, exist_ok=True)
OUTDIR.mkdir(parents=True, exist_ok=True)

PIBCI = REPO / "data" / "processed" / "omie" / "mercado_intradiario_subastas" / "programas" / "pibci_all.parquet"
UNITS_CSV = REPO / "data" / "external" / "omie_reference" / "lista_unidades.csv"

START = "2024-01-01"
END = "2026-01-01"
CRITICAL_HOURS = (18, 19, 20, 21, 22)
FLAT_HOURS = (3, 4, 5)


def parent_of(owner: str | None) -> str:
    if not isinstance(owner, str):
        return "Other"
    o = owner.upper()
    if "IBERDROLA" in o: return "IB"
    if "ENDESA" in o: return "GE"
    if "NATURGY" in o or "GAS NATURAL" in o: return "GN"
    if "EDP ESPAÑA" in o: return "HC"
    if "EDP GEM PORTUGAL" in o: return "EDP-PT"
    if "ENGIE" in o: return "Engie"
    if "REPSOL" in o: return "Repsol"
    if "TOTALENERGIES" in o: return "TotalEnergies"
    if "MOEVE" in o or "CEPSA" in o: return "Moeve"
    return "Other"


def main() -> None:
    units = classify_units(
        csv_path=str(UNITS_CSV),
        keep_columns=["unit_code", "owner_agent", "tech_group", "zone"],
    )
    units["parent"] = units["owner_agent"].apply(parent_of)
    ccgt = units[units["tech_group"] == "CCGT"][["unit_code", "parent", "zone"]].copy()
    print(f"CCGT pool: {len(ccgt)} units across {ccgt['parent'].nunique()} parents")

    con = duckdb.connect()
    con.execute("PRAGMA threads = 6")
    con.register("ccgt_pool", ccgt)

    crit_str = ",".join(map(str, CRITICAL_HOURS))
    flat_str = ",".join(map(str, FLAT_HOURS))

    # q_2 per (unit, day, clock-hour) = SUM across IDA-periods within the
    # clock-hour of (sum across sessions × period_length_hours), in MWh.
    # This is dimensionally consistent across MTU60 (pre-2025-03-19) and
    # MTU15 (post): a CCGT running at 100 MW for the full hour gives 100 MWh
    # in BOTH regimes (100×1 pre, 100×0.25×4 post).
    print("\n--- Computing q_2 in MWh per (unit, day, clock-hour) ---")
    panel = con.execute(
        f"""
        WITH pibci_summed AS (
            -- sum across sessions per (date, unit, period)
            SELECT date::DATE AS d, period,
                   ANY_VALUE(mtu_minutes) AS mtu_minutes,
                   unit_code,
                   SUM(assigned_power_mw) AS q2_mw
            FROM '{PIBCI}'
            WHERE date::DATE >= DATE '{START}' AND date::DATE < DATE '{END}'
            GROUP BY 1,2,4
        ),
        with_hour AS (
            SELECT p.*, c.parent,
                   CASE WHEN mtu_minutes = 60 THEN period - 1
                        WHEN mtu_minutes = 15 THEN (period - 1) / 4
                        ELSE NULL END AS hour,
                   q2_mw * mtu_minutes / 60.0 AS q2_mwh
            FROM pibci_summed p JOIN ccgt_pool c USING (unit_code)
        ),
        per_clock_hour AS (
            -- aggregate IDA-periods within each clock-hour (4 quarters → 1 hour
            -- post-reform; 1 period → 1 hour pre-reform)
            SELECT d, parent, unit_code, hour,
                   SUM(q2_mwh) AS q2_mwh_clock_hour,
                   SUM(GREATEST(q2_mwh, 0)) AS q2_pos_mwh_clock_hour
            FROM with_hour
            WHERE hour IS NOT NULL AND hour BETWEEN 0 AND 23
            GROUP BY 1,2,3,4
        )
        SELECT EXTRACT(YEAR FROM d) AS y,
               EXTRACT(MONTH FROM d) AS m,
               parent,
               hour,
               -- mean MWh per clock-hour per (unit, day) — comparable across regimes
               AVG(q2_mwh_clock_hour) AS q2_mwh_avg,
               AVG(q2_pos_mwh_clock_hour) AS q2_pos_mwh_avg,
               -- also keep totals for reference
               SUM(q2_mwh_clock_hour) AS q2_mwh_total,
               COUNT(*) AS n_unit_clock_hour_obs
        FROM per_clock_hour
        GROUP BY 1,2,3,4
        """
    ).df()
    panel["hour"] = panel["hour"].astype(int)
    panel["hour_class"] = panel["hour"].apply(
        lambda h: "critical_h18_22" if h in CRITICAL_HOURS else
                  ("flat_h3_5" if h in FLAT_HOURS else "other")
    )
    panel["year_month"] = pd.to_datetime(
        dict(year=panel["y"].astype(int), month=panel["m"].astype(int), day=1)
    )
    print(f"panel rows: {len(panel)}")

    # Aggregate per parent × month × hour_class — weighted average across hours-and-units
    # We want: mean MWh per (unit, clock-hour) within the class
    panel["weighted_sum_mwh"] = panel["q2_mwh_avg"] * panel["n_unit_clock_hour_obs"]
    panel["weighted_sum_pos"] = panel["q2_pos_mwh_avg"] * panel["n_unit_clock_hour_obs"]
    agg = (
        panel.groupby(["parent", "year_month", "hour_class"])
        .agg(
            weighted_sum_mwh=("weighted_sum_mwh", "sum"),
            weighted_sum_pos=("weighted_sum_pos", "sum"),
            n_obs=("n_unit_clock_hour_obs", "sum"),
        )
        .reset_index()
    )
    agg["q2_mwh_per_clock_hour"] = agg["weighted_sum_mwh"] / agg["n_obs"].replace(0, pd.NA)
    agg["q2_pos_mwh_per_clock_hour"] = agg["weighted_sum_pos"] / agg["n_obs"].replace(0, pd.NA)

    pivot = (
        agg.pivot_table(
            index=["parent", "year_month"],
            columns="hour_class",
            values="q2_mwh_per_clock_hour",
            aggfunc="sum",
        )
        .reset_index()
    )
    pivot["crit_minus_flat"] = pivot.get("critical_h18_22", 0) - pivot.get("flat_h3_5", 0)
    pivot.to_csv(OUTDIR / "q2_monthly_critical_minus_flat.csv", index=False)
    print(f"\nSaved: {OUTDIR / 'q2_monthly_critical_minus_flat.csv'}")

    # Print trajectory for treatment + key placebo firms
    for parent in ["IB", "GE", "GN", "HC", "EDP-PT", "Repsol", "TotalEnergies"]:
        sub = pivot[pivot["parent"] == parent].sort_values("year_month")
        if len(sub) == 0:
            continue
        print(f"\n--- {parent}: q_2 MWh per (unit, clock-hour), by month × hour_class ---")
        print(sub[["year_month", "critical_h18_22", "flat_h3_5", "crit_minus_flat"]].to_string(index=False))

    # Plot the trajectories
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        fig, axes = plt.subplots(2, 4, figsize=(15, 6.5), sharex=True)
        focus = ["IB", "GE", "GN", "HC", "EDP-PT", "Repsol", "TotalEnergies", "Engie"]
        for ax, parent in zip(axes.flat, focus):
            sub = pivot[pivot["parent"] == parent].sort_values("year_month")
            if len(sub) == 0:
                ax.set_title(f"{parent}: no data"); continue
            ax.plot(sub["year_month"], sub.get("critical_h18_22", 0),
                    marker="o", linewidth=1.3, label="critical h{18-22}", color="C3")
            ax.plot(sub["year_month"], sub.get("flat_h3_5", 0),
                    marker="s", linewidth=1.3, label="flat h{3-5}", color="C0")
            ax.axvline(pd.Timestamp("2025-03-19"), color="orange", linestyle=":",
                       linewidth=1.0, label="MTU15-IDA")
            ax.axvline(pd.Timestamp("2025-10-01"), color="red", linestyle="--",
                       linewidth=1.0, label="MTU15-DA")
            ax.set_title(parent)
            ax.set_ylabel("q_2 MWh per (unit, clock-hour)")
            ax.tick_params(axis="x", rotation=45, labelsize=7)
            ax.grid(alpha=0.3)
            if parent == "IB":
                ax.legend(fontsize=6, loc="upper left")
        fig.suptitle("q_2 trajectory: net IDA adjustment per period, CCGT firms. "
                     "Test: does break align with MTU15-IDA (Mar) or MTU15-DA (Oct)?",
                     fontsize=10)
        fig.tight_layout()
        out_png = FIGDIR / "q2_break_id15_vs_da15.png"
        out_pdf = FIGDIR / "q2_break_id15_vs_da15.pdf"
        fig.savefig(out_png, dpi=110, bbox_inches="tight")
        fig.savefig(out_pdf, bbox_inches="tight")
        plt.close(fig)
        print(f"\nFigures: {out_png} / .pdf")
    except Exception as e:
        print(f"Plot failed: {e}")


if __name__ == "__main__":
    main()
