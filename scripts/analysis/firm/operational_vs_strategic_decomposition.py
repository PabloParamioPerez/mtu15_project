# STATUS: ALIVE
# LAST-AUDIT: 2026-05-08
# FEEDS: identification (operational vs strategic decomposition; DA→IDA wedge)
# CLAIM: Decompose firm-hour MWh changes through the OMIE/REE chain into
# strategic IDA market activity vs REE post-IDA RT (RT2). Also compute
# DA→IDA price wedge by hour-class.
#
# Programme chain (per docs/notes/SPANISH_MARKET_STRUCTURE.md):
#   PDBC ──► PDBF ──► PDVD ──► PIBCA(s=k) ──► PHF(s=k) ──► P48
#       bilateral  pre-IDA RT   post-IDA market  post-IDA RT
#                  (Phase 1+2)    RT-free        ("RT2")
#
# Decompositions:
#   Strategic IDA = PIBCA − PDVD = sum(pibci across sessions). RT-free.
#   RT2          = PHF(max session) − PIBCA(max session)
#
# Outputs by parent firm × month × hour-class:
#   - DA cleared MWh per (unit, clock-hour)
#   - Strategic IDA increment MWh
#   - RT2 (operational post-IDA RT) MWh
#   - DA→IDA price wedge (€/MWh)

from __future__ import annotations

from pathlib import Path
import sys

import duckdb
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO / "src"))
from mtu.classification.units import classify_units  # noqa: E402

OUTDIR = REPO / "results" / "regressions" / "firm" / "operational_vs_strategic"
FIGDIR = REPO / "figures" / "working"
OUTDIR.mkdir(parents=True, exist_ok=True)
FIGDIR.mkdir(parents=True, exist_ok=True)

PDBCE = REPO / "data" / "processed" / "omie" / "mercado_diario" / "programas" / "pdbce_all.parquet"
PIBCI = REPO / "data" / "processed" / "omie" / "mercado_intradiario_subastas" / "programas" / "pibci_all.parquet"
PIBCA = REPO / "data" / "processed" / "omie" / "mercado_intradiario_subastas" / "programas" / "pibca_all.parquet"
PHF = REPO / "data" / "processed" / "omie" / "mercado_intradiario_subastas" / "programas" / "phf_all.parquet"
MARGPDBC = REPO / "data" / "processed" / "omie" / "mercado_diario" / "precios" / "marginalpdbc_all.parquet"
MARGPIBC = REPO / "data" / "processed" / "omie" / "mercado_intradiario_subastas" / "precios" / "marginalpibc_all.parquet"
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


def hour_class(h: int) -> str:
    if h in CRITICAL_HOURS: return "critical_h18_22"
    if h in FLAT_HOURS:     return "flat_h3_5"
    return "other"


def main() -> None:
    units = classify_units(
        csv_path=str(UNITS_CSV),
        keep_columns=["unit_code", "owner_agent", "tech_group", "zone"],
    )
    units["parent"] = units["owner_agent"].apply(parent_of)
    ccgt = units[units["tech_group"] == "CCGT"][["unit_code", "parent"]].copy()
    print(f"CCGT pool: {len(ccgt)}")

    con = duckdb.connect()
    con.execute("PRAGMA threads = 6")
    con.register("ccgt_pool", ccgt)

    # ============================================================
    # Part 1: Operational vs strategic decomposition (per unit-hour)
    # ============================================================
    print("\n=== Part 1: Build per (unit, day, clock-hour) MWh panel ===")

    # 1a: DA cleared MWh per (unit, day, clock-hour) from pdbce
    print("Building DA cleared aggregate...")
    da = con.execute(
        f"""
        WITH p AS (
            SELECT date::DATE AS d, period, mtu_minutes, unit_code, assigned_power_mw
            FROM '{PDBCE}'
            WHERE date::DATE >= DATE '{START}' AND date::DATE < DATE '{END}'
              AND assigned_power_mw > 0
        ),
        with_hour AS (
            SELECT p.d, p.unit_code,
                   CASE WHEN mtu_minutes = 60 THEN period - 1
                        WHEN mtu_minutes = 15 THEN (period - 1) / 4
                        ELSE NULL END AS hour,
                   p.assigned_power_mw * mtu_minutes / 60.0 AS mwh
            FROM p
        )
        SELECT d, unit_code, hour, SUM(mwh) AS da_mwh
        FROM with_hour
        WHERE hour BETWEEN 0 AND 23
        GROUP BY 1,2,3
        """
    ).df()
    print(f"  DA rows: {len(da):,}")

    # 1b: Strategic IDA = sum(pibci across sessions) per (unit, day, period),
    #     then aggregate to (unit, day, clock-hour)
    print("Building strategic IDA aggregate (sum pibci across sessions)...")
    ida_strategic = con.execute(
        f"""
        WITH per_period AS (
            SELECT date::DATE AS d, period, ANY_VALUE(mtu_minutes) AS mtu,
                   unit_code, SUM(assigned_power_mw) AS strat_mw
            FROM '{PIBCI}'
            WHERE date::DATE >= DATE '{START}' AND date::DATE < DATE '{END}'
            GROUP BY 1,2,4
        )
        SELECT d, unit_code,
               CASE WHEN mtu = 60 THEN period - 1
                    WHEN mtu = 15 THEN (period - 1) / 4
                    ELSE NULL END AS hour,
               SUM(strat_mw * mtu / 60.0) AS strategic_ida_mwh
        FROM per_period
        WHERE period IS NOT NULL AND mtu IS NOT NULL
        GROUP BY 1,2,3
        HAVING hour BETWEEN 0 AND 23
        """
    ).df()
    print(f"  Strategic IDA rows: {len(ida_strategic):,}")

    # 1c: RT2 = PHF(max session) − PIBCA(max session) per (unit, period), aggregated to clock-hour
    # Filter to CCGT pool BEFORE the window function to keep memory manageable.
    print("Building RT2 (PHF max-session − PIBCA max-session) — CCGT only...")
    con.execute("SET threads=4")
    con.execute("SET memory_limit='10GB'")
    rt2 = con.execute(
        f"""
        WITH phf_ccgt AS (
            SELECT p.date::DATE AS d, p.period, p.mtu_minutes, p.unit_code,
                   p.session_number, p.assigned_power_mw AS phf_mw
            FROM '{PHF}' p JOIN ccgt_pool USING (unit_code)
            WHERE p.date::DATE >= DATE '{START}' AND p.date::DATE < DATE '{END}'
        ),
        phf_max AS (
            SELECT d, period, mtu_minutes, unit_code, phf_mw
            FROM (
                SELECT *, ROW_NUMBER() OVER (PARTITION BY d, period, unit_code
                                             ORDER BY session_number DESC) AS rn
                FROM phf_ccgt
            ) WHERE rn = 1
        ),
        pibca_ccgt AS (
            SELECT p.date::DATE AS d, p.period, p.mtu_minutes, p.unit_code,
                   p.session_number, p.assigned_power_mw AS pibca_mw
            FROM '{PIBCA}' p JOIN ccgt_pool USING (unit_code)
            WHERE p.date::DATE >= DATE '{START}' AND p.date::DATE < DATE '{END}'
        ),
        pibca_max AS (
            SELECT d, period, mtu_minutes, unit_code, pibca_mw
            FROM (
                SELECT *, ROW_NUMBER() OVER (PARTITION BY d, period, unit_code
                                             ORDER BY session_number DESC) AS rn
                FROM pibca_ccgt
            ) WHERE rn = 1
        ),
        joined AS (
            SELECT COALESCE(a.d, b.d) AS d,
                   COALESCE(a.period, b.period) AS period,
                   COALESCE(a.mtu_minutes, b.mtu_minutes) AS mtu_minutes,
                   COALESCE(a.unit_code, b.unit_code) AS unit_code,
                   COALESCE(a.phf_mw, 0) AS phf_mw,
                   COALESCE(b.pibca_mw, 0) AS pibca_mw
            FROM phf_max a
            FULL JOIN pibca_max b
              ON a.d = b.d AND a.period = b.period AND a.unit_code = b.unit_code
        )
        SELECT d, unit_code,
               CASE WHEN mtu_minutes = 60 THEN period - 1
                    WHEN mtu_minutes = 15 THEN (period - 1) / 4
                    ELSE NULL END AS hour,
               SUM((phf_mw - pibca_mw) * mtu_minutes / 60.0) AS rt2_mwh
        FROM joined
        WHERE period IS NOT NULL AND mtu_minutes IS NOT NULL
        GROUP BY 1,2,3
        HAVING hour BETWEEN 0 AND 23
        """
    ).df()
    print(f"  RT2 rows: {len(rt2):,}")

    # 1d: merge all three on (d, unit_code, hour), join with parent
    print("Merging and aggregating by parent × month × hour-class...")
    panel = (
        da.merge(ida_strategic, on=["d", "unit_code", "hour"], how="outer")
          .merge(rt2, on=["d", "unit_code", "hour"], how="outer")
    )
    panel = panel.merge(ccgt, on="unit_code", how="inner")
    panel["da_mwh"] = panel["da_mwh"].fillna(0)
    panel["strategic_ida_mwh"] = panel["strategic_ida_mwh"].fillna(0)
    panel["rt2_mwh"] = panel["rt2_mwh"].fillna(0)
    panel["d"] = pd.to_datetime(panel["d"])
    panel["year_month"] = panel["d"].dt.to_period("M").dt.to_timestamp()
    panel["hour_class"] = panel["hour"].astype(int).apply(hour_class)

    agg = (
        panel.groupby(["parent", "year_month", "hour_class"])
        .agg(
            n_obs=("d", "count"),
            da_mwh_mean=("da_mwh", "mean"),
            strategic_ida_mwh_mean=("strategic_ida_mwh", "mean"),
            rt2_mwh_mean=("rt2_mwh", "mean"),
        )
        .reset_index()
    )
    agg["phf_mwh_mean"] = agg["da_mwh_mean"] + agg["strategic_ida_mwh_mean"] + agg["rt2_mwh_mean"]
    agg.to_csv(OUTDIR / "operational_strategic_per_firm_month_hourclass.csv", index=False)
    print(f"\nSaved: {OUTDIR / 'operational_strategic_per_firm_month_hourclass.csv'}")

    # Pivot for the dominant firms — strategic IDA × hour_class trajectory
    print("\n--- Strategic IDA MWh per (unit, clock-hour) — by firm × month × hour-class ---")
    for parent in ["IB", "GE", "GN", "HC", "EDP-PT"]:
        sub = agg[(agg["parent"] == parent) & (agg["hour_class"].isin(["critical_h18_22","flat_h3_5"]))]
        if len(sub) == 0:
            continue
        piv = sub.pivot(index="year_month", columns="hour_class",
                       values="strategic_ida_mwh_mean").round(2).sort_index()
        print(f"\n{parent}: strategic IDA")
        print(piv.to_string())

    print("\n--- RT2 (post-IDA REE) MWh per (unit, clock-hour) ---")
    for parent in ["IB", "GE", "GN", "HC", "EDP-PT"]:
        sub = agg[(agg["parent"] == parent) & (agg["hour_class"].isin(["critical_h18_22","flat_h3_5"]))]
        if len(sub) == 0:
            continue
        piv = sub.pivot(index="year_month", columns="hour_class",
                       values="rt2_mwh_mean").round(2).sort_index()
        print(f"\n{parent}: RT2 (PHF − PIBCA)")
        print(piv.to_string())

    # ============================================================
    # Part 2: DA → IDA price wedge by hour-class
    # ============================================================
    print("\n\n=== Part 2: DA → IDA price wedge by hour-class ===")
    # marginalpdbc is hourly DA price; marginalpibc is per-session IDA price.
    # Compute DA price per clock hour, IDA price = mean across sessions per clock hour.
    # Then DA-IDA wedge = DA - IDA (positive = IDA cheaper than DA).
    print("Building DA + IDA price aggregates per clock-hour...")
    print(con.execute(f"DESCRIBE SELECT * FROM '{MARGPDBC}' LIMIT 0").df())
    print(con.execute(f"DESCRIBE SELECT * FROM '{MARGPIBC}' LIMIT 0").df())

    wedge = con.execute(
        f"""
        WITH da AS (
            SELECT date::DATE AS d, period, mtu_minutes, price_es_eur_mwh AS da_p
            FROM '{MARGPDBC}'
            WHERE date::DATE >= DATE '{START}' AND date::DATE < DATE '{END}'
        ),
        da_h AS (
            SELECT d,
                   CASE WHEN mtu_minutes = 60 THEN period - 1
                        ELSE (period - 1) / 4 END AS hour,
                   AVG(da_p) AS da_p_h
            FROM da WHERE period IS NOT NULL
            GROUP BY 1,2 HAVING hour BETWEEN 0 AND 23
        ),
        ida AS (
            SELECT date::DATE AS d, period, mtu_minutes, session_number,
                   price_es_eur_mwh AS ida_p
            FROM '{MARGPIBC}'
            WHERE date::DATE >= DATE '{START}' AND date::DATE < DATE '{END}'
        ),
        ida_h AS (
            -- average across sessions and within-hour periods
            SELECT d,
                   CASE WHEN mtu_minutes = 60 THEN period - 1
                        ELSE (period - 1) / 4 END AS hour,
                   AVG(ida_p) AS ida_p_h
            FROM ida WHERE period IS NOT NULL
            GROUP BY 1,2 HAVING hour BETWEEN 0 AND 23
        )
        SELECT da_h.d, da_h.hour, da_h.da_p_h, ida_h.ida_p_h,
               (da_h.da_p_h - ida_h.ida_p_h) AS wedge_eur_mwh
        FROM da_h JOIN ida_h USING (d, hour)
        """
    ).df()
    wedge["d"] = pd.to_datetime(wedge["d"])
    wedge["year_month"] = wedge["d"].dt.to_period("M").dt.to_timestamp()
    wedge["hour_class"] = wedge["hour"].astype(int).apply(hour_class)
    wedge_agg = (
        wedge.groupby(["year_month", "hour_class"])
        .agg(da_p=("da_p_h","mean"), ida_p=("ida_p_h","mean"),
             wedge=("wedge_eur_mwh","mean"), n=("d","count"))
        .reset_index()
    )
    print("\n--- DA → IDA price wedge (€/MWh) by month × hour-class ---")
    piv_wedge = wedge_agg.pivot(index="year_month", columns="hour_class",
                                 values="wedge").round(2).sort_index()
    print(piv_wedge.tail(30).to_string())
    wedge_agg.to_csv(OUTDIR / "da_ida_price_wedge_monthly.csv", index=False)

    # Plot the operational/strategic decomposition AND price wedge
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt

        # Figure 1: Strategic IDA + RT2 by firm
        fig, axes = plt.subplots(2, 5, figsize=(17, 7), sharex=True)
        focus = ["IB", "GE", "GN", "HC", "EDP-PT"]
        for col_idx, parent in enumerate(focus):
            sub_crit = agg[(agg["parent"] == parent) & (agg["hour_class"] == "critical_h18_22")].sort_values("year_month")
            sub_flat = agg[(agg["parent"] == parent) & (agg["hour_class"] == "flat_h3_5")].sort_values("year_month")
            if len(sub_crit) == 0 and len(sub_flat) == 0:
                continue
            for ax, col, label, ylim in [
                (axes[0, col_idx], "strategic_ida_mwh_mean", "Strategic IDA", None),
                (axes[1, col_idx], "rt2_mwh_mean", "RT2 (PHF−PIBCA)", None),
            ]:
                if len(sub_crit) > 0:
                    ax.plot(sub_crit["year_month"], sub_crit[col], marker="o",
                           color="C3", label="critical h{18-22}")
                if len(sub_flat) > 0:
                    ax.plot(sub_flat["year_month"], sub_flat[col], marker="s",
                           color="C0", label="flat h{3-5}")
                ax.axvline(pd.Timestamp("2025-03-19"), color="orange",
                          linestyle=":", label="MTU15-IDA")
                ax.axvline(pd.Timestamp("2025-10-01"), color="red",
                          linestyle="--", label="MTU15-DA")
                ax.set_title(f"{parent}: {label}", fontsize=9)
                ax.set_ylabel("MWh / unit-clock-hour", fontsize=8)
                ax.tick_params(axis="x", rotation=45, labelsize=6)
                ax.grid(alpha=0.3)
                if col_idx == 0 and label == "Strategic IDA":
                    ax.legend(fontsize=6)
        fig.suptitle("Operational vs strategic decomposition by firm × month × hour-class\n"
                     "Strategic IDA = sum(pibci) (RT-free); RT2 = PHF(max session) − PIBCA(max session)",
                     fontsize=10)
        fig.tight_layout()
        out_png = FIGDIR / "operational_strategic_per_firm.png"
        fig.savefig(out_png, dpi=110, bbox_inches="tight")
        fig.savefig(FIGDIR / "operational_strategic_per_firm.pdf", bbox_inches="tight")
        plt.close(fig)
        print(f"\nFigure: {out_png}")

        # Figure 2: Price wedge by month × hour-class
        fig2, ax = plt.subplots(figsize=(11, 4.5))
        for hc, color, label in [
            ("critical_h18_22", "C3", "critical h{18-22}"),
            ("flat_h3_5", "C0", "flat h{3-5}"),
            ("other", "gray", "other"),
        ]:
            sub = wedge_agg[wedge_agg["hour_class"] == hc].sort_values("year_month")
            ax.plot(sub["year_month"], sub["wedge"], marker="o", color=color, label=label)
        ax.axhline(0, color="black", linewidth=0.5)
        ax.axvline(pd.Timestamp("2025-03-19"), color="orange", linestyle=":", label="MTU15-IDA")
        ax.axvline(pd.Timestamp("2025-10-01"), color="red", linestyle="--", label="MTU15-DA")
        ax.set_title("DA → IDA price wedge (DA price − IDA price), Spain side, by month × hour-class")
        ax.set_ylabel("€/MWh")
        ax.legend()
        ax.grid(alpha=0.3)
        ax.tick_params(axis="x", rotation=45)
        fig2.tight_layout()
        fig2.savefig(FIGDIR / "da_ida_price_wedge.png", dpi=110, bbox_inches="tight")
        fig2.savefig(FIGDIR / "da_ida_price_wedge.pdf", bbox_inches="tight")
        plt.close(fig2)
        print(f"Figure: {FIGDIR / 'da_ida_price_wedge.png'}")
    except Exception as e:
        print(f"Plot failed: {e}")


if __name__ == "__main__":
    main()
