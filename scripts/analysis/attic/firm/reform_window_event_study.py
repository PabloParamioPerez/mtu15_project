# STATUS: ALIVE
# LAST-AUDIT: 2026-05-16
# FEEDS: provisional.tex §14 (Reform-window identification)
# CLAIM: Event-study visual for the DA15 + IDA15 reform-window DDD design.
#        For each outcome (clearing price, q_1, q_2), plot monthly mean
#        (critical - flat) within-day differential, separately for 2024
#        (placebo) and 2025 (treatment). Pre-trend test: 2024 and 2025
#        series should overlap for τ < 0; post-treatment they should
#        diverge.
#
#        NOTE: this is the REDUCED-FORM event study (raw monthly means
#        of the crit - flat differential). Full event-study regression
#        with FE + SE comes next once the design is approved.

from __future__ import annotations

from pathlib import Path
import sys

import duckdb
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO / "src"))
from mtu.classification.units import (  # noqa: E402
    firm_unit_panel,
    TREATMENT_PARENTS_SHORT as PIVOTAL,
)

PDBC  = REPO / "data" / "processed" / "omie" / "mercado_diario" / "programas" / "pdbc_all.parquet"
PIBCI = REPO / "data" / "processed" / "omie" / "mercado_intradiario_subastas" / "programas" / "pibci_all.parquet"
MARGINALPDBC = REPO / "data" / "processed" / "omie" / "mercado_diario" / "precios" / "marginalpdbc_all.parquet"
MARGINALPIBC = REPO / "data" / "processed" / "omie" / "mercado_intradiario_subastas" / "precios" / "marginalpibc_all.parquet"
UNITS_CSV = REPO / "data" / "external" / "omie_reference" / "lista_unidades.csv"

OUTDIR = REPO / "results" / "regressions" / "firm" / "reform_window_ddd"
OUTDIR.mkdir(parents=True, exist_ok=True)
FIGDIR = REPO / "figures" / "working"

# Reform dates
MTU15_IDA = pd.Timestamp("2025-03-19")
BLACKOUT  = pd.Timestamp("2025-04-28")
MTU15_DA  = pd.Timestamp("2025-10-01")

# Sample window: cover both DA15 and IDA15 windows + leads/lags + 2024 placebo
START = pd.Timestamp("2023-10-01")  # 1y before IDA15 placebo pre-start
END   = pd.Timestamp("2026-02-13")

CRITICAL_HOURS = (5, 6, 7, 8, 16, 17, 18, 19, 20, 21, 22)
FLAT_HOURS     = (1, 2, 3)


def build_price_panel(parquet, table_name: str):
    """Per (date, hour): mean clearing price. Then collapse to (year-month,
    hour-class) and compute the crit - flat differential per month."""
    con = duckdb.connect(); con.execute("PRAGMA threads=4")
    crit_list = ",".join(map(str, CRITICAL_HOURS))
    flat_list = ",".join(map(str, FLAT_HOURS))
    q = f"""
    WITH p AS (
        SELECT date::DATE AS d,
               CASE WHEN mtu_minutes = 60 THEN period - 1
                    WHEN mtu_minutes = 15 THEN (period - 1) // 4
                    ELSE NULL END AS hour,
               price_es_eur_mwh AS p
        FROM '{parquet}'
        WHERE date::DATE >= DATE '{START.date()}' AND date::DATE <= DATE '{END.date()}'
          AND price_es_eur_mwh IS NOT NULL
    ),
    hourly AS (
        SELECT d, hour, AVG(p) AS p_hour
        FROM p
        WHERE hour BETWEEN 0 AND 23
        GROUP BY 1, 2
    )
    SELECT d, hour, p_hour,
           CASE WHEN hour IN ({crit_list}) THEN 'critical'
                WHEN hour IN ({flat_list}) THEN 'flat'
                ELSE 'other' END AS hour_class
    FROM hourly
    """
    df = con.execute(q).df()
    df = df[df["hour_class"].isin(("critical", "flat"))]
    df["d"] = pd.to_datetime(df["d"])
    df["ym"] = df["d"].dt.to_period("M").dt.to_timestamp()
    monthly = df.groupby(["ym", "hour_class"], as_index=False).agg(
        p_mean=("p_hour", "mean"),
        n=("p_hour", "size"))
    pv = monthly.pivot(index="ym", columns="hour_class", values="p_mean")
    pv["diff_crit_flat"] = pv["critical"] - pv["flat"]
    pv = pv.reset_index()
    pv["table"] = table_name
    return pv


def build_volume_panel(parquet, label: str):
    """Per (firm, unit, date, hour): sum of assigned_power_mw × mtu_minutes/60.
    Pivotal-firm CCGT only. Then collapse to (year-month, hour-class) firm mean."""
    units = firm_unit_panel(csv_path=str(UNITS_CSV), scheme="short", mode="primary_owner")
    keep = units[units["parent"].isin(list(PIVOTAL)) & (units["tech_group"] == "CCGT")][
        ["unit_code", "parent"]].rename(columns={"parent": "firm"})
    con = duckdb.connect(); con.execute("PRAGMA threads=4"); con.execute("SET memory_limit='6GB'")
    con.register("uft", keep)
    crit_list = ",".join(map(str, CRITICAL_HOURS))
    flat_list = ",".join(map(str, FLAT_HOURS))
    # Pre/post-MTU15: period is 1-24 (hourly) or 1-96 (15-min). Compute hour from period.
    q = f"""
    WITH base AS (
        SELECT date::DATE AS d, unit_code,
               CASE WHEN mtu_minutes = 60 THEN period - 1
                    WHEN mtu_minutes = 15 THEN (period - 1) // 4
                    ELSE NULL END AS hour,
               assigned_power_mw * (mtu_minutes / 60.0) AS mwh
        FROM '{parquet}'
        WHERE date::DATE >= DATE '{START.date()}' AND date::DATE <= DATE '{END.date()}'
          AND assigned_power_mw IS NOT NULL
    ),
    unit_hour AS (
        SELECT b.d, b.unit_code, u.firm, b.hour,
               SUM(b.mwh) AS mwh_hour
        FROM base b JOIN uft u USING (unit_code)
        WHERE b.hour BETWEEN 0 AND 23
        GROUP BY 1, 2, 3, 4
    )
    SELECT d, firm, unit_code, hour, mwh_hour,
           CASE WHEN hour IN ({crit_list}) THEN 'critical'
                WHEN hour IN ({flat_list}) THEN 'flat'
                ELSE 'other' END AS hour_class
    FROM unit_hour
    """
    df = con.execute(q).df()
    df = df[df["hour_class"].isin(("critical", "flat"))]
    df["d"] = pd.to_datetime(df["d"])
    df["ym"] = df["d"].dt.to_period("M").dt.to_timestamp()
    monthly = df.groupby(["ym", "hour_class"], as_index=False).agg(
        mwh_mean=("mwh_hour", "mean"),
        n=("mwh_hour", "size"))
    pv = monthly.pivot(index="ym", columns="hour_class", values="mwh_mean")
    pv["diff_crit_flat"] = pv["critical"] - pv["flat"]
    pv = pv.reset_index()
    pv["table"] = label
    return pv


def plot_event_study(panels: dict, fname: str, normalise: bool = True):
    """Per outcome row, per reform column: plot the monthly (crit - flat)
    differential. Overlay 2025 (red) on 2024 (blue), with vertical line
    at the reform date.

    normalise=True (default): subtract each line's τ=-1 value, so both
    lines start at 0 at τ=-1 and the visual test becomes "overlap pre,
    diverge post" (== parallel-trends + no level shift). The DDD
    coefficient β_7 is then the vertical separation between the lines
    at τ ≥ 0.
    """
    outcomes = ["price_DA", "price_IDA", "q1_DA", "q2_IDA"]
    pretty   = {"price_DA":  "DA clearing price",
                "price_IDA": "IDA clearing price (mean across 3 sessions)",
                "q1_DA":     "$q_1$: DA-cleared MWh / unit-hour (Big-4 CCGT)",
                "q2_IDA":    "$q_2$: IDA-cleared MWh / unit-hour (Big-4 CCGT)"}
    units    = {"price_DA": "EUR/MWh", "price_IDA": "EUR/MWh",
                "q1_DA": "MWh / unit-hour", "q2_IDA": "MWh / unit-hour"}

    reforms = [("IDA15", MTU15_IDA, "2025-03-19"),
                ("DA15",  MTU15_DA,  "2025-10-01")]

    fig, axes = plt.subplots(len(outcomes), len(reforms),
                              figsize=(13, 2.8 * len(outcomes)), sharey="row")
    for j, (rname, rdate, rdate_str) in enumerate(reforms):
        for i, oc in enumerate(outcomes):
            ax = axes[i, j]
            df = panels[oc].copy()
            df["tau"]   = (df["ym"].dt.year - rdate.year) * 12 + (df["ym"].dt.month - rdate.month)
            df["year"]  = df["ym"].dt.year
            df_2024 = df[df["year"].isin([2023, 2024, 2025])].copy()
            df_2024["tau_24"] = (df_2024["ym"].dt.year - (rdate.year - 1)) * 12 + (df_2024["ym"].dt.month - rdate.month)
            if rname == "IDA15":
                window = (-6, 4)
            else:
                window = (-6, 5)
            t25 = df[(df["tau"] >= window[0]) & (df["tau"] <= window[1])].copy()
            t24 = df_2024[(df_2024["tau_24"] >= window[0]) & (df_2024["tau_24"] <= window[1])].copy()

            # Normalise by subtracting the τ = -1 value from each line
            if normalise:
                ref25 = t25.loc[t25["tau"] == -1, "diff_crit_flat"]
                ref24 = t24.loc[t24["tau_24"] == -1, "diff_crit_flat"]
                if len(ref25) > 0:
                    t25["y"] = t25["diff_crit_flat"] - ref25.iloc[0]
                else:
                    t25["y"] = t25["diff_crit_flat"]
                if len(ref24) > 0:
                    t24["y"] = t24["diff_crit_flat"] - ref24.iloc[0]
                else:
                    t24["y"] = t24["diff_crit_flat"]
            else:
                t25["y"] = t25["diff_crit_flat"]
                t24["y"] = t24["diff_crit_flat"]

            # Shade pre / post regions lightly
            ax.axvspan(window[0] - 0.5, -0.5, alpha=0.05, color="blue", zorder=0)
            ax.axvspan(-0.5, window[1] + 0.5, alpha=0.05, color="red", zorder=0)

            ax.plot(t25["tau"], t25["y"], marker="o", color="tab:red",
                    lw=1.6, ms=4, label="2025 (treatment)")
            ax.plot(t24["tau_24"], t24["y"], marker="s", color="tab:blue",
                    lw=1.6, ms=4, label="2024 (placebo)")
            ax.axvline(-0.5, color="black", lw=0.8, ls="--")
            ax.axhline(0, color="grey", lw=0.5)
            if i == 0:
                ax.set_title(f"{rname} (τ = months from {rdate_str})", fontsize=10)
            if j == 0:
                ylabel = pretty[oc] + (f"\nΔ from $\\tau=-1$ [{units[oc]}]" if normalise else f"\n[{units[oc]}]")
                ax.set_ylabel(ylabel, fontsize=8)
            ax.tick_params(labelsize=8)
            ax.grid(alpha=0.3)
            if i == 0 and j == 0:
                ax.legend(loc="upper left", fontsize=8, frameon=False)

    norm_note = "Each line normalised to $0$ at $\\tau = -1$ (subtraction of the $\\tau = -1$ value). " if normalise else ""
    fig.suptitle(f"Event-study: monthly (critical $-$ flat) differential, 2025 treatment vs 2024 placebo. {norm_note}"
                  "Pre-trend test (\\textit{parallel slopes}): for $\\tau < 0$, the two lines should track each other. "
                  "Treatment effect at $\\tau \\geq 0$: the vertical gap between the red and blue lines.",
                 fontsize=9, y=1.00)
    axes[-1, 0].set_xlabel(r"$\tau$ (months from reform)")
    axes[-1, 1].set_xlabel(r"$\tau$ (months from reform)")
    plt.tight_layout(rect=[0, 0, 1, 0.96])
    out = FIGDIR / fname
    plt.savefig(f"{out}.pdf", bbox_inches="tight")
    plt.savefig(f"{out}.png", bbox_inches="tight", dpi=130)
    plt.close(fig)
    print(f"saved {out}.pdf")


def main():
    print("Loading DA prices...")
    p_da  = build_price_panel(MARGINALPDBC, "DA")
    print("Loading IDA prices...")
    p_ida = build_price_panel(MARGINALPIBC, "IDA")
    print("Loading q_1 (PDBC, CCGT pivotal)...")
    q1 = build_volume_panel(PDBC, "q1")
    print("Loading q_2 (PIBCI, CCGT pivotal)...")
    q2 = build_volume_panel(PIBCI, "q2")

    p_da.to_csv(OUTDIR / "monthly_price_da.csv", index=False)
    p_ida.to_csv(OUTDIR / "monthly_price_ida.csv", index=False)
    q1.to_csv(OUTDIR / "monthly_q1.csv", index=False)
    q2.to_csv(OUTDIR / "monthly_q2.csv", index=False)

    panels = {"price_DA": p_da, "price_IDA": p_ida, "q1_DA": q1, "q2_IDA": q2}
    plot_event_study(panels, "fig_event_study_ddd")


if __name__ == "__main__":
    main()
