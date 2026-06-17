# STATUS: ALIVE
# LAST-AUDIT: 2026-05-16
# FEEDS: q_1 drop diagnostic — was q_1 falling because firms increasingly
#        bid above DA clearing to wait for RT2 pay-as-bid?
# CLAIM: For each (firm × tech × month × hour-class), compute mean q_1 in
#        MWh per unit-day-hour (the same q_1 as in pretrends_q2_over_q1)
#        and trace its monthly evolution. Test: does CCGT q_1 drop more
#        in firms with high zonal dominance (GN, GE-Galicia) than in
#        firms without (HC, non-Big-4)?

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
    PLACEBO_PARENTS_SHORT as NON_PIVOTAL,
)

PDBC = REPO / "data" / "processed" / "omie" / "mercado_diario" / "programas" / "pdbc_all.parquet"
UNITS_CSV = REPO / "data" / "external" / "omie_reference" / "lista_unidades.csv"
ZONE_MAP = REPO / "data" / "external" / "ccgt_zonal_map.csv"
OUTDIR = REPO / "results" / "regressions" / "firm" / "q1_drop"
OUTDIR.mkdir(parents=True, exist_ok=True)
FIGDIR = REPO / "figures" / "working"

START = "2024-01-01"
END   = "2026-03-01"
MTU15_IDA = pd.Timestamp("2025-03-19")
BLACKOUT  = pd.Timestamp("2025-04-28")
MTU15_DA  = pd.Timestamp("2025-10-01")

CRIT = (5,6,7,8,16,17,18,19,20,21,22)
FLAT = (1,2,3)


def build_panel():
    units = firm_unit_panel(csv_path=str(UNITS_CSV), scheme="short", mode="primary_owner")
    keep = units[units["parent"].isin(list(PIVOTAL) + list(NON_PIVOTAL))][
        ["unit_code", "parent", "tech_group"]
    ]
    con = duckdb.connect(); con.execute("PRAGMA threads = 4"); con.execute("SET memory_limit = '10GB'")
    con.register("uft", keep)
    crit_list = ",".join(map(str, CRIT))
    flat_list = ",".join(map(str, FLAT))
    q = f"""
    WITH base AS (
        SELECT date::DATE AS d, unit_code,
               CASE WHEN mtu_minutes = 60 THEN period - 1
                    WHEN mtu_minutes = 15 THEN (period - 1) // 4
                    ELSE NULL END AS hour,
               SUM(assigned_power_mw * mtu_minutes / 60.0) AS q1_mwh
        FROM '{PDBC}'
        WHERE date::DATE >= DATE '{START}' AND date::DATE < DATE '{END}'
          AND assigned_power_mw IS NOT NULL
        GROUP BY 1, 2, 3
    )
    SELECT u.parent, u.tech_group,
           DATE_TRUNC('month', b.d) AS ym,
           CASE WHEN b.hour IN ({crit_list}) THEN 'critical'
                WHEN b.hour IN ({flat_list}) THEN 'flat'
                ELSE 'other' END AS hc,
           AVG(b.q1_mwh) AS q1_mean_mwh,
           COUNT(*) AS n_cells,
           AVG(CASE WHEN b.q1_mwh >= 5 THEN 1.0 ELSE 0.0 END) AS share_cleared
    FROM base b JOIN uft u USING (unit_code)
    WHERE b.hour BETWEEN 0 AND 23
    GROUP BY 1, 2, 3, 4
    """
    df = con.execute(q).df()
    df["ym"] = pd.to_datetime(df["ym"])
    df = df[df["hc"].isin(("critical", "flat"))]
    return df


def plot_panel(df: pd.DataFrame, fname: str, title: str):
    """Per-firm CCGT panel: monthly mean q_1 critical+flat."""
    firms_to_plot = ["IB", "GE", "GN", "HC",
                      "Repsol", "Engie", "TotalEnergies", "Moeve"]
    ccgt = df[df["tech_group"] == "CCGT"]
    fig, axes = plt.subplots(2, 4, figsize=(15, 6), sharex=True, sharey=False)
    for ax, firm in zip(axes.flatten(), firms_to_plot):
        sub = ccgt[ccgt["parent"] == firm].sort_values("ym")
        crit = sub[sub["hc"] == "critical"]
        flat = sub[sub["hc"] == "flat"]
        if len(crit):
            ax.plot(crit["ym"], crit["q1_mean_mwh"], marker="o", color="C3",
                    lw=1.4, ms=3.5, label="Critical")
        if len(flat):
            ax.plot(flat["ym"], flat["q1_mean_mwh"], marker="o", color="C0",
                    lw=1.4, ms=3.5, label="Flat")
        ax.axvline(MTU15_IDA, color="gray", ls=":", lw=0.9)
        ax.axvline(BLACKOUT,  color="black", ls="-.", lw=0.9)
        ax.axvline(MTU15_DA,  color="red",   ls="--", lw=1.0)
        ax.axhline(0, color="black", lw=0.5)
        ax.set_title(firm, fontsize=10)
        ax.grid(alpha=0.3)
        ax.tick_params(labelsize=7)
        ax.xaxis.set_major_locator(mdates.MonthLocator(interval=3))
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
        for lbl in ax.get_xticklabels():
            lbl.set_rotation(45); lbl.set_ha("right")
    axes[0, 0].legend(loc="upper left", fontsize=8, frameon=False)
    fig.suptitle(title, fontsize=11, y=1.00)
    fig.tight_layout(rect=[0, 0, 1, 0.95])
    out = FIGDIR / fname
    for ext in ("pdf", "png"):
        fig.savefig(f"{out}.{ext}", bbox_inches="tight", dpi=130 if ext == "png" else None)
    plt.close(fig)
    print(f"saved {out}.pdf")


def build_fleet_total_by_tech():
    """Total pivotal-firm DA-cleared GWh per (tech, month)."""
    units = firm_unit_panel(csv_path=str(UNITS_CSV), scheme="short", mode="primary_owner")
    keep = units[units["parent"].isin(list(PIVOTAL))][["unit_code", "parent", "tech_group"]]
    con = duckdb.connect(); con.execute("PRAGMA threads=4"); con.execute("SET memory_limit='6GB'")
    con.register("uft", keep)
    q = f"""
    SELECT u.tech_group AS tech,
           DATE_TRUNC('month', date::DATE) AS ym,
           SUM(p.assigned_power_mw * p.mtu_minutes / 60.0) / 1000.0 AS q1_gwh
    FROM '{PDBC}' p JOIN uft u USING (unit_code)
    WHERE date::DATE >= DATE '{START}' AND date::DATE < DATE '{END}'
      AND assigned_power_mw > 0
    GROUP BY 1, 2
    """
    df = con.execute(q).df()
    df["ym"] = pd.to_datetime(df["ym"])
    return df


def build_q1_qfinal_by_tech():
    """Monthly fleet GWh for q_1 (PDBC) and q_final (PHF max session) per (tech, month).
    Big-4 pivotal-firm aggregate."""
    PHF = REPO / "data" / "processed" / "omie" / "mercado_intradiario_subastas" / "programas" / "phf_all.parquet"
    units = firm_unit_panel(csv_path=str(UNITS_CSV), scheme="short", mode="primary_owner")
    keep = units[units["parent"].isin(list(PIVOTAL))][["unit_code", "parent", "tech_group"]]
    con = duckdb.connect(); con.execute("PRAGMA threads=4"); con.execute("SET memory_limit='6GB'")
    con.register("uft", keep)
    rows = []
    months = pd.date_range(START, END, freq="MS")
    for i in range(len(months) - 1):
        m0, m1 = months[i].date(), months[i + 1].date()
        q1 = con.execute(f"""
            SELECT u.tech_group AS tech,
                   SUM(p.assigned_power_mw * p.mtu_minutes / 60.0) / 1000.0 AS q1_gwh
            FROM '{PDBC}' p JOIN uft u USING (unit_code)
            WHERE date::DATE >= DATE '{m0}' AND date::DATE < DATE '{m1}'
              AND assigned_power_mw > 0
            GROUP BY 1
        """).df()
        qf = con.execute(f"""
            WITH lat AS (
                SELECT date::DATE AS d, period, unit_code,
                       assigned_power_mw, mtu_minutes,
                       ROW_NUMBER() OVER (PARTITION BY date::DATE, period, unit_code
                                          ORDER BY session_number DESC) AS rn
                FROM '{PHF}'
                WHERE date::DATE >= DATE '{m0}' AND date::DATE < DATE '{m1}'
                  AND assigned_power_mw IS NOT NULL
            )
            SELECT u.tech_group AS tech,
                   SUM(lat.assigned_power_mw * lat.mtu_minutes / 60.0) / 1000.0 AS qfinal_gwh
            FROM lat JOIN uft u USING (unit_code)
            WHERE lat.rn = 1
            GROUP BY 1
        """).df()
        m = q1.merge(qf, on="tech", how="outer").fillna(0.0)
        m["ym"] = pd.Timestamp(m0); rows.append(m)
        print(f"  {m0}: {len(m)} techs", flush=True)
    df = pd.concat(rows, ignore_index=True)
    return df


def plot_q1_vs_qfinal_by_tech(df: pd.DataFrame):
    """Per-tech CCGT panel: monthly q_1 (solid) vs q_final (dashed), GWh."""
    techs = ["CCGT", "Nuclear", "Hydro", "Hydro_pump", "Wind", "Solar PV", "Cogen", "Hybrid_RES"]
    techs = [t for t in techs if t in df["tech"].unique()]
    fig, axes = plt.subplots(2, 4, figsize=(15, 7), sharey=False, sharex=True)
    for ax, tech in zip(axes.flatten(), techs):
        sub = df[df["tech"] == tech].sort_values("ym")
        ax.plot(sub["ym"], sub["q1_gwh"], marker="o", color="C0",
                lw=1.5, ms=3, label="$q_1$ (DA cleared)")
        ax.plot(sub["ym"], sub["qfinal_gwh"], marker="s", color="C3",
                lw=1.5, ms=3, ls="--", label="$q_{final}$ (PHF)")
        ax.fill_between(sub["ym"], sub["q1_gwh"], sub["qfinal_gwh"],
                          where=sub["qfinal_gwh"] > sub["q1_gwh"],
                          alpha=0.15, color="C3", interpolate=True,
                          label="$q_{final} > q_1$" if tech == techs[0] else None)
        ax.fill_between(sub["ym"], sub["q1_gwh"], sub["qfinal_gwh"],
                          where=sub["qfinal_gwh"] < sub["q1_gwh"],
                          alpha=0.15, color="C0", interpolate=True,
                          label="$q_1 > q_{final}$" if tech == techs[0] else None)
        ax.axvline(MTU15_IDA, color="gray", ls=":", lw=0.9)
        ax.axvline(BLACKOUT,  color="black", ls="-.", lw=0.9)
        ax.axvline(MTU15_DA,  color="red",   ls="--", lw=1.0)
        ax.set_title(tech, fontsize=10)
        ax.grid(alpha=0.3)
        ax.tick_params(labelsize=7)
        ax.xaxis.set_major_locator(mdates.MonthLocator(interval=3))
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
        for lbl in ax.get_xticklabels():
            lbl.set_rotation(45); lbl.set_ha("right")
        ax.set_ylabel("GWh / month", fontsize=8)
    axes[0, 0].legend(loc="upper left", fontsize=7, frameon=False)
    fig.suptitle("Monthly fleet volume per technology: $q_1$ (PDBC, DA-cleared) vs $q_{final}$ (PHF, post-IDA + REE-RT). Red shade = REE rescues volume; blue shade = REE displaces volume.",
                 fontsize=11, y=1.00)
    fig.tight_layout(rect=[0, 0, 1, 0.95])
    out = FIGDIR / "fig_q1_vs_qfinal_by_tech"
    for ext in ("pdf", "png"):
        fig.savefig(f"{out}.{ext}", bbox_inches="tight", dpi=130 if ext == "png" else None)
    plt.close(fig)
    print(f"saved {out}.pdf")


def build_q1_qfinal_ccgt_by_firm():
    """Monthly fleet GWh for q_1 (PDBC) and q_final (PHF max session) per CCGT firm."""
    PHF = REPO / "data" / "processed" / "omie" / "mercado_intradiario_subastas" / "programas" / "phf_all.parquet"
    units = firm_unit_panel(csv_path=str(UNITS_CSV), scheme="short", mode="primary_owner")
    keep = units[units["parent"].isin(list(PIVOTAL)) & (units["tech_group"] == "CCGT")][
        ["unit_code", "parent"]]
    con = duckdb.connect(); con.execute("PRAGMA threads=4"); con.execute("SET memory_limit='6GB'")
    con.register("ccgt_units", keep)
    rows = []
    months = pd.date_range(START, END, freq="MS")
    for i in range(len(months) - 1):
        m0, m1 = months[i].date(), months[i + 1].date()
        # q_1 from PDBC
        q1 = con.execute(f"""
            SELECT u.parent AS firm,
                   SUM(p.assigned_power_mw * p.mtu_minutes / 60.0) / 1000.0 AS q1_gwh
            FROM '{PDBC}' p JOIN ccgt_units u USING (unit_code)
            WHERE date::DATE >= DATE '{m0}' AND date::DATE < DATE '{m1}'
              AND assigned_power_mw > 0
            GROUP BY 1
        """).df()
        # q_final from PHF (latest session per date×period×unit)
        qf = con.execute(f"""
            WITH lat AS (
                SELECT date::DATE AS d, period, unit_code,
                       assigned_power_mw, mtu_minutes,
                       ROW_NUMBER() OVER (PARTITION BY date::DATE, period, unit_code
                                          ORDER BY session_number DESC) AS rn
                FROM '{PHF}'
                WHERE date::DATE >= DATE '{m0}' AND date::DATE < DATE '{m1}'
                  AND assigned_power_mw IS NOT NULL
            )
            SELECT u.parent AS firm,
                   SUM(lat.assigned_power_mw * lat.mtu_minutes / 60.0) / 1000.0 AS qfinal_gwh
            FROM lat JOIN ccgt_units u USING (unit_code)
            WHERE lat.rn = 1
            GROUP BY 1
        """).df()
        m = q1.merge(qf, on="firm", how="outer").fillna(0.0)
        m["ym"] = pd.Timestamp(m0); rows.append(m)
        print(f"  {m0}: {len(m)} firms", flush=True)
    df = pd.concat(rows, ignore_index=True)
    return df


def plot_q1_vs_qfinal(df: pd.DataFrame):
    """Per-firm CCGT panel: monthly q_1 (solid) vs q_final (dashed), GWh."""
    firms = ["IB", "GE", "GN", "HC"]
    fig, axes = plt.subplots(1, 4, figsize=(15, 4), sharey=False)
    for ax, firm in zip(axes, firms):
        sub = df[df["firm"] == firm].sort_values("ym")
        ax.plot(sub["ym"], sub["q1_gwh"], marker="o", color="C0",
                lw=1.6, ms=3.5, label="$q_1$ (DA cleared)")
        ax.plot(sub["ym"], sub["qfinal_gwh"], marker="s", color="C3",
                lw=1.6, ms=3.5, ls="--", label="$q_{final}$ (PHF, post-IDA + RT)")
        ax.fill_between(sub["ym"], sub["q1_gwh"], sub["qfinal_gwh"],
                          where=sub["qfinal_gwh"] > sub["q1_gwh"],
                          alpha=0.15, color="C3", interpolate=True)
        ax.axvline(MTU15_IDA, color="gray", ls=":", lw=0.9)
        ax.axvline(BLACKOUT,  color="black", ls="-.", lw=0.9)
        ax.axvline(MTU15_DA,  color="red",   ls="--", lw=1.0)
        ax.set_title(firm, fontsize=10)
        ax.grid(alpha=0.3)
        ax.tick_params(labelsize=7)
        ax.xaxis.set_major_locator(mdates.MonthLocator(interval=3))
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
        for lbl in ax.get_xticklabels():
            lbl.set_rotation(45); lbl.set_ha("right")
        ax.set_ylabel("GWh / month", fontsize=8)
    axes[0].legend(loc="upper left", fontsize=8, frameon=False)
    fig.suptitle("CCGT monthly fleet volume: $q_1$ (DA-cleared, PDBC) vs $q_{final}$ (PHF, latest IDA + REE-RT). Shaded area = REE post-clearing intervention.",
                 fontsize=11, y=1.02)
    fig.tight_layout()
    out = FIGDIR / "fig_q1_vs_qfinal_ccgt"
    for ext in ("pdf", "png"):
        fig.savefig(f"{out}.{ext}", bbox_inches="tight", dpi=130 if ext == "png" else None)
    plt.close(fig)
    print(f"saved {out}.pdf")


def plot_fleet_total_by_tech(df: pd.DataFrame):
    """Small-multiples: monthly fleet total q_1 (GWh) per tech."""
    techs = ["CCGT", "Nuclear", "Hydro", "Hydro_pump", "Wind", "Solar PV", "Cogen", "Hybrid_RES"]
    fig, axes = plt.subplots(2, 4, figsize=(15, 6), sharex=True)
    for ax, tech in zip(axes.flatten(), techs):
        sub = df[df["tech"] == tech].sort_values("ym")
        ax.plot(sub["ym"], sub["q1_gwh"], marker="o", color="C0", lw=1.4, ms=3.5)
        ax.axvline(MTU15_IDA, color="gray", ls=":", lw=0.9)
        ax.axvline(BLACKOUT,  color="black", ls="-.", lw=0.9)
        ax.axvline(MTU15_DA,  color="red",   ls="--", lw=1.0)
        ax.set_title(tech, fontsize=10)
        ax.grid(alpha=0.3)
        ax.tick_params(labelsize=7)
        ax.xaxis.set_major_locator(mdates.MonthLocator(interval=3))
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
        for lbl in ax.get_xticklabels():
            lbl.set_rotation(45); lbl.set_ha("right")
        ax.set_ylabel("GWh / month", fontsize=8)
    fig.suptitle("DA-cleared volume by technology, pivotal-firm aggregate, monthly GWh (vertical guides: MTU15-IDA, Blackout, MTU15-DA)",
                 fontsize=11, y=1.00)
    fig.tight_layout(rect=[0, 0, 1, 0.95])
    out = FIGDIR / "fig_q1_fleet_by_tech_monthly"
    for ext in ("pdf", "png"):
        fig.savefig(f"{out}.{ext}", bbox_inches="tight", dpi=130 if ext == "png" else None)
    plt.close(fig)
    print(f"saved {out}.pdf")


def main():
    df = build_panel()
    df.to_csv(OUTDIR / "q1_by_firm_tech_month.csv", index=False)
    plot_panel(df, "fig_q1_ccgt_monthly", "CCGT q_1 (DA-cleared MWh / unit-day-hour) — by firm × hour-class")

    tech_total = build_fleet_total_by_tech()
    tech_total.to_csv(OUTDIR / "q1_fleet_total_by_tech_month.csv", index=False)

    print("\n=== Pivotal-firm fleet DA-cleared (GWh/month) by tech ===")
    pv = tech_total.pivot_table(index="ym", columns="tech", values="q1_gwh").round(0)
    print(pv.to_string())

    plot_fleet_total_by_tech(tech_total)

    print("\nbuilding q_1 vs q_final CCGT-by-firm panel...")
    q1qf = build_q1_qfinal_ccgt_by_firm()
    q1qf.to_csv(OUTDIR / "q1_vs_qfinal_ccgt.csv", index=False)
    plot_q1_vs_qfinal(q1qf)

    print("\nbuilding q_1 vs q_final per-tech panel...")
    q1qf_t = build_q1_qfinal_by_tech()
    q1qf_t.to_csv(OUTDIR / "q1_vs_qfinal_by_tech.csv", index=False)
    plot_q1_vs_qfinal_by_tech(q1qf_t)
    print(q1qf_t.pivot_table(index="ym", columns="tech",
                              values=["q1_gwh", "qfinal_gwh"]).round(0).to_string())

    # Same-cal-month comparison table for the LaTeX include
    rows = []
    for (m_pre, m_post, label) in [("2024-12", "2025-12", "Dec"),
                                     ("2024-11", "2025-11", "Nov"),
                                     ("2024-08", "2025-08", "Aug")]:
        for tech in ("CCGT", "Nuclear", "Hydro", "Hydro_pump", "Wind", "Solar PV"):
            try:
                pre  = float(pv.loc[pd.Timestamp(m_pre + "-01"), tech])
                post = float(pv.loc[pd.Timestamp(m_post + "-01"), tech])
            except KeyError:
                continue
            if not (np.isfinite(pre) and np.isfinite(post)) or pre == 0:
                continue
            rows.append({"comparison": label, "tech": tech,
                          "pre_gwh": round(pre), "post_gwh": round(post),
                          "pct": round(100 * (post / pre - 1), 1)})
    tbl = pd.DataFrame(rows, columns=["comparison", "tech", "pre_gwh", "post_gwh", "pct"])
    tbl.to_csv(OUTDIR / "same_cal_month_comparison.csv", index=False)
    print("\n=== same-cal-month table ===")
    print(tbl.to_string(index=False))

    # Emit a LaTeX-ready tabular
    tex_path = OUTDIR / "tab_q1_drop_by_tech.tex"
    techs_order = ["CCGT", "Nuclear", "Hydro", "Hydro_pump", "Wind", "Solar PV"]
    lines = [r"\begin{tabular}{l r r r r r r r r r}",
             r"\toprule",
             r" & \multicolumn{3}{c}{Dec 2024 $\to$ Dec 2025} & \multicolumn{3}{c}{Nov 2024 $\to$ Nov 2025} & \multicolumn{3}{c}{Aug 2024 $\to$ Aug 2025} \\",
             r"\cmidrule(lr){2-4}\cmidrule(lr){5-7}\cmidrule(lr){8-10}",
             r"Tech & Pre & Post & \% & Pre & Post & \% & Pre & Post & \% \\",
             r"\midrule"]
    for tech in techs_order:
        cells = [tech]
        for label in ("Dec", "Nov", "Aug"):
            row = tbl[(tbl["tech"] == tech) & (tbl["comparison"] == label)]
            if len(row):
                cells += [f"{int(row.iloc[0]['pre_gwh'])}",
                           f"{int(row.iloc[0]['post_gwh'])}",
                           f"{row.iloc[0]['pct']:+.0f}\\%"]
            else:
                cells += ["---", "---", "---"]
        # Bold CCGT row
        if tech == "CCGT":
            cells = [r"\textbf{" + c + "}" for c in cells]
        tech_disp = tech.replace("_", r"\_")
        cells[0] = (r"\textbf{" + tech_disp + "}") if tech == "CCGT" else tech_disp
        lines.append(" & ".join(cells) + r" \\")
    lines += [r"\bottomrule", r"\end{tabular}"]
    tex_path.write_text("\n".join(lines))
    print(f"\nLaTeX table at {tex_path}")


if __name__ == "__main__":
    main()
