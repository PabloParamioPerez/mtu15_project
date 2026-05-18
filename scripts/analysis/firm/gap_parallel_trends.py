# STATUS: ALIVE
# LAST-AUDIT: 2026-05-16
# FEEDS: provisional.tex §13 (q_1 vs q_final parallel-trends across techs)
# CLAIM: Test parallel-trends pre-blackout on the gap g = q_final - q_baseline
#        across pivotal-firm technologies. Two definitions of q_baseline:
#         (i) PDBC (DA auction-cleared) — gap includes bilaterals + IDA + RT
#        (ii) PDBF (DA + bilaterals)    — gap is IDA + RT only (Path A
#             definition; the clean test).
#        Pre-blackout window: 2024-01 → 2025-04-27.

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
from mtu.classification.units import firm_unit_panel, TREATMENT_PARENTS_SHORT as PIVOTAL  # noqa: E402

PDBC = REPO / "data" / "processed" / "omie" / "mercado_diario" / "programas" / "pdbc_all.parquet"
PDBF = REPO / "data" / "processed" / "omie" / "mercado_diario" / "programas" / "pdbf_all.parquet"
PHF  = REPO / "data" / "processed" / "omie" / "mercado_intradiario_subastas" / "programas" / "phf_all.parquet"
UNITS_CSV = REPO / "data" / "external" / "omie_reference" / "lista_unidades.csv"

OUTDIR = REPO / "results" / "regressions" / "firm" / "q1_drop"
OUTDIR.mkdir(parents=True, exist_ok=True)
FIGDIR = REPO / "figures" / "working"

START = "2024-01-01"
END   = "2026-03-01"
MTU15_IDA = pd.Timestamp("2025-03-19")
BLACKOUT  = pd.Timestamp("2025-04-28")
MTU15_DA  = pd.Timestamp("2025-10-01")

PRE_END   = pd.Timestamp("2025-04-28")
PRE_BASE_END = pd.Timestamp("2024-12-31")  # for normalisation baseline

TECHS = ("CCGT", "Nuclear", "Hydro", "Hydro_pump", "Wind", "Solar PV")
TECH_COLORS = {
    "CCGT": "tab:red", "Nuclear": "tab:purple",
    "Hydro": "tab:blue", "Hydro_pump": "tab:cyan",
    "Wind": "tab:green", "Solar PV": "tab:orange",
}


def build_panel():
    units = firm_unit_panel(csv_path=str(UNITS_CSV), scheme="short", mode="primary_owner")
    keep = units[units["parent"].isin(list(PIVOTAL))][["unit_code", "parent", "tech_group"]]
    con = duckdb.connect(); con.execute("PRAGMA threads=4"); con.execute("SET memory_limit='6GB'")
    con.register("uft", keep)
    rows = []
    months = pd.date_range(START, END, freq="MS")
    for i in range(len(months) - 1):
        m0, m1 = months[i].date(), months[i + 1].date()
        pdbc = con.execute(f"""
            SELECT u.tech_group AS tech,
                   SUM(p.assigned_power_mw * p.mtu_minutes / 60.0) / 1000.0 AS pdbc_gwh
            FROM '{PDBC}' p JOIN uft u USING (unit_code)
            WHERE date::DATE >= DATE '{m0}' AND date::DATE < DATE '{m1}'
              AND assigned_power_mw > 0
            GROUP BY 1
        """).df()
        pdbf = con.execute(f"""
            SELECT u.tech_group AS tech,
                   SUM(p.assigned_power_mw * p.mtu_minutes / 60.0) / 1000.0 AS pdbf_gwh
            FROM '{PDBF}' p JOIN uft u USING (unit_code)
            WHERE date::DATE >= DATE '{m0}' AND date::DATE < DATE '{m1}'
              AND assigned_power_mw IS NOT NULL
            GROUP BY 1
        """).df()
        phf = con.execute(f"""
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
                   SUM(lat.assigned_power_mw * lat.mtu_minutes / 60.0) / 1000.0 AS phf_gwh
            FROM lat JOIN uft u USING (unit_code)
            WHERE lat.rn = 1
            GROUP BY 1
        """).df()
        m = (pdbc.merge(pdbf, on="tech", how="outer")
                  .merge(phf,  on="tech", how="outer")
                  .fillna(0.0))
        m["ym"] = pd.Timestamp(m0); rows.append(m)
        print(f"  {m0}: {len(m)} techs", flush=True)
    df = pd.concat(rows, ignore_index=True)
    df["gap_vs_pdbc"] = df["phf_gwh"] - df["pdbc_gwh"]  # bilaterals + IDA + RT
    df["gap_vs_pdbf"] = df["phf_gwh"] - df["pdbf_gwh"]  # IDA + RT only (Path A)
    return df


def plot_gap_lines(df: pd.DataFrame, col: str, label: str, fname: str):
    """One panel: gap (GWh) per tech vs month, all on the same axis."""
    fig, ax = plt.subplots(figsize=(11, 5))
    for tech in TECHS:
        sub = df[df["tech"] == tech].sort_values("ym")
        if len(sub) == 0:
            continue
        ax.plot(sub["ym"], sub[col], marker="o", lw=1.6, ms=3.5,
                color=TECH_COLORS[tech], label=tech)
    ax.axvline(MTU15_IDA, color="gray", ls=":", lw=0.9, label="MTU15-IDA")
    ax.axvline(BLACKOUT,  color="black", ls="-.", lw=0.9, label="Blackout")
    ax.axvline(MTU15_DA,  color="red",   ls="--", lw=1.0, label="MTU15-DA")
    ax.axhline(0, color="grey", lw=0.4)
    ax.legend(loc="upper left", ncol=2, fontsize=8, frameon=False)
    ax.set_ylabel(f"{label} (GWh / month)")
    ax.set_title(f"Gap {label}, Big-4 pivotal aggregate, monthly")
    ax.grid(alpha=0.3)
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
    plt.setp(ax.get_xticklabels(), rotation=45, ha="right")
    plt.tight_layout()
    out = FIGDIR / fname
    plt.savefig(f"{out}.pdf", bbox_inches="tight")
    plt.savefig(f"{out}.png", bbox_inches="tight", dpi=130)
    plt.close(fig)
    print(f"saved {out}.pdf")


def plot_stacked_decomposition(df: pd.DataFrame, fname: str):
    """Per-tech small multiples: stacked area decomposition of monthly fleet
    volume into (i) DA-cleared (PDBC), (ii) Bilaterals (PDBF - PDBC),
    (iii) IDA + RT2 (PHF - PDBF). Total = PHF."""
    fig, axes = plt.subplots(2, 3, figsize=(15, 7), sharex=True, sharey=False)
    df = df.copy()
    df["bilat_gwh"] = (df["pdbf_gwh"] - df["pdbc_gwh"]).clip(lower=0)
    df["ida_rt_gwh"] = (df["phf_gwh"] - df["pdbf_gwh"]).clip(lower=0)
    for ax, tech in zip(axes.flatten(), TECHS):
        sub = df[df["tech"] == tech].sort_values("ym")
        if len(sub) == 0:
            continue
        x = sub["ym"]
        da = sub["pdbc_gwh"].values
        bi = sub["bilat_gwh"].values
        ida_rt = sub["ida_rt_gwh"].values
        ax.fill_between(x, 0, da, color="C0", alpha=0.7, label="DA cleared (PDBC)")
        ax.fill_between(x, da, da + bi, color="C2", alpha=0.7,
                          label="Bilaterals (PDBF $-$ PDBC)")
        ax.fill_between(x, da + bi, da + bi + ida_rt, color="C3", alpha=0.7,
                          label="IDA + REE-RT (PHF $-$ PDBF)")
        ax.plot(x, sub["phf_gwh"], color="black", lw=1.2, label="$q_{final}$ (PHF total)")
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
    fig.suptitle("Three-component decomposition of monthly fleet volume per technology: DA (PDBC) + Bilaterals + IDA+REE-RT = $q_{final}$ (PHF)",
                 fontsize=11, y=1.00)
    fig.tight_layout(rect=[0, 0, 1, 0.95])
    out = FIGDIR / fname
    plt.savefig(f"{out}.pdf", bbox_inches="tight")
    plt.savefig(f"{out}.png", bbox_inches="tight", dpi=130)
    plt.close(fig)
    print(f"saved {out}.pdf")


def plot_gap_normalised(df: pd.DataFrame, col: str, label: str, fname: str):
    """One panel: gap per tech normalised to its 2024 mean (deviation, GWh).
    Parallel-trends test: lines should overlap in pre-blackout window."""
    fig, ax = plt.subplots(figsize=(11, 5))
    for tech in TECHS:
        sub = df[df["tech"] == tech].sort_values("ym").copy()
        if len(sub) == 0:
            continue
        baseline = sub[sub["ym"] <= PRE_BASE_END][col].mean()
        sub["dev"] = sub[col] - baseline
        ax.plot(sub["ym"], sub["dev"], marker="o", lw=1.6, ms=3.5,
                color=TECH_COLORS[tech], label=f"{tech}  (2024 base={baseline:.0f})")
    ax.axvline(MTU15_IDA, color="gray", ls=":", lw=0.9)
    ax.axvline(BLACKOUT,  color="black", ls="-.", lw=0.9)
    ax.axvline(MTU15_DA,  color="red",   ls="--", lw=1.0)
    ax.axhline(0, color="grey", lw=0.4)
    ax.legend(loc="upper left", ncol=2, fontsize=8, frameon=False)
    ax.set_ylabel(f"{label} deviation from 2024 mean (GWh / month)")
    ax.set_title(f"Parallel-trends test on the post-clearing gap ({label}): deviation from each tech's 2024 mean")
    ax.grid(alpha=0.3)
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
    plt.setp(ax.get_xticklabels(), rotation=45, ha="right")
    plt.tight_layout()
    out = FIGDIR / fname
    plt.savefig(f"{out}.pdf", bbox_inches="tight")
    plt.savefig(f"{out}.png", bbox_inches="tight", dpi=130)
    plt.close(fig)
    print(f"saved {out}.pdf")


def pretrend_table(df: pd.DataFrame, col: str):
    rows = []
    for tech in TECHS:
        sub = df[df["tech"] == tech].sort_values("ym").copy()
        if sub.empty: continue
        pre  = sub[(sub["ym"] >= "2024-01-01") & (sub["ym"] < "2025-01-01")][col]
        win  = sub[(sub["ym"] >= "2025-01-01") & (sub["ym"] < "2025-04-28")][col]
        post = sub[(sub["ym"] >= "2025-05-01")][col]
        rows.append({
            "tech": tech,
            "2024_mean": round(pre.mean(), 1),
            "2024_std":  round(pre.std(), 1),
            "2025_pre_blackout_mean": round(win.mean(), 1),
            "2025_pre_blackout_std":  round(win.std(), 1),
            "post_blackout_mean":     round(post.mean(), 1),
            "post_blackout_std":      round(post.std(), 1),
            "pre_to_pre_shift": round(win.mean() - pre.mean(), 1),
            "pre_to_post_shift": round(post.mean() - pre.mean(), 1),
        })
    return pd.DataFrame(rows)


def main():
    df = build_panel()
    df.to_csv(OUTDIR / "gap_by_tech.csv", index=False)

    print("\n=== Pre-blackout pre-trends on gap = PHF − PDBC (incl. bilaterals + IDA + RT) ===")
    print(pretrend_table(df, "gap_vs_pdbc").to_string(index=False))
    print("\n=== Pre-blackout pre-trends on gap = PHF − PDBF (IDA + RT only, Path A) ===")
    print(pretrend_table(df, "gap_vs_pdbf").to_string(index=False))

    plot_stacked_decomposition(df, "fig_q_components_by_tech")
    plot_gap_lines     (df, "gap_vs_pdbc", "$q_{final} - q_1$ (PHF $-$ PDBC)", "fig_gap_lines_pdbc")
    plot_gap_lines     (df, "gap_vs_pdbf", "$q_{final} - q_{PDBF}$ (IDA $+$ RT only)", "fig_gap_lines_pdbf")
    plot_gap_normalised(df, "gap_vs_pdbc", "$q_{final} - q_1$ (PHF $-$ PDBC)", "fig_gap_dev_pdbc")
    plot_gap_normalised(df, "gap_vs_pdbf", "$q_{final} - q_{PDBF}$ (IDA $+$ RT only)", "fig_gap_dev_pdbf")


if __name__ == "__main__":
    main()
