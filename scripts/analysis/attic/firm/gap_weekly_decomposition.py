# STATUS: ALIVE
# LAST-AUDIT: 2026-05-16
# FEEDS: provisional.tex §13 (weekly three-component decomposition)
# CLAIM: Weekly fleet volume decomposed into (i) DA cleared (PDBC),
#        (ii) Bilaterals (PDBF − PDBC), (iii) IDA + REE-RT (PHF − PDBF).
#        Per-tech and per-CCGT-firm versions. Replaces several monthly
#        figures.

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

OUTDIR = REPO / "results" / "regressions" / "firm" / "gap_weekly"
OUTDIR.mkdir(parents=True, exist_ok=True)
FIGDIR = REPO / "figures" / "working"

START = "2023-01-01"
END   = "2026-01-09"
IDA_REFORM = pd.Timestamp("2024-06-14")
MTU15_IDA = pd.Timestamp("2025-03-19")
BLACKOUT  = pd.Timestamp("2025-04-28")
MTU15_DA  = pd.Timestamp("2025-10-01")

TECHS = ("CCGT", "Nuclear", "Hydro", "Hydro_pump", "Wind", "Solar PV")
TECH_COLORS = {
    "DA cleared (PDBC)":          "tab:blue",
    "Bilaterals (PDBF $-$ PDBC)": "tab:green",
    "IDA $+$ REE-RT (PHF $-$ PDBF)": "tab:red",
}


def build_weekly_panel(group_by_firm: bool = False):
    """Stream PDBC + PDBF + PHF in MONTHLY chunks (to keep memory bounded
    but avoid one-scan-per-week). Aggregate per chunk to (group, day), then
    resample to weekly in pandas at the end."""
    units = firm_unit_panel(csv_path=str(UNITS_CSV), scheme="short", mode="primary_owner")
    if group_by_firm:
        keep = units[units["parent"].isin(list(PIVOTAL)) & (units["tech_group"] == "CCGT")][
            ["unit_code", "parent"]].rename(columns={"parent": "group_key"})
    else:
        keep = units[units["parent"].isin(list(PIVOTAL))][["unit_code", "tech_group"]
            ].rename(columns={"tech_group": "group_key"})
    con = duckdb.connect(); con.execute("PRAGMA threads=4"); con.execute("SET memory_limit='6GB'")
    con.register("uft", keep)
    months = pd.date_range(START, END, freq="MS")
    rows = []
    for i in range(len(months) - 1):
        m0, m1 = months[i].date(), months[i + 1].date()
        # Daily aggregates per group, then resample to weekly outside
        pdbc = con.execute(f"""
            SELECT u.group_key, p.date::DATE AS day,
                   SUM(p.assigned_power_mw * p.mtu_minutes / 60.0) / 1000.0 AS pdbc_gwh
            FROM '{PDBC}' p JOIN uft u USING (unit_code)
            WHERE date::DATE >= DATE '{m0}' AND date::DATE < DATE '{m1}'
              AND assigned_power_mw > 0
            GROUP BY 1, 2
        """).df()
        pdbf = con.execute(f"""
            SELECT u.group_key, p.date::DATE AS day,
                   SUM(p.assigned_power_mw * p.mtu_minutes / 60.0) / 1000.0 AS pdbf_gwh
            FROM '{PDBF}' p JOIN uft u USING (unit_code)
            WHERE date::DATE >= DATE '{m0}' AND date::DATE < DATE '{m1}'
              AND assigned_power_mw IS NOT NULL
            GROUP BY 1, 2
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
            SELECT u.group_key, lat.d AS day,
                   SUM(lat.assigned_power_mw * lat.mtu_minutes / 60.0) / 1000.0 AS phf_gwh
            FROM lat JOIN uft u USING (unit_code)
            WHERE lat.rn = 1
            GROUP BY 1, 2
        """).df()
        m = (pdbc.merge(pdbf, on=["group_key", "day"], how="outer")
                  .merge(phf,  on=["group_key", "day"], how="outer")
                  .fillna(0.0))
        rows.append(m)
        print(f"  {m0}: {len(m)} (group, day) rows", flush=True)
    df = pd.concat(rows, ignore_index=True)
    df["day"] = pd.to_datetime(df["day"])
    # Resample to weekly (Monday-anchored)
    df["week_start"] = df["day"] - pd.to_timedelta(df["day"].dt.weekday, unit="D")
    weekly = df.groupby(["group_key", "week_start"], as_index=False)[
        ["pdbc_gwh", "pdbf_gwh", "phf_gwh"]].sum()
    weekly["bilat_gwh"]  = (weekly["pdbf_gwh"] - weekly["pdbc_gwh"]).clip(lower=0)
    weekly["ida_rt_gwh"] = (weekly["phf_gwh"]  - weekly["pdbf_gwh"]).clip(lower=0)
    return weekly


def plot_stacked(df: pd.DataFrame, groups: list, group_label: str, fname: str, ncols: int = 3):
    nrows = int(np.ceil(len(groups) / ncols))
    fig, axes = plt.subplots(nrows, ncols, figsize=(5 * ncols, 3.0 * nrows),
                              sharex=True, sharey=False)
    axes = np.atleast_2d(axes)
    for ax, gk in zip(axes.flatten(), groups):
        sub = df[df["group_key"] == gk].sort_values("week_start")
        if len(sub) == 0:
            ax.set_visible(False); continue
        x = sub["week_start"]
        da = sub["pdbc_gwh"].values
        bi = sub["bilat_gwh"].values
        ida_rt = sub["ida_rt_gwh"].values
        ax.fill_between(x, 0, da, color="tab:blue", alpha=0.7,
                          label="DA cleared (PDBC)")
        ax.fill_between(x, da, da + bi, color="tab:green", alpha=0.7,
                          label="Bilaterals (PDBF $-$ PDBC)")
        ax.fill_between(x, da + bi, da + bi + ida_rt, color="tab:red", alpha=0.7,
                          label="IDA $+$ REE-RT (PHF $-$ PDBF)")
        ax.plot(x, sub["phf_gwh"], color="black", lw=0.9, label="$q_{final}$ (PHF total)")
        ax.axvline(IDA_REFORM, color="purple", ls=":", lw=0.9)
        ax.axvline(MTU15_IDA, color="gray", ls=":", lw=0.9)
        ax.axvline(BLACKOUT,  color="black", ls="-.", lw=0.9)
        ax.axvline(MTU15_DA,  color="red",   ls="--", lw=1.0)
        ax.set_title(gk, fontsize=10)
        ax.grid(alpha=0.3)
        ax.tick_params(labelsize=7)
        ax.xaxis.set_major_locator(mdates.MonthLocator(interval=3))
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
        for lbl in ax.get_xticklabels():
            lbl.set_rotation(45); lbl.set_ha("right")
        ax.set_ylabel("GWh / week", fontsize=8)
    # one global legend
    for ax in axes.flatten()[len(groups):]:
        ax.set_visible(False)
    axes.flatten()[0].legend(loc="upper left", fontsize=7, frameon=False)
    fig.suptitle(f"Three-component decomposition (WEEKLY) per {group_label}: DA + Bilaterals + IDA+REE-RT = $q_{{final}}$.",
                 fontsize=11, y=1.00)
    fig.tight_layout(rect=[0, 0, 1, 0.97])
    out = FIGDIR / fname
    plt.savefig(f"{out}.pdf", bbox_inches="tight")
    plt.savefig(f"{out}.png", bbox_inches="tight", dpi=130)
    plt.close(fig)
    print(f"saved {out}.pdf")


def main():
    print("=== weekly per-tech panel ===")
    df_t = build_weekly_panel(group_by_firm=False)
    df_t.to_csv(OUTDIR / "weekly_decomp_by_tech.csv", index=False)
    plot_stacked(df_t, list(TECHS), "technology",
                  "fig_q_components_by_tech_weekly", ncols=3)

    print("=== weekly per-firm CCGT panel ===")
    df_f = build_weekly_panel(group_by_firm=True)
    df_f.to_csv(OUTDIR / "weekly_decomp_by_firm_ccgt.csv", index=False)
    plot_stacked(df_f, ["IB", "GE", "GN", "HC"], "CCGT firm",
                  "fig_q_components_ccgt_by_firm_weekly", ncols=2)


if __name__ == "__main__":
    main()
