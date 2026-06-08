# STATUS: ALIVE
# LAST-AUDIT: 2026-06-09
# FEEDS: thesis/presentations/.../slides.tex --- "Changes in quantity" body slide.
#        Per-tech weekly program cascade with a supply/demand mirror layout:
#        up-redispatch ribbons stack above 0, down-redispatch ribbons stack
#        below 0. The PHF line is the signed (net) sum.
#
# Chronological cascade (per docs/notes/SPANISH_MARKET_STRUCTURE.md §1):
#   PDBC -> +Bilaterals -> +IDA1/2/3 (+IDA other pre-2024-06) -> +Continuous
#         -> +REE residual (Fase 1 + Fase 2 + post-IDA RT visible in PHF)
#   Each program contributes a *positive* part (up-dispatch) above 0 and a
#   *negative* part (down-dispatch / consumption schedule) below 0. The two
#   sides are stacked independently. Sum of all signed contributions = PHF.
#
# Caveats NOT in this figure:
#   - PHFC vs PHF: continuous-market revisions after PHF publication (small).
#   - Real-time balancing (RR, aFRR, mFRR): post-PHF, not in OMIE per-unit.
#
# OUT: figures/working/fig_programs_by_tech_weekly.{pdf,png}

from pathlib import Path
import sys
import duckdb
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from mtu.classification.units import firm_unit_panel  # noqa: E402

PDBC = REPO / "data/processed/omie/mercado_diario/programas/pdbc_all.parquet"
PDBF = REPO / "data/processed/omie/mercado_diario/programas/pdbf_all.parquet"
PIBCI = REPO / "data/processed/omie/mercado_intradiario_subastas/programas/pibci_all.parquet"
PIBCIC = REPO / "data/processed/omie/mercado_intradiario_continuo/programas/pibcic_all.parquet"
PHF = REPO / "data/processed/omie/mercado_intradiario_subastas/programas/phf_all.parquet"
UNITS_CSV = REPO / "data/external/omie_reference/lista_unidades.csv"
OUTDIR = REPO / "figures/working"

START = "2024-01-01"
END = "2026-01-09"
IDA_REFORM = pd.Timestamp("2024-06-14")
MTU15_IDA = pd.Timestamp("2025-03-19")
BLACKOUT = pd.Timestamp("2025-04-28")
MTU15_DA = pd.Timestamp("2025-10-01")

TECHS = ("CCGT", "Nuclear", "Hydro", "Hydro_pump", "Wind", "Solar PV")
TECH_LABELS = {
    "CCGT":       "Gas (CCGT)",
    "Nuclear":    "Nuclear",
    "Hydro":      "Hydro (large)",
    "Hydro_pump": "Pumped-storage hydro",
    "Wind":       "Wind",
    "Solar PV":   "Solar PV",
}

PROGRAM_COLS = ["DA cleared", "Bilaterals", "IDA1", "IDA2", "IDA3",
                "IDA other", "Continuous", "REE residual"]


def build_weekly(con, units_df, techs):
    con.register("uft", units_df)

    def agg_pos_neg(parquet, where=""):
        """Per-tech, per-day positive AND negative sums (GWh, signed)."""
        q = f"""
        SELECT u.tech, CAST(p.date AS DATE) AS d,
               SUM(GREATEST(p.assigned_power_mw, 0) * p.mtu_minutes / 60.0) / 1000.0 AS pos_gwh,
               SUM(LEAST(p.assigned_power_mw, 0)    * p.mtu_minutes / 60.0) / 1000.0 AS neg_gwh
        FROM '{parquet}' p JOIN uft u USING (unit_code)
        WHERE date::DATE BETWEEN '{START}' AND '{END}' {where}
        GROUP BY 1, 2
        """
        return con.execute(q).df()

    print("PDBC (DA cleared)...", flush=True)
    pdbc = agg_pos_neg(PDBC).rename(columns={"pos_gwh": "DA cleared_pos",
                                              "neg_gwh": "DA cleared_neg"})
    print("PDBF (DA + bilaterals)...", flush=True)
    pdbf = agg_pos_neg(PDBF).rename(columns={"pos_gwh": "_pdbf_pos",
                                              "neg_gwh": "_pdbf_neg"})
    print("PIBCI per-session (IDA incremental, signed)...", flush=True)
    pibci_raw = con.execute(f"""
        SELECT u.tech, CAST(p.date AS DATE) AS d, p.session_number,
               SUM(GREATEST(p.assigned_power_mw, 0) * p.mtu_minutes / 60.0) / 1000.0 AS pos_gwh,
               SUM(LEAST(p.assigned_power_mw, 0)    * p.mtu_minutes / 60.0) / 1000.0 AS neg_gwh
        FROM '{PIBCI}' p JOIN uft u USING (unit_code)
        WHERE date::DATE BETWEEN '{START}' AND '{END}'
        GROUP BY 1, 2, 3
    """).df()

    def pivot_pibci(col):
        w = pibci_raw.pivot_table(index=["tech", "d"], columns="session_number",
                                    values=col, fill_value=0).reset_index()
        for s in (1, 2, 3):
            if s not in w.columns:
                w[s] = 0.0
        other_cols = [c for c in w.columns if isinstance(c, (int, np.integer)) and c not in (1, 2, 3)]
        other = w[other_cols].sum(axis=1) if other_cols else 0.0
        w = w[["tech", "d", 1, 2, 3]].rename(columns={1: "_ida1", 2: "_ida2", 3: "_ida3"})
        w["_ida_other"] = other
        return w

    pibci_pos = pivot_pibci("pos_gwh").rename(columns={
        "_ida1": "IDA1_pos", "_ida2": "IDA2_pos", "_ida3": "IDA3_pos",
        "_ida_other": "IDA other_pos"})
    pibci_neg = pivot_pibci("neg_gwh").rename(columns={
        "_ida1": "IDA1_neg", "_ida2": "IDA2_neg", "_ida3": "IDA3_neg",
        "_ida_other": "IDA other_neg"})

    print("PIBCIC (continuous market, signed)...", flush=True)
    pibcic = agg_pos_neg(PIBCIC).rename(columns={"pos_gwh": "Continuous_pos",
                                                  "neg_gwh": "Continuous_neg"})

    print("PHF[max session] (post-IDA + REE-RT2 final program, signed); chunked by month...", flush=True)
    months = pd.date_range(START, END, freq="MS")
    phf_chunks = []
    for i in range(len(months) - 1):
        m0, m1 = months[i].date(), months[i + 1].date()
        chunk = con.execute(f"""
            WITH lat AS (
                SELECT u.tech, CAST(p.date AS DATE) AS d, p.period, p.unit_code,
                       p.assigned_power_mw, p.mtu_minutes,
                       ROW_NUMBER() OVER (PARTITION BY CAST(p.date AS DATE), p.period, p.unit_code
                                          ORDER BY p.session_number DESC) AS rn
                FROM '{PHF}' p JOIN uft u USING (unit_code)
                WHERE date::DATE >= DATE '{m0}' AND date::DATE < DATE '{m1}'
                  AND assigned_power_mw IS NOT NULL
            )
            SELECT tech, d,
                   SUM(GREATEST(assigned_power_mw, 0) * mtu_minutes / 60.0) / 1000.0 AS phf_pos,
                   SUM(LEAST(assigned_power_mw, 0)    * mtu_minutes / 60.0) / 1000.0 AS phf_neg
            FROM lat WHERE rn = 1
            GROUP BY 1, 2
        """).df()
        phf_chunks.append(chunk)
        print(f"  PHF {m0}: {len(chunk):,} rows", flush=True)
    phf = pd.concat(phf_chunks, ignore_index=True) if phf_chunks else pd.DataFrame(
        columns=["tech", "d", "phf_pos", "phf_neg"])

    daily = (pdbc.merge(pdbf, on=["tech", "d"], how="outer")
                  .merge(pibci_pos, on=["tech", "d"], how="outer")
                  .merge(pibci_neg, on=["tech", "d"], how="outer")
                  .merge(pibcic, on=["tech", "d"], how="outer")
                  .merge(phf, on=["tech", "d"], how="outer")
                  .fillna(0.0))

    # Bilaterals per side
    daily["Bilaterals_pos"] = (daily["_pdbf_pos"] - daily["DA cleared_pos"]).clip(lower=0)
    daily["Bilaterals_neg"] = (daily["_pdbf_neg"] - daily["DA cleared_neg"]).clip(upper=0)
    # REE residual per side = PHF_side - sum(other side contributions)
    sum_pos = (daily["DA cleared_pos"] + daily["Bilaterals_pos"]
                + daily["IDA1_pos"] + daily["IDA2_pos"] + daily["IDA3_pos"]
                + daily["IDA other_pos"] + daily["Continuous_pos"])
    sum_neg = (daily["DA cleared_neg"] + daily["Bilaterals_neg"]
                + daily["IDA1_neg"] + daily["IDA2_neg"] + daily["IDA3_neg"]
                + daily["IDA other_neg"] + daily["Continuous_neg"])
    daily["REE residual_pos"] = (daily["phf_pos"] - sum_pos).clip(lower=0)
    daily["REE residual_neg"] = (daily["phf_neg"] - sum_neg).clip(upper=0)
    daily = daily.drop(columns=["_pdbf_pos", "_pdbf_neg"])

    daily["d"] = pd.to_datetime(daily["d"])
    daily["week_start"] = daily["d"] - pd.to_timedelta(daily["d"].dt.weekday, unit="D")
    keep_cols = [f"{p}_{side}" for p in PROGRAM_COLS for side in ("pos", "neg")]
    daily["phf_net"] = daily["phf_pos"] + daily["phf_neg"]
    weekly = daily.groupby(["tech", "week_start"], as_index=False)[
        keep_cols + ["phf_pos", "phf_neg", "phf_net"]].sum()
    return weekly


def render(weekly, techs, tech_labels, fname, suptitle, p48=None):
    if p48 is not None:
        weekly = weekly.merge(p48[["tech", "week_start", "p48_gwh"]],
                                on=["tech", "week_start"], how="left")
    else:
        weekly = weekly.copy()
        weekly["p48_gwh"] = float("nan")
    colors = {
        "DA cleared":     "#1f77b4",
        "Bilaterals":     "#2ca02c",
        "IDA1":           "#ffe5b3",
        "IDA2":           "#fdae6b",
        "IDA3":           "#cc6600",
        "IDA other":      "#cccccc",
        "Continuous":     "#17becf",
        "REE residual":   "#d62728",
    }
    fig, axes = plt.subplots(2, 3, figsize=(13.5, 6.5), sharex=True)
    legend_done = False
    for ax, tech in zip(axes.flatten(), techs):
        sub = weekly[weekly["tech"] == tech].sort_values("week_start").reset_index(drop=True)
        if len(sub) == 0:
            ax.set_title(f"{tech_labels.get(tech, tech)} (no data)", fontsize=10)
            ax.axis("off"); continue
        x = sub["week_start"].values
        running_pos = np.zeros(len(sub))
        running_neg = np.zeros(len(sub))
        for prog in PROGRAM_COLS:
            pos_vals = sub[f"{prog}_pos"].values
            neg_vals = sub[f"{prog}_neg"].values
            top_pos = running_pos + pos_vals
            bot_neg = running_neg + neg_vals
            ax.fill_between(x, running_pos, top_pos, color=colors[prog],
                              alpha=0.92, linewidth=0,
                              label=prog if not legend_done else None)
            ax.fill_between(x, running_neg, bot_neg, color=colors[prog],
                              alpha=0.92, linewidth=0)
            running_pos = top_pos
            running_neg = bot_neg
        # Black line: PHF net (post-IDA + REE-RT2). P48 from ESIOS has a
        # per-tech classification mismatch with OMIE per-unit, so we don't
        # overlay it.
        ax.plot(x, sub["phf_net"].values, color="black", lw=1.1, ls="-",
                  label="PHF net" if not legend_done else None)
        ax.axhline(0, color="black", lw=0.5)
        legend_done = True
        ax.axvline(IDA_REFORM, color="purple", ls=":", lw=0.9)
        ax.axvline(MTU15_IDA,  color="gray",   ls=":", lw=0.9)
        ax.axvline(BLACKOUT,   color="black",  ls="-.", lw=1.0)
        ax.axvline(MTU15_DA,   color="red",    ls="--", lw=1.0)
        ax.set_title(tech_labels.get(tech, tech), fontsize=10)
        ax.grid(alpha=0.25)
        ax.tick_params(labelsize=7.5)
        ax.xaxis.set_major_locator(mdates.MonthLocator(interval=3))
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
        for lbl in ax.get_xticklabels():
            lbl.set_rotation(40); lbl.set_ha("right")
        ax.set_ylabel("GWh / week", fontsize=8)
    axes.flatten()[0].legend(loc="upper left", fontsize=6.5, frameon=True, ncol=2)
    fig.suptitle(suptitle, fontsize=10, y=1.00)
    fig.tight_layout(rect=[0, 0, 1, 0.97])
    out = OUTDIR / fname
    fig.savefig(f"{out}.pdf", bbox_inches="tight")
    fig.savefig(f"{out}.png", bbox_inches="tight", dpi=130)
    print(f"saved {out}.pdf")


def main():
    units = firm_unit_panel(csv_path=str(UNITS_CSV), scheme="short", mode="primary_owner")
    keep = units[units["tech_group"].isin(list(TECHS))][["unit_code", "tech_group"]].rename(
        columns={"tech_group": "tech"})
    print(f"Unit coverage: {len(keep):,} units; per tech: "
          f"{keep.groupby('tech').size().to_dict()}", flush=True)

    con = duckdb.connect()
    con.execute("PRAGMA threads=4"); con.execute("SET memory_limit='5GB'")
    con.execute("SET preserve_insertion_order=false")

    weekly = build_weekly(con, keep, TECHS)
    OUTDIR.mkdir(parents=True, exist_ok=True)
    weekly.to_csv(OUTDIR / "fig_programs_by_tech_weekly.csv", index=False)

    render(weekly, TECHS, TECH_LABELS,
            fname="fig_programs_by_tech_weekly",
            suptitle="Per-tech weekly program cascade (chronological, supply above 0, consumption / down-redispatch below 0). "
                     "PDBC $\\to$ Bilaterals $\\to$ IDA1/2/3 $\\to$ Continuous $\\to$ REE post-IDA RT. Black line $=$ PHF net.")


if __name__ == "__main__":
    main()
