# STATUS: ALIVE
# LAST-AUDIT: 2026-06-09
# FEEDS: thesis/presentations/.../slides.tex --- "Changes in quantity" slide.
# CLAIM: System-aggregate weekly program-volume decomposition showing that
#        Spanish wholesale supply is split across many sequential programs
#        (DA, bilateral, IDA sessions, continuous, REE Fase I and Fase II)
#        and that program shares move dramatically across the reform sequence
#        and the post-blackout regime. This is the cleanest visual hook for
#        "different programs matter".
#
# OUT: figures/working/fig_programs_weekly_system.{pdf,png}

from pathlib import Path
import duckdb
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
PDBC = REPO / "data/processed/omie/mercado_diario/programas/pdbc_all.parquet"
PDBF = REPO / "data/processed/omie/mercado_diario/programas/pdbf_all.parquet"
PIBCI = REPO / "data/processed/omie/mercado_intradiario_subastas/programas/pibci_all.parquet"
PIBCIC = REPO / "data/processed/omie/mercado_intradiario_continuo/programas/pibcic_all.parquet"
FASE1 = REPO / "data/processed/esios/indicators/10051.parquet"
FASE2 = REPO / "data/processed/esios/indicators/10270.parquet"
OUTDIR = REPO / "figures/working"

START = "2023-01-01"
END   = "2026-01-09"
IDA_REFORM = pd.Timestamp("2024-06-14")
MTU15_IDA = pd.Timestamp("2025-03-19")
BLACKOUT  = pd.Timestamp("2025-04-28")
MTU15_DA  = pd.Timestamp("2025-10-01")


def mw_to_gwh(df, mw_col, mtu_col="mtu_minutes"):
    return df[mw_col].abs() * df[mtu_col] / 60.0 / 1000.0


def main():
    con = duckdb.connect()
    con.execute("PRAGMA threads=4"); con.execute("SET memory_limit='6GB'")

    print("PDBC (DA cleared)...", flush=True)
    pdbc = con.execute(f"""
        SELECT CAST(date AS DATE) AS d,
               SUM(ABS(assigned_power_mw) * mtu_minutes / 60.0) / 1000.0 AS gwh
        FROM '{PDBC}' WHERE date::DATE BETWEEN '{START}' AND '{END}'
          AND assigned_power_mw > 0
        GROUP BY 1
    """).df()
    pdbc["d"] = pd.to_datetime(pdbc["d"]); pdbc = pdbc.rename(columns={"gwh": "DA cleared (PDBC)"})

    print("PDBF (DA + bilaterals)...", flush=True)
    pdbf = con.execute(f"""
        SELECT CAST(date AS DATE) AS d,
               SUM(ABS(assigned_power_mw) * mtu_minutes / 60.0) / 1000.0 AS gwh
        FROM '{PDBF}' WHERE date::DATE BETWEEN '{START}' AND '{END}'
          AND assigned_power_mw > 0
        GROUP BY 1
    """).df()
    pdbf["d"] = pd.to_datetime(pdbf["d"]); pdbf = pdbf.rename(columns={"gwh": "_pdbf"})

    print("PIBCI per-session...", flush=True)
    pibci = con.execute(f"""
        SELECT CAST(date AS DATE) AS d, session_number,
               SUM(ABS(assigned_power_mw) * mtu_minutes / 60.0) / 1000.0 AS gwh
        FROM '{PIBCI}' WHERE date::DATE BETWEEN '{START}' AND '{END}'
        GROUP BY 1, 2
    """).df()
    pibci["d"] = pd.to_datetime(pibci["d"])
    pibci_w = pibci.pivot_table(index="d", columns="session_number", values="gwh",
                                  fill_value=0).reset_index()
    # Post-IDA-reform 3 sessions are S1/S2/S3; pre-reform 6 sessions pooled
    for s in (1, 2, 3):
        col = f"IDA{s}"
        if s in pibci_w.columns:
            pibci_w[col] = pibci_w[s]
        else:
            pibci_w[col] = 0.0
    other_cols = [c for c in pibci_w.columns if isinstance(c, (int, np.integer)) and c not in (1, 2, 3)]
    pibci_w["IDA other (pre-2024-06)"] = pibci_w[other_cols].sum(axis=1) if other_cols else 0.0
    pibci_w = pibci_w[["d", "IDA1", "IDA2", "IDA3", "IDA other (pre-2024-06)"]]

    print("PIBCIC (continuous)...", flush=True)
    pibcic = con.execute(f"""
        SELECT CAST(date AS DATE) AS d,
               SUM(ABS(assigned_power_mw) * mtu_minutes / 60.0) / 1000.0 AS gwh
        FROM '{PIBCIC}' WHERE date::DATE BETWEEN '{START}' AND '{END}'
        GROUP BY 1
    """).df()
    pibcic["d"] = pd.to_datetime(pibcic["d"]); pibcic = pibcic.rename(columns={"gwh": "Continuous (MIC/XBID)"})

    print("Fase I (ESIOS 10051)...", flush=True)
    f1 = pd.read_parquet(FASE1)[["ts_local", "value"]]
    f1["d"] = pd.to_datetime(f1["ts_local"]).dt.tz_localize(None).dt.normalize()
    f1 = f1.groupby("d")["value"].sum().reset_index()
    f1["Fase I (REE post-DA, pre-IDA)"] = f1["value"].abs() / 1000.0  # MW * 1h sum / 1000 -> GWh-day
    f1 = f1[["d", "Fase I (REE post-DA, pre-IDA)"]]

    print("Fase II (ESIOS 10270)...", flush=True)
    f2 = pd.read_parquet(FASE2)[["ts_local", "value"]]
    f2["d"] = pd.to_datetime(f2["ts_local"]).dt.tz_localize(None).dt.normalize()
    f2 = f2.groupby("d")["value"].sum().reset_index()
    f2["Fase II (REE real-time)"] = f2["value"].abs() / 1000.0
    f2 = f2[["d", "Fase II (REE real-time)"]]

    print("Merging and weekly-aggregating...", flush=True)
    daily = (pdbc.merge(pdbf, on="d", how="outer")
                  .merge(pibci_w, on="d", how="outer")
                  .merge(pibcic, on="d", how="outer")
                  .merge(f1, on="d", how="outer")
                  .merge(f2, on="d", how="outer")
                  .fillna(0.0)
                  .sort_values("d"))
    daily["Bilaterals (PDBF$-$PDBC)"] = (daily["_pdbf"] - daily["DA cleared (PDBC)"]).clip(lower=0)
    daily = daily.drop(columns=["_pdbf"])

    daily["week_start"] = daily["d"] - pd.to_timedelta(daily["d"].dt.weekday, unit="D")
    program_cols = [
        "DA cleared (PDBC)",
        "Bilaterals (PDBF$-$PDBC)",
        "Fase I (REE post-DA, pre-IDA)",
        "IDA other (pre-2024-06)",
        "IDA1", "IDA2", "IDA3",
        "Continuous (MIC/XBID)",
        "Fase II (REE real-time)",
    ]
    weekly = daily.groupby("week_start", as_index=False)[program_cols].sum()

    print(f"Weekly rows: {len(weekly)}; range {weekly['week_start'].min().date()} -> {weekly['week_start'].max().date()}")
    OUTDIR.mkdir(parents=True, exist_ok=True)
    weekly.to_csv(OUTDIR / "fig_programs_weekly_system.csv", index=False)

    # === FIGURE: stacked area ===
    fig, ax = plt.subplots(figsize=(11.5, 5.0))
    colors = {
        "DA cleared (PDBC)":              "#1f77b4",
        "Bilaterals (PDBF$-$PDBC)":       "#2ca02c",
        "Fase I (REE post-DA, pre-IDA)":  "#9467bd",
        "IDA other (pre-2024-06)":        "#d3d3d3",
        "IDA1":                           "#ffcc66",
        "IDA2":                           "#ff9933",
        "IDA3":                           "#cc6600",
        "Continuous (MIC/XBID)":          "#17becf",
        "Fase II (REE real-time)":        "#7f7f7f",
    }
    x = weekly["week_start"]
    ax.stackplot(x,
                 [weekly[c] for c in program_cols],
                 labels=program_cols,
                 colors=[colors[c] for c in program_cols],
                 alpha=0.92)

    for v, lab, c, sty in [
        (IDA_REFORM, "European IDA\n(6$\\to$3)", "purple", ":"),
        (MTU15_IDA,  "MTU15 IDA",                "gray",   ":"),
        (BLACKOUT,   "Blackout",                 "black",  "-."),
        (MTU15_DA,   "MTU15 DA",                 "red",    "--"),
    ]:
        ax.axvline(v, color=c, ls=sty, lw=1.0)
        ax.text(v, ax.get_ylim()[1] * 0.97, lab, rotation=0, fontsize=7.5,
                ha="center", va="top", color=c,
                bbox=dict(facecolor="white", edgecolor="none", alpha=0.85, pad=1.5))

    ax.set_ylabel("Volume (GWh / week)", fontsize=10)
    ax.set_xlabel("")
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=3))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
    for lbl in ax.get_xticklabels():
        lbl.set_rotation(40); lbl.set_ha("right")
    ax.grid(alpha=0.25)
    ax.legend(loc="upper left", fontsize=7.5, frameon=True, ncol=2)
    ax.set_title("Weekly program-volume decomposition (system aggregate, GWh/week)",
                 fontsize=11)
    fig.tight_layout()
    out = OUTDIR / "fig_programs_weekly_system"
    fig.savefig(f"{out}.pdf", bbox_inches="tight")
    fig.savefig(f"{out}.png", bbox_inches="tight", dpi=130)
    print(f"saved {out}.pdf")


if __name__ == "__main__":
    main()
