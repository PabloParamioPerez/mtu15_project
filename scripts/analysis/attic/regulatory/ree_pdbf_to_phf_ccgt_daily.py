# STATUS: ALIVE
# LAST-AUDIT: 2026-05-15
# FEEDS: provisional.tex §6 (REE post-clearing CCGT, daily granularity)
# CLAIM: Daily-granularity per-firm CCGT PHF − PDBF intervention.
#        Single panel (GWh/day). Monthly is too coarse — reform breaks
#        are most legible at daily resolution.

from __future__ import annotations

import sys
from pathlib import Path

import duckdb
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO / "src"))
from mtu.classification.units import firm_unit_panel  # noqa: E402

PDBF = REPO / "data" / "processed" / "omie" / "mercado_diario" / "programas" / "pdbf_all.parquet"
PHF  = REPO / "data" / "processed" / "omie" / "mercado_intradiario_subastas" / "programas" / "phf_all.parquet"
UNITS_CSV = REPO / "data" / "external" / "omie_reference" / "lista_unidades.csv"
OUTDIR = REPO / "results" / "regressions" / "regulatory" / "pdbf_to_phf_ccgt"
FIGDIR = REPO / "figures" / "working"

PIVOTAL = ("IB", "GE", "GN", "HC")
WINDOW = ("2024-01-01", "2026-03-01")

ISP15            = pd.Timestamp("2024-12-09")
MTU15_IDA        = pd.Timestamp("2025-03-19")
IBERIAN_BLACKOUT = pd.Timestamp("2025-04-28")
MTU15_DA         = pd.Timestamp("2025-10-01")


def _month_iter(start: str, end: str):
    s = pd.Timestamp(start); e = pd.Timestamp(end)
    cur = pd.Timestamp(s.year, s.month, 1)
    while cur < e:
        nxt = cur + pd.offsets.MonthBegin(1)
        yield cur.date(), nxt.date()
        cur = nxt


def build_daily_panel() -> pd.DataFrame:
    units = firm_unit_panel(csv_path=str(UNITS_CSV), scheme="short", mode="primary_owner")
    ccgt = units[(units["tech_group"] == "CCGT") & (units["parent"].isin(PIVOTAL))][
        ["unit_code", "parent"]
    ].rename(columns={"parent": "firm"})
    con = duckdb.connect(); con.execute("PRAGMA threads=4"); con.execute("SET memory_limit='6GB'")
    con.register("ccgt_units", ccgt)
    rows = []
    for m0, m1 in _month_iter(*WINDOW):
        q_pdbf = f"""
        SELECT date::DATE AS day, u.firm,
               SUM(assigned_power_mw * (mtu_minutes / 60.0)) AS pdbf_mwh
        FROM '{PDBF}' p JOIN ccgt_units u ON p.unit_code = u.unit_code
        WHERE date::DATE >= DATE '{m0}' AND date::DATE < DATE '{m1}'
          AND assigned_power_mw IS NOT NULL
        GROUP BY 1, 2
        """
        pdbf = con.execute(q_pdbf).df()
        q_phf = f"""
        WITH lat AS (
            SELECT date::DATE AS d, period, unit_code,
                   assigned_power_mw, mtu_minutes,
                   ROW_NUMBER() OVER (PARTITION BY date::DATE, period, unit_code
                                      ORDER BY session_number DESC) AS rn
            FROM '{PHF}'
            WHERE date::DATE >= DATE '{m0}' AND date::DATE < DATE '{m1}'
              AND assigned_power_mw IS NOT NULL
        )
        SELECT lat.d AS day, u.firm,
               SUM(lat.assigned_power_mw * (lat.mtu_minutes / 60.0)) AS phf_mwh
        FROM lat JOIN ccgt_units u ON lat.unit_code = u.unit_code
        WHERE lat.rn = 1
        GROUP BY 1, 2
        """
        phf = con.execute(q_phf).df()
        d = pdbf.merge(phf, on=["day", "firm"], how="outer").fillna(0.0)
        rows.append(d)
        print(f"  {m0}: {len(d)} (day, firm) cells", flush=True)
    df = pd.concat(rows, ignore_index=True)
    df["day"] = pd.to_datetime(df["day"])
    df["intervention_mwh"] = df["phf_mwh"] - df["pdbf_mwh"]
    return df.sort_values(["firm", "day"]).reset_index(drop=True)


def plot_daily(df: pd.DataFrame):
    colors = {"IB": "tab:green", "GE": "tab:red", "GN": "tab:orange", "HC": "tab:blue"}
    pretty = {"IB": "Iberdrola", "GE": "Endesa", "GN": "Naturgy", "HC": "EDP"}
    fig, ax = plt.subplots(figsize=(13, 4.5))
    for firm in PIVOTAL:
        sub = df[df["firm"] == firm].sort_values("day")
        ax.plot(sub["day"], sub["intervention_mwh"] / 1e3,
                lw=0.7, color=colors[firm], label=pretty[firm], alpha=0.85)
    ax.axhline(0, color="grey", lw=0.5)
    ax.set_ylabel("PHF $-$ PDBF (GWh / day)")
    ax.set_xlabel("")
    ax.set_title("Per-firm CCGT post-DA intervention (PHF $-$ PDBF), daily")
    ax.legend(loc="upper left", ncol=4, frameon=False, fontsize=9)
    for d, lab in [(ISP15, "ISP15"), (MTU15_IDA, "MTU15-IDA"),
                   (IBERIAN_BLACKOUT, "Blackout"), (MTU15_DA, "MTU15-DA")]:
        ax.axvline(d, color="black", lw=0.7, ls=":", alpha=0.7)
        ax.text(d, ax.get_ylim()[1] * 0.96, lab, rotation=90, fontsize=7,
                ha="right", va="top", alpha=0.7)
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
    plt.setp(ax.get_xticklabels(), rotation=45, ha="right")
    ax.grid(True, alpha=0.3)
    plt.tight_layout()
    out = FIGDIR / "fig_ree_pdbf_to_phf_ccgt_daily.pdf"
    plt.savefig(out, dpi=150, bbox_inches="tight")
    plt.savefig(out.with_suffix(".png"), dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"wrote {out}")


def main():
    df = build_daily_panel()
    df.to_csv(OUTDIR / "daily_ccgt_firm.csv", index=False)
    plot_daily(df)


if __name__ == "__main__":
    main()
