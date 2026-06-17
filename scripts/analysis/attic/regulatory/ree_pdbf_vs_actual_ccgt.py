# STATUS: ALIVE
# LAST-AUDIT: 2026-05-15
# FEEDS: thesis paper.tex §4 (cross-check for PHF − PDBF intervention)
# CLAIM: Path C — ENTSO-E A75 B04 (actual CCGT generation per unit)
#        minus OMIE PDBF (DA + bilaterals programme) per firm-month.
#        This captures the TOTAL post-clearing intervention horizon
#        (DA-clearing → IDA → REE RT → real-time balancing), strictly
#        wider than (PHF − PDBF) from Path A which stops at REE post-IDA.

from __future__ import annotations

import sys
from pathlib import Path

import duckdb
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO / "src"))
from mtu.classification.units import firm_unit_panel  # noqa: E402

PDBF = REPO / "data" / "processed" / "omie" / "mercado_diario" / "programas" / "pdbf_all.parquet"
A75  = REPO / "data" / "processed" / "entsoe" / "generation" / "ccgt_per_firm_panel.parquet"
UNITS_CSV = REPO / "data" / "external" / "omie_reference" / "lista_unidades.csv"
OUTDIR = REPO / "results" / "regressions" / "regulatory" / "pdbf_vs_actual_ccgt"
OUTDIR.mkdir(parents=True, exist_ok=True)
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


def pdbf_monthly_per_firm() -> pd.DataFrame:
    units = firm_unit_panel(csv_path=str(UNITS_CSV), scheme="short", mode="primary_owner")
    ccgt = units[units["tech_group"] == "CCGT"][["unit_code", "parent"]].rename(columns={"parent": "firm"})
    con = duckdb.connect(); con.execute("PRAGMA threads=4"); con.execute("SET memory_limit='6GB'")
    con.register("ccgt_units", ccgt)
    rows = []
    for m0, m1 in _month_iter(*WINDOW):
        q = f"""
        SELECT u.firm,
               SUM(assigned_power_mw * (mtu_minutes / 60.0)) AS pdbf_mwh
        FROM '{PDBF}' p
          JOIN ccgt_units u ON p.unit_code = u.unit_code
        WHERE date::DATE >= DATE '{m0}' AND date::DATE < DATE '{m1}'
          AND assigned_power_mw IS NOT NULL
        GROUP BY 1
        """
        d = con.execute(q).df(); d["month"] = pd.Timestamp(m0); rows.append(d)
    return pd.concat(rows, ignore_index=True)


def actual_monthly_per_firm() -> pd.DataFrame:
    con = duckdb.connect()
    q = f"""
    SELECT date_trunc('month', isp_start_utc) AS month,
           firm,
           SUM(mwh) AS actual_mwh
    FROM '{A75}'
    WHERE isp_start_utc >= TIMESTAMP '{WINDOW[0]}'
      AND isp_start_utc <  TIMESTAMP '{WINDOW[1]}'
      AND firm IN ('IB', 'GE', 'GN', 'HC')
    GROUP BY 1, 2 ORDER BY 1, 2
    """
    df = con.execute(q).df()
    df["month"] = pd.to_datetime(df["month"])
    return df


def main():
    print("aggregating PDBF monthly...")
    p = pdbf_monthly_per_firm()
    print("aggregating ENTSO-E A75 actuals monthly...")
    a = actual_monthly_per_firm()
    m = p.merge(a, on=["firm", "month"], how="outer").fillna(0.0)
    m["intervention_mwh"] = m["actual_mwh"] - m["pdbf_mwh"]
    m["intervention_share"] = np.where(m["pdbf_mwh"] > 0,
                                        m["intervention_mwh"] / m["pdbf_mwh"], np.nan)
    m = m.sort_values(["firm", "month"]).reset_index(drop=True)
    m.to_csv(OUTDIR / "monthly_pdbf_vs_actual_ccgt.csv", index=False)
    print(m.tail(20).to_string())

    fig, axes = plt.subplots(2, 1, figsize=(12, 8), sharex=True)
    colors = {"IB": "tab:green", "GE": "tab:red", "GN": "tab:orange", "HC": "tab:blue"}
    pretty = {"IB": "Iberdrola", "GE": "Endesa", "GN": "Naturgy", "HC": "EDP"}

    ax = axes[0]
    for firm in PIVOTAL:
        sub = m[m["firm"] == firm].sort_values("month")
        ax.plot(sub["month"], sub["intervention_mwh"] / 1e3,
                marker="o", ms=4, lw=1.5, color=colors[firm], label=pretty[firm])
    ax.axhline(0, color="grey", lw=0.5)
    ax.set_ylabel("Actual $-$ PDBF (GWh / month)")
    ax.set_title("CCGT post-DA intervention horizon (DA $\\to$ real time), by firm")
    ax.legend(loc="upper left", ncol=4, frameon=False, fontsize=9)
    ax.grid(True, alpha=0.3)
    for d, lab in [(ISP15, "ISP15"), (MTU15_IDA, "MTU15-IDA"),
                   (IBERIAN_BLACKOUT, "Blackout"), (MTU15_DA, "MTU15-DA")]:
        ax.axvline(d, color="black", lw=0.7, ls=":", alpha=0.6)
        ax.text(d, ax.get_ylim()[1] * 0.96, lab, rotation=90, fontsize=7,
                ha="right", va="top", alpha=0.7)

    ax2 = axes[1]
    for firm in PIVOTAL:
        sub = m[m["firm"] == firm].sort_values("month")
        ax2.plot(sub["month"], 100 * sub["intervention_share"],
                 marker="o", ms=4, lw=1.5, color=colors[firm], label=pretty[firm])
    ax2.axhline(0, color="grey", lw=0.5)
    ax2.set_ylabel("(Actual $-$ PDBF) / PDBF  (%)")
    ax2.set_xlabel("Month")
    ax2.grid(True, alpha=0.3)
    ax2.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
    ax2.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
    plt.setp(ax2.get_xticklabels(), rotation=45, ha="right")
    for d in (ISP15, MTU15_IDA, IBERIAN_BLACKOUT, MTU15_DA):
        ax2.axvline(d, color="black", lw=0.7, ls=":", alpha=0.6)

    plt.tight_layout()
    out = FIGDIR / "fig_ree_pdbf_vs_actual_ccgt.pdf"
    plt.savefig(out, dpi=150, bbox_inches="tight")
    plt.savefig(out.with_suffix(".png"), dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"wrote {out}")


if __name__ == "__main__":
    main()
