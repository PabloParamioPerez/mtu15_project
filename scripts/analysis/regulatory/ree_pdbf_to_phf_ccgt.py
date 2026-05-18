# STATUS: ALIVE
# LAST-AUDIT: 2026-05-15
# FEEDS: thesis paper.tex §4 (reforzada/REE post-PDBF intervention)
# CLAIM: Path A — highest-granularity test of "REE is including more CCGT
#        post-clearing". For each (date, period, CCGT unit) we observe:
#        - pdbf.assigned_power_mw     := DA + bilateral programme (market clearing baseline)
#        - phf.assigned_power_mw      := final hourly programme after the last IDA
#                                        session, including REE's post-IDA RT
#                                        adjustments (OS-established, see OMIE
#                                        v1.37 §5.2.2.4)
#        The per-period intervention is phf - pdbf. We aggregate to
#        monthly per firm and per the full Big-4 CCGT fleet, plot with
#        reform-line markers, and export CSVs for cross-check against
#        the system-aggregate ESIOS totalrp48preccierre (Path B) and
#        the ENTSO-E A75 actual-generation series (Path C).

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
PHF  = REPO / "data" / "processed" / "omie" / "mercado_intradiario_subastas" / "programas" / "phf_all.parquet"
UNITS_CSV = REPO / "data" / "external" / "omie_reference" / "lista_unidades.csv"
OUTDIR = REPO / "results" / "regressions" / "regulatory" / "pdbf_to_phf_ccgt"
OUTDIR.mkdir(parents=True, exist_ok=True)
FIGDIR = REPO / "figures" / "working"
FIGDIR.mkdir(parents=True, exist_ok=True)

PIVOTAL = ("IB", "GE", "GN", "HC")
WINDOW = ("2024-01-01", "2026-03-01")

ISP15            = pd.Timestamp("2024-12-09")
MTU15_IDA        = pd.Timestamp("2025-03-19")
IBERIAN_BLACKOUT = pd.Timestamp("2025-04-28")
MTU15_DA         = pd.Timestamp("2025-10-01")


def ccgt_unit_panel() -> pd.DataFrame:
    units = firm_unit_panel(csv_path=str(UNITS_CSV), scheme="short", mode="primary_owner")
    ccgt = units[units["tech_group"] == "CCGT"][["unit_code", "parent"]].copy()
    ccgt = ccgt.rename(columns={"parent": "firm"})
    return ccgt


def _month_iter(start: str, end: str):
    s = pd.Timestamp(start); e = pd.Timestamp(end)
    cur = pd.Timestamp(s.year, s.month, 1)
    while cur < e:
        nxt = (cur + pd.offsets.MonthBegin(1))
        yield cur.date(), nxt.date()
        cur = nxt


def build_monthly_panel() -> pd.DataFrame:
    """Per-month MWh: PDBF (DA + bilaterals), PHF (final post-IDA, REE-adjusted).

    Granularity discipline: both files have a `mtu_minutes` column. Pre-MTU15
    they are 60-minute periods; post-MTU15 they are 15-minute periods. To
    aggregate to MWh we multiply MW × (mtu_minutes / 60). Within a calendar
    month, we then sum across all (date, period, unit) cells.

    For phf, multiple sessions can write a programme for the same (date,
    period, unit); we take the latest session_number to capture REE's final
    post-IDA RT-adjusted view (cf. OMIE spec §5.2.2.4 OS-established).

    Streamed month-by-month — phf_all is 7.4 GB; an all-at-once query OOMs.
    """
    units = ccgt_unit_panel()
    pdbf_rows, phf_rows = [], []
    con = duckdb.connect(); con.execute("PRAGMA threads=4"); con.execute("SET memory_limit='6GB'")
    con.register("ccgt_units", units)
    for m0, m1 in _month_iter(*WINDOW):
        q_pdbf = f"""
        SELECT u.firm,
               SUM(assigned_power_mw * (mtu_minutes / 60.0)) AS pdbf_mwh
        FROM '{PDBF}' p
          JOIN ccgt_units u ON p.unit_code = u.unit_code
        WHERE date::DATE >= DATE '{m0}' AND date::DATE < DATE '{m1}'
          AND assigned_power_mw IS NOT NULL
        GROUP BY 1
        """
        d = con.execute(q_pdbf).df()
        d["month"] = pd.Timestamp(m0)
        pdbf_rows.append(d)

        q_phf = f"""
        WITH latest AS (
            SELECT date::DATE AS d, period, unit_code,
                   ROW_NUMBER() OVER (PARTITION BY date::DATE, period, unit_code
                                      ORDER BY session_number DESC) AS rn,
                   assigned_power_mw, mtu_minutes
            FROM '{PHF}'
            WHERE date::DATE >= DATE '{m0}' AND date::DATE < DATE '{m1}'
              AND assigned_power_mw IS NOT NULL
        )
        SELECT u.firm,
               SUM(l.assigned_power_mw * (l.mtu_minutes / 60.0)) AS phf_mwh
        FROM latest l
          JOIN ccgt_units u ON l.unit_code = u.unit_code
        WHERE l.rn = 1
        GROUP BY 1
        """
        d = con.execute(q_phf).df()
        d["month"] = pd.Timestamp(m0)
        phf_rows.append(d)
        print(f"  {m0} -> {m1}: pdbf={pdbf_rows[-1]['pdbf_mwh'].sum():.0f} MWh, "
              f"phf={phf_rows[-1]['phf_mwh'].sum():.0f} MWh", flush=True)
    pdbf_m = pd.concat(pdbf_rows, ignore_index=True)
    phf_m  = pd.concat(phf_rows, ignore_index=True)

    m = pdbf_m.merge(phf_m, on=["month", "firm"], how="outer").fillna(0.0)
    m["month"] = pd.to_datetime(m["month"])
    m["intervention_mwh"] = m["phf_mwh"] - m["pdbf_mwh"]
    m["intervention_share"] = np.where(m["pdbf_mwh"] > 0,
                                        m["intervention_mwh"] / m["pdbf_mwh"], np.nan)
    return m.sort_values(["firm", "month"]).reset_index(drop=True)


def build_unit_monthly_panel() -> pd.DataFrame:
    """Per-unit-month MWh (PDBF, PHF). Streamed month-by-month."""
    units = ccgt_unit_panel()
    con = duckdb.connect(); con.execute("PRAGMA threads=4"); con.execute("SET memory_limit='6GB'")
    con.register("ccgt_units", units)
    rows = []
    for m0, m1 in _month_iter(*WINDOW):
        q = f"""
        WITH pdbf_m AS (
            SELECT unit_code,
                   SUM(assigned_power_mw * (mtu_minutes / 60.0)) AS pdbf_mwh
            FROM '{PDBF}'
            WHERE date::DATE >= DATE '{m0}' AND date::DATE < DATE '{m1}'
              AND assigned_power_mw IS NOT NULL
            GROUP BY 1
        ),
        phf_latest AS (
            SELECT date::DATE AS d, period, unit_code,
                   assigned_power_mw, mtu_minutes,
                   ROW_NUMBER() OVER (PARTITION BY date::DATE, period, unit_code
                                      ORDER BY session_number DESC) AS rn
            FROM '{PHF}'
            WHERE date::DATE >= DATE '{m0}' AND date::DATE < DATE '{m1}'
              AND assigned_power_mw IS NOT NULL
        ),
        phf_m AS (
            SELECT unit_code,
                   SUM(assigned_power_mw * (mtu_minutes / 60.0)) AS phf_mwh
            FROM phf_latest WHERE rn = 1
            GROUP BY 1
        )
        SELECT COALESCE(p.unit_code, h.unit_code) AS unit_code,
               COALESCE(p.pdbf_mwh, 0.0) AS pdbf_mwh,
               COALESCE(h.phf_mwh,  0.0) AS phf_mwh
        FROM pdbf_m p FULL JOIN phf_m h USING (unit_code)
        """
        d = con.execute(q).df()
        d["month"] = pd.Timestamp(m0)
        rows.append(d)
    df = pd.concat(rows, ignore_index=True)
    df = df.merge(units, on="unit_code", how="inner")
    df["month"] = pd.to_datetime(df["month"])
    df["intervention_mwh"] = df["phf_mwh"] - df["pdbf_mwh"]
    return df.sort_values(["unit_code", "month"]).reset_index(drop=True)


def plot_firm_monthly(m: pd.DataFrame):
    fig, axes = plt.subplots(2, 1, figsize=(12, 8), sharex=True, height_ratios=[1.0, 1.0])

    firms = list(PIVOTAL)
    colors = {"IB": "tab:green", "GE": "tab:red", "GN": "tab:orange", "HC": "tab:blue"}
    pretty = {"IB": "Iberdrola", "GE": "Endesa", "GN": "Naturgy", "HC": "EDP"}

    ax = axes[0]
    for firm in firms:
        sub = m[m["firm"] == firm].sort_values("month")
        ax.plot(sub["month"], sub["intervention_mwh"] / 1e3,
                marker="o", ms=4, lw=1.5, color=colors[firm], label=pretty[firm])
    ax.axhline(0, color="grey", lw=0.5)
    ax.set_ylabel("PHF $-$ PDBF (GWh / month)")
    ax.set_title("CCGT post-DA programme adjustment, by firm (PHF $-$ PDBF)")
    ax.legend(loc="upper left", ncol=4, frameon=False, fontsize=9)
    ax.grid(True, alpha=0.3)

    for d, lab in [(ISP15, "ISP15"), (MTU15_IDA, "MTU15-IDA"),
                   (IBERIAN_BLACKOUT, "Blackout"), (MTU15_DA, "MTU15-DA")]:
        ax.axvline(d, color="black", lw=0.7, ls=":", alpha=0.6)
        ax.text(d, ax.get_ylim()[1] * 0.96, lab, rotation=90, fontsize=7,
                ha="right", va="top", alpha=0.7)

    ax2 = axes[1]
    for firm in firms:
        sub = m[m["firm"] == firm].sort_values("month")
        ax2.plot(sub["month"], 100 * sub["intervention_share"],
                 marker="o", ms=4, lw=1.5, color=colors[firm], label=pretty[firm])
    ax2.axhline(0, color="grey", lw=0.5)
    ax2.set_ylabel("(PHF $-$ PDBF) / PDBF  (%)")
    ax2.set_xlabel("Month")
    ax2.grid(True, alpha=0.3)
    ax2.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
    ax2.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
    plt.setp(ax2.get_xticklabels(), rotation=45, ha="right")
    for d in (ISP15, MTU15_IDA, IBERIAN_BLACKOUT, MTU15_DA):
        ax2.axvline(d, color="black", lw=0.7, ls=":", alpha=0.6)

    plt.tight_layout()
    out = FIGDIR / "fig_ree_pdbf_to_phf_ccgt.pdf"
    plt.savefig(out, dpi=150, bbox_inches="tight")
    plt.savefig(out.with_suffix(".png"), dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"wrote {out}")


def main():
    print("loading PDBF + PHF monthly per CCGT firm...")
    m = build_monthly_panel()
    m.to_csv(OUTDIR / "monthly_ccgt_firm.csv", index=False)
    print(m.tail(20).to_string())

    print("\nbuilding per-unit monthly diagnostic...")
    u = build_unit_monthly_panel()
    u.to_csv(OUTDIR / "monthly_ccgt_unit.csv", index=False)
    print("units in panel:", u["unit_code"].nunique())

    print("\nplotting...")
    plot_firm_monthly(m)

    print("\n=== sanity: pre/post Blackout monthly summary (4 firms aggregated) ===")
    fleet = m.groupby("month", as_index=False)[["pdbf_mwh", "phf_mwh", "intervention_mwh"]].sum()
    fleet["intervention_share"] = fleet["intervention_mwh"] / fleet["pdbf_mwh"]
    fleet.to_csv(OUTDIR / "monthly_ccgt_fleet.csv", index=False)
    print(fleet.to_string())


if __name__ == "__main__":
    main()
