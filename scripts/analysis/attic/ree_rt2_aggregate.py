# STATUS: ALIVE
# LAST-AUDIT: 2026-05-15
# FEEDS: thesis paper.tex §4 (cross-check for PHF − PDBF intervention)
# CLAIM: Path B — system-aggregate cross-check. ESIOS archive 28
#        (totalrp48preccierre) reports REE's post-PDBF redispatch by
#        tipo_redespacho code. Code 61 = "Resolución Restricciones
#        Técnicas Tiempo Real - Fase I" (PO 3.2), the technical
#        post-DA restrictions including reforzada. We aggregate
#        monthly the system-wide qty_up_mwh and qty_down_mwh for the
#        codes most directly representing post-clearing intervention.

from __future__ import annotations

import glob
import sys
from pathlib import Path

import duckdb
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
OUTDIR = REPO / "results" / "regressions" / "regulatory" / "ree_rt2_aggregate"
OUTDIR.mkdir(parents=True, exist_ok=True)
FIGDIR = REPO / "figures" / "working"

ISP15            = pd.Timestamp("2024-12-09")
MTU15_IDA        = pd.Timestamp("2025-03-19")
IBERIAN_BLACKOUT = pd.Timestamp("2025-04-28")
MTU15_DA         = pd.Timestamp("2025-10-01")

# tipo_redespacho codes representing post-PDBF security restrictions.
# 61 = Restricciones técnicas tiempo real Fase I (PO 3.2)
# 65 = Restricciones técnicas tiempo real (legacy)
# These together capture the "reforzada" channel of REE pushing CCGT up out of merit.
RT2_CODES = ("61", "65")


def main():
    files = sorted(glob.glob(str(REPO / "data" / "processed" / "esios" / "restricciones" /
                                  "totalrp48preccierre_*.parquet")))
    files = [f for f in files if "_all" not in f]
    print(f"reading {len(files)} monthly files")
    con = duckdb.connect()
    q = f"""
    SELECT date_trunc('month', period_start_utc) AS month,
           tipo_redespacho,
           SUM(COALESCE(qty_up_mwh, 0))   AS qty_up_mwh,
           SUM(COALESCE(qty_down_mwh, 0)) AS qty_dn_mwh
    FROM read_parquet({files})
    WHERE period_start_utc >= TIMESTAMP '2018-01-01'
      AND period_start_utc < TIMESTAMP '2026-03-01'
      AND tipo_redespacho IN {tuple(RT2_CODES)}
    GROUP BY 1, 2 ORDER BY 1, 2
    """
    df = con.execute(q).df()
    df["month"] = pd.to_datetime(df["month"]).dt.tz_localize(None)
    df.to_csv(OUTDIR / "monthly_by_tipo.csv", index=False)

    agg = df.groupby("month", as_index=False)[["qty_up_mwh", "qty_dn_mwh"]].sum()
    agg["net_up_mwh"] = agg["qty_up_mwh"] - agg["qty_dn_mwh"]
    agg.to_csv(OUTDIR / "monthly_aggregate.csv", index=False)
    print(agg.tail(20).to_string())

    fig, ax = plt.subplots(figsize=(12, 4.5))
    ax.bar(agg["month"], agg["qty_up_mwh"] / 1e3, width=22,
           color="tab:red",  alpha=0.6, label="UP (forced-on, MWh)")
    ax.bar(agg["month"], -agg["qty_dn_mwh"] / 1e3, width=22,
           color="tab:blue", alpha=0.6, label="DOWN (forced-off, MWh)")
    ax.axhline(0, color="grey", lw=0.5)
    ax.set_ylabel("RT2 redispatch (GWh / month)")
    ax.set_title("Restricciones técnicas tiempo real (tipo 61 + 65), "
                 "system aggregate — ESIOS archive 28")
    for d, lab in [(ISP15, "ISP15"), (MTU15_IDA, "MTU15-IDA"),
                   (IBERIAN_BLACKOUT, "Blackout"), (MTU15_DA, "MTU15-DA")]:
        ax.axvline(d, color="black", lw=0.7, ls=":", alpha=0.7)
        ax.text(d, ax.get_ylim()[1] * 0.95, lab, rotation=90,
                fontsize=7, ha="right", va="top", alpha=0.7)
    ax.legend(loc="upper left", frameon=False)
    ax.xaxis.set_major_locator(mdates.YearLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
    ax.grid(True, alpha=0.3)
    plt.tight_layout()
    out = FIGDIR / "fig_ree_rt2_aggregate.pdf"
    plt.savefig(out, dpi=150, bbox_inches="tight")
    plt.savefig(out.with_suffix(".png"), dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"wrote {out}")


if __name__ == "__main__":
    main()
