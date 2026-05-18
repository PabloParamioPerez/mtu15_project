# STATUS: ALIVE
# LAST-AUDIT: 2026-05-19
# FEEDS: bidding_internal.tex §4 (bilateral channel sanity check).
# CLAIM: Build the per-(tech, regime) bilateral channel volume (PDBF - PDBC)
#        sanity-check table for Part B.
#
# Reads gap_by_tech.csv produced by gap_parallel_trends.py and aggregates
# per-tech mean GWh/month within each reform regime.

from __future__ import annotations

from pathlib import Path
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
INP = REPO / "results/regressions/firm/q1_drop/gap_by_tech.csv"
OUT = REPO / "results/regressions/firm/q1_drop/tab_bilat_channel_by_tech_regime.tex"


def regime(d: pd.Timestamp) -> str | None:
    if d < pd.Timestamp("2024-06-14"): return None
    if d <= pd.Timestamp("2024-11-30"): return "3-sess"
    if d <= pd.Timestamp("2025-03-18"): return "ISP15-win"
    if d <= pd.Timestamp("2025-04-27"): return "DA60_ID15-preblk"
    if d <= pd.Timestamp("2025-09-30"): return "DA60_ID15-postblk"
    if d <= pd.Timestamp("2026-01-31"): return "DA15_ID15"
    return None


def main():
    df = pd.read_csv(INP)
    df["ym"] = pd.to_datetime(df["ym"])
    df["bilat_gwh"] = df["pdbf_gwh"] - df["pdbc_gwh"]
    df["regime"] = df["ym"].apply(regime)
    df = df.dropna(subset=["regime"])

    tech_order = ["CCGT", "Hydro", "Hydro_pump", "Nuclear", "Wind", "Solar PV"]
    df = df[df["tech"].isin(tech_order)]
    agg = df.groupby(["tech", "regime"])["bilat_gwh"].mean().round(0).reset_index()

    regime_order = ["3-sess", "ISP15-win", "DA60_ID15-preblk",
                    "DA60_ID15-postblk", "DA15_ID15"]
    piv = agg.pivot_table(index="tech", columns="regime",
                          values="bilat_gwh").reindex(tech_order)[regime_order]

    rows = []
    rows.append("% auto-built by scripts/analysis/firm/bilat_channel_check.py")
    rows.append("% Bilateral channel (PDBF - PDBC), GWh/month mean per regime")
    rows.append("\\begin{tabular}{l r r r r r}")
    rows.append("\\toprule")
    header = " & 3-sess & ISP15-win & DA60/ID15 pre-blk & DA60/ID15 post-blk & DA15/ID15 \\\\"
    rows.append(header)
    rows.append("\\midrule")
    for t in tech_order:
        if t not in piv.index:
            continue
        vals = []
        for r in regime_order:
            v = piv.loc[t, r]
            vals.append(f"{v:.0f}" if pd.notna(v) else "---")
        rows.append(t.replace("_", " ") + " & " + " & ".join(vals) + " \\\\")
    rows.append("\\bottomrule")
    rows.append("\\end{tabular}")
    OUT.write_text("\n".join(rows) + "\n")
    print(f"wrote {OUT}")


if __name__ == "__main__":
    main()
