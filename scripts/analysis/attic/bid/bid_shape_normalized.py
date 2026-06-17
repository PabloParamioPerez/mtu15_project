# STATUS: ALIVE
# LAST-AUDIT: 2026-05-18
# FEEDS: descriptive_facts.tex §3.3 (normalized bid-shape robustness)
# CLAIM: Re-aggregate the bid-shape metric as the RATIO mw_in_band / mw_total
#        (fraction of unit's offered MW falling within ±50 EUR/MWh of MCP).
#        This isolates the STRATEGIC concentration near MCP from the unit's
#        operational scale (which varies with technical commitment, especially
#        at night). The user flagged that absolute MW makes critical < flat
#        ordering misleading because flat hours have more thermal MW committed
#        than critical hours, mechanically inflating in-band MW.
#
# Output:
#   results/regressions/bid/bid_shape/
#     DA_descriptive_normalized.csv, IDA_descriptive_normalized.csv
#     tex/tab_bidshape_DA_by_regime_normalized.tex (and IDA)
#     tex/tab_bidshape_DA_ccgt_byfirm_normalized.tex (and IDA)

from __future__ import annotations
from pathlib import Path
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
OUT  = REPO / "results" / "regressions" / "bid" / "bid_shape"
TEX  = OUT / "tex"

REGIMES = {
    "3-sess":              ("2024-06-14", "2024-11-30"),
    "ISP15-win":           ("2024-12-01", "2025-03-18"),
    "DA60/ID15 pre-blk":   ("2025-03-19", "2025-04-27"),
    "DA60/ID15 post-blk":  ("2025-04-28", "2025-09-30"),
    "DA15/ID15":           ("2025-10-01", "2025-12-31"),
}
REGIME_ORDER = list(REGIMES.keys())
FOCUS_TECHS = ["CCGT", "Hydro", "Hydro_pump", "Nuclear", "Wind", "Solar PV", "Solar Thermal", "Cogen"]


def hour_class(h):
    if h in (5, 6, 7, 8, 16, 17, 18, 19, 20, 21, 22): return "Critical"
    if h in (1, 2, 3): return "Flat"
    if h in (11, 12, 13, 14): return "Midday"
    return "Dropped"


def tag_regime(date_series):
    out = pd.Series(index=date_series.index, dtype="object")
    for r, (a, b) in REGIMES.items():
        out.loc[(date_series >= a) & (date_series <= b)] = r
    return out


def process_market(market):
    print(f"=== {market} normalized ===")
    df = pd.read_parquet(OUT / f"{market}_per_cell.parquet")
    df["date"] = pd.to_datetime(df["date"])
    df["regime"] = tag_regime(df["date"])
    df = df.dropna(subset=["regime"])
    df["hour_class"] = df["hour"].apply(hour_class)

    # Compute share = mw_in_band / mw_total, per cell, clipping mw_total > 0
    df["share_in_band"] = df["mw_in_band"] / df["mw_total"].clip(lower=1e-6)
    df.loc[df["mw_total"] < 1e-6, "share_in_band"] = 0.0

    # Per-month aggregation (then mean across months within regime — for orientation only;
    # honest seasonality control is in fPCA score regressions, §3.4)
    agg_month = df.groupby(["tech_group", "regime", "hour_class", "year_month"]).agg(
        share_in_band=("share_in_band", "mean"),
        mw_in_band=("mw_in_band", "mean"),
        mw_total=("mw_total", "mean"),
        n_cells=("share_in_band", "size"),
    ).reset_index()
    agg = agg_month.groupby(["tech_group", "regime", "hour_class"]).agg(
        share_in_band=("share_in_band", "mean"),
        mw_in_band=("mw_in_band", "mean"),
        mw_total=("mw_total", "mean"),
        n_months=("n_cells", "size"),
    ).reset_index()
    agg["share_pct"] = (agg["share_in_band"] * 100).round(1)
    agg.to_csv(OUT / f"{market}_descriptive_normalized.csv", index=False)
    print(f"  wrote {market}_descriptive_normalized.csv")

    # Main tex table (tech rows × regime cols, share_pct)
    rows = []
    for hc in ["Critical", "Flat", "Midday"]:
        rows.append(f"\\multicolumn{{6}}{{l}}{{\\textit{{Hour-class: {hc}}}}} \\\\")
        sub = agg[(agg["hour_class"] == hc) & (agg["tech_group"].isin(FOCUS_TECHS))]
        piv = sub.pivot_table(index="tech_group", columns="regime", values="share_pct").round(1).fillna(0)
        piv = piv.reindex(FOCUS_TECHS).fillna(0)
        for r in REGIME_ORDER:
            if r not in piv.columns: piv[r] = 0
        piv = piv[REGIME_ORDER]
        for tech in piv.index:
            tech_label = tech.replace("_", " ")
            vals = " & ".join(f"{piv.loc[tech, r]:.1f}" for r in REGIME_ORDER)
            rows.append(f"{tech_label} & {vals} \\\\")
        rows.append("\\midrule")
    body = "\n".join(rows)
    header = " & ".join([""] + REGIME_ORDER)
    tex = (
        f"% auto-built by bid_shape_normalized.py\n"
        f"% Metric: share of unit's offered MW falling within [MCP-50, MCP+50] EUR/MWh.\n"
        f"\\begin{{tabular}}{{l r r r r r}}\n"
        f"\\toprule\n{header} \\\\\n\\midrule\n{body}\n\\bottomrule\n\\end{{tabular}}\n"
    )
    out = TEX / f"tab_bidshape_{market}_by_regime_normalized.tex"
    out.write_text(tex)
    print(f"  wrote {out}")

    # Per-firm CCGT
    sub = df[(df["tech_group"] == "CCGT") & (df["hour_class"].isin(["Critical", "Flat"]))].copy()
    if len(sub) == 0:
        return
    agg_m = sub.groupby(["firm", "regime", "hour_class", "year_month"]).agg(
        share=("share_in_band", "mean"),
    ).reset_index()
    agg_f = agg_m.groupby(["firm", "regime", "hour_class"]).agg(
        share=("share", "mean"),
    ).reset_index()
    agg_f["share_pct"] = (agg_f["share"] * 100).round(1)
    rows = []
    for hc in ["Critical", "Flat"]:
        rows.append(f"\\multicolumn{{6}}{{l}}{{\\textit{{CCGT only, {hc} hours}}}} \\\\")
        for firm in ["GN", "IB", "GE", "HC", "REP", "OTH"]:
            piv = agg_f[(agg_f["hour_class"] == hc) & (agg_f["firm"] == firm)]
            if len(piv) == 0:
                continue
            row_vals = []
            for r in REGIME_ORDER:
                v = piv[piv["regime"] == r]["share_pct"]
                row_vals.append(f"{v.iloc[0]:.1f}" if len(v) else "---")
            rows.append(f"{firm} & " + " & ".join(row_vals) + " \\\\")
        rows.append("\\midrule")
    body = "\n".join(rows)
    header = " & ".join([""] + REGIME_ORDER)
    tex = (
        f"% auto-built — normalized share metric\n"
        f"\\begin{{tabular}}{{l r r r r r}}\n"
        f"\\toprule\n{header} \\\\\n\\midrule\n{body}\n\\bottomrule\n\\end{{tabular}}\n"
    )
    out2 = TEX / f"tab_bidshape_{market}_ccgt_byfirm_normalized.tex"
    out2.write_text(tex)
    print(f"  wrote {out2}")


def main():
    for market in ["DA", "IDA"]:
        process_market(market)
    print("Done.")


if __name__ == "__main__":
    main()
