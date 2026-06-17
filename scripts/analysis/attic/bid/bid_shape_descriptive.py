# STATUS: ALIVE
# LAST-AUDIT: 2026-05-18
# FEEDS: descriptive_facts.tex §3 (Pass 2 of bidding-patterns priority — descriptive tables/figures)
# CLAIM: From the per-cell parquets, build descriptive tables and figures
#        per (tech, firm, regime, hour-class). Outputs LaTeX-ready fragments
#        and per-regime PDF figures.
#
# Outputs:
#   results/regressions/bid/bid_shape/
#     descriptive_per_regime.csv          (long format)
#     tex/tab_bidshape_DA_by_regime.tex   (CCGT / Hydro / Wind / Solar PV rows × regime cols × hour-class)
#     tex/tab_bidshape_IDA_by_regime.tex  (same for IDA)
#   figures/working/
#     bidshape_DA_per_regime_critical.pdf
#     bidshape_DA_per_regime_flat.pdf
#     bidshape_IDA_per_regime_critical.pdf
#     bidshape_IDA_per_regime_flat.pdf

from __future__ import annotations
from pathlib import Path
import duckdb
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

REPO = Path(__file__).resolve().parents[3]
IN   = REPO / "results" / "regressions" / "bid" / "bid_shape"
OUT  = IN
(OUT / "tex").mkdir(parents=True, exist_ok=True)
FIG  = REPO / "figures" / "working"
FIG.mkdir(parents=True, exist_ok=True)

REGIMES = {
    "3-sess":              ("2024-06-14", "2024-11-30"),
    "ISP15-win":           ("2024-12-01", "2025-03-18"),
    "DA60/ID15 pre-blk":   ("2025-03-19", "2025-04-27"),
    "DA60/ID15 post-blk":  ("2025-04-28", "2025-09-30"),
    "DA15/ID15":           ("2025-10-01", "2025-12-31"),
}

REGIME_ORDER = list(REGIMES.keys())

# Techs we focus on (others rolled into "Other")
FOCUS_TECHS = ["CCGT", "Hydro", "Hydro_pump", "Nuclear", "Wind", "Solar PV", "Solar Thermal", "Cogen"]


def tag_regime(date_series):
    out = pd.Series(index=date_series.index, dtype="object")
    for r, (a, b) in REGIMES.items():
        mask = (date_series >= a) & (date_series <= b)
        out.loc[mask] = r
    return out


def load_per_cell(market):
    f = IN / f"{market}_per_cell.parquet"
    if not f.exists():
        raise FileNotFoundError(f"{f} missing; run bid_shape_metrics.py first")
    df = pd.read_parquet(f)
    df["date"] = pd.to_datetime(df["date"])
    df["regime"] = tag_regime(df["date"])
    df = df.dropna(subset=["regime"])
    return df


def hour_class(h):
    if h in (5, 6, 7, 8, 16, 17, 18, 19, 20, 21, 22): return "Critical"
    if h in (1, 2, 3): return "Flat"
    if h in (11, 12, 13, 14): return "Midday"
    return "Dropped"


def descriptive_table(df, market_label):
    df = df.copy()
    df["hour_class"] = df["hour"].apply(hour_class)
    # Aggregate to (tech, regime, hour-class, calendar month) — to enable season control
    agg_month = df.groupby(["tech_group", "regime", "hour_class", "year_month"]).agg(
        mw_in_band=("mw_in_band", "mean"),
        n_tranches=("n_tranches_in_band", "mean"),
        frac_single_block=("frac_single_block", "mean"),
        mw_at_mcp=("mw_at_mcp", "mean"),
        n_cells=("mw_in_band", "size"),
    ).reset_index()
    # Then average across months within regime
    agg = agg_month.groupby(["tech_group", "regime", "hour_class"]).agg(
        mw_in_band=("mw_in_band", "mean"),
        n_tranches=("n_tranches", "mean"),
        frac_single_block=("frac_single_block", "mean"),
        mw_at_mcp=("mw_at_mcp", "mean"),
        n_months=("n_cells", "size"),
        n_cells_total=("n_cells", "sum"),
    ).reset_index()
    agg["market"] = market_label
    return agg


def write_tex_table(agg, market_label, outfile):
    """Per-tech (rows) × regime (cols), within hour-class block."""
    rows = []
    for hc in ["Critical", "Flat", "Midday"]:
        rows.append(f"\\multicolumn{{6}}{{l}}{{\\textit{{Hour-class: {hc}}}}} \\\\")
        sub = agg[(agg["hour_class"] == hc) & (agg["tech_group"].isin(FOCUS_TECHS))]
        piv = sub.pivot_table(index="tech_group", columns="regime", values="mw_in_band").round(0).fillna(0)
        piv = piv.reindex(FOCUS_TECHS).fillna(0)
        for r in REGIME_ORDER:
            if r not in piv.columns: piv[r] = 0
        piv = piv[REGIME_ORDER]
        for tech in piv.index:
            vals = " & ".join(f"{piv.loc[tech, r]:.0f}" for r in REGIME_ORDER)
            rows.append(f"{tech} & {vals} \\\\")
        rows.append("\\midrule")
    body = "\n".join(rows)
    header = " & ".join([""] + [r.replace("/", "/") for r in REGIME_ORDER])
    tex = (
        f"% auto-built by bid_shape_descriptive.py\n"
        f"% Metric: MW within band [MCP-50, MCP+50] EUR/MWh (steepness proxy)\n"
        f"\\begin{{tabular}}{{l r r r r r}}\n"
        f"\\toprule\n"
        f"{header} \\\\\n"
        f"\\midrule\n"
        f"{body}\n"
        f"\\bottomrule\n"
        f"\\end{{tabular}}\n"
    )
    outfile.write_text(tex)
    print(f"  wrote {outfile}")


def fig_per_regime(agg, market_label, hour_class_name, outfile):
    """Bar chart: per regime, mean mw_in_band by tech."""
    sub = agg[(agg["hour_class"] == hour_class_name) & (agg["tech_group"].isin(FOCUS_TECHS))]
    piv = sub.pivot_table(index="tech_group", columns="regime", values="mw_in_band").fillna(0)
    piv = piv.reindex(FOCUS_TECHS).fillna(0)
    for r in REGIME_ORDER:
        if r not in piv.columns: piv[r] = 0
    piv = piv[REGIME_ORDER]
    fig, ax = plt.subplots(figsize=(11, 5.5))
    n = len(piv.columns)
    width = 0.16
    x = np.arange(len(piv.index))
    colors = plt.cm.viridis(np.linspace(0, 1, n))
    for j, c in enumerate(piv.columns):
        ax.bar(x + j*width, piv[c], width, label=c, color=colors[j])
    ax.set_ylabel("Mean MW in band [MCP±50 EUR/MWh] per ISP cell")
    ax.set_title(f"{market_label} bid-shape — {hour_class_name} hours, per regime × tech")
    ax.set_xticks(x + (n-1)*width/2)
    ax.set_xticklabels(piv.index, rotation=15, ha="right")
    ax.legend(loc="upper right", fontsize=8, ncol=2)
    ax.grid(alpha=0.3, axis="y")
    fig.tight_layout()
    fig.savefig(outfile)
    plt.close(fig)
    print(f"  wrote {outfile}")


def per_firm_table(df, market_label, outfile):
    """Within CCGT specifically, per-firm × regime mw_in_band (the strategic story)."""
    df = df.copy()
    df["hour_class"] = df["hour"].apply(hour_class)
    sub = df[(df["tech_group"] == "CCGT") & (df["hour_class"].isin(["Critical", "Flat"]))]
    if len(sub) == 0:
        print(f"  no CCGT data for {market_label}; skipping firm table")
        return
    agg_month = sub.groupby(["firm", "regime", "hour_class", "year_month"]).agg(
        mw_in_band=("mw_in_band", "mean"),
    ).reset_index()
    agg = agg_month.groupby(["firm", "regime", "hour_class"]).agg(
        mw_in_band=("mw_in_band", "mean"),
    ).reset_index()
    # write a tex fragment: per (regime, hour_class) → firm rows
    rows = []
    for hc in ["Critical", "Flat"]:
        rows.append(f"\\multicolumn{{6}}{{l}}{{\\textit{{CCGT only, {hc} hours}}}} \\\\")
        for firm in ["GN", "IB", "GE", "HC", "REP", "OTH"]:
            piv = agg[(agg["hour_class"] == hc) & (agg["firm"] == firm)]
            if len(piv) == 0:
                continue
            row_vals = []
            for r in REGIME_ORDER:
                v = piv[piv["regime"] == r]["mw_in_band"]
                row_vals.append(f"{v.iloc[0]:.0f}" if len(v) else "---")
            rows.append(f"{firm} & " + " & ".join(row_vals) + " \\\\")
        rows.append("\\midrule")
    body = "\n".join(rows)
    header = " & ".join([""] + REGIME_ORDER)
    tex = (
        f"% auto-built\n"
        f"\\begin{{tabular}}{{l r r r r r}}\n"
        f"\\toprule\n"
        f"{header} \\\\\n"
        f"\\midrule\n"
        f"{body}\n"
        f"\\bottomrule\n"
        f"\\end{{tabular}}\n"
    )
    outfile.write_text(tex)
    print(f"  wrote {outfile}")


def main():
    for market in ["DA", "IDA"]:
        print(f"=== {market} descriptive ===")
        df = load_per_cell(market)
        agg = descriptive_table(df, market)
        agg.to_csv(OUT / f"{market}_descriptive.csv", index=False)
        write_tex_table(agg, market, OUT / "tex" / f"tab_bidshape_{market}_by_regime.tex")
        fig_per_regime(agg, market, "Critical", FIG / f"bidshape_{market}_critical.pdf")
        fig_per_regime(agg, market, "Flat",     FIG / f"bidshape_{market}_flat.pdf")
        per_firm_table(df, market, OUT / "tex" / f"tab_bidshape_{market}_ccgt_byfirm.tex")
    print("Done.")


if __name__ == "__main__":
    main()
