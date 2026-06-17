# STATUS: ALIVE
# LAST-AUDIT: 2026-05-15
# FEEDS: provisional.tex §7 (distribution of D_w by firm/tech/hour-class)
# CLAIM: Visualize the per-cell distribution of the kernel-weighted
#        within-hour quarter dissimilarity D_w. Three views:
#        (a) DA, per pivotal firm × CCGT × {critical, flat} CDF
#        (b) DA, per technology × {critical, flat} CDF (Big-4 pooled)
#        (c) DA, per (tech, firm) panel grid CDF in critical hours
#        Also compute a noise-floor threshold: P95 of flat-hour D_w
#        within each (firm, tech), and report flag rate using that
#        threshold instead of D_w > 0.

from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
DA_CELLS = REPO / "results" / "regressions" / "bid" / "quarter_dissimilarity" / "quarter_dissimilarity_cells_2025Q4.csv"
IDA_DIR  = REPO / "results" / "regressions" / "bid" / "quarter_dissimilarity_ida"
FIGDIR = REPO / "figures" / "working"
FIGDIR.mkdir(parents=True, exist_ok=True)
OUTDIR = REPO / "results" / "regressions" / "bid" / "quarter_dissim_distributions"
OUTDIR.mkdir(parents=True, exist_ok=True)

PIVOTAL = ("IB", "GE", "GN", "HC")
PRETTY  = {"IB": "Iberdrola", "GE": "Endesa", "GN": "Naturgy", "HC": "EDP"}
COLORS  = {"IB": "tab:green", "GE": "tab:red", "GN": "tab:orange", "HC": "tab:blue"}
TECHS   = ("CCGT", "Hydro", "Hydro_pump", "Nuclear", "Wind", "Solar PV",
           "Pump_load", "Retailer", "Direct_consumer")


def _quantile_bar_panel(ax, values_dict, *, ylabel, title, colors=None):
    """Per-group: two side-by-side bar charts.
    Left axis: % of cells with D_w = 0 (mass-at-zero).
    Right axis (twin): box of D_w in cells with D_w > 0
    (P25--P75 box, P50 line, P10--P90 whiskers; log y-scale).
    """
    groups = list(values_dict.keys())
    n = len(groups)
    x = np.arange(n)
    # zero shares (left)
    zero_shares = []
    bar_colors = []
    for g in groups:
        v = np.asarray(values_dict[g], float); v = v[~np.isnan(v)]
        zero_shares.append(100 * float(np.mean(v <= 1e-6)) if len(v) else np.nan)
        c = (colors or {}).get(g, COLORS.get(g, "tab:grey"))
        bar_colors.append(c)
    bars = ax.bar(x - 0.18, zero_shares, width=0.36, color=bar_colors, alpha=0.45,
                   edgecolor=bar_colors, label="% at $D_w = 0$")
    for xi, sh in zip(x - 0.18, zero_shares):
        if np.isfinite(sh):
            ax.text(xi, sh + 1, f"{sh:.0f}", ha="center", va="bottom", fontsize=7)
    ax.set_ylabel("% cells with $D_w = 0$")
    ax.set_ylim(0, 105)
    ax.set_xticks(x)
    ax.set_xticklabels(groups, fontsize=9)
    # box of positives (right axis)
    ax2 = ax.twinx()
    for i, g in enumerate(groups):
        v = np.asarray(values_dict[g], float); v = v[~np.isnan(v)]
        pos = v[v > 1e-6]
        if len(pos) < 5:
            continue
        p10, p25, p50, p75, p90 = np.percentile(pos, [10, 25, 50, 75, 90])
        xi = i + 0.18
        c = bar_colors[i]
        ax2.fill_between([xi - 0.16, xi + 0.16], [p25, p25], [p75, p75],
                          color=c, alpha=0.55, linewidth=0)
        ax2.plot([xi - 0.16, xi + 0.16], [p50, p50], color="black", lw=1.4)
        ax2.plot([xi, xi], [p10, p25], color=c, lw=1.2)
        ax2.plot([xi, xi], [p75, p90], color=c, lw=1.2)
        ax2.plot([xi - 0.06, xi + 0.06], [p10, p10], color=c, lw=1)
        ax2.plot([xi - 0.06, xi + 0.06], [p90, p90], color=c, lw=1)
    ax2.set_yscale("log")
    ax2.set_ylabel(ylabel + " (cells with $D_w > 0$)", fontsize=8)
    ax.set_title(title, fontsize=10)
    ax.grid(True, alpha=0.3, axis="y")


def plot_ccgt_firm_distributions(cells: pd.DataFrame, *, market_label: str, fname: str):
    """One-panel chart: per (firm × hour-class) show mass-at-zero (bar)
    and the box of D_w in cells with D_w > 0 (P10/25/50/75/90)."""
    ccgt = cells[cells["tech_group"] == "CCGT"]
    fig, ax = plt.subplots(figsize=(10, 4.5))
    groups, vals_dict, colors_dict = [], {}, {}
    for firm in PIVOTAL:
        for hc, c in (("critical", "tab:red"), ("flat", "tab:blue")):
            key = f"{firm}\n{hc}"
            sub = ccgt[(ccgt["firm"] == firm) & (ccgt["hour_class"] == hc)]
            vals_dict[key] = sub["d_max_w"].dropna().values
            colors_dict[key] = c
            groups.append(key)
    _quantile_bar_panel(ax, vals_dict,
                        ylabel=r"$D_w$",
                        title=f"CCGT {market_label}: % cells at $D_w = 0$ (bar) and box of $D_w > 0$ (P10/25/50/75/90)",
                        colors=colors_dict)
    plt.tight_layout()
    out = FIGDIR / fname
    plt.savefig(out, dpi=150, bbox_inches="tight")
    plt.savefig(out.with_suffix(".png"), dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"wrote {out}")


def plot_per_tech_distributions(cells: pd.DataFrame, *, market_label: str, fname: str):
    """One panel: per (tech × hour-class) mass-at-zero bar + box of D_w > 0.
    Big-4 firms pooled. Techs ordered by sample size (decreasing)."""
    techs_present = [t for t in TECHS if (cells["tech_group"] == t).sum() >= 200]
    fig, ax = plt.subplots(figsize=(min(2 + 1.2 * len(techs_present) * 2, 15), 4.5))
    vals_dict, colors_dict = {}, {}
    for tech in techs_present:
        for hc, c in (("critical", "tab:red"), ("flat", "tab:blue")):
            key = f"{tech}\n{hc}"
            sub = cells[(cells["tech_group"] == tech) & (cells["hour_class"] == hc)]
            vals_dict[key] = sub["d_max_w"].dropna().values
            colors_dict[key] = c
    _quantile_bar_panel(ax, vals_dict,
                        ylabel=r"$D_w$",
                        title=f"{market_label} by technology (Big-4 pooled): % at 0 (bar) and $D_w > 0$ box",
                        colors=colors_dict)
    plt.tight_layout()
    out = FIGDIR / fname
    plt.savefig(out, dpi=150, bbox_inches="tight")
    plt.savefig(out.with_suffix(".png"), dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"wrote {out}")


def noise_floor_table(cells: pd.DataFrame) -> pd.DataFrame:
    """Per (firm, tech, hour_class), compute:
       - P95 of flat-hour D_w → noise-floor threshold
       - Critical-hour flag rate at D_w > 0 (current)
       - Critical-hour flag rate at D_w > flat-P95 (above noise floor)
    """
    rows = []
    for tech in TECHS:
        for firm in PIVOTAL:
            sub_t = cells[(cells["firm"] == firm) & (cells["tech_group"] == tech)]
            if sub_t.empty:
                continue
            flat = sub_t[sub_t["hour_class"] == "flat"]["d_max_w"].dropna().values
            crit = sub_t[sub_t["hour_class"] == "critical"]["d_max_w"].dropna().values
            if len(crit) == 0:
                continue
            floor_p95 = np.quantile(flat, 0.95) if len(flat) >= 20 else np.nan
            rows.append({
                "tech": tech, "firm": firm,
                "n_crit": len(crit), "n_flat": len(flat),
                "flat_p95":   floor_p95,
                "frac_crit_gt0":    100 * float(np.mean(crit > 1e-6)),
                "frac_crit_above_floor": 100 * float(np.mean(crit > floor_p95))
                                          if np.isfinite(floor_p95) else np.nan,
                "frac_flat_gt0":    100 * float(np.mean(flat > 1e-6)) if len(flat) else np.nan,
            })
    return pd.DataFrame(rows)


def ida_per_firm_distribution(label: str):
    cells = pd.read_csv(IDA_DIR / f"cells_{label}.csv")
    market_label = f"IDA, {label.replace('_blackout','').replace('_','-')}-blackout"
    plot_ccgt_firm_distributions(cells, market_label=market_label,
                                  fname=f"fig_dw_box_ida_ccgt_by_firm_{label}.pdf")
    plot_per_tech_distributions(cells, market_label=f"IDA, {label.replace('_blackout','').replace('_','-')}-blackout",
                                 fname=f"fig_dw_box_ida_by_tech_{label}.pdf")


def main():
    print("loading DA cells...")
    cells = pd.read_csv(DA_CELLS)
    cells = cells[cells["hour_class"].isin(("critical", "flat"))]
    print(f"  {len(cells):,} cells (critical+flat only)")

    print("\nDA: per-firm CCGT...")
    plot_ccgt_firm_distributions(cells,
                                  market_label="DA, Oct--Dec 2025",
                                  fname="fig_dw_box_da_ccgt_by_firm.pdf")
    print("DA: per-tech...")
    plot_per_tech_distributions(cells,
                                 market_label="DA, Oct--Dec 2025",
                                 fname="fig_dw_box_da_by_tech.pdf")

    print("\nIDA pre/post-blackout per-firm CCGT + per-tech...")
    ida_per_firm_distribution("pre_blackout")
    ida_per_firm_distribution("post_blackout")

    print("\nDA noise-floor threshold analysis...")
    nf = noise_floor_table(cells)
    nf.to_csv(OUTDIR / "noise_floor_da.csv", index=False)
    print(nf.to_string(index=False))

    print("\nIDA pre noise floor:")
    pre = pd.read_csv(IDA_DIR / "cells_pre_blackout.csv")
    pre["hour_class"] = pre["hour_class"]
    pre = pre[pre["hour_class"].isin(("critical", "flat"))]
    nf_pre = noise_floor_table(pre)
    nf_pre.to_csv(OUTDIR / "noise_floor_ida_pre.csv", index=False)
    print(nf_pre[nf_pre["tech"] == "CCGT"].to_string(index=False))

    print("\nIDA post noise floor:")
    post = pd.read_csv(IDA_DIR / "cells_post_blackout.csv")
    post = post[post["hour_class"].isin(("critical", "flat"))]
    nf_post = noise_floor_table(post)
    nf_post.to_csv(OUTDIR / "noise_floor_ida_post.csv", index=False)
    print(nf_post[nf_post["tech"] == "CCGT"].to_string(index=False))


if __name__ == "__main__":
    main()
