# STATUS: ALIVE
# LAST-AUDIT: 2026-05-08
# FEEDS: thesis paper.tex tables (§3.4, §3.5, §5)
# CLAIM: Builds publication-ready LaTeX tables from B1-B5 outputs.
# Each table is a standalone .tex snippet with booktabs formatting,
# saved to thesis/paper/tables/. paper.tex \input{}s these directly.
#
# Significance discipline (per CLAUDE.md + 2026-05-08 user direction):
#   *** p < 0.001  (HEADLINE threshold; large effective sample)
#   **  p < 0.01
#   *   p < 0.05
# Headline interpretation in the thesis relies on *** only.
# Each table caption includes a footnote noting this convention.

from __future__ import annotations

from pathlib import Path
import pandas as pd
import numpy as np

REPO = Path(__file__).resolve().parents[3]
SRC = REPO / "results" / "regressions" / "firm" / "critical_hours_thesis"
OUT = REPO / "thesis" / "paper" / "tables"
OUT.mkdir(parents=True, exist_ok=True)


def stars(p: float) -> str:
    if pd.isna(p): return ""
    if p < 0.001: return "$^{***}$"
    if p < 0.01:  return "$^{**}$"
    if p < 0.05:  return "$^{*}$"
    return ""


def fmt_coef(b: float, se: float, p: float, decimals: int = 2) -> str:
    """Coefficient with stars and SE in parentheses below."""
    if pd.isna(b): return "---"
    s = stars(p)
    if pd.isna(se):
        return f"${b:.{decimals}f}${s}"
    return f"${b:+.{decimals}f}${s} \\\\ ({se:.{decimals}f})"


def write_table(content: str, filename: str) -> None:
    path = OUT / filename
    path.write_text(content)
    print(f"  wrote: {path}")


# ────────────────────────────────────────────────────────────────────────
# Table 1 — B1 headline DiD on q_2: pooled / treatment / placebo
# ────────────────────────────────────────────────────────────────────────

def build_b1_main_table():
    """Three-column main DiD table: pooled / treatment / placebo."""
    df = pd.read_csv(SRC / "B1_q2_did.csv")
    rows_we_want = ["all_firms", "treatment_only", "placebo_only"]
    sub = df[df["label"].isin(rows_we_want)].set_index("label").reindex(rows_we_want)

    def cell(row, key, dec=2):
        v = row[key]
        if pd.isna(v): return "---"
        return f"{v:.{dec}f}"

    tex = []
    tex.append(r"\begin{tabular}{l c c c}")
    tex.append(r"\toprule")
    tex.append(r" & All firms & Pivotal firms & Non-pivotal firms \\")
    tex.append(r" & (1) & (2) & (3) \\")
    tex.append(r"\midrule")

    # β₃ row
    line = r"$\beta_3$: \texttt{crit} $\times$ \texttt{post}"
    for lbl in rows_we_want:
        r = sub.loc[lbl]
        line += " & " + (f"${r['beta_3']:.2f}${stars(r['p'])}" if not pd.isna(r['beta_3']) else "---")
    line += r" \\"
    tex.append(line)
    # SE row
    line = r" \quad cluster-robust SE"
    for lbl in rows_we_want:
        r = sub.loc[lbl]
        line += " & " + (f"({r['se']:.2f})" if not pd.isna(r['se']) else "")
    line += r" \\"
    tex.append(line)
    # p row
    line = r" \quad $p$"
    for lbl in rows_we_want:
        r = sub.loc[lbl]
        line += " & " + (f"{r['p']:.4f}" if not pd.isna(r['p']) else "")
    line += r" \\"
    tex.append(line)
    tex.append(r"\midrule")

    # other rows
    for label_disp, key in [
        (r"$\beta_1$: \texttt{crit}", "beta_1_crit"),
        (r"$\beta_2$: \texttt{post}", "beta_2_post"),
    ]:
        line = label_disp
        for lbl in rows_we_want:
            r = sub.loc[lbl]
            v = r.get(key, np.nan)
            line += " & " + (f"${v:.2f}$" if not pd.isna(v) else "---")
        line += r" \\"
        tex.append(line)

    tex.append(r"\midrule")
    # mean rows
    for label_disp, key in [
        (r"$\bar y$ (pre, flat)", "y_mean_pre_flat"),
        (r"$\bar y$ (pre, crit)", "y_mean_pre_crit"),
        (r"$\bar y$ (post, flat)", "y_mean_post_flat"),
        (r"$\bar y$ (post, crit)", "y_mean_post_crit"),
    ]:
        line = label_disp
        for lbl in rows_we_want:
            r = sub.loc[lbl]
            v = r.get(key, np.nan)
            line += " & " + (f"{v:.2f}" if not pd.isna(v) else "---")
        line += r" \\"
        tex.append(line)
    tex.append(r"\midrule")
    # n + clusters
    line = r"$n$"
    for lbl in rows_we_want:
        r = sub.loc[lbl]
        line += f" & {int(r['n']):,}"
    line += r" \\"
    tex.append(line)
    line = r"clusters $G$ (dates)"
    for lbl in rows_we_want:
        r = sub.loc[lbl]
        line += f" & {int(r['n_clusters'])}"
    line += r" \\"
    tex.append(line)
    tex.append(r"\bottomrule")
    tex.append(r"\end{tabular}")

    write_table("\n".join(tex), "tab_B1_main.tex")


# ────────────────────────────────────────────────────────────────────────
# Table 2 — B1 tech-stratified β_3 (treatment + placebo side-by-side)
# ────────────────────────────────────────────────────────────────────────

def build_b1_tech_table():
    df = pd.read_csv(SRC / "B1_q2_did.csv")
    techs = ["CCGT", "Hydro", "Hydro_pump", "Coal", "Nuclear", "Wind", "Biomass"]
    treat = df[df["label"].str.startswith("treatment_") & ~df["label"].isin(["treatment_only"])]
    plac = df[df["label"].str.startswith("placebo_") & ~df["label"].isin(["placebo_only"])]
    treat = treat.assign(tech=treat["label"].str.replace("treatment_", "")).set_index("tech")
    plac  = plac.assign(tech=plac["label"].str.replace("placebo_", "")).set_index("tech")

    tex = []
    tex.append(r"\begin{tabular}{l c c c c c}")
    tex.append(r"\toprule")
    tex.append(r" & \multicolumn{2}{c}{Pivotal firms} & \multicolumn{2}{c}{Non-pivotal firms} & \\")
    tex.append(r"\cmidrule(lr){2-3} \cmidrule(lr){4-5}")
    tex.append(r"Technology & $\beta_3$ & SE / $n$ & $\beta_3$ & SE / $n$ & Predicted \\")
    tex.append(r"\midrule")

    pred = {
        "CCGT": "+", "Coal": "+", "Hydro": "+", "Hydro_pump": "+",
        "Nuclear": "0", "Wind": "0", "Biomass": "0",
    }
    for t in techs:
        line = t.replace("_", r"\_")
        for src in [treat, plac]:
            if t in src.index:
                r = src.loc[t]
                if pd.notna(r.get("beta_3", np.nan)):
                    line += f" & ${r['beta_3']:.2f}${stars(r['p'])} & ({r['se']:.2f}) / {int(r['n']):,}"
                else:
                    line += " & --- & ---"
            else:
                line += " & --- & ---"
        line += f" & {pred[t]} \\\\"
        tex.append(line)

    tex.append(r"\bottomrule")
    tex.append(r"\end{tabular}")
    write_table("\n".join(tex), "tab_B1_tech_stratified.tex")


# ────────────────────────────────────────────────────────────────────────
# Table 3 — B1 per-firm β_3 (treatment group; q_2 + B3 DA cleared joint)
# ────────────────────────────────────────────────────────────────────────

FIRM_DISPLAY = {
    "IB": "Iberdrola",
    "GE": "Endesa",
    "GN": "Naturgy",
    "HC": "EDP-Spain",
    "EDP-PT": "EDP-Portugal",
    "Engie": "Engie España",
    "Repsol": "Repsol",
    "TotalEnergies": "TotalEnergies",
    "Moeve": "Moeve",
}


def build_per_firm_table():
    b1 = pd.read_csv(SRC / "B1_q2_did.csv")
    b3 = pd.read_csv(SRC / "B3_da_cleared_did.csv")
    firms = ["IB", "GE", "GN", "HC", "EDP-PT"]
    b1 = b1[b1["label"].str.startswith("firm_")].assign(firm=b1["label"].str.replace("firm_","")).set_index("firm")
    b3 = b3[b3["label"].str.startswith("firm_")].assign(firm=b3["label"].str.replace("firm_","")).set_index("firm")

    tex = []
    tex.append(r"\begin{tabular}{l c c c c}")
    tex.append(r"\toprule")
    tex.append(r" & \multicolumn{2}{c}{$q_2$ outcome (B1)} & \multicolumn{2}{c}{DA cleared (B3)} \\")
    tex.append(r"\cmidrule(lr){2-3} \cmidrule(lr){4-5}")
    tex.append(r"Firm & $\beta_3$ & SE & $\beta_3$ & SE \\")
    tex.append(r"\midrule")

    for f in firms:
        line = FIRM_DISPLAY.get(f, f)
        for src in [b1, b3]:
            if f in src.index:
                r = src.loc[f]
                if pd.notna(r.get("beta_3", np.nan)):
                    line += f" & ${r['beta_3']:.2f}${stars(r['p'])} & ({r['se']:.2f})"
                else:
                    line += " & --- & ---"
            else:
                line += " & --- & ---"
        line += r" \\"
        tex.append(line)

    tex.append(r"\bottomrule")
    tex.append(r"\end{tabular}")
    write_table("\n".join(tex), "tab_per_firm_q2_da.tex")


# ────────────────────────────────────────────────────────────────────────
# Table 4 — B2 DA-IDA wedge DiD
# ────────────────────────────────────────────────────────────────────────

def build_b2_wedge_table():
    df = pd.read_csv(SRC / "B2_wedge_did.csv").set_index("label")
    rows = [
        ("samecal", "Same-cal-month (Oct-Dec 2024 vs 2025)"),
        ("full_no_ctrl", "Full window, no controls"),
        ("full_reforzada_ctrl", "Full window, + reforzada $\\times$ crit"),
        ("full_both_ctrls", "Full window, + reforzada + MTU15-IDA $\\times$ crit"),
    ]
    tex = []
    tex.append(r"\begin{tabular}{l c c c c}")
    tex.append(r"\toprule")
    tex.append(r"Specification & $\beta_3$ & SE & $p$ & $G$ \\")
    tex.append(r"\midrule")
    for key, lbl in rows:
        if key not in df.index: continue
        r = df.loc[key]
        line = lbl
        if pd.notna(r["beta_3"]):
            line += f" & ${r['beta_3']:.2f}${stars(r['p'])} & ({r['se']:.2f}) & {r['p']:.4f} & {int(r['n_clusters'])}"
        else:
            line += " & --- & --- & --- & ---"
        line += r" \\"
        tex.append(line)
    tex.append(r"\bottomrule")
    tex.append(r"\end{tabular}")
    write_table("\n".join(tex), "tab_B2_wedge.tex")


# ────────────────────────────────────────────────────────────────────────
# Table 5 — B4 CPT spec stack
# ────────────────────────────────────────────────────────────────────────

def build_b4_cpt_table():
    df = pd.read_csv(SRC / "B4_cpt_panel.csv").set_index("label")
    order = ["1_baseline_B1", "2_plus_wind_solar_levels", "3_plus_crit_x_VRE", "4_plus_cal_month_FE"]
    labels = {
        "1_baseline_B1": "(1) Baseline (firm + DOW FE)",
        "2_plus_wind_solar_levels": "(2) + wind, solar levels",
        "3_plus_crit_x_VRE": r"(3) + crit $\times$ VRE",
        "4_plus_cal_month_FE": "(4) + calendar-month FE",
    }
    tex = []
    tex.append(r"\begin{tabular}{l c c c c}")
    tex.append(r"\toprule")
    tex.append(r"Specification & $\beta_3$ & SE & $p$ & $G$ \\")
    tex.append(r"\midrule")
    for key in order:
        if key not in df.index: continue
        r = df.loc[key]
        line = labels[key]
        if pd.notna(r["beta_3"]):
            line += f" & ${r['beta_3']:.2f}${stars(r['p'])} & ({r['se']:.2f}) & {r['p']:.4f} & {int(r['n_clusters'])}"
        else:
            line += " & --- & --- & --- & ---"
        line += r" \\"
        tex.append(line)
    tex.append(r"\bottomrule")
    tex.append(r"\end{tabular}")
    write_table("\n".join(tex), "tab_B4_cpt.tex")


# ────────────────────────────────────────────────────────────────────────
# Table 6 — B5 robustness panel
# ────────────────────────────────────────────────────────────────────────

def build_b5_robustness_table():
    df = pd.read_csv(SRC / "B5_robustness.csv").set_index("label")
    sections = [
        ("B5.1 — Critical-hours definition", [
            ("B5.1_canonical_demand_surge_vre_transition_h5_6_7_8_16_17_18_19", r"\textbf{Canonical} h\{5-8, 16-19\} (demand surge $\cup$ VRE transition)"),
            ("B5.1_supply_ramp_h7_8_16_17_18", "supply\\_ramp h\\{7,8,16,17,18\\}"),
            ("B5.1_price_peak_h18_19_20_21_22", "price\\_peak h\\{18-22\\}"),
            ("B5.1_demand_peak_h16_17_18_19_20", "demand\\_peak h\\{16-20\\}"),
            ("B5.1_joint_h7_8_16_17_18_19_20_21_22", "joint h\\{7-8, 16-22\\}"),
        ]),
        ("B5.2 — Firm partition", [
            ("B5.2a_pivotality_treatment_set", "Pivotality-based firm set"),
            ("B5.2b_admin_IB_GE_GN_HC", "Administrative IB/GE/GN/HC"),
        ]),
        ("B5.3 / B5.5 — Window", [
            ("B5.3a_full_window_2024_2025", "Full window 2024-2025"),
            ("B5.5_full_window_drop_Apr_Sep_2025", "Full window minus Apr-Sep 2025"),
        ]),
        ("B5.4 — Sample exclusions", [
            ("B5.4a_drop_EDP-PT", "Drop EDP-PT"),
            ("B5.4b_drop_ABO2G", "Drop ABO2G"),
        ]),
        ("B5.6 — DST transition days (CET $\\leftrightarrow$ CEST)", [
            ("B5.6a_samecal_drop_DST_days", "Same-cal-month, drop DST transition days"),
            ("B5.6b_full_window_drop_DST_days", "Full window, drop DST transition days"),
        ]),
        ("B5.7 — DST regime separation (clock-hour semantics differ across CEST/CET)", [
            ("B5.7a_samecal_CEST_only", "Same-cal-month, CEST days only (Oct 1--25)"),
            ("B5.7b_samecal_CET_only",  "Same-cal-month, CET days only (Oct 27--Dec 31)"),
        ]),
    ]
    tex = []
    tex.append(r"\begin{tabular}{l c c c c}")
    tex.append(r"\toprule")
    tex.append(r"Sensitivity & $\beta_3$ & SE & $p$ & $G$ \\")
    tex.append(r"\midrule")
    for header, items in sections:
        tex.append(r"\multicolumn{5}{l}{\textit{" + header + r"}} \\")
        for key, lbl in items:
            if key not in df.index: continue
            r = df.loc[key]
            line = r"\quad " + lbl
            if pd.notna(r["beta_3"]):
                line += f" & ${r['beta_3']:.2f}${stars(r['p'])} & ({r['se']:.2f}) & {r['p']:.4f} & {int(r['n_clusters'])}"
            else:
                line += " & --- & --- & --- & ---"
            line += r" \\"
            tex.append(line)
        tex.append(r"\addlinespace")
    tex.append(r"\bottomrule")
    tex.append(r"\end{tabular}")
    write_table("\n".join(tex), "tab_B5_robustness.tex")


def main():
    print("Building thesis LaTeX tables...")
    build_b1_main_table()
    build_b1_tech_table()
    build_per_firm_table()
    build_b2_wedge_table()
    build_b4_cpt_table()
    build_b5_robustness_table()
    print("\nDone.")


if __name__ == "__main__":
    main()
