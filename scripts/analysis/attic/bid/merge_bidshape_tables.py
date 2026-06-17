# STATUS: ALIVE
# LAST-AUDIT: 2026-05-19
# CLAIM: Merge separate DA + IDA bid-shape tables into a single side-by-side
#        panel for the doc, saving ~2 pages. Same for the MIC-filtered pair.
#        Pure text-manipulation: parses the existing tex outputs and rewrites
#        a wide tabular with DA columns + IDA columns per (hour-class, tech) row.
#
# IN:  results/regressions/bid/seasonality_adjusted/tab_bidshape_DA_by_regime_deseasonalized.tex
#      results/regressions/bid/seasonality_adjusted/tab_bidshape_IDA_by_regime_deseasonalized.tex
#      results/regressions/bid/bid_shape/tex/tab_bidshape_DA_by_regime_mic.tex
#      results/regressions/bid/bid_shape/tex/tab_bidshape_IDA_by_regime_mic.tex
#
# OUT: results/regressions/bid/seasonality_adjusted/tab_bidshape_DA_IDA_combined.tex
#      results/regressions/bid/bid_shape/tex/tab_bidshape_DA_IDA_mic_combined.tex

from __future__ import annotations
from pathlib import Path
import re

REPO = Path(__file__).resolve().parents[3]
SA_DIR = REPO / "results/regressions/bid/seasonality_adjusted"
MIC_DIR = REPO / "results/regressions/bid/bid_shape/tex"

REGIME_SHORT = ["3-sess", "ISP15-win", "DA60/ID15 pre", "DA60/ID15 post", "DA15/ID15"]


def parse_panel(tex_content: str) -> dict[str, list[list[str]]]:
    """Parse a hour-class panel-based bidshape tex tabular.

    Returns: {hour_class: [[tech_label, cell1, cell2, cell3, cell4, cell5], ...]}.
    """
    panels: dict[str, list[list[str]]] = {}
    current_hc = None
    for line in tex_content.split("\n"):
        stripped = line.strip()
        m = re.match(r'\\multicolumn\{\d+\}\{l\}\{\\textit\{Hour-class:\s*(\w+)\}\}', stripped)
        if m:
            current_hc = m.group(1)
            panels[current_hc] = []
            continue
        if current_hc is None:
            continue
        if any(stripped.startswith(s) for s in (r"\toprule", r"\midrule", r"\bottomrule",
                                                  r"\begin", r"\end", r"\multicolumn", r"\addlinespace",
                                                  r"%", r"\cmidrule")):
            continue
        if r"\\" not in stripped:
            continue
        # data row: split by ' & '
        row = stripped.rstrip(r" \\").strip()
        cols = [c.strip() for c in row.split(" & ")]
        if len(cols) >= 2:
            panels[current_hc].append(cols)
    return panels


def build_combined_panel_table(da_panels, ida_panels, label_prefix=""):
    """Build a tex tabular with DA on the left and IDA on the right per row.

    Layout: tech | DA × 5 regimes | IDA × 5 regimes  (11 columns total).
    """
    lines = [
        f"% Combined DA / IDA bid-shape table; merged from the two per-market tex files{(' ' + label_prefix) if label_prefix else ''}.",
        r"\begin{tabular}{@{}l " + ("r " * 5) + " " + ("r " * 5) + r"@{}}",
        r"\toprule",
        r" & \multicolumn{5}{c}{\textbf{DA market}} & \multicolumn{5}{c}{\textbf{IDA market}} \\",
        r"\cmidrule(lr){2-6}\cmidrule(lr){7-11}",
        r" & " + " & ".join(REGIME_SHORT) + " & " + " & ".join(REGIME_SHORT) + r" \\",
        r"\midrule",
    ]
    for hc in ["Critical", "Flat", "Midday"]:
        if hc not in da_panels and hc not in ida_panels:
            continue
        lines.append(r"\multicolumn{11}{@{}l}{\textit{Hour-class: " + hc + r"}} \\")
        da_rows = {r[0]: r[1:6] for r in da_panels.get(hc, []) if len(r) >= 6}
        ida_rows = {r[0]: r[1:6] for r in ida_panels.get(hc, []) if len(r) >= 6}
        techs = list(da_rows.keys()) + [t for t in ida_rows.keys() if t not in da_rows]
        for tech in techs:
            da_cells = da_rows.get(tech, ["--"] * 5)
            ida_cells = ida_rows.get(tech, ["--"] * 5)
            lines.append(" & ".join([tech] + da_cells + ida_cells) + r" \\")
        if hc != "Midday":
            lines.append(r"\midrule")
    lines.extend([r"\bottomrule", r"\end{tabular}"])
    return "\n".join(lines) + "\n"


def main():
    # SA-deseasonalised pair
    da_sa = parse_panel((SA_DIR / "tab_bidshape_DA_by_regime_deseasonalized.tex").read_text())
    ida_sa = parse_panel((SA_DIR / "tab_bidshape_IDA_by_regime_deseasonalized.tex").read_text())
    out = build_combined_panel_table(da_sa, ida_sa, label_prefix="(SA-deseasonalised)")
    (SA_DIR / "tab_bidshape_DA_IDA_combined.tex").write_text(out)
    print(f"wrote tab_bidshape_DA_IDA_combined.tex (Critical: {len(da_sa.get('Critical', []))} DA / {len(ida_sa.get('Critical', []))} IDA rows)")

    # MIC-filtered pair
    da_mic_p = MIC_DIR / "tab_bidshape_DA_by_regime_mic.tex"
    ida_mic_p = MIC_DIR / "tab_bidshape_IDA_by_regime_mic.tex"
    if da_mic_p.exists() and ida_mic_p.exists():
        da_mic = parse_panel(da_mic_p.read_text())
        ida_mic = parse_panel(ida_mic_p.read_text())
        out_mic = build_combined_panel_table(da_mic, ida_mic, label_prefix="(MIC-filtered)")
        (MIC_DIR / "tab_bidshape_DA_IDA_mic_combined.tex").write_text(out_mic)
        print(f"wrote tab_bidshape_DA_IDA_mic_combined.tex")


if __name__ == "__main__":
    main()
