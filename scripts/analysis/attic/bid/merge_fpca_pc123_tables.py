# STATUS: ALIVE
# LAST-AUDIT: 2026-05-19
# CLAIM: Merge separate fPCA PC1/PC2/PC3 tables (ISP15, MTU15-IDA, MTU15-DA)
#        into one table with three reform panels side by side, saving 2 pages.
#
# IN:  tab_fpca_pairwise_ISP15_sa_pc123.tex
#      tab_fpca_pairwise_MTU15-IDA_sa_pc123.tex
#      tab_fpca_pairwise_MTU15-DA_sa_pc123.tex
# OUT: tab_fpca_pairwise_all_reforms_sa_pc123.tex

from __future__ import annotations
from pathlib import Path
import re

REPO = Path(__file__).resolve().parents[3]
IN = REPO / "results/regressions/bid/fpca/tex"
REFORMS = [
    ("ISP15", "ISP15", "tab_fpca_pairwise_ISP15_sa_pc123.tex"),
    ("MTU15-IDA", "MTU15-IDA", "tab_fpca_pairwise_MTU15-IDA_sa_pc123.tex"),
    ("MTU15-DA", "MTU15-DA", "tab_fpca_pairwise_MTU15-DA_sa_pc123.tex"),
]


def parse_table(tex_content):
    """Returns {(tech, firm): (pc1, pc2, pc3)}."""
    rows = {}
    current_tech = None
    for line in tex_content.split("\n"):
        s_check = line.strip()
        if any(s_check.startswith(x) for x in (r"\toprule", r"\midrule", r"\bottomrule",
                                                  r"\begin", r"\end", r"%", r"\addlinespace",
                                                  r"Tech")):
            continue
        if r"\\" not in s_check:
            continue
        # Strip trailing only, preserve leading-empty cell on continuation rows
        body = re.sub(r"\s*\\\\\s*$", "", line.rstrip())
        cols = [c.strip() for c in body.split(" & ")]
        if len(cols) < 5:
            continue
        if cols[0]:
            current_tech = cols[0]
        tech = cols[0] if cols[0] else current_tech
        firm = cols[1]
        rows[(tech, firm)] = (cols[2], cols[3], cols[4])
    return rows


def main():
    parsed = {label: parse_table((IN / fname).read_text()) for label, _, fname in REFORMS}

    # Build merged key list (tech, firm) — order them by tech then firm, taking union across reforms
    all_keys = set()
    for p in parsed.values():
        all_keys |= p.keys()
    # Order
    techs_order = ["CCGT", "Hydro", "Hydro pump", "Nuclear", "Wind", "Solar PV", "Solar Thermal", "Cogen"]
    firms_order = ["GE", "GN", "HC", "IB", "AXPO", "REP", "OTH"]
    keys_sorted = sorted(all_keys, key=lambda k: (techs_order.index(k[0]) if k[0] in techs_order else 99,
                                                   firms_order.index(k[1]) if k[1] in firms_order else 99))

    lines = [
        r"% Merged fPCA PC1/PC2/PC3 tables for all three reforms (functional-SA scores).",
        r"\begin{tabular}{@{}l l " + ("r r r " * 3) + r"@{}}",
        r"\toprule",
        r" & & \multicolumn{3}{c}{\textbf{ISP15}} & \multicolumn{3}{c}{\textbf{MTU15-IDA}} & \multicolumn{3}{c}{\textbf{MTU15-DA}} \\",
        r"\cmidrule(lr){3-5}\cmidrule(lr){6-8}\cmidrule(lr){9-11}",
        r"Tech & Firm & PC1 & PC2 & PC3 & PC1 & PC2 & PC3 & PC1 & PC2 & PC3 \\",
        r"\midrule",
    ]
    last_tech = None
    for (tech, firm) in keys_sorted:
        tech_lbl = tech if tech != last_tech else ""
        if last_tech is not None and tech != last_tech:
            lines.append(r"\addlinespace")
        last_tech = tech
        row_cells = [tech_lbl, firm]
        for label, _, _ in REFORMS:
            v = parsed[label].get((tech, firm), ("---", "---", "---"))
            row_cells.extend(v)
        lines.append(" & ".join(row_cells) + r" \\")
    lines.extend([r"\bottomrule", r"\end{tabular}"])
    out = IN / "tab_fpca_pairwise_all_reforms_sa_pc123.tex"
    out.write_text("\n".join(lines) + "\n")
    print(f"wrote {out.name}")


if __name__ == "__main__":
    main()
