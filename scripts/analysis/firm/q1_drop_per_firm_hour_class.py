# STATUS: ALIVE
# LAST-AUDIT: 2026-05-19
# CLAIM: Per-(firm, tech, hour-class) same-calendar-month q1 drop. Reads the
#        existing q1_by_firm_tech_month.csv and produces a tex table showing
#        the q1 mean MWh per firm in DA, by (tech, firm, hour-class), for the
#        Dec24->Dec25 same-calendar comparison (the cleanest reform window).
#        Per-tech aggregate in tab_q1_drop_by_tech.tex collapses these.
#
# OUT: results/regressions/firm/q1_drop/tab_q1_drop_per_firm_hour_class.tex

from pathlib import Path
import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
IN = REPO / "results/regressions/firm/q1_drop/q1_by_firm_tech_month.csv"
OUT = REPO / "results/regressions/firm/q1_drop/tab_q1_drop_per_firm_hour_class.tex"

TECHS = ["CCGT", "Hydro", "Hydro_pump", "Nuclear", "Wind", "Solar PV"]
FIRMS_ORDER = ["IB", "GE", "GN", "HC", "REP"]
MONTH_PAIRS = [
    ("2024-12-01", "2025-12-01", "Dec 24 -> Dec 25"),
    ("2024-11-01", "2025-11-01", "Nov 24 -> Nov 25"),
    ("2024-08-01", "2025-08-01", "Aug 24 -> Aug 25"),
]


def main():
    df = pd.read_csv(IN)
    df["ym"] = pd.to_datetime(df["ym"]).dt.strftime("%Y-%m-%d")
    df = df[df["tech_group"].isin(TECHS) & df["parent"].isin(FIRMS_ORDER + ["Endesa", "Iberdrola", "Naturgy", "EDP"])]

    rows = [r"\begin{tabular}{l l l " + ("r r r " * len(MONTH_PAIRS)) + r"}",
            r"\toprule",
            r" & & & " + " & ".join(
                fr"\multicolumn{{3}}{{c}}{{{label}}}" for _, _, label in MONTH_PAIRS) + r" \\",
            "".join(fr"\cmidrule(lr){{{3+i*3+1}-{3+i*3+3}}}" for i in range(len(MONTH_PAIRS))),
            r"Tech & Firm & Hour-cl & " + " & ".join(["Pre", "Post", "\\%"] * len(MONTH_PAIRS)) + r" \\",
            r"\midrule"]
    last_tech, last_firm = None, None
    for tech in TECHS:
        for firm in FIRMS_ORDER:
            for hc in ["critical", "flat", "midday"]:
                sub = df[(df["tech_group"] == tech) & (df["parent"] == firm) & (df["hc"] == hc)]
                if sub.empty:
                    continue
                # need at least one month pair with both Pre and Post observed
                row_has_data = False
                row_cells = []
                for pre, post, _ in MONTH_PAIRS:
                    pre_v = sub[sub["ym"] == pre]["q1_mean_mwh"]
                    post_v = sub[sub["ym"] == post]["q1_mean_mwh"]
                    if pre_v.empty or post_v.empty:
                        row_cells.extend(["---", "---", "---"])
                        continue
                    p, q = float(pre_v.iloc[0]), float(post_v.iloc[0])
                    if abs(p) < 0.01:
                        pct = "---"
                    else:
                        pct = f"{(q - p) / abs(p) * 100:+.0f}\\%"
                    row_cells.extend([f"{p:+.0f}", f"{q:+.0f}", pct])
                    row_has_data = True
                if not row_has_data:
                    continue
                tech_lbl = tech.replace("_", " ") if tech != last_tech else ""
                firm_lbl = firm if (tech != last_tech or firm != last_firm) else ""
                if tech != last_tech and last_tech is not None:
                    rows.append(r"\addlinespace")
                last_tech, last_firm = tech, firm
                rows.append(" & ".join([tech_lbl, firm_lbl, hc.capitalize()] + row_cells) + r" \\")
    rows.extend([r"\bottomrule", r"\end{tabular}"])
    OUT.write_text("% Per-(firm, tech, hour-class) q1 mean MWh same-calendar month comparison.\n"
                   + "\n".join(rows))
    print(f"wrote {OUT}")


if __name__ == "__main__":
    main()
