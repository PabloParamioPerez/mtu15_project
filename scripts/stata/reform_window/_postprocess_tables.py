# STATUS: ALIVE
# LAST-AUDIT: 2026-05-16
# CLAIM: Post-process LaTeX tables emitted by esttab in this folder's .do
#        files. The OLS-FE (tab_ddd_*) tables come from esttab and need:
#          (i) "$\beta\_7$" -> "$\beta_7$" (esttab over-escapes _ inside math)
#          (ii) \label{} after \caption{} for cross-references
#        The DR-DiD (tab_csdid_*) tables are written manually by file write
#        and already contain their own \label{}, so they're skipped.
# Idempotent: safe to re-run.

from __future__ import annotations

import re
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]
TBLDIR = REPO / "thesis" / "paper" / "tables" / "reform_window"

# Only the esttab-generated OLS-FE tables need labels and unescaping; the
# DR-DiD tables are written manually with their own \label{}.
ESTTAB_LABELS = {
    "tab_ddd_da15_da.tex":          "tab:reform_ddd_da15_da",
    "tab_ddd_da15_ida.tex":         "tab:reform_ddd_da15_ida",
    "tab_ddd_ida15_da.tex":         "tab:reform_ddd_ida15_da",
    "tab_ddd_ida15_ida.tex":        "tab:reform_ddd_ida15_ida",
    "tab_ddd_firm_q1_da15.tex":     "tab:reform_ddd_firm_q1_da15",
    "tab_ddd_firm_q1_ida15.tex":    "tab:reform_ddd_firm_q1_ida15",
    "tab_ddd_firm_q2_da15.tex":     "tab:reform_ddd_firm_q2_da15",
    "tab_ddd_firm_q2_ida15.tex":    "tab:reform_ddd_firm_q2_ida15",
}


def patch(path: Path, label: str) -> None:
    s = path.read_text()
    s = s.replace(r"$\beta\_7$", r"$\beta_7$")
    s = s.replace(r"$\beta\_{1234}$", r"$\beta_{1234}$")
    if r"\label{" not in s:
        m = re.search(r"\\caption\{[^}]+\}", s)
        if m:
            s = s[: m.end()] + f"\\label{{{label}}}" + s[m.end() :]
    path.write_text(s)


def main() -> None:
    for name, lbl in ESTTAB_LABELS.items():
        p = TBLDIR / name
        if not p.exists():
            print(f"missing: {p}")
            continue
        patch(p, lbl)
        print(f"patched {name}")


if __name__ == "__main__":
    main()
