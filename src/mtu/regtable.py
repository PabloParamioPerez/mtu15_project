"""Publication-style regression tables from statsmodels OLS results.

Produces standard economics-paper format: coefficient over SE in parentheses,
significance stars (*** p<0.01, ** p<0.05, * p<0.1), N + R² + FE indicators
in footer rows.

Two output formats:
  - LaTeX: standalone tabular block ready for \\input{}
  - Markdown: pipe-table format for notebooks / Slack / quick review

Usage:
    from mtu.regtable import RegTable

    table = RegTable(
        results=[model1, model2, model3],
        column_labels=['Sparse', 'Augmented', 'Aug. + cluster SE'],
        coef_order=['regime_A', 'regime_B', 'regime_C', 'Big4'],
        coef_labels={
            'regime_A': '3-sess (vs pre-IDA)',
            'regime_B': 'ISP15-win (vs pre-IDA)',
            'Big4':     'Big-4 main effect',
        },
        fe_rows={
            'Period FE':    [False, True,  True],
            'DOW FE':       [False, True,  True],
            'Cluster SE':   ['No',  'No',  'date×hour'],
        },
        title='Big-4 strategic IDA repositioning by regime',
        outcome='q_2 (MWh per firm-ISP)',
    )

    table.to_latex('results/tables/b9_main.tex')
    table.to_markdown('results/tables/b9_main.md')
    print(table.to_markdown())
"""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


def _stars(p: float) -> str:
    """Significance stars per econ convention."""
    if p < 0.01:
        return "***"
    if p < 0.05:
        return "**"
    if p < 0.10:
        return "*"
    return ""


def _fmt(x: float, digits: int = 3) -> str:
    """Format a number with a fixed number of decimal places."""
    if x is None:
        return ""
    if abs(x) < 10**-digits and x != 0:
        return f"{x:.{digits}e}"
    return f"{x:.{digits}f}"


@dataclass
class RegTable:
    """A publication-style multi-spec regression table."""

    results: list[Any]
    column_labels: list[str]
    coef_order: list[str] = field(default_factory=list)
    coef_labels: dict[str, str] = field(default_factory=dict)
    fe_rows: dict[str, list[Any]] = field(default_factory=dict)
    title: str = ""
    outcome: str = ""
    notes: str = ""
    digits: int = 3

    def __post_init__(self) -> None:
        n = len(self.results)
        assert len(self.column_labels) == n, (
            f"column_labels length {len(self.column_labels)} != results length {n}"
        )
        for label, row in self.fe_rows.items():
            assert len(row) == n, (
                f"fe_rows[{label!r}] length {len(row)} != n_results {n}"
            )
        # If coef_order is empty, take the union (preserving first-seen order)
        if not self.coef_order:
            seen: dict[str, None] = {}
            for r in self.results:
                for name in r.params.index:
                    if name not in seen:
                        seen[name] = None
            self.coef_order = list(seen.keys())

    # ----------------------------------------------------------------------
    # Core: extract coef, SE, p-value per result × coef
    # ----------------------------------------------------------------------
    def _cell(self, result: Any, coef: str) -> tuple[str, str]:
        """Return (top, bottom) cell strings for a coefficient.

        Top    = "0.123***"
        Bottom = "(0.045)"
        Returns ("", "") if coef not in this result.
        """
        if coef not in result.params.index:
            return "", ""
        b  = float(result.params[coef])
        se = float(result.bse[coef])
        p  = float(result.pvalues[coef])
        top    = f"{_fmt(b, self.digits)}{_stars(p)}"
        bottom = f"({_fmt(se, self.digits)})"
        return top, bottom

    # ----------------------------------------------------------------------
    # Markdown output
    # ----------------------------------------------------------------------
    def to_markdown(self, path: Path | str | None = None) -> str:
        n = len(self.results)
        col_widths = [max(14, len(lbl) + 2) for lbl in self.column_labels]
        coef_col_w = max(28,
                         max((len(self.coef_labels.get(c, c)) for c in self.coef_order),
                             default=20))

        lines: list[str] = []
        if self.title:
            lines.append(f"**Table.** {self.title}")
            lines.append("")
        if self.outcome:
            lines.append(f"_Dependent variable: {self.outcome}_")
            lines.append("")

        # Header
        head = ["", *[f"({i+1})" for i in range(n)]]
        head_line = "| " + " | ".join(
            [head[0].ljust(coef_col_w)]
            + [head[i + 1].center(col_widths[i]) for i in range(n)]
        ) + " |"
        sep_line = "|" + "|".join(["-" * (coef_col_w + 2)]
                                  + ["-" * (col_widths[i] + 2) for i in range(n)]) + "|"
        # Spec labels row
        spec_line = "| " + " | ".join(
            ["".ljust(coef_col_w)]
            + [self.column_labels[i].center(col_widths[i]) for i in range(n)]
        ) + " |"
        lines.append(head_line)
        lines.append(spec_line)
        lines.append(sep_line)

        # Coefficient rows
        for coef in self.coef_order:
            label = self.coef_labels.get(coef, coef)
            tops:    list[str] = []
            bottoms: list[str] = []
            any_present = False
            for r in self.results:
                top, bot = self._cell(r, coef)
                if top:
                    any_present = True
                tops.append(top)
                bottoms.append(bot)
            if not any_present:
                continue
            lines.append("| " + " | ".join(
                [label.ljust(coef_col_w)]
                + [tops[i].center(col_widths[i]) for i in range(n)]
            ) + " |")
            lines.append("| " + " | ".join(
                ["".ljust(coef_col_w)]
                + [bottoms[i].center(col_widths[i]) for i in range(n)]
            ) + " |")

        # FE rows
        if self.fe_rows:
            lines.append(sep_line)
            for label, row in self.fe_rows.items():
                cells = [str(v) if not isinstance(v, bool) else ("Yes" if v else "No")
                         for v in row]
                lines.append("| " + " | ".join(
                    [label.ljust(coef_col_w)]
                    + [cells[i].center(col_widths[i]) for i in range(n)]
                ) + " |")

        # Footer: N + R²
        lines.append(sep_line)
        n_obs = [f"{int(r.nobs):,}" for r in self.results]
        lines.append("| " + " | ".join(
            ["Observations".ljust(coef_col_w)]
            + [n_obs[i].center(col_widths[i]) for i in range(n)]
        ) + " |")
        r2 = [_fmt(float(r.rsquared), 3) if hasattr(r, "rsquared") else "" for r in self.results]
        lines.append("| " + " | ".join(
            ["R²".ljust(coef_col_w)]
            + [r2[i].center(col_widths[i]) for i in range(n)]
        ) + " |")

        lines.append("")
        lines.append("Notes: Standard errors in parentheses. \\* p<0.10, \\*\\* p<0.05, \\*\\*\\* p<0.01.")
        if self.notes:
            lines.append(self.notes)

        out = "\n".join(lines)
        if path is not None:
            Path(path).write_text(out)
        return out

    # ----------------------------------------------------------------------
    # LaTeX output
    # ----------------------------------------------------------------------
    def to_latex(self, path: Path | str | None = None) -> str:
        n = len(self.results)
        col_align = "l" + "c" * n

        lines: list[str] = []
        lines.append("% Auto-generated by mtu.regtable; ready for \\input{}")
        if self.title:
            lines.append(f"% Table: {self.title}")
        if self.outcome:
            lines.append(f"% Outcome: {self.outcome}")
        lines.append("\\begin{tabular}{" + col_align + "}")
        lines.append("\\toprule")
        # Column number row
        cols = " & ".join(["\\multicolumn{1}{c}{(" + str(i + 1) + ")}" for i in range(n)])
        lines.append(" & " + cols + " \\\\")
        # Spec label row
        spec = " & ".join(self.column_labels)
        lines.append(" & " + spec + " \\\\")
        lines.append("\\midrule")

        # Coefficient rows
        for coef in self.coef_order:
            label = self.coef_labels.get(coef, coef).replace("_", "\\_")
            tops:    list[str] = []
            bottoms: list[str] = []
            any_present = False
            for r in self.results:
                top, bot = self._cell(r, coef)
                if top:
                    any_present = True
                # Convert significance stars to LaTeX superscripts in one pass
                # (avoid cascading replacements that nest the substitutions).
                if top.endswith("***"):
                    top_tex = top[:-3] + "$^{***}$"
                elif top.endswith("**"):
                    top_tex = top[:-2] + "$^{**}$"
                elif top.endswith("*"):
                    top_tex = top[:-1] + "$^{*}$"
                else:
                    top_tex = top
                tops.append(top_tex)
                bottoms.append(bot)
            if not any_present:
                continue
            lines.append(label + " & " + " & ".join(tops) + " \\\\")
            lines.append(" & " + " & ".join(bottoms) + " \\\\")

        # FE rows
        if self.fe_rows:
            lines.append("\\midrule")
            for label, row in self.fe_rows.items():
                cells = [str(v) if not isinstance(v, bool) else ("Yes" if v else "No")
                         for v in row]
                lines.append(label.replace("_", "\\_") + " & " + " & ".join(cells) + " \\\\")

        # Footer
        lines.append("\\midrule")
        n_obs = [f"{int(r.nobs):,}" for r in self.results]
        lines.append("Observations & " + " & ".join(n_obs) + " \\\\")
        r2 = [_fmt(float(r.rsquared), 3) if hasattr(r, "rsquared") else ""
              for r in self.results]
        lines.append("$R^2$ & " + " & ".join(r2) + " \\\\")
        lines.append("\\bottomrule")
        lines.append("\\end{tabular}")
        lines.append("")
        lines.append("% Notes: Standard errors in parentheses. * p<0.10, ** p<0.05, *** p<0.01.")
        if self.notes:
            lines.append("% " + self.notes)

        out = "\n".join(lines)
        if path is not None:
            Path(path).write_text(out)
        return out


def reg_table_from_dict(rows: list[dict], title: str = "", outcome: str = "") -> str:
    """Build a markdown table from a list of dicts (no statsmodels result needed).

    Each dict has keys: regime, big4_effect, se, p (or omit p for no stars).
    Useful when the regression result is already on disk as a CSV.
    """
    out: list[str] = []
    if title:
        out.append(f"**Table.** {title}\n")
    if outcome:
        out.append(f"_Dependent variable: {outcome}_\n")
    out.append("| Regime | Big-4 effect | SE | t | p |")
    out.append("|---|---|---|---|---|")
    for row in rows:
        b = row.get("big4_effect", row.get("beta"))
        se = row.get("se")
        p = row.get("p")
        t = b / se if (b is not None and se and se > 0) else None
        star = _stars(p) if p is not None else ""
        out.append(
            f"| {row.get('regime', '')} | "
            f"{_fmt(b)}{star} | {_fmt(se)} | {_fmt(t, 2)} | {_fmt(p, 4) if p else ''} |"
        )
    out.append("")
    out.append("Notes: \\* p<0.10, \\*\\* p<0.05, \\*\\*\\* p<0.01.")
    return "\n".join(out)
