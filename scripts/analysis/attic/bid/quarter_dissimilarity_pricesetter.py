# STATUS: ALIVE
# LAST-AUDIT: 2026-05-16
# FEEDS: thesis paper.tex Table 5 + Appendix §A.14 (price-setter-restricted W1)
# CLAIM: Re-runs the W1 / kernel-weighted W1 dissimilarity test on TWO
#        restrictions of the cell sample:
#        (A) STRICT — keep only cells where the unit was the price-setter
#            (within EPS of clearing) in at least one of the 4 quarters of
#            that hour.
#        (B) FREQUENT — keep only cells whose unit price-sets in at least
#            FREQ_THRESHOLD of all (date, period) cells in the window.
#        Reuses the per-cell dissimilarity numbers from
#        `quarter_dissimilarity_ot.py`. Also adds a flag for cells where
#        the four-quarter clearing prices have stdev > STD_FLAG_THRESHOLD.

from __future__ import annotations

from pathlib import Path
import sys

import duckdb
import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO / "src"))

DET   = REPO / "data" / "processed" / "omie" / "mercado_diario" / "ofertas" / "det_all.parquet"
CAB   = REPO / "data" / "processed" / "omie" / "mercado_diario" / "ofertas" / "cab_all.parquet"
MPDBC = REPO / "data" / "processed" / "omie" / "mercado_diario" / "precios" / "marginalpdbc_all.parquet"
CELLS_CSV = REPO / "results" / "regressions" / "bid" / "quarter_dissimilarity" / "quarter_dissimilarity_cells_2025Q4.csv"
TABDIR = REPO / "thesis" / "paper" / "tables"

EPS = 0.01                # EUR/MWh — same as marginal_tech_by_hour.py
FREQ_THRESHOLD = 0.01     # 1% of periods — "frequent" price-setter
STD_FLAG_THRESHOLD = 5.0  # EUR/MWh — within-hour std-of-clearing flag
WINDOW = ("2025-10-01", "2026-01-01")

PIVOTAL = ("IB", "GE", "GN", "HC")
SUPPLY_TECHS = ("CCGT", "Hydro", "Hydro_pump", "Nuclear", "Wind", "Solar PV")
DEMAND_TECHS = ("Pump_load", "Retailer", "Direct_consumer")
TECHS = SUPPLY_TECHS + DEMAND_TECHS
TECH_CATEGORY = {**{t: "Supply" for t in SUPPLY_TECHS},
                 **{t: "Demand" for t in DEMAND_TECHS}}


def load_price_setter_map() -> pd.DataFrame:
    """Per (date, period) the unit whose top accepted sell tranche sits
    within EPS of clearing. Same definition as marginal_tech_by_hour.py."""
    q = f"""
    WITH cab AS (
        SELECT date::DATE AS d, offer_code, version, unit_code,
               ROW_NUMBER() OVER (PARTITION BY date::DATE, offer_code, unit_code
                                  ORDER BY version DESC) AS rn
        FROM '{CAB}'
        WHERE buy_sell = 'V'
          AND date::DATE >= DATE '{WINDOW[0]}' AND date::DATE < DATE '{WINDOW[1]}'
    ),
    cab_l AS (SELECT * FROM cab WHERE rn = 1),
    prices AS (
        SELECT date::DATE AS d, period, price_es_eur_mwh AS p_clear
        FROM '{MPDBC}'
        WHERE date::DATE >= DATE '{WINDOW[0]}' AND date::DATE < DATE '{WINDOW[1]}'
          AND price_es_eur_mwh IS NOT NULL
    ),
    det AS (
        SELECT date::DATE AS d, offer_code, version, period,
               price_eur_mwh AS p_bid
        FROM '{DET}'
        WHERE date::DATE >= DATE '{WINDOW[0]}' AND date::DATE < DATE '{WINDOW[1]}'
          AND price_eur_mwh IS NOT NULL AND quantity_mw > 0
    ),
    joined AS (
        SELECT pr.d, pr.period,
               ((pr.period - 1) // 4)::INT AS hour,
               c.unit_code, d.p_bid, pr.p_clear,
               RANK() OVER (PARTITION BY pr.d, pr.period ORDER BY d.p_bid DESC) AS rk
        FROM det d
          JOIN cab_l c USING (d, offer_code, version)
          JOIN prices pr ON pr.d = d.d AND pr.period = d.period
        WHERE d.p_bid <= pr.p_clear
    )
    SELECT d AS date, period, hour, unit_code
    FROM joined
    WHERE rk = 1 AND abs(p_clear - p_bid) <= {EPS}
    """
    return duckdb.execute(q).df()


def load_clearing_std() -> pd.DataFrame:
    """Per (date, hour), std of the 4 quarters' clearing prices."""
    return duckdb.execute(f"""
        SELECT date::DATE AS date,
               ((period - 1) / 4)::INT AS hour,
               STDDEV_SAMP(price_es_eur_mwh) AS p_clear_std,
               AVG(price_es_eur_mwh)         AS p_clear_mean
        FROM '{MPDBC}'
        WHERE date::DATE >= DATE '{WINDOW[0]}' AND date::DATE < DATE '{WINDOW[1]}'
          AND price_es_eur_mwh IS NOT NULL
        GROUP BY 1, 2
    """).df()


def augment_cells(cells: pd.DataFrame, ps_df: pd.DataFrame, std_df: pd.DataFrame) -> pd.DataFrame:
    """Add three columns to the per-cell dissimilarity table:
       - is_ps_cell      : unit was price-setter in >=1 quarter of this hour
       - unit_ps_frequency: fraction of total (date, period) cells in the
                            window where this unit was the price-setter
       - p_clear_std     : stdev of the 4 quarter clearing prices for this hour
       - p_clear_std_flag: 1 if p_clear_std > STD_FLAG_THRESHOLD.
    """
    cells["date"] = pd.to_datetime(cells["date"]).dt.date
    std_df["date"] = pd.to_datetime(std_df["date"]).dt.date
    ps_df["date"] = pd.to_datetime(ps_df["date"]).dt.date

    # is_ps_cell: (unit, date, hour) appears anywhere in ps_df
    ps_cell_keys = set(zip(ps_df["unit_code"], ps_df["date"], ps_df["hour"]))
    cells["is_ps_cell"] = [(u, d, h) in ps_cell_keys
                            for u, d, h in zip(cells["unit_code"], cells["date"], cells["hour"])]

    # unit-level frequency: count of (date, period) cells per unit / total
    total_periods = ps_df.groupby(["date", "period"]).ngroups
    unit_counts = ps_df["unit_code"].value_counts()
    unit_freq = (unit_counts / total_periods).rename("unit_ps_frequency")
    cells = cells.merge(unit_freq.reset_index().rename(columns={"index": "unit_code"}),
                         on="unit_code", how="left")
    cells["unit_ps_frequency"] = cells["unit_ps_frequency"].fillna(0)

    # clearing-price std
    cells = cells.merge(std_df, on=["date", "hour"], how="left")
    cells["p_clear_std_flag"] = (cells["p_clear_std"] > STD_FLAG_THRESHOLD).astype(int)
    return cells


def summarise(cells: pd.DataFrame, eps_flag=1e-6) -> pd.DataFrame:
    """Aggregate to (tech, firm, hour-class) for the FULL sample."""
    cells = cells[cells["hour_class"].isin(("critical", "flat"))].copy()
    cells["flagged"] = (cells["d_max"] > eps_flag).astype(int)
    flagged = cells[cells["flagged"] == 1].copy()
    flagged["ratio"] = flagged["d_max_w"] / flagged["d_max"].replace(0, np.nan)
    overall = cells.groupby(["tech_group", "firm", "hour_class"]).agg(
        n_cells=("d_max", "size"),
        n_high_std=("p_clear_std_flag", "sum"),
        frac_flagged=("flagged", "mean"),
    ).reset_index()
    g_flag = flagged.groupby(["tech_group", "firm", "hour_class"]).agg(
        median_d=("d_max", "median"),
        median_dw=("d_max_w", "median"),
        median_ratio=("ratio", "median"),
    ).reset_index()
    return overall.merge(g_flag, on=["tech_group", "firm", "hour_class"], how="left")


def _fmt_pct(v: float) -> str:
    if pd.isna(v): return "--"
    pct = 100.0 * v
    if pct == 0: return "0"
    if pct < 1.0: return f"{pct:.2f}"
    return f"{pct:.1f}"


def _fmt_num(v: float, places: int = 2) -> str:
    return "--" if pd.isna(v) else f"{v:.{places}f}"


def write_tex_main_pricesetter(g_full: pd.DataFrame, g_strict: pd.DataFrame, g_freq: pd.DataFrame, out_path: Path):
    """Main-text Table 5: CCGT only, 3 sample versions side by side.
    Columns: N (full), %flag (full), medD (full), medDw (full)
             ‖ N (strict), %flag (strict), medD (strict), medDw (strict)
             ‖ N (freq),   %flag (freq),   medD (freq),   medDw (freq)
    Compact: keep medD + medDw + medRatio per panel; drop full %flag per row.
    """
    def keep_ccgt(g):
        s = g[g["tech_group"] == "CCGT"].copy()
        return s

    full   = keep_ccgt(g_full).set_index(["firm", "hour_class"])
    strict = keep_ccgt(g_strict).set_index(["firm", "hour_class"])
    freq   = keep_ccgt(g_freq).set_index(["firm", "hour_class"])

    firm_order = {f: i for i, f in enumerate(PIVOTAL)}
    rows = sorted(set(full.index) | set(strict.index) | set(freq.index),
                  key=lambda x: (firm_order.get(x[0], 99), 0 if x[1] == "critical" else 1))

    lines = [
        r"\begin{tabular}{@{}l l r r r | r r r r | r r r r@{}}",
        r"\toprule",
        r"  &  & \multicolumn{3}{c|}{Full sample} & \multicolumn{4}{c|}{Strict price-setter only} & \multicolumn{4}{c}{Frequent price-setter only ($\geq 1\%$)} \\",
        r"Firm & Hour-class & N & \%flag & med $D_w$ & N & \%flag & med $D_w$ & ratio & N & \%flag & med $D_w$ & ratio \\",
        r"\midrule",
    ]
    last_firm = None
    for (firm, hc) in rows:
        firm_disp = firm if firm != last_firm else ""
        last_firm = firm
        rf = full.loc[(firm, hc)] if (firm, hc) in full.index else None
        rs = strict.loc[(firm, hc)] if (firm, hc) in strict.index else None
        rq = freq.loc[(firm, hc)] if (firm, hc) in freq.index else None
        def cell(d, col, fmt):
            return fmt(d[col]) if d is not None and col in d else "--"

        def n_(d):    return f"{int(d['n_cells']):,}" if d is not None else "--"
        def pct_(d):  return _fmt_pct(d["frac_flagged"]) if d is not None else "--"
        def dw_(d):   return _fmt_num(d.get("median_dw", float("nan"))) if d is not None else "--"
        def r_(d):    return _fmt_num(d.get("median_ratio", float("nan"))) if d is not None else "--"

        lines.append(
            f"{firm_disp} & {hc.capitalize()} & "
            f"{n_(rf)} & {pct_(rf)} & {dw_(rf)} & "
            f"{n_(rs)} & {pct_(rs)} & {dw_(rs)} & {r_(rs)} & "
            f"{n_(rq)} & {pct_(rq)} & {dw_(rq)} & {r_(rq)} \\\\"
        )
    lines += [r"\bottomrule", r"\end{tabular}"]
    Path(out_path).write_text("\n".join(lines) + "\n")
    print(f"  saved {out_path}")


def write_tex_full_3panel(g_full, g_strict, g_freq, out_path):
    """Appendix longtable: all techs, 3 sample-version panels stacked."""
    def fmt_section(g, title):
        out = []
        out.append(rf"\multicolumn{{8}}{{l}}{{\textit{{{title}}}}} \\")
        out.append(r"\midrule")
        last_side = None; last_tech = None
        for _, r in g.iterrows():
            side = TECH_CATEGORY.get(r["tech_group"], "")
            tech = r["tech_group"].replace("_", r"\_")
            if last_side is not None and last_side != side:
                out.append(r"\midrule")
            elif last_tech is not None and last_tech != r["tech_group"]:
                out.append(r"\addlinespace")
            side_disp = side if side != last_side else ""
            tech_disp = tech if r["tech_group"] != last_tech else ""
            last_side = side; last_tech = r["tech_group"]
            out.append(
                f"{side_disp} & {tech_disp} & {r['firm']} & {r['hour_class'].capitalize()} & "
                f"{int(r['n_cells']):,} & {_fmt_pct(r['frac_flagged'])} & "
                f"{_fmt_num(r.get('median_dw', float('nan')))} & "
                f"{_fmt_num(r.get('median_ratio', float('nan')))} \\\\"
            )
        return out

    lines = [
        r"\small",
        r"\begin{longtable}{@{}l l l l r r r r@{}}",
        r"\caption{\textbf{Quarter-by-quarter bid dissimilarity, 3 sample versions, October--December 2025.} "
        r"Full sample = every cell; Strict PS = cells where the unit was the price-setter in $\geq$1 of 4 quarters; Frequent PS = cells where the unit price-sets in $\geq 1\%$ of all (date, period) cells in the window. The Wasserstein-style L1 area is kernel-weighted by an Epanechnikov ($h=20$~EUR/MWh) centred at the period's mean DA clearing price. \%\,flagged = fraction with unweighted $D > 0$. Median ratio = $D_w / D$ per flagged cell.}\label{tab:quarter_dissim}\\",
        r"\toprule",
        r"Side & Tech & Firm & Hour-class & N cells & \% flagged & median $D_w$ & median ratio \\",
        r"\midrule",
        r"\endfirsthead",
        r"\multicolumn{8}{l}{\emph{(continued from previous page)}} \\",
        r"\toprule",
        r"Side & Tech & Firm & Hour-class & N cells & \% flagged & median $D_w$ & median ratio \\",
        r"\midrule",
        r"\endhead",
        r"\midrule",
        r"\multicolumn{8}{r}{\emph{continued on next page}} \\",
        r"\endfoot",
        r"\bottomrule",
        r"\endlastfoot",
    ]
    lines += fmt_section(g_full,   "Panel A: Full sample (every cell)")
    lines += [r"\midrule"]
    lines += fmt_section(g_strict, r"Panel B: Strict price-setter (unit sets price in $\geq 1$ quarter of this hour)")
    lines += [r"\midrule"]
    lines += fmt_section(g_freq,   r"Panel C: Frequent price-setter (unit price-sets in $\geq 1\%$ of all periods)")
    lines += [r"\end{longtable}", r"\normalsize"]
    Path(out_path).write_text("\n".join(lines) + "\n")
    print(f"  saved {out_path}")


def main():
    print("loading cells from existing CSV...")
    cells = pd.read_csv(CELLS_CSV)
    print(f"  {len(cells):,} cells loaded")

    print("\nloading per-period price-setter map (DET+CAB+marginalpdbc)...")
    ps_df = load_price_setter_map()
    print(f"  {len(ps_df):,} (date, period) rows with a within-EPS price-setter")
    print(f"  top 10 most-frequent price-setting units:")
    print(ps_df["unit_code"].value_counts().head(10).to_string())

    print("\nloading per-(date,hour) clearing-price std...")
    std_df = load_clearing_std()
    print(f"  {len(std_df):,} (date, hour) cells; "
          f"{int((std_df['p_clear_std'] > STD_FLAG_THRESHOLD).sum())} flagged (std > {STD_FLAG_THRESHOLD})")

    print("\naugmenting cells with PS flags + std...")
    cells = augment_cells(cells, ps_df, std_df)
    print(f"  is_ps_cell=True: {int(cells['is_ps_cell'].sum()):,} of {len(cells):,}")
    print(f"  unit_ps_frequency >= {FREQ_THRESHOLD}: "
          f"{int((cells['unit_ps_frequency'] >= FREQ_THRESHOLD).sum()):,} cells")

    # 3 subsets
    g_full   = summarise(cells)
    g_strict = summarise(cells[cells["is_ps_cell"]])
    g_freq   = summarise(cells[cells["unit_ps_frequency"] >= FREQ_THRESHOLD])

    # order rows
    tech_order = {t: i for i, t in enumerate(TECHS)}
    firm_order = {f: i for i, f in enumerate(PIVOTAL)}
    for g in (g_full, g_strict, g_freq):
        g["t_ord"] = g["tech_group"].map(tech_order)
        g["f_ord"] = g["firm"].map(firm_order)
        g["h_ord"] = g["hour_class"].map({"critical": 0, "flat": 1})
        g.sort_values(["t_ord", "f_ord", "h_ord"], inplace=True)
        g.drop(columns=["t_ord", "f_ord", "h_ord"], inplace=True)

    print("\n=== Full sample, CCGT only ===")
    print(g_full[g_full["tech_group"] == "CCGT"].to_string(index=False))
    print("\n=== Strict price-setter, CCGT only ===")
    print(g_strict[g_strict["tech_group"] == "CCGT"].to_string(index=False))
    print("\n=== Frequent price-setter, CCGT only ===")
    print(g_freq[g_freq["tech_group"] == "CCGT"].to_string(index=False))

    # Save tables
    write_tex_main_pricesetter(g_full, g_strict, g_freq, TABDIR / "tab_quarter_dissimilarity_pricesetter_main.tex")
    write_tex_full_3panel(g_full, g_strict, g_freq, TABDIR / "tab_quarter_dissimilarity_pricesetter.tex")


if __name__ == "__main__":
    main()
