# STATUS: ALIVE
# LAST-AUDIT: 2026-05-15
# FEEDS: thesis paper.tex §4.2 / §4.4 (within-hour quarter dissimilarity flag)
# CLAIM: For each (firm, unit, date, hour) cell, compute the L1 area
#        between the cumulative bid curves of pairs of 15-min quarters
#        (1D Wasserstein-style flag treating bids as discrete measures
#        on price weighted by quantity). Cells with all-zero D are
#        bit-identical across the 4 quarters (mechanical); cells with
#        D > 0 have some within-hour variation in price or quantity
#        that the averaged Q1-Q4 plot may hide.
#
#        The integration domain is the union of all bid prices in the
#        two quarters being compared. Step-function cum_qty is evaluated
#        at each interval boundary; the integral is the sum of
#        |q_i(p) - q_j(p)| * delta_p across consecutive intervals.

from __future__ import annotations

from pathlib import Path
import sys
from itertools import combinations

import duckdb
import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO / "src"))
from mtu.classification.units import firm_unit_panel  # noqa: E402

DET = REPO / "data" / "processed" / "omie" / "mercado_diario" / "ofertas" / "det_all.parquet"
CAB = REPO / "data" / "processed" / "omie" / "mercado_diario" / "ofertas" / "cab_all.parquet"
MARGINALPDBC = REPO / "data" / "processed" / "omie" / "mercado_diario" / "precios" / "marginalpdbc_all.parquet"
UNITS_CSV = REPO / "data" / "external" / "omie_reference" / "lista_unidades.csv"
OUTDIR = REPO / "results" / "regressions" / "bid" / "quarter_dissimilarity"
OUTDIR.mkdir(parents=True, exist_ok=True)
TABDIR = REPO / "thesis" / "paper" / "tables"

# Kernel for "strategic-zone" weighting. Epanechnikov, compact support, no
# leakage to the price-cap mass. Bandwidth tuned to ~20 EUR/MWh, the width
# of the band around clearing where bid-shading is plausibly strategic.
KERNEL_BANDWIDTH = 20.0

CRIT = (5, 6, 7, 8, 16, 17, 18, 19, 20, 21, 22)
FLAT = (1, 2, 3)
MID  = (11, 12, 13, 14)

PIVOTAL = ("IB", "GE", "GN", "HC")
# Supply-side (V) and demand-side (C) techs. Ordered for display.
SUPPLY_TECHS = ("CCGT", "Hydro", "Hydro_pump", "Nuclear", "Wind", "Solar PV")
DEMAND_TECHS = ("Pump_load", "Retailer", "Direct_consumer")
TECHS = SUPPLY_TECHS + DEMAND_TECHS
SIDE = {**{t: "V" for t in SUPPLY_TECHS}, **{t: "C" for t in DEMAND_TECHS}}
TECH_CATEGORY = {**{t: "Supply" for t in SUPPLY_TECHS},
                 **{t: "Demand" for t in DEMAND_TECHS}}


def load_tranches(window=("2025-10-01", "2026-01-01")):
    """Pull tranches for the four benchmark techs, pivotal firms only."""
    units = firm_unit_panel(csv_path=str(UNITS_CSV), scheme="short", mode="primary_owner")
    keep = units[units["parent"].isin(PIVOTAL) & units["tech_group"].isin(TECHS)][
        ["unit_code", "parent", "tech_group"]
    ].rename(columns={"parent": "firm"})
    con = duckdb.connect(); con.execute("PRAGMA threads=4"); con.execute("SET memory_limit='10GB'")
    con.register("uft", keep)
    rows = []
    for tech in TECHS:
        side = SIDE[tech]
        q = f"""
        WITH cab AS (
            SELECT date::DATE AS d, offer_code, version, unit_code,
                   ROW_NUMBER() OVER (PARTITION BY date::DATE, offer_code, unit_code
                                      ORDER BY version DESC) AS rn
            FROM '{CAB}'
            WHERE buy_sell = '{side}'
              AND date::DATE >= DATE '{window[0]}' AND date::DATE < DATE '{window[1]}'
        ),
        cab_l AS (SELECT * FROM cab WHERE rn = 1),
        det AS (
            SELECT date::DATE AS d, offer_code, version, period,
                   price_eur_mwh AS p, quantity_mw AS q
            FROM '{DET}'
            WHERE date::DATE >= DATE '{window[0]}' AND date::DATE < DATE '{window[1]}'
              AND price_eur_mwh IS NOT NULL AND quantity_mw > 0
        )
        SELECT d.d AS date, c.unit_code, u.firm,
               '{tech}' AS tech_group,
               ((d.period - 1) // 4)::INT AS hour,
               (((d.period - 1) % 4) + 1)::INT AS quarter,
               d.p, d.q
        FROM det d
          JOIN cab_l c USING (d, offer_code, version)
          JOIN uft   u ON c.unit_code = u.unit_code
        WHERE u.tech_group = '{tech}'
        """
        rows.append(con.execute(q).df())
    return pd.concat(rows, ignore_index=True)


def _epanechnikov_integral_unit(p, center, h):
    """Indefinite integral of the unit-amplitude Epanechnikov kernel
    K(p) = max(0, 1 - ((p - center) / h)**2), evaluated at price p.
    K has MAX VALUE 1 at the center (not a normalized density), so the
    weighted L1 has the same units as the unweighted L1 and
    `D_w / D <= 1` always."""
    u = np.clip((p - center) / h, -1.0, 1.0)
    return h * (u - u ** 3 / 3.0)


def l1_between_bid_curves(prices_i, qtys_i, prices_j, qtys_j,
                          kernel_center: float | None = None,
                          kernel_h: float = KERNEL_BANDWIDTH):
    """Exact L1 area between two cumulative bid curves, optionally
    weighted by an Epanechnikov kernel centered at `kernel_center` with
    bandwidth `kernel_h`. When kernel_center is None, the unweighted
    L1 area is returned (the "raw" 1-D Wasserstein-style flag).

    Each input is a sequence of (price, quantity) tranches; the
    cumulative curve at price p is sum of q for tranches with
    p_tranche <= p. Integration domain is the union of all bid prices
    in the two quarters; the weighted variant integrates over the
    intersection of that domain with [center - h, center + h]."""
    pi = np.asarray(prices_i, dtype=float); qi = np.asarray(qtys_i, dtype=float)
    pj = np.asarray(prices_j, dtype=float); qj = np.asarray(qtys_j, dtype=float)
    # Sort by price
    oi = np.argsort(pi); pi, qi = pi[oi], qi[oi]
    oj = np.argsort(pj); pj, qj = pj[oj], qj[oj]
    cum_i = np.cumsum(qi)
    cum_j = np.cumsum(qj)
    # Union of breakpoints
    grid = np.unique(np.concatenate([pi, pj]))
    if len(grid) < 2:
        return abs(cum_i[-1] - cum_j[-1]) if (len(cum_i) and len(cum_j)) else 0.0
    integral = 0.0
    for k in range(len(grid) - 1):
        p_lo = grid[k]; p_hi = grid[k + 1]
        ki = np.searchsorted(pi, p_lo, side="right") - 1
        kj = np.searchsorted(pj, p_lo, side="right") - 1
        ci = cum_i[ki] if ki >= 0 else 0.0
        cj = cum_j[kj] if kj >= 0 else 0.0
        gap = abs(ci - cj)
        if gap == 0:
            continue
        if kernel_center is None:
            integral += gap * (p_hi - p_lo)
        else:
            # Integrate K(p - center) over [p_lo, p_hi] using the
            # unit-amplitude Epanechnikov kernel (max value 1 at center)
            w = _epanechnikov_integral_unit(p_hi, kernel_center, kernel_h) \
                - _epanechnikov_integral_unit(p_lo, kernel_center, kernel_h)
            integral += gap * w
    return integral


def cell_dissimilarity(quarter_to_tranches, kernel_center: float | None = None):
    """Given a dict {q: DataFrame(p, q)}, return max and mean L1 area
    across the 6 pairs of quarters. If kernel_center is provided, the
    L1 is Epanechnikov-weighted around that price.

    Returns (d_max, d_mean). If fewer than 4 quarters present, returns
    (NaN, NaN)."""
    if len(quarter_to_tranches) != 4:
        return np.nan, np.nan
    pair_d = []
    for qi, qj in combinations((1, 2, 3, 4), 2):
        d = l1_between_bid_curves(
            quarter_to_tranches[qi]["p"].values, quarter_to_tranches[qi]["q"].values,
            quarter_to_tranches[qj]["p"].values, quarter_to_tranches[qj]["q"].values,
            kernel_center=kernel_center,
        )
        pair_d.append(d)
    return float(np.max(pair_d)), float(np.mean(pair_d))


def hour_class(h):
    if h in CRIT: return "critical"
    if h in FLAT: return "flat"
    if h in MID:  return "midday"
    return "other"


def load_hourly_clearing(window=("2025-10-01", "2026-01-01")):
    """Per (date, hour): mean DA clearing price + std across the 4 quarters.

    The kernel center is the hour-average; the std flags hours where the
    4 quarters' clearing prices are heterogeneous enough that a single-
    center kernel is a poor representation of the strategic price band.
    """
    con = duckdb.connect()
    df = con.execute(f"""
        SELECT date::DATE AS date,
               ((period - 1) / 4)::INT AS hour,
               AVG(price_es_eur_mwh) AS p_clear,
               STDDEV_SAMP(price_es_eur_mwh) AS p_clear_std,
               COUNT(*) AS n_qtrs
        FROM '{MARGINALPDBC}'
        WHERE date::DATE >= DATE '{window[0]}' AND date::DATE < DATE '{window[1]}'
          AND price_es_eur_mwh IS NOT NULL
        GROUP BY 1, 2
    """).df()
    return df


def load_price_setters(window=("2025-10-01", "2026-01-01"), eps: float = 0.01):
    """For each (date, period), identify the price-setting unit using the
    same rank-1 / gap ≤ EPS rule as `scripts/analysis/firm/marginal_tech_by_hour.py`.

    Returns: DataFrame [date, period, unit_code, gap_to_clear] with one row
    per (date, period) — the unit whose highest-accepted sell tranche set
    the clearing price for that 15-min period.
    """
    con = duckdb.connect()
    con.execute("PRAGMA threads=4")
    con.execute("SET memory_limit='10GB'")
    sql = f"""
    WITH prices AS (
        SELECT date::DATE AS d, period, price_es_eur_mwh AS p_clear
        FROM '{MARGINALPDBC}'
        WHERE date::DATE >= DATE '{window[0]}' AND date::DATE < DATE '{window[1]}'
          AND price_es_eur_mwh IS NOT NULL
    ),
    cab AS (
        SELECT date::DATE AS d, offer_code, version, unit_code,
               ROW_NUMBER() OVER (PARTITION BY date::DATE, offer_code, unit_code
                                  ORDER BY version DESC) AS rn
        FROM '{CAB}'
        WHERE buy_sell = 'V'
          AND date::DATE >= DATE '{window[0]}' AND date::DATE < DATE '{window[1]}'
    ),
    cab_l AS (SELECT * FROM cab WHERE rn = 1),
    det AS (
        SELECT date::DATE AS d, offer_code, version, period, price_eur_mwh AS p_bid
        FROM '{DET}'
        WHERE date::DATE >= DATE '{window[0]}' AND date::DATE < DATE '{window[1]}'
          AND price_eur_mwh IS NOT NULL AND quantity_mw > 0
    ),
    accepted AS (
        SELECT pr.d, pr.period, c.unit_code, d.p_bid, pr.p_clear,
               RANK() OVER (PARTITION BY pr.d, pr.period ORDER BY d.p_bid DESC) AS rk
        FROM prices pr
        JOIN det d   ON d.d = pr.d AND d.period = pr.period
        JOIN cab_l c ON c.d = d.d AND c.offer_code = d.offer_code AND c.version = d.version
        WHERE d.p_bid <= pr.p_clear
    )
    SELECT d AS date, period, unit_code,
           (p_clear - p_bid) AS gap_to_clear
    FROM accepted
    WHERE rk = 1 AND (p_clear - p_bid) <= {eps}
    """
    return con.execute(sql).df()


def compute(df, clearing: pd.DataFrame, price_setters: pd.DataFrame,
            frequent_pct: float = 1.0):
    """Returns per-cell dissimilarity table.

    Columns:
      - d_max, d_mean: max / mean unweighted pairwise L1 across 6 quarter pairs
      - d_max_w, d_mean_w: same with Epanechnikov-weighted L1, kernel centered
        on hour-average DA clearing price
      - p_clear: hour-average clearing price
      - p_clear_std: std across the 4 quarters' clearing prices (high std →
        the single-center kernel is a poor representation of the strategic band)
      - is_ps_in_hour (bool): the unit was the price-setter in ≥1 of the 4
        quarters of this hour (strict per-cell restriction)
      - is_frequent_ps_unit (bool): the unit appears as a price-setter in
        ≥`frequent_pct`% of the (date, period) cells in the window
    """
    cl_map = {(row.date, row.hour): (row.p_clear, row.p_clear_std)
              for row in clearing.itertuples()}

    # (date, period) -> price-setting unit
    ps_map: dict[tuple[pd.Timestamp, int], str] = {}
    for r in price_setters.itertuples():
        ps_map[(r.date, int(r.period))] = r.unit_code
    # unit -> frequency of price-setting (as fraction of total periods)
    n_total_periods = price_setters[["date", "period"]].drop_duplicates().shape[0]
    unit_ps_freq = price_setters.groupby("unit_code").size() / max(n_total_periods, 1)
    freq_set = set(unit_ps_freq[unit_ps_freq >= frequent_pct / 100.0].index)
    print(f"  price-setter map: {len(ps_map):,} (date, period) -> unit")
    print(f"  unique price-setting units: {price_setters['unit_code'].nunique()}")
    print(f"  frequent (≥{frequent_pct}% of periods) units: {len(freq_set)}")

    out = []
    grp_cols = ["firm", "tech_group", "unit_code", "date", "hour"]
    for keys, g in df.groupby(grp_cols, sort=False):
        per_q = {q: sub for q, sub in g.groupby("quarter")}
        d_max, d_mean = cell_dissimilarity(per_q)
        if np.isnan(d_max):
            continue
        p_info = cl_map.get((keys[3], keys[4]))
        if p_info is None:
            d_max_w, d_mean_w = np.nan, np.nan
            p_clear, p_clear_std = np.nan, np.nan
        else:
            p_clear, p_clear_std = p_info
            d_max_w, d_mean_w = cell_dissimilarity(per_q, kernel_center=float(p_clear))
        # Strict price-setter flag: this unit set the price in ≥1 of the
        # four periods making up this (date, hour) cell.
        hour_idx = int(keys[4])
        date_v = keys[3]
        unit_v = keys[2]
        is_ps_in_hour = any(
            ps_map.get((date_v, hour_idx * 4 + q)) == unit_v
            for q in (1, 2, 3, 4)
        )
        is_frequent_ps_unit = unit_v in freq_set
        out.append((*keys, d_max, d_mean, d_max_w, d_mean_w,
                    p_clear, p_clear_std,
                    is_ps_in_hour, is_frequent_ps_unit))
    cells = pd.DataFrame(out, columns=grp_cols + [
        "d_max", "d_mean", "d_max_w", "d_mean_w",
        "p_clear", "p_clear_std",
        "is_ps_in_hour", "is_frequent_ps_unit"])
    cells["hour_class"] = cells["hour"].apply(hour_class)
    return cells


def summarise(cells, eps=1e-6, std_threshold: float = 5.0):
    """Aggregate per (tech_group, firm, hour_class).

    `frac_flagged` is the unweighted flag rate (D > eps). The median $D$,
    median $D_w$, and median ratio are all computed on the SAME subset of
    flagged cells (D > eps), so the unweighted and weighted columns are
    directly comparable. Ratio = D_w / D per cell: high means within-cell
    variation sits near the clearing price (strategic); low means it
    sits at the extremes (price-cap padding, not strategic).

    `frac_high_std` is the fraction of cells where the 4 quarters' clearing
    prices have std > `std_threshold` EUR/MWh — flagging hours where the
    single-center kernel is a poor representation of "the strategic band".
    """
    cells = cells[cells["hour_class"].isin(("critical", "flat"))].copy()
    cells["flagged"] = (cells["d_max"] > eps).astype(int)
    cells["high_std"] = (cells["p_clear_std"].fillna(0) > std_threshold).astype(int)
    flagged = cells[cells["flagged"] == 1].copy()
    flagged["ratio"] = flagged["d_max_w"] / flagged["d_max"].replace(0, np.nan)
    overall = cells.groupby(["tech_group", "firm", "hour_class"]).agg(
        n_cells=("d_max", "size"),
        frac_flagged=("flagged", "mean"),
        frac_high_std=("high_std", "mean"),
    ).reset_index()
    g_flag = flagged.groupby(["tech_group", "firm", "hour_class"]).agg(
        median_d=("d_max", "median"),
        median_dw=("d_max_w", "median"),
        median_ratio=("ratio", "median"),
    ).reset_index()
    g = overall.merge(g_flag, on=["tech_group", "firm", "hour_class"], how="left")
    return g


def write_tex_full(g, out_path):
    """Appendix-grade longtable: every (tech, firm, hour-class) row.
    Unweighted + kernel-weighted W1 columns side by side."""
    lines = [
        r"\small",
        r"\begin{longtable}{@{}l l l l r r r r r@{}}",
        r"\caption{\textbf{Quarter-by-quarter bid dissimilarity, October--December 2025.} "
        r"For each (firm, unit, date, hour) cell with all four 15-min quarters present, $D$ is the maximum pairwise L1 area between cumulative bid curves over the full price range. $D_w$ is the same L1 area weighted by an Epanechnikov kernel ($h = 20$ EUR/MWh) centered at the period's mean clearing price, isolating bid variation around the strategically-relevant window. Flag = fraction of cells with $D > 0$. The ratio column reports the per-cell median of $D_w/D$ among flagged cells: values near 1 indicate variation concentrated near the clearing price (strategic); values close to 0 indicate variation at the extremes (price-cap padding, not strategic).}\label{tab:quarter_dissim}\\",
        r"\toprule",
        r"Side & Tech & Firm & Hour-class & N cells & \% flagged & median $D$ & median $D_w$ & median ratio \\",
        r"\midrule",
        r"\endfirsthead",
        r"\multicolumn{9}{l}{\emph{(continued from previous page)}} \\",
        r"\toprule",
        r"Side & Tech & Firm & Hour-class & N cells & \% flagged & median $D$ & median $D_w$ & median ratio \\",
        r"\midrule",
        r"\endhead",
        r"\midrule",
        r"\multicolumn{9}{r}{\emph{continued on next page}} \\",
        r"\endfoot",
        r"\bottomrule",
        r"\endlastfoot",
    ]
    last_side = None
    last_tech = None
    for _, r in g.iterrows():
        side = TECH_CATEGORY.get(r["tech_group"], "")
        tech = r["tech_group"].replace("_", r"\_")
        if last_side is not None and last_side != side:
            lines.append(r"\midrule")
        elif last_tech is not None and last_tech != r["tech_group"]:
            lines.append(r"\addlinespace")
        side_disp = side if side != last_side else ""
        tech_disp = tech if r["tech_group"] != last_tech else ""
        last_side = side; last_tech = r["tech_group"]
        lines.append(
            f"{side_disp} & {tech_disp} & {r['firm']} & {r['hour_class'].capitalize()} & "
            f"{int(r['n_cells']):,} & {_fmt_pct(r['frac_flagged'])} & "
            f"{_fmt_num(r['median_d'])} & "
            f"{_fmt_num(r.get('median_dw', float('nan')))} & "
            f"{_fmt_num(r.get('median_ratio', float('nan')))} \\\\"
        )
    lines += [r"\end{longtable}", r"\normalsize"]
    Path(out_path).write_text("\n".join(lines) + "\n")
    print(f"  saved {out_path}")


def _fmt_pct(v: float) -> str:
    """Format a [0, 1] fraction as a percentage; preserve sub-1% precision."""
    if pd.isna(v):
        return "--"
    pct = 100.0 * v
    if pct == 0:
        return "0"
    if pct < 1.0:
        return f"{pct:.2f}"
    return f"{pct:.1f}"


def _fmt_num(v: float, places: int = 2) -> str:
    return "--" if pd.isna(v) else f"{v:.{places}f}"


def write_tex_main(g, out_path):
    """Main-text table: CCGT only, 4 firms x 2 hour-classes = 8 rows."""
    sub = g[g["tech_group"] == "CCGT"].copy()
    firm_order = {f: i for i, f in enumerate(PIVOTAL)}
    sub["f_ord"] = sub["firm"].map(firm_order)
    sub["h_ord"] = sub["hour_class"].map({"critical": 0, "flat": 1})
    sub = sub.sort_values(["f_ord", "h_ord"]).drop(columns=["f_ord", "h_ord"])
    lines = [
        r"\begin{tabular}{@{}l l r r r r r r@{}}",
        r"\toprule",
        r"Firm & Hour-class & N cells & \% flagged & median $D$ & median $D_w$ & median ratio & \% high-$\sigma_{p}$ \\",
        r"\midrule",
    ]
    last_firm = None
    for _, r in sub.iterrows():
        firm_disp = r["firm"] if r["firm"] != last_firm else ""
        last_firm = r["firm"]
        lines.append(
            f"{firm_disp} & {r['hour_class'].capitalize()} & {int(r['n_cells']):,} & "
            f"{_fmt_pct(r['frac_flagged'])} & {_fmt_num(r['median_d'])} & "
            f"{_fmt_num(r.get('median_dw', float('nan')))} & "
            f"{_fmt_num(r.get('median_ratio', float('nan')))} & "
            f"{_fmt_pct(r.get('frac_high_std', float('nan')))} \\\\"
        )
    lines += [r"\bottomrule", r"\end{tabular}"]
    Path(out_path).write_text("\n".join(lines) + "\n")
    print(f"  saved {out_path}")


def _sorted(g):
    """Apply canonical tech / firm / hour-class ordering."""
    tech_order = {t: i for i, t in enumerate(TECHS)}
    firm_order = {f: i for i, f in enumerate(PIVOTAL)}
    g = g.copy()
    g["t_ord"] = g["tech_group"].map(tech_order)
    g["f_ord"] = g["firm"].map(firm_order)
    g["h_ord"] = g["hour_class"].map({"critical": 0, "flat": 1})
    g = g.sort_values(["t_ord", "f_ord", "h_ord"]).drop(columns=["t_ord", "f_ord", "h_ord"])
    return g


def write_tex_combined_full(panels: dict, out_path: Path):
    """Appendix-grade 3-panel longtable: all techs/firms/hour-classes,
    with one panel per sample version (Full / Strict PS / Frequent PS)
    stacked vertically.

    `panels` is a dict mapping a display label → the corresponding summary
    DataFrame.
    """
    lines = [
        r"\small",
        r"\begin{longtable}{@{}l l l l r r r r r r@{}}",
        r"\caption{\textbf{Quarter-by-quarter bid dissimilarity, October--December 2025 --- three-panel sample comparison.} "
        r"$D$ = max pairwise L1 area between quarter bid curves (Wasserstein-style); $D_w$ = Epanechnikov-weighted ($h = 20$ EUR/MWh) on the strategic band around the cell's hour-average DA clearing price; ratio $= D_w/D$ on flagged cells. Three sample versions: \emph{Full sample} (all bidding cells); \emph{Strict PS} (cells where the unit was the rank-1 price-setter in $\ge 1$ of the 4 quarters of the hour); \emph{Frequent PS} (cells whose unit price-sets in $\ge 1$\% of all (date, period) cells in the window). Last column flags within-hour heterogeneity in the clearing price (std $> 5$ EUR/MWh).}\label{tab:quarter_dissim}\\",
        r"\toprule",
        r"Panel & Side & Tech & Firm & Hour-class & N cells & \% flagged & median $D$ & median $D_w$ & ratio \\",
        r"\midrule",
        r"\endfirsthead",
        r"\multicolumn{10}{l}{\emph{(continued from previous page)}} \\",
        r"\toprule",
        r"Panel & Side & Tech & Firm & Hour-class & N cells & \% flagged & median $D$ & median $D_w$ & ratio \\",
        r"\midrule",
        r"\endhead",
        r"\midrule",
        r"\multicolumn{10}{r}{\emph{continued on next page}} \\",
        r"\endfoot",
        r"\bottomrule",
        r"\endlastfoot",
    ]
    for panel_label, g in panels.items():
        last_side = None
        last_tech = None
        last_panel = None
        for _, r in g.iterrows():
            side = TECH_CATEGORY.get(r["tech_group"], "")
            tech = r["tech_group"].replace("_", r"\_")
            if last_panel != panel_label:
                lines.append(r"\midrule")
                last_panel = panel_label; last_side = None; last_tech = None
            elif last_side is not None and last_side != side:
                lines.append(r"\midrule")
            elif last_tech is not None and last_tech != r["tech_group"]:
                lines.append(r"\addlinespace")
            panel_disp = panel_label if panel_label != last_panel else ""
            side_disp = side if side != last_side else ""
            tech_disp = tech if r["tech_group"] != last_tech else ""
            last_side = side; last_tech = r["tech_group"]
            lines.append(
                f"{panel_disp} & {side_disp} & {tech_disp} & {r['firm']} & "
                f"{r['hour_class'].capitalize()} & {int(r['n_cells']):,} & "
                f"{_fmt_pct(r['frac_flagged'])} & {_fmt_num(r['median_d'])} & "
                f"{_fmt_num(r.get('median_dw', float('nan')))} & "
                f"{_fmt_num(r.get('median_ratio', float('nan')))} \\\\"
            )
    lines += [r"\end{longtable}", r"\normalsize"]
    Path(out_path).write_text("\n".join(lines) + "\n")
    print(f"  saved {out_path}")


def write_tex_combined_main(panels: dict, out_path: Path):
    """Combined main-text Table 5: three vertically-stacked panels for CCGT.

    `panels` is a dict mapping a display label → the corresponding summary
    DataFrame (e.g. {"Full sample": g_full, "Strict PS": g_strict,
    "Frequent PS": g_freq}). Each panel is filtered to CCGT rows and shows
    firm × hour-class breakdown.
    """
    firm_order = {f: i for i, f in enumerate(PIVOTAL)}
    lines = [
        r"\begin{tabular}{@{}l l r r r r r r@{}}",
        r"\toprule",
        r"Firm & Hour-class & N cells & \% flagged & median $D$ & median $D_w$ & "
        r"median ratio & \% high-$\sigma_{p}$ \\",
        r"\midrule",
    ]
    last_panel_label = None
    for label, g in panels.items():
        sub = g[g["tech_group"] == "CCGT"].copy()
        sub["f_ord"] = sub["firm"].map(firm_order)
        sub["h_ord"] = sub["hour_class"].map({"critical": 0, "flat": 1})
        sub = sub.sort_values(["f_ord", "h_ord"]).drop(columns=["f_ord", "h_ord"])
        if last_panel_label is not None:
            lines.append(r"\midrule")
        lines.append(
            r"\multicolumn{8}{l}{\textit{" + label + r"}} \\"
        )
        last_firm = None
        for _, r in sub.iterrows():
            firm_disp = r["firm"] if r["firm"] != last_firm else ""
            last_firm = r["firm"]
            lines.append(
                f"{firm_disp} & {r['hour_class'].capitalize()} & "
                f"{int(r['n_cells']):,} & {_fmt_pct(r['frac_flagged'])} & "
                f"{_fmt_num(r['median_d'])} & {_fmt_num(r.get('median_dw', float('nan')))} & "
                f"{_fmt_num(r.get('median_ratio', float('nan')))} & "
                f"{_fmt_pct(r.get('frac_high_std', float('nan')))} \\\\"
            )
        last_panel_label = label
    lines += [r"\bottomrule", r"\end{tabular}"]
    Path(out_path).write_text("\n".join(lines) + "\n")
    print(f"  saved {out_path}")


def main():
    print("loading tranches (Oct-Dec 2025, pivotal firms, 4 techs)...")
    df = load_tranches()
    print(f"  {len(df):,} tranches, {df['unit_code'].nunique()} units")
    print(f"  techs: {df['tech_group'].value_counts().to_dict()}")

    print("\nloading hourly DA clearing prices for kernel centers + within-hour std...")
    clearing = load_hourly_clearing()
    print(f"  {len(clearing):,} (date, hour) cells")

    print("\nloading per-period price-setting units (rank-1, gap ≤ 0.01 EUR/MWh)...")
    price_setters = load_price_setters()
    print(f"  {len(price_setters):,} (date, period) → price-setter rows")

    print("\ncomputing per-cell L1 area between quarter pairs + price-setter flags...")
    cells = compute(df, clearing, price_setters, frequent_pct=1.0)
    print(f"  {len(cells):,} (firm,unit,date,hour) cells with all 4 quarters present")
    print(f"  strict price-setter cells:    {cells['is_ps_in_hour'].sum():,}")
    print(f"  frequent price-setter cells:  {cells['is_frequent_ps_unit'].sum():,}")
    cells.to_csv(OUTDIR / "quarter_dissimilarity_cells_2025Q4.csv", index=False)

    # Three summaries: full sample, strict per-cell PS, frequent-PS unit
    print("\n=== summary: FULL sample (all bidding cells) ===")
    g_full = _sorted(summarise(cells))
    print(g_full.to_string(index=False))
    g_full.to_csv(OUTDIR / "quarter_dissimilarity_summary_full_2025Q4.csv", index=False)
    write_tex_full(g_full, TABDIR / "tab_quarter_dissimilarity.tex")
    write_tex_main(g_full, TABDIR / "tab_quarter_dissimilarity_main.tex")

    print("\n=== summary: STRICT — cells where THIS unit was price-setter in ≥1 quarter ===")
    g_strict = _sorted(summarise(cells[cells["is_ps_in_hour"]]))
    print(g_strict.to_string(index=False))
    g_strict.to_csv(OUTDIR / "quarter_dissimilarity_summary_strict_ps_2025Q4.csv", index=False)
    write_tex_full(g_strict, TABDIR / "tab_quarter_dissimilarity_strict_ps.tex")
    write_tex_main(g_strict, TABDIR / "tab_quarter_dissimilarity_main_strict_ps.tex")

    print("\n=== summary: FREQUENT — cells of units that are price-setter in ≥1% of all periods ===")
    g_freq = _sorted(summarise(cells[cells["is_frequent_ps_unit"]]))
    print(g_freq.to_string(index=False))
    g_freq.to_csv(OUTDIR / "quarter_dissimilarity_summary_frequent_ps_2025Q4.csv", index=False)
    write_tex_full(g_freq, TABDIR / "tab_quarter_dissimilarity_frequent_ps.tex")
    write_tex_main(g_freq, TABDIR / "tab_quarter_dissimilarity_main_frequent_ps.tex")

    # Combined main table: CCGT, 3 panels stacked (Full / Strict PS / Frequent PS).
    write_tex_combined_main(
        {"Full sample": g_full, "Strict PS": g_strict, "Frequent PS": g_freq},
        TABDIR / "tab_quarter_dissimilarity_pricesetter_main.tex",
    )
    # Appendix companion: same 3 panels, all techs, all firms, longtable.
    write_tex_combined_full(
        {"Full sample": g_full, "Strict PS": g_strict, "Frequent PS": g_freq},
        TABDIR / "tab_quarter_dissimilarity_pricesetter.tex",
    )


if __name__ == "__main__":
    main()
