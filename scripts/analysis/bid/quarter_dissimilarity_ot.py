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
UNITS_CSV = REPO / "data" / "external" / "omie_reference" / "lista_unidades.csv"
OUTDIR = REPO / "results" / "regressions" / "bid" / "quarter_dissimilarity"
OUTDIR.mkdir(parents=True, exist_ok=True)
TABDIR = REPO / "thesis" / "paper" / "tables"

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


def l1_between_bid_curves(prices_i, qtys_i, prices_j, qtys_j):
    """Exact L1 area between two cumulative bid curves, handling the
    union-of-prices integration domain. Each input is a sequence of
    (price, quantity) tranches; the cumulative curve at price p is
    sum of q for tranches with p_tranche <= p."""
    pi = np.asarray(prices_i, dtype=float); qi = np.asarray(qtys_i, dtype=float)
    pj = np.asarray(prices_j, dtype=float); qj = np.asarray(qtys_j, dtype=float)
    # Sort by price
    oi = np.argsort(pi); pi, qi = pi[oi], qi[oi]
    oj = np.argsort(pj); pj, qj = pj[oj], qj[oj]
    cum_i = np.cumsum(qi)
    cum_j = np.cumsum(qj)
    # Union of breakpoints (also include 0 and an upper sentinel)
    grid = np.unique(np.concatenate([pi, pj]))
    if len(grid) < 2:
        return abs(cum_i[-1] - cum_j[-1]) if (len(cum_i) and len(cum_j)) else 0.0
    integral = 0.0
    for k in range(len(grid) - 1):
        p_lo = grid[k]; p_hi = grid[k + 1]
        # cum_qty at price <= p_lo (note: tranches at p_lo are included)
        ki = np.searchsorted(pi, p_lo, side="right") - 1
        kj = np.searchsorted(pj, p_lo, side="right") - 1
        ci = cum_i[ki] if ki >= 0 else 0.0
        cj = cum_j[kj] if kj >= 0 else 0.0
        integral += abs(ci - cj) * (p_hi - p_lo)
    return integral


def cell_dissimilarity(quarter_to_tranches):
    """Given a dict {q: DataFrame(p, q)}, return max and mean L1 area
    across the 6 pairs of quarters. If fewer than 4 quarters present,
    returns NaN."""
    if len(quarter_to_tranches) != 4:
        return np.nan, np.nan
    pair_d = []
    for qi, qj in combinations((1, 2, 3, 4), 2):
        d = l1_between_bid_curves(
            quarter_to_tranches[qi]["p"].values, quarter_to_tranches[qi]["q"].values,
            quarter_to_tranches[qj]["p"].values, quarter_to_tranches[qj]["q"].values,
        )
        pair_d.append(d)
    return float(np.max(pair_d)), float(np.mean(pair_d))


def hour_class(h):
    if h in CRIT: return "critical"
    if h in FLAT: return "flat"
    if h in MID:  return "midday"
    return "other"


def compute(df):
    """Returns per-cell dissimilarity table."""
    out = []
    # group by (firm, tech_group, unit_code, date, hour)
    grp_cols = ["firm", "tech_group", "unit_code", "date", "hour"]
    for keys, g in df.groupby(grp_cols, sort=False):
        per_q = {q: sub for q, sub in g.groupby("quarter")}
        d_max, d_mean = cell_dissimilarity(per_q)
        if np.isnan(d_max):
            continue
        out.append((*keys, d_max, d_mean))
    cells = pd.DataFrame(out, columns=grp_cols + ["d_max", "d_mean"])
    cells["hour_class"] = cells["hour"].apply(hour_class)
    return cells


def summarise(cells, eps=1e-6):
    """Aggregate per (tech_group, firm, hour_class)."""
    cells = cells[cells["hour_class"].isin(("critical", "flat"))].copy()
    cells["flagged"] = (cells["d_max"] > eps).astype(int)
    g = cells.groupby(["tech_group", "firm", "hour_class"]).agg(
        n_cells=("d_max", "size"),
        frac_flagged=("flagged", "mean"),
        median_d=("d_max", "median"),
        p75_d=("d_max", lambda x: np.percentile(x, 75)),
    ).reset_index()
    return g


def write_tex(g, out_path):
    """Long table: side x tech x firm x hour_class -> n, flag%, median d."""
    lines = [
        r"\begin{tabular}{l l l l r r r}",
        r"\toprule",
        r"Side & Tech & Firm & Hour-class & N cells & \% flagged & median $D$ \\",
        r"\midrule",
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
        firm_disp = r["firm"] if (r["tech_group"] != last_tech or side != last_side
                                   or last_tech is None) else r["firm"]
        last_side = side; last_tech = r["tech_group"]
        lines.append(
            f"{side_disp} & {tech_disp} & {r['firm']} & {r['hour_class'].capitalize()} & "
            f"{int(r['n_cells']):,} & {100*r['frac_flagged']:.1f} & {r['median_d']:.2f} \\\\"
        )
    lines += [r"\bottomrule", r"\end{tabular}"]
    Path(out_path).write_text("\n".join(lines) + "\n")
    print(f"  saved {out_path}")


def main():
    print("loading tranches (Oct-Dec 2025, pivotal firms, 4 techs)...")
    df = load_tranches()
    print(f"  {len(df):,} tranches, {df['unit_code'].nunique()} units")
    print(f"  techs: {df['tech_group'].value_counts().to_dict()}")

    print("\ncomputing per-cell L1 area between quarter pairs...")
    cells = compute(df)
    print(f"  {len(cells):,} (firm,unit,date,hour) cells with all 4 quarters present")
    cells.to_csv(OUTDIR / "quarter_dissimilarity_cells_2025Q4.csv", index=False)

    print("\nsummary by (tech, firm, hour-class):")
    g = summarise(cells)
    # ordering
    tech_order = {t: i for i, t in enumerate(TECHS)}
    firm_order = {f: i for i, f in enumerate(PIVOTAL)}
    g["t_ord"] = g["tech_group"].map(tech_order)
    g["f_ord"] = g["firm"].map(firm_order)
    g["h_ord"] = g["hour_class"].map({"critical": 0, "flat": 1})
    g = g.sort_values(["t_ord", "f_ord", "h_ord"]).drop(columns=["t_ord", "f_ord", "h_ord"])
    print(g.to_string(index=False))
    g.to_csv(OUTDIR / "quarter_dissimilarity_summary_2025Q4.csv", index=False)
    write_tex(g, TABDIR / "tab_quarter_dissimilarity.tex")


if __name__ == "__main__":
    main()
