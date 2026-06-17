# STATUS: ALIVE
# LAST-AUDIT: 2026-05-18
# FEEDS: descriptive_facts.tex §3.3 (MIC-filtered descriptive)
# CLAIM: Apply Portela Phase 1 (empirical equivalent) — drop unit-days
#        where pdbc shows 0 cleared MWh across the entire day. These are
#        units rejected by MIC, indivisibility, exclusivity, or paradoxical
#        rejection — their bids entered casación but did not constrain
#        clearing. Re-aggregate bid-shape metrics, generate _mic side-by-side
#        tex tables for §3.3.
#
# Note: cab/det contain "ofertas que entran en casación" (per ficherosomie137
#       §5.1.4) — submitted offers that entered EUPHEMIA. MIC parameters
#       (Fijoeuro = MIC_fix) are observable in cab.fixed_term_eur. We use
#       the ex post filter (cleared > 0) which captures ALL complex-condition
#       rejections, not just MIC.
#
# Output:
#   results/regressions/bid/bid_shape/
#     DA_per_cell_mic_filtered.parquet, IDA_per_cell_mic_filtered.parquet
#     DA_agg_firm_tech_mic.csv,        IDA_agg_firm_tech_mic.csv
#     tex/tab_bidshape_DA_by_regime_mic.tex, tab_bidshape_IDA_by_regime_mic.tex
#     tex/tab_bidshape_DA_ccgt_byfirm_mic.tex (and IDA)
#     tex/tab_mic_rates_by_tech_regime.tex     (the table of MIC rates)

from __future__ import annotations
from pathlib import Path
import duckdb
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
OUT  = REPO / "results" / "regressions" / "bid" / "bid_shape"
TEX  = OUT / "tex"
TEX.mkdir(parents=True, exist_ok=True)

PDBC = REPO / "data/processed/omie/mercado_diario/programas/pdbc_all.parquet"
PIBCI = REPO / "data/processed/omie/mercado_intradiario_subastas/programas/pibci_all.parquet"

REGIMES = {
    "3-sess":              ("2024-06-14", "2024-11-30"),
    "ISP15-win":           ("2024-12-01", "2025-03-18"),
    "DA60/ID15 pre-blk":   ("2025-03-19", "2025-04-27"),
    "DA60/ID15 post-blk":  ("2025-04-28", "2025-09-30"),
    "DA15/ID15":           ("2025-10-01", "2025-12-31"),
}
REGIME_ORDER = list(REGIMES.keys())
FOCUS_TECHS = ["CCGT", "Hydro", "Hydro_pump", "Nuclear", "Wind", "Solar PV", "Solar Thermal", "Cogen"]


def tag_regime(date_series):
    out = pd.Series(index=date_series.index, dtype="object")
    for r, (a, b) in REGIMES.items():
        out.loc[(date_series >= a) & (date_series <= b)] = r
    return out


def hour_class(h):
    if h in (5, 6, 7, 8, 16, 17, 18, 19, 20, 21, 22): return "Critical"
    if h in (1, 2, 3): return "Flat"
    if h in (11, 12, 13, 14): return "Midday"
    return "Dropped"


def build_daily_clearance_map(con, source_path, market_label):
    """Returns dataframe (date, unit_code, cleared_today) for the relevant market."""
    print(f"  Building daily clearance map from {market_label}...")
    sql = f"""
    SELECT date::DATE AS date,
           unit_code,
           CASE WHEN SUM(assigned_power_mw) > 0 THEN 1 ELSE 0 END AS cleared_today
    FROM '{source_path}'
    WHERE date::DATE BETWEEN '2024-06-14' AND '2026-01-31'
    GROUP BY 1, 2
    """
    df = con.execute(sql).df()
    df["date"] = pd.to_datetime(df["date"])
    print(f"    {len(df):,} (date, unit_code) rows; {df['cleared_today'].mean()*100:.1f}% cleared")
    return df


def filter_and_aggregate(market, per_cell_path, clearance_df, out_label):
    print(f"=== {market} ({out_label}) ===")
    df = pd.read_parquet(per_cell_path)
    df["date"] = pd.to_datetime(df["date"])
    df["regime"] = tag_regime(df["date"])
    df = df.dropna(subset=["regime"])

    # MIC filter: drop unit-days where unit cleared 0 across the entire day.
    # For DA, use pdbc clearance; for IDA use pibci clearance.
    df = df.merge(clearance_df, on=["date", "unit_code"], how="left")
    n_pre = len(df)
    df = df[df["cleared_today"] == 1].copy()
    n_post = len(df)
    print(f"  MIC filter (drop unit-days with daily cleared=0): {n_pre:,} → {n_post:,} cells ({100*n_post/n_pre:.1f}% retained)")

    # Also compute hour_class
    df["hour_class"] = df["hour"].apply(hour_class)

    # Share = mw_in_band / mw_total (matches bid_shape_normalized.py for the unfiltered tables)
    df["share_in_band"] = df["mw_in_band"] / df["mw_total"].clip(lower=1e-6)
    df.loc[df["mw_total"] < 1e-6, "share_in_band"] = 0.0

    # Aggregate per (tech, regime, hour_class, year_month) — month-mean then regime-mean
    agg_month = df.groupby(["tech_group", "regime", "hour_class", "year_month"]).agg(
        share_in_band=("share_in_band", "mean"),
        mw_in_band=("mw_in_band", "mean"),
        mw_total=("mw_total", "mean"),
        n_tranches=("n_tranches_in_band", "mean"),
        frac_single_block=("frac_single_block", "mean"),
        mw_at_mcp=("mw_at_mcp", "mean"),
        n_cells=("share_in_band", "size"),
    ).reset_index()
    agg = agg_month.groupby(["tech_group", "regime", "hour_class"]).agg(
        share_in_band=("share_in_band", "mean"),
        mw_in_band=("mw_in_band", "mean"),
        mw_total=("mw_total", "mean"),
        n_tranches=("n_tranches", "mean"),
        frac_single_block=("frac_single_block", "mean"),
        mw_at_mcp=("mw_at_mcp", "mean"),
        n_months=("n_cells", "size"),
    ).reset_index()
    agg["share_pct"] = (agg["share_in_band"] * 100).round(1)
    agg.to_csv(OUT / f"{market}_descriptive_mic.csv", index=False)
    print(f"  wrote {market}_descriptive_mic.csv")

    # Build tex tables (share-based to match unfiltered normalized tables)
    write_tex_main(agg, market)
    write_tex_ccgt_byfirm(df, market)
    return df


def write_tex_main(agg, market):
    rows = []
    for hc in ["Critical", "Flat", "Midday"]:
        rows.append(f"\\multicolumn{{6}}{{l}}{{\\textit{{Hour-class: {hc}}}}} \\\\")
        sub = agg[(agg["hour_class"] == hc) & (agg["tech_group"].isin(FOCUS_TECHS))]
        piv = sub.pivot_table(index="tech_group", columns="regime", values="share_pct").round(1).fillna(0)
        piv = piv.reindex(FOCUS_TECHS).fillna(0)
        for r in REGIME_ORDER:
            if r not in piv.columns: piv[r] = 0
        piv = piv[REGIME_ORDER]
        for tech in piv.index:
            tech_label = tech.replace("_", " ")
            vals = " & ".join(f"{piv.loc[tech, r]:.1f}" for r in REGIME_ORDER)
            rows.append(f"{tech_label} & {vals} \\\\")
        rows.append("\\midrule")
    body = "\n".join(rows)
    header = " & ".join([""] + REGIME_ORDER)
    tex = (
        f"% auto-built by bid_shape_mic_filter.py — MIC-filtered version (share metric)\n"
        f"% Metric: share (%) of unit's offered MW falling within [MCP-50, MCP+50] EUR/MWh,\n"
        f"% on the MIC-filtered subsample (unit-days with pdbc/pibci cleared>0 only).\n"
        f"\\begin{{tabular}}{{l r r r r r}}\n"
        f"\\toprule\n{header} \\\\\n\\midrule\n{body}\n\\bottomrule\n\\end{{tabular}}\n"
    )
    out = TEX / f"tab_bidshape_{market}_by_regime_mic.tex"
    out.write_text(tex)
    print(f"  wrote {out}")


def write_tex_ccgt_byfirm(df, market):
    sub = df[(df["tech_group"] == "CCGT") & (df["hour_class"].isin(["Critical", "Flat"]))].copy()
    if len(sub) == 0:
        print(f"  no CCGT data for {market}")
        return
    agg_month = sub.groupby(["firm", "regime", "hour_class", "year_month"]).agg(
        share_in_band=("share_in_band", "mean"),
    ).reset_index()
    agg = agg_month.groupby(["firm", "regime", "hour_class"]).agg(
        share_in_band=("share_in_band", "mean"),
    ).reset_index()
    agg["share_pct"] = (agg["share_in_band"] * 100).round(1)
    rows = []
    for hc in ["Critical", "Flat"]:
        rows.append(f"\\multicolumn{{6}}{{l}}{{\\textit{{CCGT only, {hc} hours}}}} \\\\")
        for firm in ["GN", "IB", "GE", "HC", "REP", "OTH"]:
            piv = agg[(agg["hour_class"] == hc) & (agg["firm"] == firm)]
            if len(piv) == 0:
                continue
            row_vals = []
            for r in REGIME_ORDER:
                v = piv[piv["regime"] == r]["share_pct"]
                row_vals.append(f"{v.iloc[0]:.1f}" if len(v) else "---")
            rows.append(f"{firm} & " + " & ".join(row_vals) + " \\\\")
        rows.append("\\midrule")
    body = "\n".join(rows)
    header = " & ".join([""] + REGIME_ORDER)
    tex = (
        f"% auto-built — MIC-filtered share metric\n"
        f"\\begin{{tabular}}{{l r r r r r}}\n"
        f"\\toprule\n{header} \\\\\n\\midrule\n{body}\n\\bottomrule\n\\end{{tabular}}\n"
    )
    out = TEX / f"tab_bidshape_{market}_ccgt_byfirm_mic.tex"
    out.write_text(tex)
    print(f"  wrote {out}")


def write_mic_rates_table(da_filtered_df, da_clearance_df):
    """Per (tech, regime), report the share of bid-shape (unit, date, period) cells
    that the MIC filter drops. Denominator is the DA bid-shape per_cell sample
    (cab+det entered into casación); numerator is cells whose (date, unit_code)
    either (a) does not appear in pdbc (LEFT JOIN gives NaN), or (b) appears with
    cleared_today=0. With sparse-zero pdbc the (a) channel dominates for
    thermal/RES; (b) dominates for pumped storage."""
    df = pd.read_parquet(OUT / "DA_per_cell.parquet")
    df["date"] = pd.to_datetime(df["date"])
    df["regime"] = tag_regime(df["date"])
    df = df.dropna(subset=["regime"])
    df = df.merge(da_clearance_df, on=["date", "unit_code"], how="left")
    df["dropped"] = (df["cleared_today"].fillna(0) == 0).astype(int)
    agg = df.groupby(["tech_group", "regime"]).agg(
        n_dropped=("dropped", "sum"),
        n_total=("dropped", "size"),
    ).reset_index()
    agg["dropped_pct"] = (100.0 * agg["n_dropped"] / agg["n_total"]).round(1)
    piv = agg.pivot_table(index="tech_group", columns="regime", values="dropped_pct").round(1).fillna(0)
    piv = piv.reindex(FOCUS_TECHS).fillna(0)
    for r in REGIME_ORDER:
        if r not in piv.columns: piv[r] = 0
    piv = piv[REGIME_ORDER]
    rows = []
    for tech in piv.index:
        tech_label = tech.replace("_", " ")
        vals = " & ".join(f"{piv.loc[tech, r]:.1f}" for r in REGIME_ORDER)
        rows.append(f"{tech_label} & {vals} \\\\")
    body = "\n".join(rows)
    header = " & ".join([""] + REGIME_ORDER)
    tex = (
        f"% auto-built by bid_shape_mic_filter.py\n"
        f"% Denominator: DA bid-shape per_cell sample (cab+det entered into casación).\n"
        f"% Numerator: cells dropped by the MIC filter — either absent from pdbc\n"
        f"% (sparse-zero) or present with daily SUM(assigned_power_mw)=0.\n"
        f"\\begin{{tabular}}{{l r r r r r}}\n"
        f"\\toprule\n{header} \\\\\n\\midrule\n{body}\n\\bottomrule\n\\end{{tabular}}\n"
    )
    out = TEX / "tab_mic_rates_by_tech_regime.tex"
    out.write_text(tex)
    print(f"  wrote {out}")
    print("\n=== MIC drop rates by (tech, regime), % of bid-shape cells dropped ===")
    print(piv.to_string())


def main():
    con = duckdb.connect()
    con.execute("PRAGMA threads=4")
    con.execute("SET memory_limit='6GB'")

    # DA clearance map
    da_clearance = build_daily_clearance_map(con, PDBC, "pdbc (DA)")
    da_filtered = filter_and_aggregate("DA", OUT / "DA_per_cell.parquet", da_clearance, "DA, MIC-filtered")

    # IDA clearance map
    ida_clearance = build_daily_clearance_map(con, PIBCI, "pibci (IDA)")
    filter_and_aggregate("IDA", OUT / "IDA_per_cell.parquet", ida_clearance, "IDA, MIC-filtered")

    write_mic_rates_table(da_filtered, da_clearance)
    print("Done.")


if __name__ == "__main__":
    main()
