# STATUS: ALIVE
# LAST-AUDIT: 2026-05-17
# FEEDS: exploratory; outputs at results/regressions/firm/marginal_tech/deep_dive/
# CLAIM: Within-hour DA bid-shape variation for the top wind aggregator
#        portfolios, comparing post-MTU15-DA (Oct-Dec 2025, when 4 quarter
#        curves per hour exist) against IDA pre-MTU15-IDA (where IDA still
#        had 4 quarter-segment bids per session post-2025-03-19). We measure:
#          - mean/p50/p90 within-hour SD of bid prices across the 4 quarters
#          - mean/p50 within-hour SD of bid quantity at strategic-band prices
#          - share of (unit, hour) cells where all 4 quarters are bit-identical
#          - share where at least one quarter differs by >5 EUR/MWh
#        Per unit (top 12 wind units by event count in DA15/ID15) and per
#        firm aggregate. The metric is purely descriptive — no kernel
#        weighting, no Wasserstein-1; we want a fast diagnostic of whether
#        wind aggregators exploit quarter granularity at all.

from __future__ import annotations
from pathlib import Path
import duckdb
import pandas as pd

REPO  = Path(__file__).resolve().parents[3]
DET   = REPO / "data" / "processed" / "omie" / "mercado_diario" / "ofertas" / "det_all.parquet"
CAB   = REPO / "data" / "processed" / "omie" / "mercado_diario" / "ofertas" / "cab_all.parquet"
MPDBC = REPO / "data" / "processed" / "omie" / "mercado_diario" / "precios" / "marginalpdbc_all.parquet"
UNITS = REPO / "data" / "external"  / "omie_reference" / "lista_unidades.csv"

OUT = REPO / "results" / "regressions" / "firm" / "marginal_tech" / "deep_dive"
OUT.mkdir(parents=True, exist_ok=True)


def firm_of(owner: str) -> str:
    if not isinstance(owner, str): return "OTHER"
    s = owner.upper()
    if "IBERDROLA" in s: return "IB"
    if "ENDESA" in s: return "GE"
    if "NATURGY" in s or "GAS NATURAL" in s: return "GN"
    if "HIDROCANTABRICO" in s or " EDP" in s or s.startswith("EDP "): return "HC"
    if "REPSOL" in s: return "REP"
    if "ACCIONA" in s: return "ACC"
    if "GESTERNOVA" in s: return "GST"
    if "AXPO" in s: return "AXPO"
    if "SHELL" in s: return "SHELL"
    if "ENGIE" in s: return "ENGIE"
    if "NEXUS" in s: return "NEXUS"
    if "IGNIS" in s: return "IGNIS"
    return "OTHER"


def per_unit_within_hour_dispersion(start: str, end: str, units_to_include: list[str]) -> pd.DataFrame:
    """For each (unit, date, hour), compute within-hour dispersion of bid
    prices and (separately) of the *total quantity* offered in a strategic
    band of MCP±50 across the 4 quarters of that hour. Returns one row per
    (unit, date, hour)."""
    con = duckdb.connect()
    con.execute("PRAGMA threads=4"); con.execute("SET memory_limit='10GB'")
    con.register("uw", pd.DataFrame({"unit_code": units_to_include}))
    sql = f"""
    WITH prices AS (
        SELECT date::DATE AS d, period, price_es_eur_mwh AS p_clear, mtu_minutes
        FROM '{MPDBC}'
        WHERE date::DATE BETWEEN DATE '{start}' AND DATE '{end}'
          AND price_es_eur_mwh IS NOT NULL
    ),
    cab AS (
        SELECT date::DATE AS d, offer_code, version, unit_code,
               ROW_NUMBER() OVER (PARTITION BY date::DATE, offer_code, unit_code ORDER BY version DESC) AS rn
        FROM '{CAB}'
        WHERE buy_sell='V' AND date::DATE BETWEEN DATE '{start}' AND DATE '{end}'
    ),
    cab_l AS (SELECT * FROM cab WHERE rn=1),
    det AS (
        SELECT date::DATE AS d, offer_code, version, period,
               price_eur_mwh AS p_bid, quantity_mw AS q_bid
        FROM '{DET}'
        WHERE date::DATE BETWEEN DATE '{start}' AND DATE '{end}'
          AND price_eur_mwh IS NOT NULL AND quantity_mw > 0
    ),
    det_unit AS (
        SELECT dv.d, dv.period, c.unit_code, dv.p_bid, dv.q_bid, pr.p_clear, pr.mtu_minutes,
               CAST(CASE WHEN pr.mtu_minutes = 60 THEN dv.period - 1
                         ELSE CAST(FLOOR((dv.period - 1) / 4.0) AS INT) END AS INT) AS hour,
               CAST(CASE WHEN pr.mtu_minutes = 60 THEN 1
                         ELSE ((dv.period - 1) % 4) + 1 END AS INT) AS quarter
        FROM det dv
        JOIN cab_l c USING (d, offer_code, version)
        JOIN prices pr USING (d, period)
        JOIN uw USING (unit_code)
    ),
    -- Per (unit, date, hour, quarter): summary of bid stack
    quarter_summary AS (
        SELECT d, unit_code, hour, quarter,
               AVG(p_clear) AS p_clear,
               -- Within-band metrics: bids in MCP +/- 50
               SUM(q_bid) FILTER (WHERE p_bid BETWEEN p_clear - 50 AND p_clear + 50) AS q_band,
               AVG(p_bid) FILTER (WHERE p_bid BETWEEN p_clear - 50 AND p_clear + 50) AS p_band_mean,
               MIN(p_bid) FILTER (WHERE p_bid BETWEEN p_clear - 50 AND p_clear + 50) AS p_band_min,
               MAX(p_bid) FILTER (WHERE p_bid BETWEEN p_clear - 50 AND p_clear + 50) AS p_band_max,
               COUNT(*) FILTER (WHERE p_bid BETWEEN p_clear - 50 AND p_clear + 50) AS n_band_tranches,
               COUNT(*) AS n_tranches_all,
               -- Reference quantile of all bids in the quarter
               quantile_cont(p_bid, 0.5) AS p_bid_p50,
               SUM(q_bid) AS q_total
        FROM det_unit
        GROUP BY d, unit_code, hour, quarter
    ),
    -- Per (unit, date, hour): SD across the 4 quarters
    hour_summary AS (
        SELECT d, unit_code, hour,
               COUNT(*) AS n_quarters,
               stddev_samp(q_band) AS sd_q_band,
               AVG(q_band) AS mean_q_band,
               stddev_samp(p_band_mean) AS sd_p_band_mean,
               AVG(p_band_mean) AS mean_p_band_mean,
               stddev_samp(p_bid_p50) AS sd_p_bid_p50,
               AVG(p_bid_p50) AS mean_p_bid_p50,
               MAX(p_band_max) - MIN(p_band_min) AS range_p_band,
               MAX(n_band_tranches) AS max_n_band_tr,
               MIN(n_band_tranches) AS min_n_band_tr,
               AVG(p_clear) AS p_clear_mean
        FROM quarter_summary
        GROUP BY d, unit_code, hour
        HAVING COUNT(*) = 4   -- complete 4-quarter cells only
    )
    SELECT * FROM hour_summary
    """
    return con.execute(sql).df()


def aggregate_per_unit(disp: pd.DataFrame, units_meta: pd.DataFrame) -> pd.DataFrame:
    """Aggregate per-(unit, date, hour) dispersion to per-unit summary."""
    g = disp.groupby("unit_code").agg(
        n_cells=("d", "count"),
        mean_sd_p_p50=("sd_p_bid_p50", "mean"),
        mean_sd_p_band=("sd_p_band_mean", "mean"),
        mean_sd_q_band=("sd_q_band", "mean"),
        mean_mean_q_band=("mean_q_band", "mean"),
        mean_range_p_band=("range_p_band", "mean"),
        share_sd_p50_zero=("sd_p_bid_p50", lambda s: (s.fillna(0) <= 0.01).mean()),
        share_sd_q_band_zero=("sd_q_band", lambda s: (s.fillna(0) <= 0.001).mean()),
        share_p_range_gt5=("range_p_band", lambda s: (s.fillna(0) > 5).mean()),
        share_p_range_gt20=("range_p_band", lambda s: (s.fillna(0) > 20).mean()),
    ).reset_index()
    g = g.merge(units_meta, on="unit_code", how="left")
    return g.sort_values("n_cells", ascending=False)


def main():
    units = pd.read_csv(UNITS)
    units["firm"] = units["owner_agent"].apply(firm_of)
    units_meta = units[["unit_code", "technology", "firm", "owner_agent"]]

    # Top wind aggregator units in DA15/ID15 by event count
    top_wind = ["IBEVD11", "GSVD116", "HCGVD12", "HCGVD14", "HCGVD25",
                "EGVD476", "EGVD489", "EGVD451", "AXPVD12", "ENGVD11",
                "NEXVD11", "IGNVD10"]
    # Top CCGT units (Big-4 owned)
    top_ccgt = ["SROQ2", "PGR5", "BES3", "BES5", "ESC6", "ACE3",
                "ARCOS1", "ARCOS2", "ARCOS3", "PGR4", "BES4", "ESC5"]
    # Top hydro reservoir + pump-storage units
    top_hydro = ["MUEL", "DUER", "TAJO", "SIL", "MLTG", "GDLQ"]
    all_units = sorted(set(top_wind + top_ccgt + top_hydro) & set(units["unit_code"].tolist()))
    print(f"Inspecting {len(all_units)} units: {all_units}")

    # Post-MTU15-DA window: the only window where DA has 4 quarters per hour
    print("\n=== DA15/ID15 (Oct-Dec 2025) within-hour dispersion ===")
    disp = per_unit_within_hour_dispersion("2025-10-01", "2025-12-31", all_units)
    summary = aggregate_per_unit(disp, units_meta)
    summary.to_csv(OUT / "11_wind_within_hour_disp_DA15.csv", index=False)
    print(summary[["unit_code", "technology", "firm", "n_cells",
                   "mean_sd_p_p50", "mean_sd_q_band", "mean_range_p_band",
                   "share_sd_p50_zero", "share_p_range_gt5", "share_p_range_gt20"]].round(2).to_string(index=False))

    # Also: per-firm aggregate for wind only
    print("\n=== Per-firm wind aggregate, DA15/ID15 ===")
    disp_w = disp[disp["unit_code"].isin(top_wind)]
    if len(disp_w):
        merged = disp_w.merge(units_meta[["unit_code", "firm"]], on="unit_code")
        per_firm = merged.groupby("firm").agg(
            n_cells=("d", "count"),
            n_units=("unit_code", "nunique"),
            mean_sd_p_p50=("sd_p_bid_p50", "mean"),
            mean_sd_q_band=("sd_q_band", "mean"),
            share_sd_p50_zero=("sd_p_bid_p50", lambda s: (s.fillna(0) <= 0.01).mean()),
            share_p_range_gt5=("range_p_band", lambda s: (s.fillna(0) > 5).mean()),
        ).round(2)
        print(per_firm.to_string())

    # CCGT comparison block (same units inspected; CCGTs only)
    print("\n=== Per-firm CCGT aggregate (comparison), DA15/ID15 ===")
    disp_c = disp[disp["unit_code"].isin(top_ccgt)]
    if len(disp_c):
        merged = disp_c.merge(units_meta[["unit_code", "firm"]], on="unit_code")
        per_firm = merged.groupby("firm").agg(
            n_cells=("d", "count"),
            n_units=("unit_code", "nunique"),
            mean_sd_p_p50=("sd_p_bid_p50", "mean"),
            mean_sd_q_band=("sd_q_band", "mean"),
            share_sd_p50_zero=("sd_p_bid_p50", lambda s: (s.fillna(0) <= 0.01).mean()),
            share_p_range_gt5=("range_p_band", lambda s: (s.fillna(0) > 5).mean()),
        ).round(2)
        print(per_firm.to_string())


if __name__ == "__main__":
    main()
