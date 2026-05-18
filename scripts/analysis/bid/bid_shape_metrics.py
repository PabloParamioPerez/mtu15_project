# STATUS: ALIVE
# LAST-AUDIT: 2026-05-18
# FEEDS: bidding_internal.tex §3 (Setup: bid-shape metrics)
# CLAIM: Comprehensive per-cell bid-shape metrics for DA and IDA sell bids.
#        For each (date, period, unit) compute:
#          - mw_in_band     = MW in [MCP-h, MCP+h], h=50 EUR/MWh (steepness proxy)
#          - n_tranches     = distinct prices in band (granularity)
#          - frac_single_block = 1 if single bid in band, else 0
#          - vw_price_in_band  = MW-weighted mean price in band
#          - mw_at_MCP_exact = MW at p_bid in [MCP-0.01, MCP+0.01]
#        Then aggregate to (firm, tech, year_month, hour, period_within_hour).
#        Period is 1 quarter if available (MTU15), 1 hour otherwise.
#
# Output:
#   results/regressions/bid/bid_shape/
#     DA_per_cell.parquet      one row per (date, period, unit_code), DA market
#     IDA_per_cell.parquet     one row per (date, session, period, unit_code), IDA market
#     DA_agg_firm_tech.csv     aggregated (tech, firm, ym, hour, quarter)
#     IDA_agg_firm_tech.csv    aggregated (tech, firm, ym, hour, quarter)

from __future__ import annotations
from pathlib import Path
import duckdb
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
OUT = REPO / "results" / "regressions" / "bid" / "bid_shape"
OUT.mkdir(parents=True, exist_ok=True)

DET   = REPO / "data/processed/omie/mercado_diario/ofertas/det_all.parquet"
CAB   = REPO / "data/processed/omie/mercado_diario/ofertas/cab_all.parquet"
IDET  = REPO / "data/processed/omie/mercado_intradiario_subastas/ofertas/idet_all.parquet"
ICAB  = REPO / "data/processed/omie/mercado_intradiario_subastas/ofertas/icab_all.parquet"
MP_DA = REPO / "data/processed/omie/mercado_diario/precios/marginalpdbc_all.parquet"
MP_IDA= REPO / "data/processed/omie/mercado_intradiario_subastas/precios/marginalpibc_all.parquet"
UNITS = REPO / "data/external/omie_reference/lista_unidades.csv"

H = 50.0   # kernel-band half-width in EUR/MWh
EPS = 0.01

# Restrict to reform-window (2024-06-14 onward), to keep runtime + parquet sizes manageable
START = "2024-06-14"
END   = "2026-01-31"


def map_tech(s):
    if not isinstance(s, str): return "Other"
    t = s.lower()
    if "ciclo combinado"   in t: return "CCGT"
    if "nuclear"           in t: return "Nuclear"
    if "carbón" in t or "carbon" in t or "hulla" in t: return "Coal"
    if "fuel" in t or t.strip() == "gas" or "gas natural" in t or "turbina de gas" in t: return "Fuel/Gas"
    if "bombeo mixto" in t or "consumo bombeo" in t: return "Pump_load"
    if "bombeo puro" in t or ("bombeo" in t and "turb" in t): return "Hydro_pump"
    if "hidráulica generación" in t: return "Hydro"
    if "re mercado hidráulica" in t: return "Hydro_RES"
    if "re mercado eólica" in t: return "Wind"
    if "re mercado solar fotovolt" in t: return "Solar PV"
    if "re mercado solar térmica" in t: return "Solar Thermal"
    if "re mercado térmica no renovab" in t: return "Cogen"
    if "comercializador" in t: return "Retailer"
    return "Other"


def map_firm(unit_owner):
    """Coarse firm grouping; matches existing analyses."""
    if not isinstance(unit_owner, str): return "Other"
    o = unit_owner.lower()
    if "iberdrola" in o: return "IB"
    if "endesa" in o: return "GE"
    if "naturgy" in o or "gas natural" in o: return "GN"
    if "edp" in o or "energias de portugal" in o or "hidroel" in o: return "HC"
    if "repsol" in o: return "REP"
    if "acciona" in o: return "ACC"
    if "axpo" in o: return "AXPO"
    if "gesternova" in o or "gester" in o: return "GST"
    if "nexus" in o or "ree" in o: return "OTH"
    return "OTH"


def build_units_df():
    raw = pd.read_csv(UNITS)
    raw["tech_group"] = raw["technology"].apply(map_tech)
    raw['firm'] = raw['owner_agent'].apply(map_firm)
    return raw[["unit_code", "tech_group", "firm"]].drop_duplicates("unit_code")


def build_DA():
    print("=== Building DA per-cell bid-shape metrics ===")
    con = duckdb.connect()
    units = build_units_df()
    con.register("uft", units)

    sql = f"""
    WITH cab AS (
        SELECT date::DATE AS d, offer_code, version, unit_code,
               ROW_NUMBER() OVER (PARTITION BY date::DATE, offer_code, unit_code ORDER BY version DESC) AS rn
        FROM '{CAB}'
        WHERE buy_sell='V' AND date::DATE BETWEEN '{START}' AND '{END}'
    ),
    cab_l AS (SELECT * FROM cab WHERE rn=1),
    det AS (
        SELECT date::DATE AS d, offer_code, version, period,
               price_eur_mwh AS p, quantity_mw AS q, mtu_minutes
        FROM '{DET}'
        WHERE date::DATE BETWEEN '{START}' AND '{END}'
          AND quantity_mw > 0
    ),
    prices AS (
        SELECT date::DATE AS d, period, price_es_eur_mwh AS p_clear, mtu_minutes AS mtu_p
        FROM '{MP_DA}'
        WHERE date::DATE BETWEEN '{START}' AND '{END}' AND price_es_eur_mwh IS NOT NULL
    ),
    joined AS (
        SELECT pr.d, pr.period, c.unit_code, dv.p, dv.q, pr.p_clear,
               (dv.p BETWEEN pr.p_clear - {H} AND pr.p_clear + {H}) AS in_band,
               (dv.p BETWEEN pr.p_clear - {EPS} AND pr.p_clear + {EPS}) AS at_mcp,
               (dv.p < pr.p_clear) AS below,
               COALESCE(pr.mtu_p, dv.mtu_minutes) AS mtu_minutes
        FROM det dv
        JOIN cab_l c USING (d, offer_code, version)
        JOIN prices pr USING (d, period)
    )
    SELECT d AS date, period, unit_code, p_clear,
           CAST(mtu_minutes AS INT) AS mtu_minutes,
           SUM(CASE WHEN in_band THEN q ELSE 0 END) AS mw_in_band,
           SUM(CASE WHEN at_mcp  THEN q ELSE 0 END) AS mw_at_mcp,
           SUM(CASE WHEN below   THEN q ELSE 0 END) AS mw_below,
           SUM(q) AS mw_total,
           COUNT(DISTINCT CASE WHEN in_band THEN p END) AS n_tranches_in_band,
           SUM(CASE WHEN in_band THEN q*p ELSE 0 END) / NULLIF(SUM(CASE WHEN in_band THEN q ELSE 0 END), 0) AS vw_price_in_band
    FROM joined
    GROUP BY 1, 2, 3, 4, 5
    """
    df = con.execute(sql).df()
    print(f"  DA per-cell rows: {len(df):,}")

    # Augment with tech_group, firm, year_month, hour, quarter
    df = df.merge(units, on="unit_code", how="left")
    df["tech_group"] = df["tech_group"].fillna("Other")
    df["firm"] = df["firm"].fillna("OTH")
    df["year_month"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m")
    df["hour"] = (df.apply(lambda r: int((r["period"] - 1) / 4) if r["mtu_minutes"] == 15
                                       else int(r["period"] - 1), axis=1))
    df["quarter"] = (df.apply(lambda r: int(((r["period"] - 1) % 4) + 1) if r["mtu_minutes"] == 15
                                            else 1, axis=1))
    df["frac_single_block"] = (df["n_tranches_in_band"] == 1).astype(int)
    df.to_parquet(OUT / "DA_per_cell.parquet", index=False)
    print(f"  wrote {OUT / 'DA_per_cell.parquet'}")
    return df


def build_IDA():
    print("=== Building IDA per-cell bid-shape metrics ===")
    con = duckdb.connect()
    units = build_units_df()
    con.register("uft", units)

    sql = f"""
    WITH icab AS (
        SELECT date::DATE AS d, session_number AS session, offer_code, version, unit_code,
               ROW_NUMBER() OVER (PARTITION BY date::DATE, session, offer_code, unit_code ORDER BY version DESC) AS rn
        FROM '{ICAB}'
        WHERE buy_sell='V' AND date::DATE BETWEEN '{START}' AND '{END}'
    ),
    icab_l AS (SELECT * FROM icab WHERE rn=1),
    idet AS (
        SELECT date::DATE AS d, session_number AS session, offer_code, version, period,
               price_eur_mwh AS p, quantity_mw AS q, mtu_minutes
        FROM '{IDET}'
        WHERE date::DATE BETWEEN '{START}' AND '{END}'
          AND quantity_mw > 0
    ),
    prices AS (
        SELECT date::DATE AS d, session_number AS session, period, price_es_eur_mwh AS p_clear, mtu_minutes AS mtu_p
        FROM '{MP_IDA}'
        WHERE date::DATE BETWEEN '{START}' AND '{END}' AND price_es_eur_mwh IS NOT NULL
    ),
    joined AS (
        SELECT pr.d, pr.session, pr.period, c.unit_code, dv.p, dv.q, pr.p_clear,
               (dv.p BETWEEN pr.p_clear - {H} AND pr.p_clear + {H}) AS in_band,
               (dv.p BETWEEN pr.p_clear - {EPS} AND pr.p_clear + {EPS}) AS at_mcp,
               (dv.p < pr.p_clear) AS below,
               COALESCE(pr.mtu_p, dv.mtu_minutes) AS mtu_minutes
        FROM idet dv
        JOIN icab_l c USING (d, session, offer_code, version)
        JOIN prices pr USING (d, session, period)
    )
    SELECT d AS date, session, period, unit_code, p_clear,
           CAST(mtu_minutes AS INT) AS mtu_minutes,
           SUM(CASE WHEN in_band THEN q ELSE 0 END) AS mw_in_band,
           SUM(CASE WHEN at_mcp  THEN q ELSE 0 END) AS mw_at_mcp,
           SUM(CASE WHEN below   THEN q ELSE 0 END) AS mw_below,
           SUM(q) AS mw_total,
           COUNT(DISTINCT CASE WHEN in_band THEN p END) AS n_tranches_in_band,
           SUM(CASE WHEN in_band THEN q*p ELSE 0 END) / NULLIF(SUM(CASE WHEN in_band THEN q ELSE 0 END), 0) AS vw_price_in_band
    FROM joined
    GROUP BY 1, 2, 3, 4, 5, 6
    """
    df = con.execute(sql).df()
    print(f"  IDA per-cell rows: {len(df):,}")

    df = df.merge(units, on="unit_code", how="left")
    df["tech_group"] = df["tech_group"].fillna("Other")
    df["firm"] = df["firm"].fillna("OTH")
    df["year_month"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m")
    df["hour"] = (df.apply(lambda r: int((r["period"] - 1) / 4) if r["mtu_minutes"] == 15
                                       else int(r["period"] - 1), axis=1))
    df["quarter"] = (df.apply(lambda r: int(((r["period"] - 1) % 4) + 1) if r["mtu_minutes"] == 15
                                            else 1, axis=1))
    df["frac_single_block"] = (df["n_tranches_in_band"] == 1).astype(int)
    df.to_parquet(OUT / "IDA_per_cell.parquet", index=False)
    print(f"  wrote {OUT / 'IDA_per_cell.parquet'}")
    return df


def aggregate(df, label):
    """Aggregate to (tech, firm, year_month, hour-class, quarter): mean of metrics, count of cells."""
    df = df.copy()
    def hc(h):
        if h in (5, 6, 7, 8, 16, 17, 18, 19, 20, 21, 22): return "Critical"
        if h in (1, 2, 3): return "Flat"
        if h in (11, 12, 13, 14): return "Midday"
        return "Dropped"
    df["hour_class"] = df["hour"].apply(hc)
    agg = df.groupby(["tech_group", "firm", "year_month", "hour_class", "quarter", "mtu_minutes"]).agg(
        n_cells=("mw_in_band", "size"),
        mw_in_band_mean=("mw_in_band", "mean"),
        mw_at_mcp_mean=("mw_at_mcp", "mean"),
        mw_below_mean=("mw_below", "mean"),
        n_tranches_mean=("n_tranches_in_band", "mean"),
        frac_single_block_mean=("frac_single_block", "mean"),
        vw_price_in_band_mean=("vw_price_in_band", "mean"),
        p_clear_mean=("p_clear", "mean"),
    ).reset_index()
    agg.to_csv(OUT / f"{label}_agg_firm_tech.csv", index=False)
    print(f"  wrote {OUT / f'{label}_agg_firm_tech.csv'} ({len(agg):,} rows)")
    return agg


def main():
    import sys
    # Allow re-running just the IDA half
    if len(sys.argv) > 1 and sys.argv[1] == "--ida-only":
        ida_df = build_IDA()
        aggregate(ida_df, "IDA")
        return
    da_df = build_DA()
    aggregate(da_df, "DA")
    ida_df = build_IDA()
    aggregate(ida_df, "IDA")


if __name__ == "__main__":
    main()
