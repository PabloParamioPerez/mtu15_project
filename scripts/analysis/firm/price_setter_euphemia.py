# STATUS: ALIVE
# LAST-AUDIT: 2026-05-17
# FEEDS: thesis/provisional/bidding_internal.tex (§Who price-sets)
# CLAIM: Price-setter identification for the Spanish DA market using the
#        at-the-money partial-acceptance rule consistent with how EUPHEMIA
#        treats stepwise orders. For each (date, period), a unit is a
#        price-setter iff its at-MCP step was *partially* accepted ---
#        cleared MWh strictly between the cumulative strictly-below-MCP
#        bid quantity and the cumulative at-or-below-MCP bid quantity.
#
# WHY: Per the EUPHEMIA description (docs/omie/euphemia_functioning_1812.pdf),
#      stepwise orders are fully accepted in-the-money, fully rejected
#      out-of-the-money, and CURTAILED (partial acceptance) at-the-money.
#      The naive rule (marginal_tech_by_hour.py) flags every unit whose
#      top accepted step is AT MCP, pooling three different cases:
#        (a) curtailed at MCP -> actual price-setter (we want this)
#        (b) fully accepted at MCP -> inframarginal, did NOT set the price
#        (c) curtailed to zero -> the strictly-below steps cleared, this
#            unit's at-MCP step was not the price-discovering step
#      The partial-acceptance test isolates (a) and excludes (b) and (c).
#
# NOTE on paradoxical rejection: this is a property of BLOCK orders, not
# stepwise orders. Block orders are a different product handled by
# EUPHEMIA's branch-and-cut over fill-or-kill blocks; OMIE step bids in
# det_all.parquet are not subject to it. We therefore do NOT need a
# paradoxical-rejection filter --- the partial-acceptance test addresses
# a different, more pervasive issue (case b above).
#
# Data sources (strict):
#  * Clearing price: marginalpdbc_all.parquet (PDBC clearing, no bilaterals)
#  * Bid stack:     det_all.parquet + cab_all.parquet (sell side, latest version)
#  * Cleared MWh:   pdbc_all.parquet  (NOT pdbf, NOT phf:
#                                       pdbf includes bilateral executions,
#                                       phf includes REE post-clearing redispatch.
#                                       We want the auction's own clearing only.)

from __future__ import annotations

from pathlib import Path
import duckdb
import pandas as pd

REPO  = Path(__file__).resolve().parents[3]
DET   = REPO / "data" / "processed" / "omie" / "mercado_diario" / "ofertas"   / "det_all.parquet"
CAB   = REPO / "data" / "processed" / "omie" / "mercado_diario" / "ofertas"   / "cab_all.parquet"
PDBC  = REPO / "data" / "processed" / "omie" / "mercado_diario" / "programas" / "pdbc_all.parquet"
MPDBC = REPO / "data" / "processed" / "omie" / "mercado_diario" / "precios"   / "marginalpdbc_all.parquet"
UNITS = REPO / "data" / "external"  / "omie_reference" / "lista_unidades.csv"

OUT   = REPO / "results" / "regressions" / "firm" / "marginal_tech"
OUT.mkdir(parents=True, exist_ok=True)

EPS_PRICE = 0.01   # EUR/MWh — "p_bid exactly equals p_clear" tolerance
EPS_QTY   = 1e-3   # MW    — slack on the q_below / q_at_clear inequalities

CRIT = (5, 6, 7, 8, 16, 17, 18, 19, 20, 21, 22)
FLAT = (1, 2, 3)
MID  = (11, 12, 13, 14)


def map_tech(tech_str: str) -> str:
    if not isinstance(tech_str, str): return "Other"
    t = tech_str.lower()
    if "ciclo combinado"   in t: return "CCGT"
    if "nuclear"           in t: return "Nuclear"
    if "carbón" in t or "carbon" in t or "hulla" in t: return "Coal"
    if "fuel"              in t: return "Fuel/Gas"
    if t.strip() == "gas" or "gas natural" in t or "turbina de gas" in t: return "Fuel/Gas"
    if "bombeo mixto" in t or "consumo bombeo" in t or "consumo de bombeo" in t: return "Pump_load"
    if "bombeo puro" in t or ("bombeo" in t and ("turbin" in t or "hidráulica" in t or "hidraulica" in t)): return "Hydro_pump"
    if "hidráulica generación" in t or "hidraulica generacion" in t: return "Hydro"
    if "re mercado hidráulica" in t or "re mercado hidraulica" in t: return "Hydro_RES"
    if "re mercado eólica" in t or "re mercado eolica" in t: return "Wind"
    if "re mercado solar fotovolt" in t: return "Solar PV"
    if "re mercado solar térmica" in t or "re mercado solar termica" in t: return "Solar Thermal"
    if "re mercado térmica renovable" in t or "re mercado termica renovable" in t: return "Biomass"
    if "re mercado térmica no renovab" in t or "re mercado termica no renovab" in t: return "Cogen"
    if "híbrida" in t or "hibrida" in t or "hibridación" in t or "hibridacion" in t: return "Hybrid_RES"
    if "almacenamiento" in t: return "Storage"
    if "import" in t or "contrato internacional" in t: return "Import"
    return "Other"


def price_setter_table(window_start: str, window_end: str) -> pd.DataFrame:
    """Returns one row per (date, period) with the Euphemia-aware price-setter
    unit, the unit's tech, its at-clearing bid quantity, its strictly-below
    cumulative quantity, its PDBC cleared MWh, and a flag indicating whether
    the at-clearing tranche was partially accepted (= true price-setter)."""
    raw = pd.read_csv(UNITS)
    raw["tech_group"] = raw["technology"].apply(map_tech)
    units_all = raw[["unit_code", "tech_group"]].drop_duplicates("unit_code")

    con = duckdb.connect()
    con.execute("PRAGMA threads=4")
    con.execute("SET memory_limit='12GB'")
    con.register("uft", units_all)

    sql = f"""
    WITH prices AS (
        SELECT date::DATE AS d, period, price_es_eur_mwh AS p_clear
        FROM   '{MPDBC}'
        WHERE  date::DATE BETWEEN DATE '{window_start}' AND DATE '{window_end}'
          AND  price_es_eur_mwh IS NOT NULL
    ),
    cab_v AS (
        SELECT date::DATE AS d, offer_code, version, unit_code,
               ROW_NUMBER() OVER (PARTITION BY date::DATE, offer_code, unit_code
                                  ORDER BY version DESC) AS rn
        FROM   '{CAB}'
        WHERE  buy_sell = 'V'
          AND  date::DATE BETWEEN DATE '{window_start}' AND DATE '{window_end}'
    ),
    cab_l AS (SELECT * FROM cab_v WHERE rn = 1),
    det_v AS (
        SELECT date::DATE AS d, offer_code, version, period,
               price_eur_mwh AS p_bid, quantity_mw AS q_bid
        FROM   '{DET}'
        WHERE  date::DATE BETWEEN DATE '{window_start}' AND DATE '{window_end}'
          AND  price_eur_mwh IS NOT NULL AND quantity_mw > 0
    ),
    -- Unit-level sell bids, joined to clearing price for the period
    det_unit AS (
        SELECT dv.d, dv.period, c.unit_code, dv.p_bid, dv.q_bid, pr.p_clear
        FROM   det_v dv
        JOIN   cab_l c USING (d, offer_code, version)
        JOIN   prices pr USING (d, period)
    ),
    -- Per (d, period, unit): cumulative mass strictly below clearing,
    -- mass exactly at clearing (within EPS_PRICE), and the top accepted price.
    unit_curve AS (
        SELECT d, period, unit_code,
               SUM(q_bid) FILTER (WHERE p_bid < p_clear - {EPS_PRICE}) AS q_below,
               SUM(q_bid) FILTER (WHERE ABS(p_bid - p_clear) <= {EPS_PRICE}) AS q_at,
               MAX(p_bid) FILTER (WHERE p_bid <= p_clear + {EPS_PRICE})    AS p_top_accepted
        FROM   det_unit
        GROUP  BY d, period, unit_code
    ),
    -- Per (d, period, unit): PDBC cleared MWh (sell side, offer_type=1).
    unit_assigned AS (
        SELECT date::DATE AS d, period, unit_code,
               SUM(assigned_power_mw) AS q_assigned
        FROM   '{PDBC}'
        WHERE  date::DATE BETWEEN DATE '{window_start}' AND DATE '{window_end}'
          AND  offer_type = 1
        GROUP  BY 1, 2, 3
    ),
    joined AS (
        SELECT uc.d, uc.period, uc.unit_code, uc.p_top_accepted,
               COALESCE(uc.q_below, 0) AS q_below,
               COALESCE(uc.q_at,    0) AS q_at,
               COALESCE(ua.q_assigned, 0) AS q_assigned,
               pr.p_clear
        FROM   unit_curve uc
        JOIN   prices pr USING (d, period)
        LEFT JOIN unit_assigned ua USING (d, period, unit_code)
    ),
    -- At-the-money partial-acceptance rule: the unit's top accepted bid is
    -- at MCP AND the PDBC cleared MWh sits strictly between q_below and
    -- (q_below + q_at). I.e., the at-MCP step was CURTAILED, which is the
    -- definition of price-setting for stepwise orders.
    -- Excluded: q_assigned <= q_below (at-MCP step curtailed to zero by
    -- the algorithm; the strictly-below steps cleared but the at-MCP step
    -- was not the price-discovering one) and q_assigned >= q_below + q_at
    -- (at-MCP step fully accepted; the unit is inframarginal at MCP, the
    -- marginal MWh that set the price was somewhere else in the stack).
    setters AS (
        SELECT d, period, unit_code, q_below, q_at, q_assigned, p_top_accepted, p_clear
        FROM   joined
        WHERE  p_top_accepted IS NOT NULL
          AND  ABS(p_top_accepted - p_clear) <= {EPS_PRICE}
          AND  q_at > {EPS_QTY}
          AND  q_assigned > q_below + {EPS_QTY}
          AND  q_assigned < q_below + q_at - {EPS_QTY}
    )
    SELECT s.d AS date, s.period, s.unit_code, u.tech_group,
           s.q_below, s.q_at, s.q_assigned,
           s.p_clear, s.p_top_accepted
    FROM   setters s
    LEFT JOIN uft u USING (unit_code)
    """
    return con.execute(sql).df()


DA_MTU15_CUTOFF = "2025-10-01"  # First date with quarter-hourly DA (period 1..96)


def matched_periods(window_start: str, window_end: str) -> pd.DataFrame:
    """Total (d, period) cells in the window, with the date-driven period
    scheme used to map period -> clock-hour (1..24 pre-MTU15-DA, 1..96 post).
    Uses MPDBC's mtu_minutes column to be robust to DST and the reform date."""
    con = duckdb.connect()
    sql = f"""
    SELECT date::DATE AS d,
           CAST(period AS INT) AS period,
           CAST(CASE WHEN mtu_minutes = 60
                     THEN CAST(period AS INT) - 1
                     ELSE CAST(FLOOR((CAST(period AS INT) - 1) / 4.0) AS INT)
                END AS INT) AS hour
    FROM   '{MPDBC}'
    WHERE  date::DATE BETWEEN DATE '{window_start}' AND DATE '{window_end}'
      AND  price_es_eur_mwh IS NOT NULL
    """
    return con.execute(sql).df()


def hour_class(h: int) -> str:
    if h in CRIT: return "Critical"
    if h in FLAT: return "Flat"
    if h in MID:  return "Midday"
    return "Dropped"


def summarise(setters: pd.DataFrame, periods: pd.DataFrame, label: str) -> pd.DataFrame:
    """For each (period) compute the MW-weighted price-setter share of each
    tech (weight = the at-clearing partially-accepted quantity = q_at), then
    average over periods within each hour-class. Hour mapping comes from
    `periods` to ensure denominator includes price-setter-unmatched periods."""
    periods_r = periods.rename(columns={"d": "date"})
    s = setters.merge(periods_r, on=["date", "period"], how="left")
    s["hour_class"] = s["hour"].apply(hour_class)
    # MW-weighted: weight = q_at (the at-clearing partially-accepted MWh)
    grp = s.groupby(["hour_class", "tech_group"], dropna=False)["q_at"].sum().unstack(fill_value=0)
    # Normalize each hour-class row to sum to 1
    shares = grp.div(grp.sum(axis=1), axis=0)
    # Diagnostic: matched fraction per hour-class
    matched_per_class = periods_r.assign(hour_class=periods_r["hour"].apply(hour_class))\
                              .merge(s[["date", "period"]].drop_duplicates().assign(matched=1),
                                     on=["date", "period"], how="left")
    matched_per_class["matched"] = matched_per_class["matched"].fillna(0)
    diag = matched_per_class.groupby("hour_class")["matched"].agg(["mean", "count"])
    diag.columns = [f"matched_share_{label}", f"n_periods_{label}"]
    return shares.assign(window=label), diag


def main():
    # Regime windows for the project's reform calendar:
    #  - 3-sess: 2024-06-14 (IDA 6->3 sessions) to 2024-11-30 (pre-ISP15)
    #  - ISP15-win: 2024-12-01 (ISP15) to 2025-03-18 (pre-MTU15-IDA)
    #  - DA60/ID15 pre-blackout: 2025-03-19 (MTU15-IDA) to 2025-04-27
    #  - DA60/ID15 post-blackout: 2025-04-28 (blackout) to 2025-09-30 (pre-MTU15-DA)
    #  - DA15/ID15: 2025-10-01 (MTU15-DA) to 2025-12-31
    windows = {
        "3-sess (Jun-Nov 2024)":           ("2024-06-14", "2024-11-30"),
        "ISP15-win (Dec24-Mar25)":         ("2024-12-01", "2025-03-18"),
        "DA60/ID15 pre-blk (Mar19-Apr27)": ("2025-03-19", "2025-04-27"),
        "DA60/ID15 post-blk (Apr28-Sep)":  ("2025-04-28", "2025-09-30"),
        "DA15/ID15 (Oct-Dec 2025)":        ("2025-10-01", "2025-12-31"),
    }
    big = []
    diags = []
    for label, (a, b) in windows.items():
        print(f"\n=== {label}: {a} -> {b} ===")
        setters = price_setter_table(a, b)
        periods = matched_periods(a, b)
        print(f"  matched price-setter rows: {len(setters):,}")
        print(f"  total (d, period) cells:   {len(periods):,}")
        shares, diag = summarise(setters, periods, label)
        big.append(shares)
        diags.append(diag)
    out_shares = pd.concat(big).reset_index()
    out_shares.to_csv(OUT / "price_setter_euphemia_shares.csv", index=False)
    out_diag = pd.concat(diags, axis=1)
    out_diag.to_csv(OUT / "price_setter_euphemia_matched.csv")
    print("\nPer-hour-class price-setter shares (Euphemia-aware, MW-weighted):")
    for label in windows:
        d = out_shares[out_shares["window"] == label].set_index("hour_class").drop(columns="window")
        d = d.loc[["Critical", "Flat", "Midday", "Dropped"]].fillna(0)
        keep = d.columns[d.max(axis=0) > 0.005]
        top = d[keep].T.sort_values("Critical", ascending=False)
        print(f"\n[{label}] (%)")
        print((top * 100).round(1).to_string())
    print("\nMatched-period diagnostics:")
    print(out_diag)


if __name__ == "__main__":
    main()
