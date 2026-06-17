# STATUS: ALIVE
# LAST-AUDIT: 2026-05-17
# FEEDS: exploratory; outputs at results/regressions/firm/marginal_tech/deep_dive/
# CLAIM: Comprehensive multi-dimensional exploration of price-setter identity
#        across regimes, hour-classes, hour-of-day, technologies, firms,
#        units, weightings, and bid-stack shapes. Uses the at-the-money
#        partial-acceptance rule (sell side and buy side) consistent with
#        EUPHEMIA stepwise-order handling.
#
# Outputs (CSV) under deep_dive/:
#   01_firm_shares.csv              — by regime × hour-class × firm × tech
#   02_top_units_count.csv          — top units by frequency of being price-setter
#   02_top_units_qat.csv            — top units by sum of at-MCP MW
#   03_bid_shape.csv                — distribution of q_below=0 share, partial-acc frac
#   04_weightings.csv               — q_at vs q_marginal vs cell-count, by tech
#   05_hour_of_day.csv              — 24-hour profile, by tech, per regime
#   06_buy_side.csv                 — buy-side price-setter shares by tech
#   07_indeterminacy.csv            — periods with no at-MCP step on either side
#   08_scarcity_quintiles.csv       — share by MCP quintile and tech
#   09_concentration.csv            — HHI and top-K firm share per regime
#   summary.md                       — narrative summary

from __future__ import annotations

from pathlib import Path
import duckdb
import pandas as pd
import numpy as np

REPO  = Path(__file__).resolve().parents[3]
DET   = REPO / "data" / "processed" / "omie" / "mercado_diario" / "ofertas"   / "det_all.parquet"
CAB   = REPO / "data" / "processed" / "omie" / "mercado_diario" / "ofertas"   / "cab_all.parquet"
PDBC  = REPO / "data" / "processed" / "omie" / "mercado_diario" / "programas" / "pdbc_all.parquet"
MPDBC = REPO / "data" / "processed" / "omie" / "mercado_diario" / "precios"   / "marginalpdbc_all.parquet"
UNITS = REPO / "data" / "external"  / "omie_reference" / "lista_unidades.csv"

OUT = REPO / "results" / "regressions" / "firm" / "marginal_tech" / "deep_dive"
OUT.mkdir(parents=True, exist_ok=True)

EPS_PRICE = 0.01
EPS_QTY   = 1e-3

CRIT = (5, 6, 7, 8, 16, 17, 18, 19, 20, 21, 22)
FLAT = (1, 2, 3)
MID  = (11, 12, 13, 14)

WINDOWS = {
    "3-sess (Jun-Nov 2024)":           ("2024-06-14", "2024-11-30"),
    "ISP15-win (Dec24-Mar25)":         ("2024-12-01", "2025-03-18"),
    "DA60/ID15 pre-blk (Mar19-Apr27)": ("2025-03-19", "2025-04-27"),
    "DA60/ID15 post-blk (Apr28-Sep)":  ("2025-04-28", "2025-09-30"),
    "DA15/ID15 (Oct-Dec 2025)":        ("2025-10-01", "2025-12-31"),
}


def map_tech(s: str) -> str:
    if not isinstance(s, str): return "Other"
    t = s.lower()
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
    if "comercializador" in t or "compras comercializaci" in t: return "Retailer"
    if "consumo directo" in t or "consumidor directo" in t: return "Direct_consumer"
    if "import" in t or "contrato internacional" in t: return "Import"
    if "portfolio" in t or "porfolio" in t: return "Portfolio"
    return "Other"


def map_firm(owner: str) -> str:
    if not isinstance(owner, str): return "OTHER"
    s = owner.upper()
    if "IBERDROLA" in s: return "IB"
    if "ENDESA" in s: return "GE"
    if "NATURGY" in s or "GAS NATURAL" in s: return "GN"
    if "HIDROCANTABRICO" in s or "HC ENER" in s or s.startswith("EDP ") or " EDP " in s: return "HC"
    if "REPSOL" in s: return "REP"
    if "ACCIONA" in s: return "ACC"
    if "RED ELECT" in s: return "REE"
    return "OTHER"


def load_units() -> pd.DataFrame:
    u = pd.read_csv(UNITS)
    u["tech_group"] = u["technology"].apply(map_tech)
    u["firm"]       = u["owner_agent"].apply(map_firm)
    return u[["unit_code", "tech_group", "firm", "zone"]].drop_duplicates("unit_code")


def get_price_setters(con, start: str, end: str, side: str = "V") -> pd.DataFrame:
    """At-the-money partial-acceptance price-setters for one side.

    side='V' (sell): partial acceptance iff
        q_below_strict < q_assigned_abs < q_below_strict + q_at
      where q_below_strict = sum of sell bids at p_bid < p_clear,
            q_at           = sum of sell bids at p_bid = p_clear,
            q_assigned_abs = abs(PDBC assigned_power_mw)  (positive for sells)

    side='C' (buy): in-the-money for buyers is p_bid > p_clear, so
        q_above_strict < q_assigned_abs < q_above_strict + q_at
      where q_above_strict = sum of buy bids at p_bid > p_clear,
            q_at           = sum of buy bids at p_bid = p_clear,
            q_assigned_abs = abs(PDBC assigned_power_mw).
    """
    if side == "V":
        below_cond = f"p_bid < p_clear - {EPS_PRICE}"
    else:
        below_cond = f"p_bid > p_clear + {EPS_PRICE}"

    sql = f"""
    WITH prices AS (
        SELECT date::DATE AS d, period, price_es_eur_mwh AS p_clear, mtu_minutes
        FROM   '{MPDBC}'
        WHERE  date::DATE BETWEEN DATE '{start}' AND DATE '{end}'
          AND  price_es_eur_mwh IS NOT NULL
    ),
    cab_s AS (
        SELECT date::DATE AS d, offer_code, version, unit_code,
               ROW_NUMBER() OVER (PARTITION BY date::DATE, offer_code, unit_code
                                  ORDER BY version DESC) AS rn
        FROM   '{CAB}'
        WHERE  buy_sell = '{side}'
          AND  date::DATE BETWEEN DATE '{start}' AND DATE '{end}'
    ),
    cab_l AS (SELECT * FROM cab_s WHERE rn = 1),
    det_v AS (
        SELECT date::DATE AS d, offer_code, version, period,
               price_eur_mwh AS p_bid, quantity_mw AS q_bid
        FROM   '{DET}'
        WHERE  date::DATE BETWEEN DATE '{start}' AND DATE '{end}'
          AND  price_eur_mwh IS NOT NULL AND quantity_mw > 0
    ),
    det_unit AS (
        SELECT dv.d, dv.period, c.unit_code, dv.p_bid, dv.q_bid, pr.p_clear, pr.mtu_minutes
        FROM   det_v dv
        JOIN   cab_l c USING (d, offer_code, version)
        JOIN   prices pr USING (d, period)
    ),
    unit_curve AS (
        SELECT d, period, unit_code, MAX(mtu_minutes) AS mtu_minutes,
               MAX(p_clear) AS p_clear,
               COALESCE(SUM(q_bid) FILTER (WHERE {below_cond}), 0)                AS q_below,
               COALESCE(SUM(q_bid) FILTER (WHERE ABS(p_bid - p_clear) <= {EPS_PRICE}), 0) AS q_at,
               MAX(p_bid) FILTER (WHERE p_bid <= p_clear + {EPS_PRICE} AND p_bid >= p_clear - {EPS_PRICE}) AS p_at_max
        FROM   det_unit
        GROUP  BY d, period, unit_code
    ),
    unit_assigned AS (
        SELECT date::DATE AS d, period, unit_code,
               SUM(ABS(assigned_power_mw)) AS q_assigned
        FROM   '{PDBC}'
        WHERE  date::DATE BETWEEN DATE '{start}' AND DATE '{end}'
          AND  offer_type = 1
        GROUP  BY 1, 2, 3
    )
    SELECT uc.d AS date, uc.period, uc.unit_code, uc.mtu_minutes, uc.p_clear,
           uc.q_below, uc.q_at,
           COALESCE(ua.q_assigned, 0) AS q_assigned,
           (COALESCE(ua.q_assigned, 0) - uc.q_below) AS q_marginal,
           CAST(CASE WHEN uc.mtu_minutes = 60
                     THEN uc.period - 1
                     ELSE CAST(FLOOR((uc.period - 1) / 4.0) AS INT)
                END AS INT) AS hour
    FROM   unit_curve uc
    LEFT JOIN unit_assigned ua USING (d, period, unit_code)
    WHERE  uc.q_at > {EPS_QTY}
      AND  COALESCE(ua.q_assigned, 0) > uc.q_below + {EPS_QTY}
      AND  COALESCE(ua.q_assigned, 0) < uc.q_below + uc.q_at - {EPS_QTY}
    """
    return con.execute(sql).df()


def hour_class(h: int) -> str:
    if h in CRIT: return "Critical"
    if h in FLAT: return "Flat"
    if h in MID:  return "Midday"
    return "Dropped"


def annotate(ps: pd.DataFrame, units: pd.DataFrame) -> pd.DataFrame:
    """Add tech_group, firm, hour_class to a price-setter dataframe."""
    ps = ps.merge(units, on="unit_code", how="left")
    ps["hour_class"] = ps["hour"].apply(hour_class)
    ps["dow"] = pd.to_datetime(ps["date"]).dt.dayofweek  # 0=Mon
    ps["is_weekend"] = ps["dow"].isin([5, 6])
    return ps


# ---------------- Analyses ----------------

def firm_shares(ps_by_regime: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Per-firm × tech (CCGT focus) shares by regime × hour-class, MW-weighted by q_at."""
    rows = []
    for regime, ps in ps_by_regime.items():
        # CCGT only
        d = ps[ps["tech_group"] == "CCGT"].copy()
        for hc in ["Critical", "Flat", "Midday", "Dropped"]:
            sub = d[d["hour_class"] == hc]
            total = sub["q_at"].sum()
            if total == 0:
                continue
            for firm, g in sub.groupby("firm"):
                rows.append({
                    "regime": regime, "hour_class": hc, "firm": firm,
                    "share_pct_q_at": g["q_at"].sum() / total * 100,
                    "share_pct_q_marg": g["q_marginal"].sum() / max(sub["q_marginal"].sum(), 1e-9) * 100,
                    "n_obs": len(g),
                    "sum_q_at_mw": g["q_at"].sum(),
                })
    return pd.DataFrame(rows)


def top_units(ps_by_regime: dict[str, pd.DataFrame], by: str = "n_obs", top_n: int = 15) -> pd.DataFrame:
    """Top N price-setting units by count or by q_at sum, per regime."""
    rows = []
    for regime, ps in ps_by_regime.items():
        g = ps.groupby(["unit_code", "tech_group", "firm"]).agg(
            n_obs=("date", "count"),
            sum_q_at_mw=("q_at", "sum"),
            sum_q_marg_mw=("q_marginal", "sum"),
            mean_q_at_mw=("q_at", "mean"),
            mean_partial_frac=("q_at", lambda s: (
                (ps.loc[s.index, "q_marginal"] / s).mean()
            )),
        ).reset_index()
        if by == "n_obs":
            g = g.sort_values("n_obs", ascending=False).head(top_n)
        else:
            g = g.sort_values("sum_q_at_mw", ascending=False).head(top_n)
        g.insert(0, "regime", regime)
        rows.append(g)
    return pd.concat(rows, ignore_index=True)


def bid_shape(ps_by_regime: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Distribution of bid-stack shape for price-setters: q_below=0 fraction,
    partial-acceptance fraction (q_marginal / q_at), etc."""
    rows = []
    for regime, ps in ps_by_regime.items():
        for tech in sorted(ps["tech_group"].dropna().unique()):
            sub = ps[ps["tech_group"] == tech]
            if len(sub) == 0:
                continue
            frac = sub["q_marginal"] / sub["q_at"]
            rows.append({
                "regime": regime, "tech": tech,
                "n_obs": len(sub),
                "share_q_below_zero": (sub["q_below"] <= EPS_QTY).mean(),
                "mean_q_at_mw": sub["q_at"].mean(),
                "median_q_at_mw": sub["q_at"].median(),
                "mean_partial_frac": frac.mean(),
                "median_partial_frac": frac.median(),
                "n_distinct_units": sub["unit_code"].nunique(),
            })
    return pd.DataFrame(rows)


def weightings(ps_by_regime: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Three weightings of price-setter share: q_at, q_marginal, cell-count."""
    rows = []
    for regime, ps in ps_by_regime.items():
        for hc in ["Critical", "Flat", "Midday", "Dropped"]:
            sub = ps[ps["hour_class"] == hc]
            if len(sub) == 0:
                continue
            tot_qat   = sub["q_at"].sum()
            tot_qmarg = sub["q_marginal"].sum()
            tot_cells = len(sub)
            for tech, g in sub.groupby("tech_group", dropna=False):
                rows.append({
                    "regime": regime, "hour_class": hc, "tech": tech if pd.notna(tech) else "_unmapped_",
                    "share_pct_q_at":   g["q_at"].sum() / tot_qat * 100 if tot_qat > 0 else 0,
                    "share_pct_q_marg": g["q_marginal"].sum() / tot_qmarg * 100 if tot_qmarg > 0 else 0,
                    "share_pct_cells":  len(g) / tot_cells * 100,
                    "n_obs": len(g),
                })
    return pd.DataFrame(rows)


def hour_of_day(ps_by_regime: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """24-hour profile of price-setter tech mix per regime, MW-weighted by q_at."""
    rows = []
    for regime, ps in ps_by_regime.items():
        for hr in range(24):
            sub = ps[ps["hour"] == hr]
            if len(sub) == 0:
                continue
            tot = sub["q_at"].sum()
            for tech, g in sub.groupby("tech_group", dropna=False):
                rows.append({
                    "regime": regime, "hour": hr,
                    "tech": tech if pd.notna(tech) else "_unmapped_",
                    "share_pct_q_at": g["q_at"].sum() / tot * 100 if tot > 0 else 0,
                    "n_obs": len(g),
                })
    return pd.DataFrame(rows)


def scarcity_quintiles(ps_by_regime: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Price-setter tech mix by MCP quintile per regime."""
    rows = []
    for regime, ps in ps_by_regime.items():
        if len(ps) == 0:
            continue
        ps2 = ps.copy()
        # Quintile based on the distribution of p_clear in this regime
        try:
            ps2["mcp_q"] = pd.qcut(ps2["p_clear"], q=5, labels=["Q1_low","Q2","Q3","Q4","Q5_high"], duplicates="drop")
        except ValueError:
            # Fall back to non-labelled qcut if duplicates collapsed bins
            ps2["mcp_q"] = pd.qcut(ps2["p_clear"], q=5, duplicates="drop").astype(str)
        for q in ps2["mcp_q"].dropna().unique():
            sub = ps2[ps2["mcp_q"] == q]
            tot = sub["q_at"].sum()
            for tech, g in sub.groupby("tech_group", dropna=False):
                rows.append({
                    "regime": regime, "mcp_quintile": str(q),
                    "p_clear_min": sub["p_clear"].min(),
                    "p_clear_max": sub["p_clear"].max(),
                    "tech": tech if pd.notna(tech) else "_unmapped_",
                    "share_pct_q_at": g["q_at"].sum() / tot * 100 if tot > 0 else 0,
                    "n_obs": len(g),
                })
    return pd.DataFrame(rows)


def concentration(ps_by_regime: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """HHI and top-K share by firm, per regime × hour-class."""
    rows = []
    for regime, ps in ps_by_regime.items():
        for hc in ["Critical", "Flat", "Midday", "Dropped"]:
            sub = ps[ps["hour_class"] == hc]
            if len(sub) == 0:
                continue
            firm_shares = (sub.groupby("firm")["q_at"].sum() / sub["q_at"].sum() * 100).sort_values(ascending=False)
            hhi = (firm_shares ** 2).sum()  # 0..10000
            top1 = firm_shares.head(1).sum() if len(firm_shares) >= 1 else 0
            top3 = firm_shares.head(3).sum() if len(firm_shares) >= 3 else firm_shares.sum()
            unit_shares = (sub.groupby("unit_code")["q_at"].sum() / sub["q_at"].sum() * 100).sort_values(ascending=False)
            unit_hhi = (unit_shares ** 2).sum()
            unit_top5 = unit_shares.head(5).sum()
            rows.append({
                "regime": regime, "hour_class": hc,
                "firm_HHI": hhi, "firm_top1_pct": top1, "firm_top3_pct": top3,
                "unit_HHI": unit_hhi, "unit_top5_pct": unit_top5,
                "n_distinct_firms": len(firm_shares), "n_distinct_units": len(unit_shares),
                "n_obs": len(sub),
            })
    return pd.DataFrame(rows)


def indeterminacy_zones(con) -> pd.DataFrame:
    """Periods where neither side has an at-MCP step (EUPHEMIA mid-point rule).
    Compute per regime × hour-class."""
    rows = []
    for regime, (start, end) in WINDOWS.items():
        sql = f"""
        WITH prices AS (
          SELECT date::DATE AS d, period, price_es_eur_mwh AS p_clear, mtu_minutes
          FROM '{MPDBC}' WHERE date::DATE BETWEEN DATE '{start}' AND DATE '{end}'
            AND price_es_eur_mwh IS NOT NULL
        ),
        cab AS (
          SELECT date::DATE AS d, offer_code, version, unit_code, buy_sell,
                 ROW_NUMBER() OVER (PARTITION BY date::DATE, offer_code, unit_code ORDER BY version DESC) AS rn
          FROM '{CAB}' WHERE date::DATE BETWEEN DATE '{start}' AND DATE '{end}'
        ),
        cab_l AS (SELECT * FROM cab WHERE rn=1),
        det AS (
          SELECT date::DATE AS d, offer_code, version, period, price_eur_mwh AS p_bid
          FROM '{DET}' WHERE date::DATE BETWEEN DATE '{start}' AND DATE '{end}'
            AND price_eur_mwh IS NOT NULL
        ),
        joined AS (
          SELECT pr.d, pr.period, c.buy_sell, dv.p_bid, pr.p_clear, pr.mtu_minutes
          FROM det dv JOIN cab_l c USING (d, offer_code, version) JOIN prices pr USING (d, period)
        ),
        flags AS (
          SELECT d, period, MAX(mtu_minutes) AS mtu_minutes,
                 MAX(CASE WHEN buy_sell='V' AND ABS(p_bid - p_clear) <= 0.01 THEN 1 ELSE 0 END) AS has_sell_at,
                 MAX(CASE WHEN buy_sell='C' AND ABS(p_bid - p_clear) <= 0.01 THEN 1 ELSE 0 END) AS has_buy_at
          FROM joined GROUP BY 1, 2
        )
        SELECT
          CAST(CASE WHEN mtu_minutes = 60 THEN period - 1 ELSE CAST(FLOOR((period - 1) / 4.0) AS INT) END AS INT) AS hour,
          has_sell_at, has_buy_at, COUNT(*) AS n_periods
        FROM flags GROUP BY 1, 2, 3
        """
        df = con.execute(sql).df()
        df["hour_class"] = df["hour"].apply(hour_class)
        agg = df.groupby("hour_class").apply(lambda g: pd.Series({
            "n_periods_total": g["n_periods"].sum(),
            "frac_sell_only":  g.loc[(g["has_sell_at"]==1) & (g["has_buy_at"]==0), "n_periods"].sum() / g["n_periods"].sum(),
            "frac_buy_only":   g.loc[(g["has_sell_at"]==0) & (g["has_buy_at"]==1), "n_periods"].sum() / g["n_periods"].sum(),
            "frac_both":       g.loc[(g["has_sell_at"]==1) & (g["has_buy_at"]==1), "n_periods"].sum() / g["n_periods"].sum(),
            "frac_neither":    g.loc[(g["has_sell_at"]==0) & (g["has_buy_at"]==0), "n_periods"].sum() / g["n_periods"].sum(),
        }), include_groups=False).reset_index()
        agg.insert(0, "regime", regime)
        rows.append(agg)
    return pd.concat(rows, ignore_index=True)


def main():
    units = load_units()
    con = duckdb.connect()
    con.execute("PRAGMA threads=4")
    con.execute("SET memory_limit='12GB'")

    ps_sell_by_regime: dict[str, pd.DataFrame] = {}
    ps_buy_by_regime:  dict[str, pd.DataFrame] = {}
    for label, (a, b) in WINDOWS.items():
        print(f"\n=== {label}: {a} -> {b} ===")
        ps_sell = annotate(get_price_setters(con, a, b, side="V"), units)
        ps_buy  = annotate(get_price_setters(con, a, b, side="C"), units)
        print(f"  sell-side price-setter rows: {len(ps_sell):,}")
        print(f"  buy-side price-setter rows:  {len(ps_buy):,}")
        ps_sell_by_regime[label] = ps_sell
        ps_buy_by_regime[label]  = ps_buy

    print("\n--- 01_firm_shares (CCGT focus, sell side) ---")
    firm_shares(ps_sell_by_regime).to_csv(OUT / "01_firm_shares_ccgt.csv", index=False)
    print(f"  wrote {OUT/'01_firm_shares_ccgt.csv'}")

    print("\n--- 02_top_units ---")
    top_units(ps_sell_by_regime, by="n_obs", top_n=15).to_csv(OUT / "02_top_units_count.csv", index=False)
    top_units(ps_sell_by_regime, by="q_at",  top_n=15).to_csv(OUT / "02_top_units_qat.csv",   index=False)

    print("\n--- 03_bid_shape ---")
    bid_shape(ps_sell_by_regime).to_csv(OUT / "03_bid_shape_sell.csv", index=False)
    bid_shape(ps_buy_by_regime).to_csv(OUT / "03_bid_shape_buy.csv",   index=False)

    print("\n--- 04_weightings ---")
    weightings(ps_sell_by_regime).to_csv(OUT / "04_weightings_sell.csv", index=False)
    weightings(ps_buy_by_regime).to_csv(OUT / "04_weightings_buy.csv",   index=False)

    print("\n--- 05_hour_of_day ---")
    hour_of_day(ps_sell_by_regime).to_csv(OUT / "05_hour_of_day_sell.csv", index=False)
    hour_of_day(ps_buy_by_regime).to_csv(OUT / "05_hour_of_day_buy.csv",   index=False)

    print("\n--- 06_buy_side_summary ---")
    # Already in 04_weightings_buy
    print("  (in 04_weightings_buy.csv)")

    print("\n--- 07_indeterminacy ---")
    indeterminacy_zones(con).to_csv(OUT / "07_indeterminacy.csv", index=False)

    print("\n--- 08_scarcity_quintiles ---")
    scarcity_quintiles(ps_sell_by_regime).to_csv(OUT / "08_scarcity_quintiles_sell.csv", index=False)

    print("\n--- 09_concentration ---")
    concentration(ps_sell_by_regime).to_csv(OUT / "09_concentration_sell.csv", index=False)
    concentration(ps_buy_by_regime).to_csv(OUT / "09_concentration_buy.csv",   index=False)

    # Day-of-week split (extra)
    print("\n--- 10_weekday_weekend ---")
    rows = []
    for regime, ps in ps_sell_by_regime.items():
        for is_wknd, lbl in [(False, "weekday"), (True, "weekend")]:
            sub = ps[ps["is_weekend"] == is_wknd]
            for hc in ["Critical", "Flat", "Midday", "Dropped"]:
                ssub = sub[sub["hour_class"] == hc]
                tot = ssub["q_at"].sum()
                if tot == 0: continue
                for tech, g in ssub.groupby("tech_group", dropna=False):
                    rows.append({"regime": regime, "day_type": lbl, "hour_class": hc,
                                 "tech": tech if pd.notna(tech) else "_unmapped_",
                                 "share_pct_q_at": g["q_at"].sum()/tot*100,
                                 "n_obs": len(g)})
    pd.DataFrame(rows).to_csv(OUT / "10_weekday_weekend_sell.csv", index=False)

    # Persist the raw price-setter dataframes for follow-up exploration
    print("\n--- raw price-setter parquet exports ---")
    for label, ps in ps_sell_by_regime.items():
        slug = (label.replace(" ", "_").replace("/", "-")
                     .replace("(", "").replace(")", "")
                     .replace(",", "").lower())
        ps.to_parquet(OUT / f"raw_sell_{slug}.parquet", index=False)
        ps_buy_by_regime[label].to_parquet(OUT / f"raw_buy_{slug}.parquet", index=False)

    print(f"\nAll outputs in: {OUT}")


if __name__ == "__main__":
    main()
