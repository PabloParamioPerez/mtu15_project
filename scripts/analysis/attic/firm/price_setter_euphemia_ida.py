# STATUS: ALIVE
# LAST-AUDIT: 2026-05-17
# FEEDS: descriptive_facts.tex §2.2 (IDA price-setter shares)
# CLAIM: At-the-money partial-acceptance price-setter identification for the
#        Spanish IDA market — direct analogue of price_setter_euphemia.py but
#        with IDA data: marginalpibc (clearing), idet+icab (bid stack),
#        pibci (cleared MWh per unit). Pools across IDA sessions per period.
#        Uses the same hour-class taxonomy (critical / flat / midday) and the
#        same date-cutoff handling for the MTU15-IDA reform (2025-03-19).
#
# Outputs:
#   results/regressions/firm/marginal_tech/price_setter_euphemia_ida_shares.csv
#   results/regressions/firm/marginal_tech/price_setter_euphemia_ida_matched.csv

from __future__ import annotations
from pathlib import Path
import duckdb
import pandas as pd

REPO  = Path(__file__).resolve().parents[3]
IDET  = REPO / "data" / "processed" / "omie" / "mercado_intradiario_subastas" / "ofertas"   / "idet_all.parquet"
ICAB  = REPO / "data" / "processed" / "omie" / "mercado_intradiario_subastas" / "ofertas"   / "icab_all.parquet"
PIBCI = REPO / "data" / "processed" / "omie" / "mercado_intradiario_subastas" / "programas" / "pibci_all.parquet"
MPIBC = REPO / "data" / "processed" / "omie" / "mercado_intradiario_subastas" / "precios"   / "marginalpibc_all.parquet"
UNITS = REPO / "data" / "external" / "omie_reference" / "lista_unidades.csv"

OUT   = REPO / "results" / "regressions" / "firm" / "marginal_tech"
OUT.mkdir(parents=True, exist_ok=True)

EPS_PRICE = 0.01
EPS_QTY   = 1e-3

CRIT = (5, 6, 7, 8, 16, 17, 18, 19, 20, 21, 22)
FLAT = (1, 2, 3)
MID  = (11, 12, 13, 14)

# IDA-relevant regimes (3 sessions per day applies post 2024-06-14)
WINDOWS = {
    "3-sess (Jun-Nov 2024)":           ("2024-06-14", "2024-11-30"),
    "ISP15-win (Dec24-Mar25)":         ("2024-12-01", "2025-03-18"),
    "DA60/ID15 pre-blk (Mar19-Apr27)": ("2025-03-19", "2025-04-27"),
    "DA60/ID15 post-blk (Apr28-Sep)":  ("2025-04-28", "2025-09-30"),
    "DA15/ID15 (Oct-Dec 2025)":        ("2025-10-01", "2025-12-31"),
}


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
    if "comercializador" in t or "compras comercializaci" in t: return "Retailer"
    if "consumo directo" in t or "consumidor directo" in t: return "Direct_consumer"
    if "import" in t or "contrato internacional" in t: return "Import"
    return "Other"


def load_units():
    u = pd.read_csv(UNITS)
    u["tech_group"] = u["technology"].apply(map_tech)
    return u[["unit_code", "tech_group", "zone"]].drop_duplicates("unit_code")


def price_setters_ida(con, start: str, end: str) -> pd.DataFrame:
    """At-the-money partial-acceptance price-setters for IDA.
    Pools across IDA sessions per (date, period). Returns one row per
    (date, session, period, unit) cell that satisfies the partial-
    acceptance condition."""
    sql = f"""
    WITH prices AS (
        SELECT date::DATE AS d, session_number AS s, period,
               price_es_eur_mwh AS p_clear, mtu_minutes
        FROM '{MPIBC}'
        WHERE date::DATE BETWEEN DATE '{start}' AND DATE '{end}'
          AND price_es_eur_mwh IS NOT NULL
    ),
    cab_v AS (
        SELECT date::DATE AS d, session_number AS s, offer_code, version, unit_code,
               ROW_NUMBER() OVER (PARTITION BY date::DATE, session_number, offer_code, unit_code
                                  ORDER BY version DESC) AS rn
        FROM '{ICAB}'
        WHERE buy_sell='V' AND date::DATE BETWEEN DATE '{start}' AND DATE '{end}'
    ),
    cab_l AS (SELECT * FROM cab_v WHERE rn = 1),
    det_v AS (
        SELECT date::DATE AS d, session_number AS s, offer_code, version, period,
               price_eur_mwh AS p_bid, quantity_mw AS q_bid
        FROM '{IDET}'
        WHERE date::DATE BETWEEN DATE '{start}' AND DATE '{end}'
          AND price_eur_mwh IS NOT NULL AND quantity_mw > 0
    ),
    det_unit AS (
        SELECT dv.d, dv.s, dv.period, c.unit_code, dv.p_bid, dv.q_bid, pr.p_clear, pr.mtu_minutes
        FROM det_v dv
        JOIN cab_l c USING (d, s, offer_code, version)
        JOIN prices pr USING (d, s, period)
    ),
    unit_curve AS (
        SELECT d, s, period, unit_code, MAX(mtu_minutes) AS mtu_minutes,
               MAX(p_clear) AS p_clear,
               COALESCE(SUM(q_bid) FILTER (WHERE p_bid < p_clear - {EPS_PRICE}), 0) AS q_below,
               COALESCE(SUM(q_bid) FILTER (WHERE ABS(p_bid - p_clear) <= {EPS_PRICE}), 0) AS q_at,
               MAX(p_bid) FILTER (WHERE p_bid <= p_clear + {EPS_PRICE}) AS p_top_accepted
        FROM det_unit
        GROUP BY d, s, period, unit_code
    ),
    unit_assigned AS (
        SELECT date::DATE AS d, session_number AS s, period, unit_code,
               SUM(ABS(assigned_power_mw)) AS q_assigned
        FROM '{PIBCI}'
        WHERE date::DATE BETWEEN DATE '{start}' AND DATE '{end}'
          AND offer_type = 1
        GROUP BY 1, 2, 3, 4
    )
    SELECT uc.d AS date, uc.s AS session, uc.period, uc.unit_code, uc.mtu_minutes,
           uc.p_clear, uc.q_below, uc.q_at,
           COALESCE(ua.q_assigned, 0) AS q_assigned,
           CAST(CASE WHEN uc.mtu_minutes = 60
                     THEN uc.period - 1
                     ELSE CAST(FLOOR((uc.period - 1) / 4.0) AS INT)
                END AS INT) AS hour
    FROM unit_curve uc
    LEFT JOIN unit_assigned ua USING (d, s, period, unit_code)
    WHERE uc.q_at > {EPS_QTY}
      AND COALESCE(ua.q_assigned, 0) > uc.q_below + {EPS_QTY}
      AND COALESCE(ua.q_assigned, 0) < uc.q_below + uc.q_at - {EPS_QTY}
    """
    return con.execute(sql).df()


def matched_periods_ida(con, start, end):
    sql = f"""
    SELECT date::DATE AS d, session_number AS s,
           CAST(period AS INT) AS period,
           CAST(CASE WHEN mtu_minutes = 60
                     THEN CAST(period AS INT) - 1
                     ELSE CAST(FLOOR((CAST(period AS INT) - 1) / 4.0) AS INT)
                END AS INT) AS hour
    FROM '{MPIBC}'
    WHERE date::DATE BETWEEN DATE '{start}' AND DATE '{end}'
      AND price_es_eur_mwh IS NOT NULL
    """
    return con.execute(sql).df()


def hour_class(h):
    if h in CRIT: return "Critical"
    if h in FLAT: return "Flat"
    if h in MID:  return "Midday"
    return "Dropped"


def summarise(setters, periods, units, label):
    s = setters.merge(units, on="unit_code", how="left")
    s["hour_class"] = s["hour"].apply(hour_class)
    grp = s.groupby(["hour_class", "tech_group"], dropna=False)["q_at"].sum().unstack(fill_value=0)
    shares = grp.div(grp.sum(axis=1), axis=0)
    p2 = periods.assign(hour_class=periods["hour"].apply(hour_class))
    matched = p2.merge(s[["date","session","period"]].drop_duplicates().assign(matched=1),
                       left_on=["d","s","period"], right_on=["date","session","period"], how="left")
    matched["matched"] = matched["matched"].fillna(0)
    diag = matched.groupby("hour_class")["matched"].agg(["mean", "count"])
    diag.columns = [f"matched_share_{label}", f"n_periods_{label}"]
    return shares.assign(window=label), diag


def main():
    units = load_units()
    con = duckdb.connect()
    con.execute("PRAGMA threads=4"); con.execute("SET memory_limit='12GB'")

    big = []; diags = []
    for label, (a, b) in WINDOWS.items():
        print(f"\n=== {label}: {a} -> {b} ===")
        ps = price_setters_ida(con, a, b)
        per = matched_periods_ida(con, a, b)
        print(f"  matched setter rows: {len(ps):,};  total (d,s,t) cells: {len(per):,}")
        shares, diag = summarise(ps, per, units, label)
        big.append(shares); diags.append(diag)

    out_shares = pd.concat(big).reset_index()
    out_shares.to_csv(OUT / "price_setter_euphemia_ida_shares.csv", index=False)
    out_diag = pd.concat(diags, axis=1)
    out_diag.to_csv(OUT / "price_setter_euphemia_ida_matched.csv")
    print("\nIDA per-hour-class price-setter shares (Euphemia-aware, MW-weighted by q_at):")
    for label in WINDOWS:
        d = out_shares[out_shares["window"] == label].set_index("hour_class").drop(columns="window")
        d = d.loc[["Critical","Flat","Midday","Dropped"]].fillna(0)
        keep = d.columns[d.max(axis=0) > 0.005]
        top = d[keep].T.sort_values("Critical", ascending=False)
        print(f"\n[{label}] (%)")
        print((top * 100).round(1).to_string())
    print("\nMatched-period diagnostics:")
    print(out_diag)


if __name__ == "__main__":
    main()
