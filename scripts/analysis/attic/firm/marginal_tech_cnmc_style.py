# STATUS: ALIVE
# LAST-AUDIT: 2026-05-18
# FEEDS: descriptive_facts.tex §2 (CNMC-style marginal-tech share)
# CLAIM: CNMC's "marginal tech share" metric (% of hours where a given
#        technology is the marginal-cost cleared technology) applied to
#        our 5 reform-window regimes. CNMC reports this metric for
#        CCGT explicitly (e.g. "21.7% de las horas en septiembre 2023"
#        in IS/DE/013/24).
#
# Rule (CNMC-style, naive partial-acceptance-agnostic):
#   For each (date, period):
#     marginal_tech := tech of the unit whose highest accepted sell-bid
#                      tranche has p_bid ≤ p_clear (within EPS) and is
#                      the max p_bid among such tranches (rank-1).
#     If multiple units tie at rank-1, split the "marginal vote" equally.
#   Per (regime, hour-class), report the MW-weighted share of each tech
#   (the SAME share concept CNMC uses in Cuadro 11 of IS/DE/013/24).
#
# This metric DIFFERS from the partial-acceptance one in scripts/analysis/firm/
# price_setter_euphemia.py: there a unit is only counted if its at-MCP step
# was strictly partially accepted (q_below < q_assigned < q_below + q_at).
# Here we count any unit whose highest-accepted-step lands at MCP, even if
# fully accepted (inframarginal at MCP). The CNMC metric is the BROADER one.

from __future__ import annotations
from pathlib import Path
import duckdb
import pandas as pd

REPO  = Path(__file__).resolve().parents[3]
DET   = REPO / "data" / "processed" / "omie" / "mercado_diario" / "ofertas"   / "det_all.parquet"
CAB   = REPO / "data" / "processed" / "omie" / "mercado_diario" / "ofertas"   / "cab_all.parquet"
MPDBC = REPO / "data" / "processed" / "omie" / "mercado_diario" / "precios"   / "marginalpdbc_all.parquet"
UNITS = REPO / "data" / "external"  / "omie_reference" / "lista_unidades.csv"

OUT = REPO / "results" / "regressions" / "firm" / "marginal_tech"
OUT.mkdir(parents=True, exist_ok=True)

EPS_PRICE = 0.01

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


def cnmc_marginal_tech_share(con, start, end, units_df):
    """Naive CNMC-style metric: per (date, period), the rank-1 accepted sell
    tranche (p_bid ≤ p_clear, max p_bid) sets the marginal tech. Returns
    per-hour-class shares of each tech across the window."""
    con.register("uft", units_df)
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
    accepted AS (
        SELECT pr.d, pr.period, c.unit_code, dv.p_bid, dv.q_bid, pr.p_clear, pr.mtu_minutes,
               RANK() OVER (PARTITION BY pr.d, pr.period ORDER BY dv.p_bid DESC) AS rk
        FROM det dv
        JOIN cab_l c USING (d, offer_code, version)
        JOIN prices pr USING (d, period)
        WHERE dv.p_bid <= pr.p_clear
    ),
    rank1 AS (
        SELECT d, period, unit_code, p_bid, q_bid, p_clear, mtu_minutes
        FROM accepted
        WHERE rk = 1
          AND (p_clear - p_bid) <= {EPS_PRICE}
    ),
    tagged AS (
        SELECT r.d, r.period, r.unit_code, r.q_bid, r.mtu_minutes, u.tech_group,
               1.0 / COUNT(*) OVER (PARTITION BY r.d, r.period) AS weight
        FROM rank1 r
        LEFT JOIN uft u USING (unit_code)
    )
    SELECT d AS date,
           CAST(CASE WHEN mtu_minutes = 60 THEN period - 1
                     ELSE CAST(FLOOR((period - 1) / 4.0) AS INT) END AS INT) AS hour,
           tech_group, weight
    FROM tagged
    """
    return con.execute(sql).df()


def hour_class(h):
    if h in CRIT: return "Critical"
    if h in FLAT: return "Flat"
    if h in MID:  return "Midday"
    return "Dropped"


def main():
    raw = pd.read_csv(UNITS)
    raw["tech_group"] = raw["technology"].apply(map_tech)
    units = raw[["unit_code", "tech_group"]].drop_duplicates("unit_code")

    con = duckdb.connect()
    con.execute("PRAGMA threads=4")
    con.execute("SET memory_limit='10GB'")

    rows = []
    for label, (a, b) in WINDOWS.items():
        print(f"\n=== {label}: {a} -> {b} ===")
        df = cnmc_marginal_tech_share(con, a, b, units)
        if len(df) == 0:
            print("  no rows")
            continue
        df["hour_class"] = df["hour"].apply(hour_class)
        # Per hour-class, sum weights by tech, normalise so shares sum to 1 within (regime, hour-class)
        agg = df.groupby(["hour_class", "tech_group"], dropna=False)["weight"].sum().unstack(fill_value=0)
        shares = agg.div(agg.sum(axis=1), axis=0) * 100
        for hc in shares.index:
            for tech in shares.columns:
                rows.append({
                    "regime": label, "hour_class": hc, "tech_group": tech,
                    "share_pct_cnmc": shares.loc[hc, tech],
                })
        # Diagnostic: matched fraction
        all_periods = con.execute(f"""
            SELECT COUNT(*) FROM '{MPDBC}'
            WHERE date::DATE BETWEEN DATE '{a}' AND DATE '{b}'
              AND price_es_eur_mwh IS NOT NULL
        """).fetchone()[0]
        matched = df.drop_duplicates(["date", "hour"]).shape[0]
        print(f"  matched (date, period) cells with a rank-1-at-MCP unit: {matched:,} / total ~{all_periods:,}")

    out = pd.DataFrame(rows)
    out.to_csv(OUT / "cnmc_marginal_tech_shares.csv", index=False)
    # Print headline: CCGT critical and flat shares per regime
    ccgt = out[out["tech_group"] == "CCGT"].pivot(index="regime", columns="hour_class", values="share_pct_cnmc").round(1)
    print("\n=== CCGT CNMC-style marginal-tech share (% of MW-weighted votes) ===")
    print(ccgt[["Critical", "Flat", "Midday", "Dropped"]].to_string())
    # And the multi-tech headline for DA15/ID15 critical+flat
    print("\n=== Headline tech shares, DA15/ID15 critical+flat (CNMC-style) ===")
    sub = out[out["regime"] == "DA15/ID15 (Oct-Dec 2025)"]
    top = sub.pivot(index="tech_group", columns="hour_class", values="share_pct_cnmc").fillna(0)
    top = top.loc[top.max(axis=1).sort_values(ascending=False).index[:10]]
    print(top[["Critical", "Flat", "Midday", "Dropped"]].round(1).to_string())


if __name__ == "__main__":
    main()
