# STATUS: ALIVE
# LAST-AUDIT: 2026-06-09
# FEEDS: appendix + body bid-shape slides of the June 2026 deck.
#
# Per-(unit, date, period[, session]) in-band sell-bid curve metrics for
# ALL FIRMS and the extended tech list
#   {CCGT, Hydro, Hydro_pump, Wind, Cogen, Coal, Hybrid, Biomass}
# and critical-vs-flat DiD (also morning-vs-flat) at the bid-curve level
# for each (reform x market x tech) on four outcomes:
#   sigma_p  (MW-weighted SD of in-band prices)
#   beta     (linear slope of price on cum-MW)
#   gamma    (quadratic curvature)
#   hhi      (1 / N_eff = Herfindahl of MW shares)
#   level    (MW-weighted mean in-band price minus MCP; level-neutrality check)
# Unit FE + day-clustered SE. DiD coefficient = theta on (post x crit).
#
# Per-curve metrics are computed inside DuckDB via window cumQ + groupby
# aggregations of cross-moments; closed-form beta/gamma from the 3x3
# normal equation in numpy. No per-curve Python apply.
#
# Also writes a daily-tech-hour_class panel for parallel-trends figures.
#
# OUT:
#   results/regressions/bid/mtu15_critical_flat/bid_shape_did_extended.csv
#   data/derived/panels/bid_shape_daily_means_extended.parquet

from pathlib import Path
import duckdb
import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
DET   = REPO / "data/processed/omie/mercado_diario/ofertas/det_all.parquet"
CAB   = REPO / "data/processed/omie/mercado_diario/ofertas/cab_all.parquet"
MPDBC = REPO / "data/processed/omie/mercado_diario/precios/marginalpdbc_all.parquet"
IDET  = REPO / "data/processed/omie/mercado_intradiario_subastas/ofertas/idet_all.parquet"
ICAB  = REPO / "data/processed/omie/mercado_intradiario_subastas/ofertas/icab_all.parquet"
MPIBC = REPO / "data/processed/omie/mercado_intradiario_subastas/precios/marginalpibc_all.parquet"
UNITS = REPO / "data/external/omie_reference/lista_unidades.csv"
OUT_CSV    = REPO / "results/regressions/bid/mtu15_critical_flat/bid_shape_did_extended.csv"
OUT_DAILY  = REPO / "data/derived/panels/bid_shape_daily_means_extended.parquet"

CRITICAL     = {5, 6, 7, 8, 16, 17, 18, 19, 20, 21, 22}
MORNING_RAMP = {5, 6, 7, 8}
EVENING_RAMP = {16, 17, 18, 19, 20, 21, 22}
FLAT         = {1, 2, 3}

TECHS = ["CCGT", "Hydro", "Hydro_pump", "Wind",
         "Cogen", "Coal", "Hybrid", "Biomass"]

WINDOWS = {
    "ID15": {"pre_lo": "2024-06-14", "pre_hi": "2025-03-18",
             "post_lo": "2025-03-19", "post_hi": "2025-04-27", "h_ida": 62},
    "DA15": {"pre_lo": "2025-04-28", "pre_hi": "2025-09-30",
             "post_lo": "2025-10-01", "post_hi": "2026-03-06", "h_ida": 58},
}
H_DA = 50  # both reforms use h=50 on the DA side per legacy spec


def tech_bucket(t):
    if t is None: return "Other"
    t = str(t).lower()
    if "ciclo combinado" in t: return "CCGT"
    if "hidráulica generación" in t: return "Hydro"
    if "bombeo" in t: return "Hydro_pump"
    if "eólica" in t: return "Wind"
    if "carbón" in t or "carbon" in t: return "Coal"
    if "híbrid" in t or "hibrid" in t: return "Hybrid"
    if "térmica renovable" in t or "termica renovable" in t: return "Biomass"
    if "térmica no renovab" in t or "termica no renovab" in t: return "Cogen"
    return "Other"


def firm_bucket(o):
    if not isinstance(o, str): return "OTH"
    o = o.lower()
    if "iberdrola" in o: return "IB"
    if "endesa" in o: return "GE"
    if "naturgy" in o or "gas natural" in o: return "GN"
    if "edp" in o or "hidroel" in o: return "HC"
    if "repsol" in o: return "REP"
    return "OTH"


def hour_class(h):
    if h in MORNING_RAMP: return "MorningRamp"
    if h in EVENING_RAMP: return "EveningRamp"
    if h in FLAT:         return "Flat"
    return "Other"


def units_table(con):
    units = pd.read_csv(UNITS)
    units["tech"] = units["technology"].apply(tech_bucket)
    units["firm"] = units["owner_agent"].apply(firm_bucket)
    units = units[units["tech"].isin(TECHS)][
        ["unit_code", "firm", "tech"]
    ].drop_duplicates("unit_code")
    con.register("u", units)


def per_curve_query_da(lo, hi, h):
    # Group key for DA per curve: (d, period, unit_code).
    return f"""
    WITH cab_l AS (
      SELECT d, offer_code, unit_code FROM (
        SELECT CAST(date AS DATE) d, offer_code, unit_code, version,
               ROW_NUMBER() OVER (PARTITION BY CAST(date AS DATE), offer_code,
                                                unit_code
                                  ORDER BY version DESC) rn
        FROM '{CAB}' WHERE date BETWEEN '{lo}' AND '{hi}' AND buy_sell='V'
      ) WHERE rn=1
    ),
    det AS (
      SELECT CAST(date AS DATE) d, offer_code, period,
             price_eur_mwh p, quantity_mw q, COALESCE(mtu_minutes, 60) mtu
      FROM '{DET}' WHERE date BETWEEN '{lo}' AND '{hi}' AND quantity_mw > 0
    ),
    mp AS (
      SELECT CAST(date AS DATE) d, period, price_es_eur_mwh p_clear,
             COALESCE(mtu_minutes, 60) mtu_p
      FROM '{MPDBC}' WHERE date BETWEEN '{lo}' AND '{hi}'
                     AND price_es_eur_mwh IS NOT NULL
    ),
    inband AS (
      SELECT mp.d, mp.period, c.unit_code, u.firm, u.tech,
             CASE WHEN COALESCE(mp.mtu_p, dv.mtu) = 60 THEN mp.period - 1
                  ELSE CAST(FLOOR((mp.period - 1) / 4.0) AS INT) END AS clock_hour,
             dv.q, dv.p, mp.p_clear
      FROM det dv JOIN cab_l c ON dv.d=c.d AND dv.offer_code=c.offer_code
        JOIN mp ON mp.d=dv.d AND mp.period=dv.period
        JOIN u ON c.unit_code = u.unit_code
      WHERE dv.p BETWEEN mp.p_clear - {h} AND mp.p_clear + {h}
    ),
    ordered AS (
      SELECT d, period, clock_hour, unit_code, firm, tech, q, p, p_clear,
             SUM(q) OVER (PARTITION BY d, period, unit_code
                          ORDER BY p ROWS UNBOUNDED PRECEDING) AS cumQ
      FROM inband
    )
    SELECT d, period, clock_hour, unit_code, firm, tech,
           COUNT(*)                       AS K,
           SUM(q)                         AS s_w,
           SUM(q*p)                       AS s_wp,
           SUM(q*p*p)                     AS s_wp2,
           SUM(q*q)                       AS s_w2,
           SUM(cumQ)                      AS s_x,
           SUM(cumQ*cumQ)                 AS s_x2,
           SUM(cumQ*cumQ*cumQ)            AS s_x3,
           SUM(cumQ*cumQ*cumQ*cumQ)       AS s_x4,
           SUM(p)                         AS s_y,
           SUM(cumQ*p)                    AS s_xy,
           SUM(cumQ*cumQ*p)               AS s_x2y,
           MAX(p_clear)                   AS p_clear
    FROM ordered
    GROUP BY 1, 2, 3, 4, 5, 6
    HAVING s_w > 0
    """


def per_curve_query_ida(lo, hi, h):
    # Group key for IDA per curve: (d, session_number, period, unit_code).
    return f"""
    WITH icab_l AS (
      SELECT d, session_number, offer_code, version, unit_code FROM (
        SELECT CAST(date AS DATE) d, session_number, offer_code, version, unit_code,
               ROW_NUMBER() OVER (PARTITION BY CAST(date AS DATE), session_number,
                                                offer_code, unit_code
                                  ORDER BY version DESC) rn
        FROM '{ICAB}' WHERE date BETWEEN '{lo}' AND '{hi}' AND buy_sell='V'
      ) WHERE rn=1
    ),
    idet AS (
      SELECT CAST(date AS DATE) d, session_number, offer_code, version, unit_code,
             period, price_eur_mwh p, quantity_mw q, COALESCE(mtu_minutes, 60) mtu
      FROM '{IDET}' WHERE date BETWEEN '{lo}' AND '{hi}' AND quantity_mw > 0
    ),
    mp AS (
      SELECT CAST(date AS DATE) d, session_number, period,
             price_es_eur_mwh p_clear, COALESCE(mtu_minutes, 60) mtu_p
      FROM '{MPIBC}' WHERE date BETWEEN '{lo}' AND '{hi}'
                     AND price_es_eur_mwh IS NOT NULL
    ),
    inband AS (
      SELECT mp.d, mp.session_number, mp.period, c.unit_code, u.firm, u.tech,
             CASE WHEN COALESCE(mp.mtu_p, dv.mtu) = 60 THEN mp.period - 1
                  ELSE CAST(FLOOR((mp.period - 1) / 4.0) AS INT) END AS clock_hour,
             dv.q, dv.p, mp.p_clear
      FROM idet dv
        JOIN icab_l c ON dv.d=c.d AND dv.session_number=c.session_number
                      AND dv.offer_code=c.offer_code AND dv.version=c.version
                      AND dv.unit_code=c.unit_code
        JOIN mp ON mp.d=dv.d AND mp.session_number=dv.session_number
                AND mp.period=dv.period
        JOIN u ON c.unit_code = u.unit_code
      WHERE dv.p BETWEEN mp.p_clear - {h} AND mp.p_clear + {h}
    ),
    ordered AS (
      SELECT d, session_number, period, clock_hour, unit_code, firm, tech, q, p, p_clear,
             SUM(q) OVER (PARTITION BY d, session_number, period, unit_code
                          ORDER BY p ROWS UNBOUNDED PRECEDING) AS cumQ
      FROM inband
    )
    SELECT d, session_number, period, clock_hour, unit_code, firm, tech,
           COUNT(*)                       AS K,
           SUM(q)                         AS s_w,
           SUM(q*p)                       AS s_wp,
           SUM(q*p*p)                     AS s_wp2,
           SUM(q*q)                       AS s_w2,
           SUM(cumQ)                      AS s_x,
           SUM(cumQ*cumQ)                 AS s_x2,
           SUM(cumQ*cumQ*cumQ)            AS s_x3,
           SUM(cumQ*cumQ*cumQ*cumQ)       AS s_x4,
           SUM(p)                         AS s_y,
           SUM(cumQ*p)                    AS s_xy,
           SUM(cumQ*cumQ*p)               AS s_x2y,
           MAX(p_clear)                   AS p_clear
    FROM ordered
    GROUP BY 1, 2, 3, 4, 5, 6, 7
    HAVING s_w > 0
    """


def add_metrics(df):
    """Closed-form per-curve metrics from the DuckDB cross-moments."""
    s_w  = df["s_w"].to_numpy(dtype=float)
    s_wp = df["s_wp"].to_numpy(dtype=float)
    s_wp2= df["s_wp2"].to_numpy(dtype=float)
    s_w2 = df["s_w2"].to_numpy(dtype=float)
    K    = df["K"].to_numpy(dtype=float)
    # MW-weighted mean & SD of in-band prices
    mean_p = s_wp / s_w
    var_p = np.clip(s_wp2 / s_w - mean_p ** 2, 0, None)
    df["sigma_p"] = np.sqrt(var_p)
    # bid-curve level relative to the clearing price (level-neutrality check)
    df["level"] = mean_p - df["p_clear"].to_numpy(dtype=float)
    # Herfindahl on MW shares
    n_eff = (s_w ** 2) / s_w2
    df["n_eff"] = n_eff
    df["hhi"]   = 1.0 / n_eff
    # Linear OLS of p on cumQ:  beta = (K*s_xy - s_x*s_y) / (K*s_x2 - s_x^2)
    s_x  = df["s_x"].to_numpy(dtype=float)
    s_x2 = df["s_x2"].to_numpy(dtype=float)
    s_x3 = df["s_x3"].to_numpy(dtype=float)
    s_x4 = df["s_x4"].to_numpy(dtype=float)
    s_y  = df["s_y"].to_numpy(dtype=float)
    s_xy = df["s_xy"].to_numpy(dtype=float)
    s_x2y = df["s_x2y"].to_numpy(dtype=float)
    den_lin = K * s_x2 - s_x ** 2
    with np.errstate(divide="ignore", invalid="ignore"):
        beta = np.where(den_lin > 0,
                         (K * s_xy - s_x * s_y) / den_lin, np.nan)
    df["beta"] = beta
    # Quadratic OLS gamma via 3x3 normal equation determinant ratio.
    a11, a12, a13 = K,   s_x,  s_x2
    a21, a22, a23 = s_x, s_x2, s_x3
    a31, a32, a33 = s_x2, s_x3, s_x4
    detA = (a11 * (a22 * a33 - a23 * a32)
            - a12 * (a21 * a33 - a23 * a31)
            + a13 * (a21 * a32 - a22 * a31))
    # gamma = det([a11 a12 s_y; a21 a22 s_xy; a31 a32 s_x2y]) / detA
    detG = (a11 * (a22 * s_x2y - s_xy * a32)
            - a12 * (a21 * s_x2y - s_xy * a31)
            + s_y * (a21 * a32 - a22 * a31))
    with np.errstate(divide="ignore", invalid="ignore"):
        gamma = np.where(np.abs(detA) > 1e-9, detG / detA, np.nan)
    gamma = np.where(K >= 3, gamma, np.nan)
    df["gamma"] = gamma
    return df


def clustered_ols(y, X, cluster):
    XtX_inv = np.linalg.inv(X.T @ X)
    beta_ = XtX_inv @ (X.T @ y)
    e = y - X @ beta_
    meat = np.zeros((X.shape[1], X.shape[1]))
    for g in np.unique(cluster):
        m = cluster == g
        s = X[m].T @ e[m]
        meat += np.outer(s, s)
    G = len(np.unique(cluster)); n, k = X.shape
    adj = (G / (G - 1)) * ((n - 1) / (n - k))
    V = adj * (XtX_inv @ meat @ XtX_inv)
    return beta_, np.sqrt(np.diag(V))


def did_unit_FE(p_in, outcome, crit_set):
    """Critical-vs-flat DiD with unit FE; clustered SE at day."""
    d = p_in.dropna(subset=[outcome]).copy()
    d["crit"] = d["clock_hour"].isin(crit_set).astype(int)
    d["flat"] = d["clock_hour"].isin(FLAT).astype(int)
    d = d[(d["crit"] == 1) | (d["flat"] == 1)]
    if len(d) < 100:
        return None
    d["post_crit"] = d["post"] * d["crit"]
    for c in [outcome, "post", "crit", "post_crit"]:
        m = d.groupby("unit_code")[c].transform("mean")
        d[c + "_w"] = d[c] - m
    for c in ["post_w", "crit_w", "post_crit_w"]:
        if d[c].abs().max() < 1e-10:
            return None
    X = np.column_stack([np.ones(len(d)),
                         d["post_w"].values, d["crit_w"].values,
                         d["post_crit_w"].values])
    y = d[outcome + "_w"].values
    try:
        beta_, se_ = clustered_ols(y, X, d["d"].astype(str).values)
    except np.linalg.LinAlgError:
        return None
    return {"theta": beta_[3], "se": se_[3], "t": beta_[3] / se_[3],
            "n": len(d), "n_units": d["unit_code"].nunique()}


def main():
    con = duckdb.connect()
    con.execute("SET memory_limit='12GB'"); con.execute("SET threads=4")
    units_table(con)

    OUTCOMES = ["sigma_p", "beta", "gamma", "hhi", "level"]
    rows = []
    daily_panel = []

    for reform, w in WINDOWS.items():
        for market in ("da", "ida"):
            h = H_DA if market == "da" else w["h_ida"]
            lo, hi = w["pre_lo"], w["post_hi"]
            print(f"\n[{reform} {market}] window {lo} -> {hi}, h={h}", flush=True)
            sql = (per_curve_query_da(lo, hi, h) if market == "da"
                    else per_curve_query_ida(lo, hi, h))
            df = con.execute(sql).fetchdf()
            df["d"] = pd.to_datetime(df["d"])
            df = add_metrics(df)
            df["post"] = (df["d"] >= pd.to_datetime(w["post_lo"])).astype(int)
            df["hour_class"] = df["clock_hour"].apply(hour_class)
            print(f"  {len(df):,} curves; running DiD...", flush=True)

            # daily-mean panel for parallel trends
            day = (df.groupby(["d", "tech", "hour_class"], observed=True)
                      [OUTCOMES].mean().reset_index())
            day["reform"] = reform; day["market"] = market
            daily_panel.append(day)

            for tech in TECHS:
                m_tech = df[df["tech"] == tech]
                if len(m_tech) < 200:
                    continue
                for outcome in OUTCOMES:
                    for label, crit_set in [("crit_vs_flat", CRITICAL),
                                              ("morning_vs_flat", MORNING_RAMP)]:
                        r = did_unit_FE(m_tech, outcome, crit_set)
                        if r is None:
                            continue
                        rows.append({"reform": reform, "market": market,
                                      "tech": tech, "outcome": outcome,
                                      "comparison": label, **r})
                        print(f"    {tech:11s} {outcome:7s} {label:15s} "
                              f"theta={r['theta']:+10.4f}  se={r['se']:.4f}  "
                              f"n={r['n']:,}  units={r['n_units']}", flush=True)

    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(OUT_CSV, index=False)
    print(f"\nWrote {OUT_CSV}", flush=True)
    pd.concat(daily_panel, ignore_index=True).to_parquet(OUT_DAILY, index=False)
    print(f"Wrote {OUT_DAILY}", flush=True)


if __name__ == "__main__":
    main()
