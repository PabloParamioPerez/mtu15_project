# STATUS: ALIVE
# LAST-AUDIT: 2026-05-26
# FEEDS: thesis/provisional/advisor_memo.tex sec 4 -- primary critical/flat DiD
#        (Spec A per-curve bid shape sigma_p / N_eff). Companion BSTS results
#        for prices and per-tech cleared energy are produced by
#        bsts_daily_longpre.R (headline) and bsts_daily_mh.R (robustness).
# CLAIM: Critical/flat × pre/post DiD of MTU15 reform effects on
#        (A) per-curve functionals sigma_p, N_eff;
#        (B) aggregate clearing prices;
#        (C) within-hour dispersion of quarter mean prices and offered MW
#            (post-only cross-sectional -- the Delta_p / Delta_q analogues
#            at h=140, consistent with the per-curve band).
#        Two reforms: MTU15-IDA (2025-03-19) and MTU15-DA (2025-10-01).
#        Tight windows so the reforzada level is constant across pre/post:
#          ID15 pre = 2024-12-19 to 2025-03-18 (ISP15-win, pre-reforzada)
#          ID15 post= 2025-03-19 to 2025-04-27 (40d, clean, pre-blackout)
#          DA15 pre = 2025-07-01 to 2025-09-30 (reforzada active)
#          DA15 post= 2025-10-01 to 2025-12-31 (reforzada active)
#
#        Specs (Critical+Flat hours only, all others dropped):
#          A. Y_{u,d,p} ~ post + post*crit + unit FE + date FE + session FE
#                         (per curve; outcome = sigma_p or N_eff)
#          B. Y_{d,p}   ~ post + post*crit + date FE + session FE
#                         (per period; outcome = clearing price)
#          C. D_{u,d,h} ~ crit + unit FE + date FE   (post-only, no pre)
#                         (within-hour SD/CV across 4 quarters)
#        SEs clustered by date.
#
# OUT: data/derived/panels/per_curve_metrics_ida.parquet
#      results/regressions/bid/mtu15_critical_flat/{ida,da}_specA.csv,
#                                                  {ida,da}_specB.csv,
#                                                  {ida,da}_specC.csv,
#                                                  summary.csv

from pathlib import Path
import duckdb
import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
P = REPO / "data/processed/omie"
DET = P / "mercado_diario/ofertas/det_all.parquet"
CAB = P / "mercado_diario/ofertas/cab_all.parquet"
MPDBC = P / "mercado_diario/precios/marginalpdbc_all.parquet"
IDET = P / "mercado_intradiario_subastas/ofertas/idet_all.parquet"
ICAB = P / "mercado_intradiario_subastas/ofertas/icab_all.parquet"
MPIBC = P / "mercado_intradiario_subastas/precios/marginalpibc_all.parquet"
UNITS = REPO / "data/external/omie_reference/lista_unidades.csv"
DA_PANEL = REPO / "data/derived/panels/per_curve_metrics_da.parquet"
PANEL_DIR = REPO / "data/derived/panels"
PANEL_DIR.mkdir(parents=True, exist_ok=True)
IDA_PANEL = PANEL_DIR / "per_curve_metrics_ida.parquet"
OUT_DIR = REPO / "results/regressions/bid/mtu15_critical_flat"
OUT_DIR.mkdir(parents=True, exist_ok=True)

H = 140.0
CRITICAL = {5, 6, 7, 8, 16, 17, 18, 19, 20, 21, 22}
FLAT = {1, 2, 3}
MIDDAY = {11, 12, 13, 14}  # solar-marginal low-variance falsification target
TECHS = ["CCGT", "Hydro", "Hydro_pump"]
FIRMS = ["IB", "GE", "GN", "HC"]

WINDOWS = {
    "ID15": {
        "pre_lo": "2024-12-19", "pre_hi": "2025-03-18",
        "post_lo": "2025-03-19", "post_hi": "2025-04-27",
        "reform_date": pd.Timestamp("2025-03-19"),
    },
    "DA15": {
        "pre_lo": "2025-07-01", "pre_hi": "2025-09-30",
        "post_lo": "2025-10-01", "post_hi": "2025-12-31",
        "reform_date": pd.Timestamp("2025-10-01"),
    },
}


def tech_bucket(t):
    t = str(t).lower()
    if "ciclo combinado" in t: return "CCGT"
    if "hidráulica generación" in t: return "Hydro"
    if "bombeo" in t: return "Hydro_pump"
    return "Other"


# Fuller tech bucket for per-tech cleared-MW DiD: keeps all generation
# technologies separate (CCGT, Hydro run-of-river, Hydro pump, Hydro RE,
# Wind, Solar PV, Solar thermal, Nuclear, Coal/other non-RE thermal,
# Biomass/RE thermal). Demand-side and intermediary categories collapsed.
def tech_bucket_full(t):
    t = str(t).lower()
    if "ciclo combinado" in t:                        return "CCGT"
    if "hidráulica generación" in t:                  return "Hydro_run"
    if "bombeo" in t:                                 return "Hydro_pump"
    if "re mercado hidráulica" in t:                  return "Hydro_RE"
    if "eólica" in t:                                 return "Wind"
    if "fotovolt" in t:                               return "Solar_PV"
    if "solar térmica" in t or "solar termica" in t:  return "Solar_thermal"
    if "nuclear" in t:                                return "Nuclear"
    if "térmica no renovab" in t:                     return "Coal_other_thermal"
    if "térmica renovable" in t:                      return "Biomass_RE"
    return "Other"


def firm_bucket(o):
    o = str(o).lower()
    if "iberdrola" in o: return "IB"
    if "endesa" in o: return "GE"
    if "naturgy" in o or "gas natural" in o: return "GN"
    if "edp" in o or "hidroel" in o: return "HC"
    return "OTH"


def hour_class_label(h):
    if h in CRITICAL: return "Critical"
    if h in FLAT: return "Flat"
    if h in MIDDAY: return "Midday"
    return "Other"


def clustered_ols(y, X, cluster):
    """OLS with cluster-robust SEs. X already includes an intercept column."""
    XtX_inv = np.linalg.inv(X.T @ X)
    beta = XtX_inv @ (X.T @ y)
    e = y - X @ beta
    meat = np.zeros((X.shape[1], X.shape[1]))
    for g in np.unique(cluster):
        m = cluster == g
        s = X[m].T @ e[m]
        meat += np.outer(s, s)
    G = len(np.unique(cluster))
    n, k = X.shape
    adj = (G / (G - 1)) * ((n - 1) / (n - k))
    V = adj * (XtX_inv @ meat @ XtX_inv)
    return beta, np.sqrt(np.diag(V))


# ============================================================================
# Panel builders
# ============================================================================

def build_ida_panel(lo, hi):
    """Per (unit, date, session, period) IDA in-band sigma_p and N_eff.

    AGGREGATION LEVEL: the bid-shape metrics are computed on each unit's
    COMBINED in-band supply curve, pooling every offer the unit stacks into the
    same (session, period). The GROUP BY below keys on unit_code, NOT offer_code,
    so the tranches from all of a unit's offers are merged before sigma_p,
    n_eff (1/HHI_tr), and n_tranche are formed. This is the economically correct
    object: EUPHEMIA clears the UNION of a unit's tranches, so the schedule the
    market actually faces is the merged per-unit curve, not any single offer.
    Measuring per-offer would (i) impose an artificial 5-step ceiling (the cap is
    per OFFER, not per unit -- a unit stacking k offers can present up to 5k
    steps) and (ii) miss the discrimination a unit achieves by splitting its
    schedule across offers. We do NOT aggregate further to the firm level: a
    firm's units are physically distinct assets, so the bid-shape primitive is
    per-unit. One caveat this creates -- the multi-offer SHARE itself shifts
    across the reforms, so part of the per-unit granularity change can be
    mechanical -- is checked in offer_stacking_confounder.py (single-offer-
    restricted DiD).
    """
    con = duckdb.connect()
    con.execute("SET memory_limit='12GB'")
    con.execute("SET threads=4")
    con.execute("SET preserve_insertion_order=false")
    units = pd.read_csv(UNITS)
    units["tech"] = units["technology"].apply(tech_bucket)
    units["firm"] = units["owner_agent"].apply(firm_bucket)
    units = units[units["tech"].isin(TECHS) & units["firm"].isin(FIRMS)][
        ["unit_code", "firm", "tech"]
    ].drop_duplicates("unit_code")
    con.register("u", units)
    sql = f"""
    WITH icab_l AS (
      SELECT d, session_number, offer_code, version, unit_code FROM (
        SELECT CAST(date AS DATE) d, session_number, offer_code, version, unit_code,
               ROW_NUMBER() OVER (PARTITION BY CAST(date AS DATE), session_number, offer_code, unit_code
                                  ORDER BY version DESC) rn
        FROM '{ICAB}' WHERE date BETWEEN '{lo}' AND '{hi}' AND buy_sell='V'
      ) WHERE rn=1
    ),
    idet AS (
      SELECT CAST(date AS DATE) d, session_number, offer_code, version,
             unit_code, period,
             price_eur_mwh p, quantity_mw q, COALESCE(mtu_minutes, 60) AS mtu
      FROM '{IDET}' WHERE date BETWEEN '{lo}' AND '{hi}' AND quantity_mw > 0
    ),
    mp AS (
      SELECT CAST(date AS DATE) d, session_number, period, price_es_eur_mwh p_clear,
             COALESCE(mtu_minutes, 60) AS mtu_p
      FROM '{MPIBC}' WHERE date BETWEEN '{lo}' AND '{hi}' AND price_es_eur_mwh IS NOT NULL
    ),
    inband AS (
      SELECT mp.d, mp.session_number, mp.period, c.unit_code, dv.q, dv.p,
             CASE WHEN COALESCE(mp.mtu_p, dv.mtu) = 60 THEN mp.period - 1
                  ELSE CAST(FLOOR((mp.period - 1) / 4.0) AS INT) END AS clock_hour
      FROM idet dv JOIN icab_l c
        ON dv.d=c.d AND dv.session_number=c.session_number
       AND dv.offer_code=c.offer_code AND dv.version=c.version AND dv.unit_code=c.unit_code
      JOIN mp ON mp.d=dv.d AND mp.session_number=dv.session_number AND mp.period=dv.period
      WHERE dv.p BETWEEN mp.p_clear - {H} AND mp.p_clear + {H}
    )
    SELECT i.d, i.session_number, i.period, i.clock_hour, i.unit_code, u.firm, u.tech,
           SUM(i.q) AS sum_w, SUM(i.q*i.p) AS sum_wp,
           SUM(i.q*i.p*i.p) AS sum_wp2, SUM(i.q*i.q) AS sum_w2,
           COUNT(*) AS n_tranche
    FROM inband i JOIN u ON i.unit_code = u.unit_code
    GROUP BY 1,2,3,4,5,6,7  -- by unit, NOT offer_code: merge a unit's stacked offers
    HAVING SUM(i.q) > 0
    """
    df = con.execute(sql).fetchdf()
    df["d"] = pd.to_datetime(df["d"])
    mean_p = df["sum_wp"] / df["sum_w"]
    var_p = df["sum_wp2"] / df["sum_w"] - mean_p ** 2
    df["sigma_p"] = np.sqrt(var_p.clip(lower=0))
    df["n_eff"] = df["sum_w"] ** 2 / df["sum_w2"]
    df["hour_class"] = df["clock_hour"].map(hour_class_label)
    return df[["d", "session_number", "period", "clock_hour", "hour_class",
               "unit_code", "firm", "tech", "n_tranche", "sigma_p", "n_eff",
               "sum_w", "sum_wp"]]


def build_da_cleared_mw(lo, hi, by_tech=False):
    """DA cleared MW per (date, period), optionally disaggregated by tech.
    Sum of cleared quantity from pdbc_all. Column 'p_clear' = sum MW so it
    plugs into run_spec_B unchanged. If by_tech=True, returns a dict
    {tech: panel} where each panel is the per-(date,period) sum of that
    tech's cleared MW (run_spec_B is then called once per tech)."""
    PDBC = REPO / "data/processed/omie/mercado_diario/programas/pdbc_all.parquet"
    con = duckdb.connect()
    con.execute("SET memory_limit='8GB'")
    if not by_tech:
        sql = f"""
        SELECT CAST(date AS DATE) d, CAST(NULL AS INT) session_number, period,
               SUM(assigned_power_mw) AS p_clear,
               COALESCE(mtu_minutes, 60) mtu
        FROM '{PDBC}' WHERE date BETWEEN '{lo}' AND '{hi}'
          AND assigned_power_mw > 0
        GROUP BY 1, period, mtu_minutes
        """
        df = con.execute(sql).fetchdf()
        df["d"] = pd.to_datetime(df["d"])
        df["clock_hour"] = np.where(df["mtu"] == 60, df["period"] - 1,
                                    ((df["period"] - 1) // 4).astype(int))
        df["hour_class"] = df["clock_hour"].map(hour_class_label)
        return df
    # By-tech: join lista_unidades, group by (date, period, tech).
    units = pd.read_csv(UNITS)
    units["tech_full"] = units["technology"].apply(tech_bucket_full)
    units = units[units["tech_full"] != "Other"][["unit_code", "tech_full"]].drop_duplicates("unit_code")
    con.register("u", units)
    sql = f"""
    SELECT CAST(p.date AS DATE) d, CAST(NULL AS INT) session_number, p.period,
           u.tech_full AS tech, SUM(p.assigned_power_mw) AS p_clear,
           COALESCE(p.mtu_minutes, 60) mtu
    FROM '{PDBC}' p JOIN u ON p.unit_code = u.unit_code
    WHERE p.date BETWEEN '{lo}' AND '{hi}' AND p.assigned_power_mw > 0
    GROUP BY 1, period, tech, mtu_minutes
    """
    df = con.execute(sql).fetchdf()
    df["d"] = pd.to_datetime(df["d"])
    df["clock_hour"] = np.where(df["mtu"] == 60, df["period"] - 1,
                                ((df["period"] - 1) // 4).astype(int))
    df["hour_class"] = df["clock_hour"].map(hour_class_label)
    return {t: df[df["tech"] == t].drop(columns=["tech"]).copy()
            for t in sorted(df["tech"].unique())}


def build_aggregate_price(market, lo, hi):
    """Per (date, [session,] period) clearing price."""
    con = duckdb.connect()
    con.execute("SET memory_limit='8GB'")
    if market == "DA":
        sql = f"""SELECT CAST(date AS DATE) d, CAST(NULL AS INT) session_number,
                         period, price_es_eur_mwh p_clear,
                         COALESCE(mtu_minutes, 60) mtu
                  FROM '{MPDBC}' WHERE date BETWEEN '{lo}' AND '{hi}'
                  AND price_es_eur_mwh IS NOT NULL"""
    else:
        sql = f"""SELECT CAST(date AS DATE) d, session_number, period,
                         price_es_eur_mwh p_clear, COALESCE(mtu_minutes, 60) mtu
                  FROM '{MPIBC}' WHERE date BETWEEN '{lo}' AND '{hi}'
                  AND price_es_eur_mwh IS NOT NULL"""
    df = con.execute(sql).fetchdf()
    df["d"] = pd.to_datetime(df["d"])
    df["clock_hour"] = np.where(df["mtu"] == 60, df["period"] - 1,
                                 ((df["period"] - 1) // 4).astype(int))
    df["hour_class"] = df["clock_hour"].map(hour_class_label)
    return df


def build_within_hour_dispersion(per_curve, market):
    """Post-only: per (unit, date, [session,] clock-hour) with all 4 quarters,
    SD across quarters of (MW-weighted in-band mean price) + CV of MW."""
    pc = per_curve.copy()
    pc["mean_p"] = pc["sum_wp"] / pc["sum_w"] if "sum_wp" in pc.columns else \
                   (pc["sigma_p"] * 0 + np.nan)
    # for DA panel (no sum_wp), reconstruct mean_p differently if needed -
    # the DA panel stored sigma_p/n_eff/n_tranche, not raw sums.
    # We need to rebuild from raw data for DA. So this function works on
    # per_curve panels that include sum_w and sum_wp.
    pc["quarter"] = pc["period"].mod(4)  # in-hour quarter 0..3 (for MTU15)
    group_cols = ["unit_code", "d", "clock_hour"]
    if "session_number" in pc.columns and pc["session_number"].notna().any():
        group_cols.append("session_number")
    # Require 4 quarters per cell
    nq = pc.groupby(group_cols)["quarter"].nunique().rename("nq").reset_index()
    pc = pc.merge(nq, on=group_cols)
    pc = pc[pc["nq"] == 4].copy()
    if pc.empty:
        return pd.DataFrame()
    # Per-quarter mean price (MW-weighted in-band) and MW
    pc["pq"] = pc["sum_wp"] / pc["sum_w"]
    pc["mq"] = pc["sum_w"]
    # Aggregate to (unit, date, [session], hour)
    def _cv(x):
        m = x.mean()
        return np.std(x.values, ddof=1) / m if m > 0 else np.nan

    agg = (pc.groupby(group_cols + ["hour_class"])
             .agg(D_price=("pq", lambda x: np.std(x.values, ddof=1)),
                  D_qty=("mq", _cv),
                  mean_p=("pq", "mean"),
                  mean_q=("mq", "mean"))
             .reset_index())
    return agg


def build_price_dispersion(price_panel):
    """Within-hour SD of clearing prices across 4 quarters (post-only system level)."""
    pp = price_panel.copy()
    pp["quarter"] = pp["period"].mod(4)
    group_cols = ["d", "clock_hour"]
    if "session_number" in pp.columns and pp["session_number"].notna().any():
        group_cols.append("session_number")
    nq = pp.groupby(group_cols)["quarter"].nunique().rename("nq").reset_index()
    pp = pp.merge(nq, on=group_cols)
    pp = pp[pp["nq"] == 4].copy()
    if pp.empty:
        return pd.DataFrame()
    agg = (pp.groupby(group_cols + ["hour_class"])
             .agg(SD_price=("p_clear", lambda x: np.std(x.values, ddof=1)),
                  mean_p=("p_clear", "mean"))
             .reset_index())
    return agg


# ============================================================================
# Spec runners
# ============================================================================

def run_spec_A(panel, reform, tech_filter=None, firm_filter=None,
               treated="Critical", control="Flat"):
    """Per-curve DiD on sigma_p and N_eff. If tech_filter is set
    (e.g. 'CCGT'), restricts the panel to that tech first; firm_filter
    further restricts to a single firm. treated/control control which
    hour-classes are compared (defaults Critical vs Flat; use
    treated='Midday' for a falsification)."""
    w = WINDOWS[reform]
    pre_lo, pre_hi = pd.Timestamp(w["pre_lo"]), pd.Timestamp(w["pre_hi"])
    post_lo, post_hi = pd.Timestamp(w["post_lo"]), pd.Timestamp(w["post_hi"])
    p = panel.copy()
    p["d"] = pd.to_datetime(p["d"])
    if tech_filter is not None:
        p = p[p["tech"] == tech_filter].copy()
    if firm_filter is not None:
        p = p[p["firm"] == firm_filter].copy()
    in_pre = (p["d"] >= pre_lo) & (p["d"] <= pre_hi)
    in_post = (p["d"] >= post_lo) & (p["d"] <= post_hi)
    p = p[(in_pre | in_post) & p["hour_class"].isin([treated, control])].copy()
    if p.empty:
        return None
    p["post"] = (p["d"] >= post_lo).astype(int)
    p["crit"] = (p["hour_class"] == treated).astype(int)
    p["post_crit"] = p["post"] * p["crit"]
    out = []
    for outcome in ["sigma_p", "n_eff"]:
        # Build design matrix: intercept + post + crit + post*crit + date FE + unit FE
        # Use a parsimonious version: post + crit + post*crit, cluster SE by date.
        # (date FE not included; the DiD coefficient is interpretable without it
        # given the tight window, and date FE adds many dummies.)
        # Add unit FE via demeaning within unit for speed.
        d = p.dropna(subset=[outcome]).copy()
        if len(d) < 50:
            out.append({"outcome": outcome, "n": len(d), "DiD": np.nan,
                        "se": np.nan, "t": np.nan, "pre_crit": np.nan,
                        "post_crit": np.nan, "pre_flat": np.nan,
                        "post_flat": np.nan})
            continue
        # 2x2 means
        cell = d.groupby(["post", "crit"])[outcome].mean().unstack()
        # within-unit demean of outcome and post*crit (absorbs unit FE)
        gm = d.groupby("unit_code")[outcome].transform("mean")
        d["y_w"] = d[outcome] - gm
        for c in ["post", "crit", "post_crit"]:
            gmc = d.groupby("unit_code")[c].transform("mean")
            d[c + "_w"] = d[c] - gmc
        X = np.column_stack([np.ones(len(d)), d["post_w"].values,
                             d["crit_w"].values, d["post_crit_w"].values])
        beta, se = clustered_ols(d["y_w"].values, X, d["d"].astype(str).values)
        # beta[3] is the DiD (post*crit interaction)
        out.append({"outcome": outcome, "n": len(d),
                    "DiD": beta[3], "se": se[3], "t": beta[3] / se[3],
                    "pre_crit": cell.loc[0, 1] if (0,1) in cell.stack().index else np.nan,
                    "post_crit": cell.loc[1, 1] if (1,1) in cell.stack().index else np.nan,
                    "pre_flat": cell.loc[0, 0] if (0,0) in cell.stack().index else np.nan,
                    "post_flat": cell.loc[1, 0] if (1,0) in cell.stack().index else np.nan})
    return pd.DataFrame(out)


def run_spec_B(price_panel, reform):
    """Aggregate clearing-price DiD. Two specs reported:
       B0: raw OLS (post + crit + post*crit, no FE) -- the naive DiD,
           sensitive to seasonal/gas-price trends in the crit-flat differential.
       B1: with DATE FE + CRIT FE (post is absorbed by date FE; the DiD
           coefficient on post*crit is identified strictly off within-day
           crit-vs-flat variation across the cutover, absorbing all
           common date-level shocks including seasonal and gas-price drift).
    """
    w = WINDOWS[reform]
    pre_lo, pre_hi = pd.Timestamp(w["pre_lo"]), pd.Timestamp(w["pre_hi"])
    post_lo, post_hi = pd.Timestamp(w["post_lo"]), pd.Timestamp(w["post_hi"])
    p = price_panel.copy()
    p["d"] = pd.to_datetime(p["d"])
    in_pre = (p["d"] >= pre_lo) & (p["d"] <= pre_hi)
    in_post = (p["d"] >= post_lo) & (p["d"] <= post_hi)
    p = p[(in_pre | in_post) & p["hour_class"].isin(["Critical", "Flat"])].copy()
    if p.empty:
        return None
    p["post"] = (p["d"] >= post_lo).astype(int)
    p["crit"] = (p["hour_class"] == "Critical").astype(int)
    p["post_crit"] = p["post"] * p["crit"]
    cell = p.groupby(["post", "crit"])["p_clear"].mean().unstack()

    rows = []
    # B0 -- naive (no FE)
    X0 = np.column_stack([np.ones(len(p)), p["post"].values,
                          p["crit"].values, p["post_crit"].values])
    b0, se0 = clustered_ols(p["p_clear"].values, X0, p["d"].astype(str).values)
    rows.append({"spec": "B0_noFE", "outcome": "p_clear", "n": len(p),
                 "DiD": b0[3], "se": se0[3], "t": b0[3] / se0[3],
                 "pre_crit": cell.loc[0, 1], "post_crit": cell.loc[1, 1],
                 "pre_flat": cell.loc[0, 0], "post_flat": cell.loc[1, 0]})
    # B1 -- date FE + crit FE (post is absorbed by date FE)
    dd = pd.get_dummies(p["d"].astype(str), prefix="d", drop_first=True).astype(float).values
    X1 = np.column_stack([np.ones(len(p)), p["crit"].values.astype(float),
                          p["post_crit"].values.astype(float), dd])
    b1, se1 = clustered_ols(p["p_clear"].values, X1, p["d"].astype(str).values)
    rows.append({"spec": "B1_dateFE", "outcome": "p_clear", "n": len(p),
                 "DiD": b1[2], "se": se1[2], "t": b1[2] / se1[2],
                 "pre_crit": cell.loc[0, 1], "post_crit": cell.loc[1, 1],
                 "pre_flat": cell.loc[0, 0], "post_flat": cell.loc[1, 0]})
    return pd.DataFrame(rows)


def run_spec_C(disp_panel, reform, label):
    """Post-only cross-sectional crit-vs-flat on within-hour dispersion outcomes,
    with DATE FE (absorbed via within-date demeaning of Y and crit). SE
    clustered by date."""
    w = WINDOWS[reform]
    post_lo, post_hi = pd.Timestamp(w["post_lo"]), pd.Timestamp(w["post_hi"])
    p = disp_panel.copy()
    p["d"] = pd.to_datetime(p["d"])
    p = p[(p["d"] >= post_lo) & (p["d"] <= post_hi)
          & p["hour_class"].isin(["Critical", "Flat"])].copy()
    if p.empty:
        return None
    p["crit"] = (p["hour_class"] == "Critical").astype(int)
    out = []
    outcomes = [c for c in ["D_price", "D_qty", "SD_price"] if c in p.columns]
    for outcome in outcomes:
        d = p.dropna(subset=[outcome]).copy()
        if len(d) < 30:
            continue
        means = d.groupby("crit")[outcome].mean()
        # Within-date demean Y and crit (absorbs date FE).
        d["y_d"] = d[outcome] - d.groupby("d")[outcome].transform("mean")
        d["crit_d"] = d["crit"] - d.groupby("d")["crit"].transform("mean")
        X = np.column_stack([np.ones(len(d)), d["crit_d"].values])
        beta, se = clustered_ols(d["y_d"].values, X, d["d"].astype(str).values)
        out.append({"label": label, "outcome": outcome, "n": len(d),
                    "theta_crit": beta[1], "se": se[1], "t": beta[1] / se[1],
                    "mean_flat": means.get(0, np.nan),
                    "mean_crit": means.get(1, np.nan)})
    return pd.DataFrame(out)


# ============================================================================
# Main
# ============================================================================

def main():
    # ---- IDA panel (build once for the ID15 reform window) ------------------
    print("Building IDA per-curve panel for ID15 window...")
    ida = build_ida_panel(WINDOWS["ID15"]["pre_lo"], WINDOWS["ID15"]["post_hi"])
    print(f"  {len(ida):,} IDA in-band curves")
    ida.to_parquet(IDA_PANEL, index=False)

    # ---- DA panel (load existing, but we need raw sums to compute within-hour
    # dispersion -- rebuild a slim DA per-curve panel with sum_w/sum_wp) ------
    print("Building slim DA per-curve panel (sum_w, sum_wp) for both windows...")
    da_window_lo = WINDOWS["DA15"]["pre_lo"]
    da_window_hi = WINDOWS["DA15"]["post_hi"]
    con = duckdb.connect()
    con.execute("SET memory_limit='12GB'"); con.execute("SET threads=4")
    units = pd.read_csv(UNITS)
    units["tech"] = units["technology"].apply(tech_bucket)
    units["firm"] = units["owner_agent"].apply(firm_bucket)
    units = units[units["tech"].isin(TECHS) & units["firm"].isin(FIRMS)][
        ["unit_code", "firm", "tech"]].drop_duplicates("unit_code")
    con.register("u", units)
    sql = f"""
    WITH cab_l AS (
      SELECT CAST(date AS DATE) d, offer_code, unit_code FROM (
        SELECT CAST(date AS DATE) date, offer_code, unit_code,
               ROW_NUMBER() OVER (PARTITION BY CAST(date AS DATE), offer_code, unit_code
                                  ORDER BY version DESC) rn
        FROM '{CAB}' WHERE date BETWEEN '{da_window_lo}' AND '{da_window_hi}' AND buy_sell='V'
      ) WHERE rn=1
    ),
    det AS (
      SELECT CAST(date AS DATE) d, offer_code, period,
             price_eur_mwh p, quantity_mw q, COALESCE(mtu_minutes, 60) AS mtu
      FROM '{DET}' WHERE date BETWEEN '{da_window_lo}' AND '{da_window_hi}'
        AND quantity_mw > 0
    ),
    mp AS (
      SELECT CAST(date AS DATE) d, period, price_es_eur_mwh p_clear,
             COALESCE(mtu_minutes, 60) mtu_p
      FROM '{MPDBC}' WHERE date BETWEEN '{da_window_lo}' AND '{da_window_hi}'
        AND price_es_eur_mwh IS NOT NULL
    ),
    inband AS (
      SELECT mp.d, mp.period, c.unit_code, dv.q, dv.p,
             CASE WHEN COALESCE(mp.mtu_p, dv.mtu) = 60 THEN mp.period - 1
                  ELSE CAST(FLOOR((mp.period - 1) / 4.0) AS INT) END AS clock_hour
      FROM det dv JOIN cab_l c ON dv.d=c.d AND dv.offer_code=c.offer_code
        JOIN mp ON mp.d=dv.d AND mp.period=dv.period
      WHERE dv.p BETWEEN mp.p_clear - {H} AND mp.p_clear + {H}
    )
    SELECT i.d, i.period, i.clock_hour, i.unit_code, u.firm, u.tech,
           SUM(i.q) sum_w, SUM(i.q*i.p) sum_wp, SUM(i.q*i.p*i.p) sum_wp2,
           SUM(i.q*i.q) sum_w2, COUNT(*) n_tranche
    FROM inband i JOIN u ON i.unit_code = u.unit_code
    GROUP BY 1,2,3,4,5,6 HAVING SUM(i.q) > 0  -- by unit, NOT offer_code (see build_ida_panel docstring)
    """
    da = con.execute(sql).fetchdf()
    da["d"] = pd.to_datetime(da["d"])
    mean_p = da["sum_wp"] / da["sum_w"]
    var_p = da["sum_wp2"] / da["sum_w"] - mean_p ** 2
    da["sigma_p"] = np.sqrt(var_p.clip(lower=0))
    da["n_eff"] = da["sum_w"] ** 2 / da["sum_w2"]
    da["hour_class"] = da["clock_hour"].map(hour_class_label)
    print(f"  {len(da):,} DA in-band curves (DA15 window)")

    # ---- Spec A: per-curve DiD (pooled + per-tech + per-CCGT-firm) -----------
    print("\n=== Spec A: per-curve DiD (sigma_p, N_eff), pooled + per-tech "
          "+ per-CCGT-firm ===")
    a_rows = []
    for reform, panel, market in [("ID15", ida, "IDA"), ("DA15", da, "DA")]:
        # Pooled (all techs)
        a_p = run_spec_A(panel, reform, tech_filter=None)
        if a_p is not None:
            a_p.insert(0, "reform", reform); a_p.insert(1, "market", market)
            a_p.insert(2, "tech", "All"); a_p.insert(3, "firm", "All")
            a_rows.append(a_p)
        # Per tech (firm-pooled)
        for tech in TECHS:
            a_t = run_spec_A(panel, reform, tech_filter=tech)
            if a_t is not None:
                a_t.insert(0, "reform", reform); a_t.insert(1, "market", market)
                a_t.insert(2, "tech", tech); a_t.insert(3, "firm", "All")
                a_rows.append(a_t)
        # Per CCGT firm (Idea 1: firm-level heterogeneity within CCGT)
        for firm in FIRMS:
            a_f = run_spec_A(panel, reform, tech_filter="CCGT", firm_filter=firm)
            if a_f is not None:
                a_f.insert(0, "reform", reform); a_f.insert(1, "market", market)
                a_f.insert(2, "tech", "CCGT"); a_f.insert(3, "firm", firm)
                a_rows.append(a_f)
    a = pd.concat(a_rows, ignore_index=True)
    print(a.to_string(index=False))
    a.to_csv(OUT_DIR / "specA_per_curve_did.csv", index=False)

    # ---- Spec A FALSIFICATION on Midday (Idea 6) -----------------------------
    print("\n=== Spec A FALSIFICATION: midday (11-14) vs flat -- DiD should "
          "be ~0 (solar-marginal hours have no within-hour variation for "
          "MTU15 to unlock) ===")
    f_rows = []
    for reform, panel, market in [("ID15", ida, "IDA"), ("DA15", da, "DA")]:
        # Pooled
        a_p = run_spec_A(panel, reform, treated="Midday", control="Flat")
        if a_p is not None:
            a_p.insert(0, "reform", reform); a_p.insert(1, "market", market)
            a_p.insert(2, "tech", "All"); a_rows_falsif = a_p
            f_rows.append(a_p)
        for tech in TECHS:
            a_t = run_spec_A(panel, reform, tech_filter=tech,
                             treated="Midday", control="Flat")
            if a_t is not None:
                a_t.insert(0, "reform", reform); a_t.insert(1, "market", market)
                a_t.insert(2, "tech", tech); f_rows.append(a_t)
    if f_rows:
        f = pd.concat(f_rows, ignore_index=True)
        print(f.to_string(index=False))
        f.to_csv(OUT_DIR / "specA_falsification_midday.csv", index=False)

    # ---- Additional robustness suite for Spec A pooled -----------------------
    print("\n=== Additional robustness (Spec A pooled, sigma_p + N_eff) ===")
    # R1: extend ID15 pre window back to 2024-06-14 (3-sess + ISP15-win)
    WINDOWS["ID15_ext"] = {
        "pre_lo": "2024-06-14", "pre_hi": "2025-03-18",
        "post_lo": "2025-03-19", "post_hi": "2025-04-27",
        "reform_date": pd.Timestamp("2025-03-19"),
    }
    # R2a/b: placebo reform date in mid-pre (DiD should be ~ 0)
    WINDOWS["ID15_placebo"] = {
        "pre_lo": "2024-12-19", "pre_hi": "2025-01-30",
        "post_lo": "2025-01-31", "post_hi": "2025-03-18",
        "reform_date": pd.Timestamp("2025-01-31"),
    }
    WINDOWS["DA15_placebo"] = {
        "pre_lo": "2025-07-01", "pre_hi": "2025-08-14",
        "post_lo": "2025-08-15", "post_hi": "2025-09-30",
        "reform_date": pd.Timestamp("2025-08-15"),
    }
    # R3: weekday-only filter (Mon-Fri)
    ida_wd = ida[ida["d"].dt.dayofweek < 5].copy()
    da_wd = da[da["d"].dt.dayofweek < 5].copy()

    # R1 needs a wider IDA panel (the default `ida` covers only Dec 2024 onward).
    print("    building extended IDA panel for R1 (pre back to 2024-06-14)...")
    ida_ext = build_ida_panel(WINDOWS["ID15_ext"]["pre_lo"],
                              WINDOWS["ID15_ext"]["post_hi"])
    print(f"      {len(ida_ext):,} extended IDA curves")

    # R4: extend the flat control set. Hour 0 (sigma_within=412 MW) is the
    # next-quietest hour after the {1,2,3} flat trio (sigma_within in [336,357]);
    # hour 23 (510 MW) and hour 4 (587 MW) are already in evening fall-off /
    # morning ramp respectively. R4a adds hour 0; R4b adds hours 0 and 23.
    def _relabel_flat(panel, extra_hours):
        p = panel.copy()
        p.loc[p["clock_hour"].isin(extra_hours), "hour_class"] = "Flat"
        return p
    ida_f4a = _relabel_flat(ida, [0])
    da_f4a  = _relabel_flat(da,  [0])
    ida_f4b = _relabel_flat(ida, [0, 23])
    da_f4b  = _relabel_flat(da,  [0, 23])

    # R5: cross-border control for ID15. The cross-border DiD on net flow
    # is +8,965 MW (t=5.12) at the MTU15-IDA cutover -- a sibling treatment
    # effect on the same reform (the pan-European IDA SIDC auctions and XBID
    # continuous moved 60->15 min on 2025-03-19). Adding the hourly net
    # cross-border flow as a linear control partials out the cross-border
    # channel; the residual theta is the bid-shape effect net of cross-border-
    # allocation-timing.
    def _add_xb_control(panel, lo, hi):
        from pathlib import Path as _P
        IND = REPO / "data/processed/esios/indicators"
        sql_xb = f"""
        SELECT date, hour,
               SUM(COALESCE(f1.value,0) + COALESCE(f3.value,0)
                 - COALESCE(f2.value,0) - COALESCE(f4.value,0)) AS xb_net_mw
        FROM '{IND}/535.parquet' f1
        FULL JOIN '{IND}/536.parquet' f2 USING (date, hour)
        FULL JOIN '{IND}/539.parquet' f3 USING (date, hour)
        FULL JOIN '{IND}/540.parquet' f4 USING (date, hour)
        WHERE date BETWEEN '{lo}' AND '{hi}' GROUP BY date, hour
        """
        xb = duckdb.connect().execute(sql_xb).fetchdf()
        xb["d"] = pd.to_datetime(xb["date"])
        xb = xb[["d", "hour", "xb_net_mw"]].rename(columns={"hour": "clock_hour"})
        p = panel.merge(xb, on=["d", "clock_hour"], how="left")
        p["xb_net_mw"] = p["xb_net_mw"].fillna(0)
        return p

    def run_spec_A_xb(panel, reform):
        """Spec A with hourly cross-border net flow as a linear control."""
        w = WINDOWS[reform]
        pre_lo, pre_hi = pd.Timestamp(w["pre_lo"]), pd.Timestamp(w["pre_hi"])
        post_lo, post_hi = pd.Timestamp(w["post_lo"]), pd.Timestamp(w["post_hi"])
        p = panel.copy()
        p["d"] = pd.to_datetime(p["d"])
        in_pre = (p["d"] >= pre_lo) & (p["d"] <= pre_hi)
        in_post = (p["d"] >= post_lo) & (p["d"] <= post_hi)
        p = p[(in_pre | in_post) & p["hour_class"].isin(["Critical", "Flat"])].copy()
        if p.empty:
            return None
        p["post"] = (p["d"] >= post_lo).astype(int)
        p["crit"] = (p["hour_class"] == "Critical").astype(int)
        p["post_crit"] = p["post"] * p["crit"]
        out = []
        for outcome in ["sigma_p", "n_eff"]:
            d = p.dropna(subset=[outcome, "xb_net_mw"]).copy()
            if len(d) < 50: continue
            gm = d.groupby("unit_code")[outcome].transform("mean")
            d["y_w"] = d[outcome] - gm
            for c in ["post", "crit", "post_crit", "xb_net_mw"]:
                gmc = d.groupby("unit_code")[c].transform("mean")
                d[c + "_w"] = d[c] - gmc
            X = np.column_stack([np.ones(len(d)), d["post_w"].values,
                                 d["crit_w"].values, d["post_crit_w"].values,
                                 d["xb_net_mw_w"].values])
            beta, se = clustered_ols(d["y_w"].values, X, d["d"].astype(str).values)
            out.append({"outcome": outcome, "n": len(d),
                        "DiD": beta[3], "se": se[3], "t": beta[3] / se[3],
                        "beta_xb": beta[4], "se_xb": se[4]})
        return pd.DataFrame(out)

    print("\n  building IDA panel with xb control for R5...")
    ida_xb = _add_xb_control(ida, WINDOWS["ID15"]["pre_lo"],
                                  WINDOWS["ID15"]["post_hi"])

    rob_specs = [
        ("R1_ID15_ext_pre",    run_spec_A(ida_ext, "ID15_ext")),
        ("R2a_ID15_placebo",   run_spec_A(ida, "ID15_placebo")),
        ("R2b_DA15_placebo",   run_spec_A(da, "DA15_placebo")),
        ("R3a_ID15_weekday",   run_spec_A(ida_wd, "ID15")),
        ("R3b_DA15_weekday",   run_spec_A(da_wd, "DA15")),
        ("R4a_ID15_flat0",     run_spec_A(ida_f4a, "ID15")),
        ("R4a_DA15_flat0",     run_spec_A(da_f4a,  "DA15")),
        ("R4b_ID15_flat023",   run_spec_A(ida_f4b, "ID15")),
        ("R4b_DA15_flat023",   run_spec_A(da_f4b,  "DA15")),
        ("R5_ID15_xb_control", run_spec_A_xb(ida_xb, "ID15")),
    ]
    rob_rows = []
    for label, r in rob_specs:
        if r is None:
            continue
        r = r.copy()
        r.insert(0, "label", label)
        rob_rows.append(r)
        for _, row in r.iterrows():
            print(f"  {label:24s} {row['outcome']:8s}  DiD={row['DiD']:+8.3f}  "
                  f"se={row['se']:6.3f}  t={row['t']:+6.2f}  n={int(row['n']):,}")
    if rob_rows:
        pd.concat(rob_rows, ignore_index=True).to_csv(
            OUT_DIR / "robustness_specA.csv", index=False)

    # ---- Spec B: aggregate clearing-price DiD --------------------------------
    print("\n=== Spec B: aggregate clearing-price DiD ===")
    ida_prices = build_aggregate_price("IDA", WINDOWS["ID15"]["pre_lo"],
                                       WINDOWS["ID15"]["post_hi"])
    da_prices = build_aggregate_price("DA", WINDOWS["DA15"]["pre_lo"],
                                      WINDOWS["DA15"]["post_hi"])
    b_ida = run_spec_B(ida_prices, "ID15")
    b_ida.insert(0, "reform", "ID15"); b_ida.insert(1, "market", "IDA")
    b_da = run_spec_B(da_prices, "DA15")
    b_da.insert(0, "reform", "DA15"); b_da.insert(1, "market", "DA")
    # Add cleared-MW outcome for DA15 (Idea 4: quantity-side parallel to price)
    print("  building DA cleared-MW panel (sum across units, per period)...")
    da_cleared = build_da_cleared_mw(WINDOWS["DA15"]["pre_lo"],
                                      WINDOWS["DA15"]["post_hi"])
    b_da_q = run_spec_B(da_cleared, "DA15")
    if b_da_q is not None:
        b_da_q.insert(0, "reform", "DA15"); b_da_q.insert(1, "market", "DA")
        b_da_q["outcome"] = "cleared_MW"
    b = pd.concat([b_ida, b_da, b_da_q] if b_da_q is not None else
                  [b_ida, b_da], ignore_index=True)
    print(b.to_string(index=False))
    b.to_csv(OUT_DIR / "specB_aggregate_price_did.csv", index=False)

    # ---- Per-tech cleared-MW DiD (DA15 only): is the -2,115 MW withholding
    # signature concentrated in CCGT, or system-wide?
    print("\n=== Spec B: per-tech DA cleared-MW DiD (DA15) ===")
    da_cleared_by_tech = build_da_cleared_mw(WINDOWS["DA15"]["pre_lo"],
                                              WINDOWS["DA15"]["post_hi"],
                                              by_tech=True)
    bt_rows = []
    for tech in ["CCGT", "Hydro_run", "Hydro_pump", "Hydro_RE", "Wind",
                 "Solar_PV", "Solar_thermal", "Nuclear",
                 "Coal_other_thermal", "Biomass_RE"]:
        panel = da_cleared_by_tech.get(tech)
        if panel is None or panel.empty:
            continue
        r = run_spec_B(panel, "DA15")
        if r is None:
            continue
        r.insert(0, "reform", "DA15"); r.insert(1, "market", "DA")
        r["tech"] = tech; r["outcome"] = "cleared_MW"
        # Keep only the date-FE spec for the by-tech display.
        r = r[r["spec"] == "B1_dateFE"]
        bt_rows.append(r)
        row = r.iloc[0]
        print(f"  {tech:20s}  n={int(row['n']):,}  DiD={row['DiD']:+9.1f} MW  "
              f"se={row['se']:7.1f}  t={row['t']:+6.2f}  "
              f"pre_crit={row['pre_crit']:8.0f} post_crit={row['post_crit']:8.0f}")
    if bt_rows:
        pd.concat(bt_rows, ignore_index=True).to_csv(
            OUT_DIR / "specB_cleared_mw_per_tech.csv", index=False)

    # ---- Spec C: within-hour dispersion (post-only) --------------------------
    print("\n=== Spec C: within-hour dispersion (post-only) ===")
    # IDA dispersion (per-unit Delta_p / Delta_q, post-MTU15-IDA only)
    ida_disp = build_within_hour_dispersion(ida, "IDA")
    ida_pricedisp = build_price_dispersion(ida_prices)
    if not ida_pricedisp.empty:
        # Merge system-level SD_price into the dispersion frame as a separate panel
        pass
    da_disp = build_within_hour_dispersion(da, "DA")
    da_pricedisp = build_price_dispersion(da_prices)

    c_parts = []
    for reform, disp, label in [("ID15", ida_disp, "IDA per-unit"),
                                 ("DA15", da_disp, "DA per-unit")]:
        if disp is None or disp.empty:
            continue
        r = run_spec_C(disp, reform, label)
        if r is not None:
            r.insert(0, "reform", reform)
            c_parts.append(r)
    for reform, disp, label in [("ID15", ida_pricedisp, "IDA system price"),
                                 ("DA15", da_pricedisp, "DA system price")]:
        if disp is None or disp.empty:
            continue
        r = run_spec_C(disp, reform, label)
        if r is not None:
            r.insert(0, "reform", reform)
            c_parts.append(r)
    if c_parts:
        c = pd.concat(c_parts, ignore_index=True)
        print(c.to_string(index=False))
        c.to_csv(OUT_DIR / "specC_within_hour_dispersion.csv", index=False)
    else:
        print("  (no Spec C results)")

    print(f"\nAll outputs in {OUT_DIR}")


if __name__ == "__main__":
    main()
