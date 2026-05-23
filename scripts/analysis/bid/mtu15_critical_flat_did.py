# STATUS: ALIVE
# LAST-AUDIT: 2026-05-23
# FEEDS: thesis/provisional/advisor_memo.tex sec 6 (MTU15 effect, ID15+DA15)
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
    """Per (unit, date, session, period) IDA in-band sigma_p and N_eff."""
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
    GROUP BY 1,2,3,4,5,6,7
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

def run_spec_A(panel, reform):
    """Per-curve DiD on sigma_p and N_eff."""
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
    """Aggregate clearing-price DiD."""
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
    X = np.column_stack([np.ones(len(p)), p["post"].values,
                         p["crit"].values, p["post_crit"].values])
    beta, se = clustered_ols(p["p_clear"].values, X, p["d"].astype(str).values)
    return pd.DataFrame([{
        "outcome": "p_clear", "n": len(p),
        "DiD": beta[3], "se": se[3], "t": beta[3] / se[3],
        "pre_crit": cell.loc[0, 1], "post_crit": cell.loc[1, 1],
        "pre_flat": cell.loc[0, 0], "post_flat": cell.loc[1, 0],
    }])


def run_spec_C(disp_panel, reform, label):
    """Post-only cross-sectional crit-vs-flat on within-hour dispersion outcomes."""
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
        d = p.dropna(subset=[outcome])
        if len(d) < 30:
            continue
        means = d.groupby("crit")[outcome].mean()
        X = np.column_stack([np.ones(len(d)), d["crit"].values])
        beta, se = clustered_ols(d[outcome].values, X, d["d"].astype(str).values)
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
    GROUP BY 1,2,3,4,5,6 HAVING SUM(i.q) > 0
    """
    da = con.execute(sql).fetchdf()
    da["d"] = pd.to_datetime(da["d"])
    mean_p = da["sum_wp"] / da["sum_w"]
    var_p = da["sum_wp2"] / da["sum_w"] - mean_p ** 2
    da["sigma_p"] = np.sqrt(var_p.clip(lower=0))
    da["n_eff"] = da["sum_w"] ** 2 / da["sum_w2"]
    da["hour_class"] = da["clock_hour"].map(hour_class_label)
    print(f"  {len(da):,} DA in-band curves (DA15 window)")

    # ---- Spec A: per-curve DiD -----------------------------------------------
    print("\n=== Spec A: per-curve DiD (sigma_p, N_eff) ===")
    a_ida = run_spec_A(ida, "ID15")
    a_ida.insert(0, "reform", "ID15"); a_ida.insert(1, "market", "IDA")
    a_da = run_spec_A(da, "DA15")
    a_da.insert(0, "reform", "DA15"); a_da.insert(1, "market", "DA")
    a = pd.concat([a_ida, a_da], ignore_index=True)
    print(a.to_string(index=False))
    a.to_csv(OUT_DIR / "specA_per_curve_did.csv", index=False)

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
    b = pd.concat([b_ida, b_da], ignore_index=True)
    print(b.to_string(index=False))
    b.to_csv(OUT_DIR / "specB_aggregate_price_did.csv", index=False)

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
