# STATUS: ALIVE
# LAST-AUDIT: 2026-06-06
# FEEDS: thesis/paper/thesis.tex --- tests Claim C from NeuroDATE_II
#        (3 Dec 2025): "los precios son más extremos en las últimas
#        subastas intradiarias". Per-session IDA bid-shape DiD at the
#        MTU15-IDA reform reveals the session-order pattern that
#        pooled-IDA analysis washes out.
#
# Approach: per (date, session, unit_code, period) in-band quadratic
#        fit at bandwidth 150 EUR/MWh around the session's clearing
#        price. Compute (sigma_p, beta, N_eff) per curve. Aggregate
#        weekly by hour-class. Run critical-flat DiD per session
#        at the MTU15-IDA reform (2025-03-19) for CCGT (supply, V) and
#        the demand side (buy_sell C). VECTORIZED with DuckDB sums.
#
# OUT: results/regressions/bid/per_session_did.csv
#      Console summary of session-order pattern.

from pathlib import Path
import duckdb
import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
ICAB = REPO / "data/processed/omie/mercado_intradiario_subastas/ofertas/icab_all.parquet"
IDET = REPO / "data/processed/omie/mercado_intradiario_subastas/ofertas/idet_all.parquet"
MPIBC = REPO / "data/processed/omie/mercado_intradiario_subastas/precios/marginalpibc_all.parquet"
UMAP = REPO / "data/derived/panels/bid_shape_critical_flat/_unit_map.parquet"
OUT = REPO / "results/regressions/bid/per_session_did.csv"

H_BAND = 150.0
CRIT = set(range(5, 9)) | set(range(16, 23))
FLAT = {1, 2, 3}

WINDOWS = [
    ("pre",  "2024-06-14", "2025-03-18"),
    ("post", "2025-03-19", "2025-04-27"),
]


def fetch_per_curve_sums(con, side, sess):
    """Per (date, sess, unit_code, period): cumulative MW for q_mid + per-curve sums.

    Returns one row per curve with the sufficient statistics for sigma_p, beta, N_eff.
    """
    buy_sell = "V" if side == "supply" else "C"
    if side == "supply":
        tech_join = "JOIN '"+str(UMAP)+"' u ON c.unit_code = u.unit_code"
        tech_filter = "AND u.tech_group = 'CCGT'"
    else:
        tech_join = ""
        tech_filter = ""
    q = f"""
    WITH mcp_raw AS (
      SELECT CAST(date AS DATE) AS d, session_number, period,
             price_es_eur_mwh AS mcp,
             ROW_NUMBER() OVER (PARTITION BY date::DATE, session_number, period
                                 ORDER BY mtu_minutes ASC) AS rn
      FROM '{MPIBC}'
      WHERE session_number = {sess}
        AND price_es_eur_mwh IS NOT NULL
    ),
    mcp AS (SELECT d, session_number, period, mcp FROM mcp_raw WHERE rn=1),
    banded AS (
      SELECT CAST(c.date AS DATE) AS d, c.session_number AS sess,
             c.offer_code, c.unit_code, d.period,
             d.price_eur_mwh AS p, d.quantity_mw AS q
      FROM '{ICAB}' c JOIN '{IDET}' d
        ON c.date = d.date AND c.session_number = d.session_number
       AND c.offer_code = d.offer_code AND c.version = d.version
      {tech_join}
      JOIN mcp m ON CAST(c.date AS DATE) = m.d
                AND c.session_number = m.session_number
                AND d.period = m.period
      WHERE c.buy_sell = '{buy_sell}'
        AND c.session_number = {sess}
        AND c.block_order_avg_price_eur IS NULL
        AND d.price_eur_mwh IS NOT NULL AND d.quantity_mw > 0
        AND ABS(d.price_eur_mwh - m.mcp) <= {H_BAND}
        {tech_filter}
    ),
    with_qcum AS (
      SELECT d, sess, unit_code, period, p, q,
             SUM(q) OVER (PARTITION BY d, sess, unit_code, period
                          ORDER BY p
                          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS q_cum
      FROM banded
    ),
    with_qmid AS (
      SELECT d, sess, unit_code, period, p, q, q_cum - q/2.0 AS q_mid
      FROM with_qcum
    )
    SELECT d, sess, unit_code, period,
           COUNT(*)            AS n,
           SUM(q)              AS sw,
           SUM(q*q)            AS sw2,
           SUM(q*p)            AS swp,
           SUM(q*p*p)          AS swpp,
           SUM(q*q_mid)        AS swq,
           SUM(q*q_mid*q_mid)  AS swqq,
           SUM(q*q_mid*p)      AS swqp
    FROM with_qmid
    GROUP BY 1, 2, 3, 4
    """
    return con.execute(q).df()


def fit_from_sums(df):
    """Compute sigma_p, beta, N_eff from the per-curve sums table."""
    df = df.copy()
    sw = df["sw"]; sw2 = df["sw2"]
    swp = df["swp"]; swpp = df["swpp"]
    swq = df["swq"]; swqq = df["swqq"]; swqp = df["swqp"]

    # MW-weighted mean of p; SD; N_eff.
    p_bar = swp / sw
    var_p = (swpp - swp * swp / sw) / sw
    sigma_p = np.sqrt(np.clip(var_p, 0, None))
    n_eff = (sw * sw) / sw2

    # Beta = Σw(q_mid-q̄)(p-p̄) / Σw(q_mid-q̄)²
    #      = (Σw q_mid p - Σw q_mid · Σw p / Σw) / (Σw q_mid² - (Σw q_mid)² / Σw)
    q_bar = swq / sw
    num_b   = swqp - (swq * swp) / sw
    denom_b = swqq - (swq * swq) / sw
    beta = np.where(denom_b > 1e-9, num_b / denom_b, np.nan)

    df["sigma_p"] = sigma_p
    df["beta"] = beta
    df["n_eff"] = n_eff
    df["n_tranche"] = df["n"].astype(float)
    return df


def did_critical_flat(res, pre_lo, pre_hi, post_lo, post_hi, outcome):
    res = res.copy()
    res["d"] = pd.to_datetime(res["d"])
    res["hour"] = res["period"].apply(
        lambda p: int(p) if int(p) <= 24 else int(np.ceil(int(p) / 4.0)))
    res["hour_class"] = res["hour"].apply(
        lambda h: "critical" if h in CRIT else ("flat" if h in FLAT else None))
    res = res.dropna(subset=["hour_class"])
    if outcome == "beta":
        res = res[res["n_tranche"] >= 4]
    pre_lo = pd.to_datetime(pre_lo); pre_hi = pd.to_datetime(pre_hi)
    post_lo = pd.to_datetime(post_lo); post_hi = pd.to_datetime(post_hi)
    res["regime"] = np.where(
        (res["d"] >= pre_lo) & (res["d"] <= pre_hi), "pre",
        np.where((res["d"] >= post_lo) & (res["d"] <= post_hi), "post", None))
    res = res.dropna(subset=["regime"])
    if len(res) == 0:
        return {"crit_pre": np.nan, "crit_post": np.nan,
                "flat_pre": np.nan, "flat_post": np.nan,
                "did": np.nan, "n_curves": 0}
    g = (res.groupby(["regime", "hour_class"], observed=True)[outcome]
            .median().reset_index())
    pivot = g.set_index(["regime", "hour_class"])[outcome]
    try:
        cp_pre  = pivot.loc[("pre",  "critical")]
        cp_post = pivot.loc[("post", "critical")]
        fl_pre  = pivot.loc[("pre",  "flat")]
        fl_post = pivot.loc[("post", "flat")]
        did = (cp_post - cp_pre) - (fl_post - fl_pre)
    except KeyError:
        cp_pre = cp_post = fl_pre = fl_post = did = np.nan
    return {"crit_pre": cp_pre, "crit_post": cp_post,
            "flat_pre": fl_pre, "flat_post": fl_post,
            "did": did, "n_curves": int(len(res))}


def main():
    con = duckdb.connect()
    con.execute("SET threads=4; SET memory_limit='6GB'")

    rows = []
    for side in ("supply", "demand"):
        for sess in (1, 2, 3):
            print(f"[{side} IDA{sess}] querying per-curve sums ...", flush=True)
            sums = fetch_per_curve_sums(con, side, sess)
            print(f"  curves: {len(sums):,}", flush=True)
            if len(sums) == 0:
                continue
            res = fit_from_sums(sums)
            for outcome in ("sigma_p", "beta", "n_eff"):
                r = did_critical_flat(res,
                                      WINDOWS[0][1], WINDOWS[0][2],
                                      WINDOWS[1][1], WINDOWS[1][2],
                                      outcome)
                rows.append({"side": side, "sess": sess, "outcome": outcome, **r})
                print(f"  {outcome:>8s}: crit pre={r['crit_pre']:.3f} -> "
                      f"post={r['crit_post']:.3f}  flat pre={r['flat_pre']:.3f} -> "
                      f"post={r['flat_post']:.3f}  DiD={r['did']:+.3f}  "
                      f"n={r['n_curves']:,}", flush=True)
    con.close()

    out_df = pd.DataFrame(rows)
    OUT.parent.mkdir(parents=True, exist_ok=True)
    out_df.to_csv(OUT, index=False)
    print(f"\nWrote {OUT}.\n")

    print("=== Per-session MTU15-IDA Spec C DiD, critical-flat (medians) ===\n")
    for side in ("supply", "demand"):
        print(f"--- {side.upper()} side ---")
        sub = out_df[out_df["side"] == side]
        if sub.empty:
            print("  (no data)\n"); continue
        piv = sub.pivot(index="outcome", columns="sess", values="did")
        piv.columns = [f"IDA{c}" for c in piv.columns]
        print(piv.round(4).to_string())
        print()


if __name__ == "__main__":
    main()
