# STATUS: ALIVE
# LAST-AUDIT: 2026-06-17
# FEEDS: thesis appendix robustness -- a SCALE-INVARIANT price-side bid-shape
#        metric, the price-gap Herfindahl HHI_price = sum_k (dp_k / R)^2 over the
#        consecutive in-band price gaps dp_k = p_{(k+1)}-p_{(k)}, normalised by
#        the in-band price range R = p_max - p_min. Invariant to any affine map
#        p -> lambda p + c (unlike sigma_p, which carries the multiplicative
#        scale, and the coefficient of variation, which blows up at p~0).
#        Runs the SAME critical/flat within-day DiD as the headline bid-shape
#        table, per technology, for the two own-market headline cells (ID15 IDA,
#        DA15 DA), alongside sigma_p and HHI_tr on the identical curves, plus a
#        pre-period parallel-trends test on the critical-minus-flat HHI_price gap.
#        Per-window delta (ID15 IDA = 62, DA15 DA = 50). Sessions pooled with
#        per-curve session identity (matches perfirm_sigma_hhi_did).
#
# OUT: results/regressions/bid/mtu15_critical_flat/hhi_price_did.csv
#      results/regressions/bid/mtu15_critical_flat/hhi_price_pretrend.csv

from pathlib import Path
import sys
import duckdb
import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO / "scripts/analysis/bid"))
from mtu15_critical_flat_did import (  # noqa: E402
    CAB, DET, MPDBC, ICAB, IDET, MPIBC, UNITS,
    tech_bucket, firm_bucket, hour_class_label, clustered_ols, WINDOWS)

OUTDIR = REPO / "results/regressions/bid/mtu15_critical_flat"
TECHS = ["CCGT", "Hydro", "Hydro_pump", "Wind"]
# headline own-market cells with their per-window p90 bandwidth
CELLS = {
    "ID15 IDA": {"market": "ida", "band": 62.0,
                 "win": (WINDOWS["ID15"]["pre_lo"], WINDOWS["ID15"]["pre_hi"],
                         WINDOWS["ID15"]["post_lo"], WINDOWS["ID15"]["post_hi"])},
    "DA15 DA":  {"market": "da", "band": 50.0,
                 "win": (WINDOWS["DA15"]["pre_lo"], WINDOWS["DA15"]["pre_hi"],
                         WINDOWS["DA15"]["post_lo"], WINDOWS["DA15"]["post_hi"])},
}


def units_df():
    u = pd.read_csv(UNITS)
    u["tech"] = u["technology"].apply(tech_bucket)
    u = u[u["tech"].isin(TECHS)][["unit_code", "tech"]].drop_duplicates("unit_code")
    return u


def build(market, lo, hi, band):
    """Per-curve in-band panel: sigma_p / HHI_tr moments PLUS the price-gap
    statistics (sum of squared consecutive gaps and the price range) over the
    distinct in-band prices of each curve."""
    con = duckdb.connect(); con.execute("SET memory_limit='14GB'"); con.execute("SET threads=4")
    con.register("u", units_df())
    if market == "da":
        inband = f"""
        WITH cab_l AS (
          SELECT CAST(date AS DATE) d, offer_code, unit_code FROM (
            SELECT CAST(date AS DATE) date, offer_code, unit_code,
                   ROW_NUMBER() OVER (PARTITION BY CAST(date AS DATE), offer_code, unit_code ORDER BY version DESC) rn
            FROM '{CAB}' WHERE date BETWEEN '{lo}' AND '{hi}' AND buy_sell='V') WHERE rn=1),
        det AS (SELECT CAST(date AS DATE) d, offer_code, period, price_eur_mwh p, quantity_mw q,
                       COALESCE(mtu_minutes,60) mtu FROM '{DET}'
                WHERE date BETWEEN '{lo}' AND '{hi}' AND quantity_mw>0),
        mp AS (SELECT CAST(date AS DATE) d, period, price_es_eur_mwh pc, COALESCE(mtu_minutes,60) mtu_p
               FROM '{MPDBC}' WHERE date BETWEEN '{lo}' AND '{hi}' AND price_es_eur_mwh IS NOT NULL),
        inband AS (
          SELECT mp.d, 0 AS sess, mp.period, c.unit_code, u.tech, dv.q, dv.p,
                 CASE WHEN COALESCE(mp.mtu_p,dv.mtu)=60 THEN mp.period-1 ELSE CAST(FLOOR((mp.period-1)/4.0) AS INT) END clock_hour
          FROM det dv JOIN cab_l c ON dv.d=c.d AND dv.offer_code=c.offer_code
          JOIN u ON c.unit_code=u.unit_code
          JOIN mp ON mp.d=dv.d AND mp.period=dv.period WHERE dv.p BETWEEN mp.pc-{band} AND mp.pc+{band})
        """
    else:
        inband = f"""
        WITH icab_l AS (
          SELECT d, session_number, offer_code, version, unit_code FROM (
            SELECT CAST(date AS DATE) d, session_number, offer_code, version, unit_code,
                   ROW_NUMBER() OVER (PARTITION BY CAST(date AS DATE), session_number, offer_code, unit_code ORDER BY version DESC) rn
            FROM '{ICAB}' WHERE date BETWEEN '{lo}' AND '{hi}' AND buy_sell='V') WHERE rn=1),
        idet AS (SELECT CAST(date AS DATE) d, session_number, offer_code, version, unit_code, period,
                        price_eur_mwh p, quantity_mw q, COALESCE(mtu_minutes,60) mtu FROM '{IDET}'
                 WHERE date BETWEEN '{lo}' AND '{hi}' AND quantity_mw>0),
        mp AS (SELECT CAST(date AS DATE) d, session_number, period, price_es_eur_mwh pc, COALESCE(mtu_minutes,60) mtu_p
               FROM '{MPIBC}' WHERE date BETWEEN '{lo}' AND '{hi}' AND price_es_eur_mwh IS NOT NULL),
        inband AS (
          SELECT mp.d, mp.session_number AS sess, mp.period, c.unit_code, u.tech, dv.q, dv.p,
                 CASE WHEN COALESCE(mp.mtu_p,dv.mtu)=60 THEN mp.period-1 ELSE CAST(FLOOR((mp.period-1)/4.0) AS INT) END clock_hour
          FROM idet dv JOIN icab_l c ON dv.d=c.d AND dv.session_number=c.session_number
            AND dv.offer_code=c.offer_code AND dv.version=c.version AND dv.unit_code=c.unit_code
          JOIN u ON c.unit_code=u.unit_code
          JOIN mp ON mp.d=dv.d AND mp.session_number=dv.session_number AND mp.period=dv.period
          WHERE dv.p BETWEEN mp.pc-{band} AND mp.pc+{band})
        """
    sql = inband + """,
    moments AS (
      SELECT d, sess, period, clock_hour, unit_code, tech,
             SUM(q) sum_w, SUM(q*p) sum_wp, SUM(q*p*p) sum_wp2, SUM(q*q) sum_w2
      FROM inband GROUP BY 1,2,3,4,5,6 HAVING SUM(q)>0),
    dprice AS (SELECT DISTINCT d, sess, period, unit_code, p FROM inband),
    gaps AS (
      SELECT d, sess, period, unit_code, p,
             p - LAG(p) OVER (PARTITION BY d, sess, period, unit_code ORDER BY p) gap
      FROM dprice),
    gapstats AS (
      SELECT d, sess, period, unit_code, SUM(gap*gap) sum_gap2,
             MAX(p)-MIN(p) p_range, COUNT(*) n_price
      FROM gaps GROUP BY 1,2,3,4)
    SELECT m.d, m.sess, m.period, m.clock_hour, m.unit_code, m.tech,
           m.sum_w, m.sum_wp, m.sum_wp2, m.sum_w2,
           g.sum_gap2, g.p_range, g.n_price
    FROM moments m JOIN gapstats g USING (d, sess, period, unit_code)
    """
    df = con.execute(sql).fetchdf()
    df["d"] = pd.to_datetime(df["d"])
    pbar = df["sum_wp"] / df["sum_w"]
    df["sigma_p"] = np.sqrt((df["sum_wp2"] / df["sum_w"] - pbar**2).clip(lower=0))
    df["hhi_tr"] = df["sum_w2"] / df["sum_w"]**2 * 100.0            # tranche-HHI x100
    # price-gap Herfindahl, only where there are >=2 distinct in-band prices
    valid = (df["n_price"] >= 2) & (df["p_range"] > 0)
    df["hhi_price"] = np.where(valid, df["sum_gap2"] / df["p_range"]**2 * 100.0, np.nan)
    df["hour_class"] = df["clock_hour"].map(hour_class_label)
    return df


def did(panel, tech, win, outcome):
    pre_lo, pre_hi, post_lo, post_hi = map(pd.Timestamp, win)
    q = panel[panel["tech"] == tech].copy()
    inw = ((q.d >= pre_lo) & (q.d <= pre_hi)) | ((q.d >= post_lo) & (q.d <= post_hi))
    q = q[inw & q["hour_class"].isin(["Critical", "Flat"])].dropna(subset=[outcome])
    if len(q) < 50 or q["unit_code"].nunique() < 2:
        return (np.nan, np.nan, np.nan, len(q))
    q["post"] = (q.d >= post_lo).astype(int); q["crit"] = (q.hour_class == "Critical").astype(int)
    q["pc"] = q["post"] * q["crit"]
    q["y_w"] = q[outcome] - q.groupby("unit_code")[outcome].transform("mean")
    for c in ["post", "crit", "pc"]:
        q[c + "_w"] = q[c] - q.groupby("unit_code")[c].transform("mean")
    X = np.column_stack([np.ones(len(q)), q.post_w, q.crit_w, q.pc_w])
    b, se = clustered_ols(q["y_w"].values, X, q.d.astype(str).values)
    return (b[3], se[3], b[3] / se[3] if se[3] else np.nan, len(q))


def pretrend(panel, tech, win, outcome):
    """Pre-period parallel-trends test: daily critical-minus-flat gap of the
    outcome, regressed on a linear day index. A flat (insignificant) slope is
    consistent with parallel trends absent the reform."""
    pre_lo, pre_hi = pd.Timestamp(win[0]), pd.Timestamp(win[1])
    q = panel[(panel["tech"] == tech) & (panel.d >= pre_lo) & (panel.d <= pre_hi)].copy()
    q = q[q["hour_class"].isin(["Critical", "Flat"])].dropna(subset=[outcome])
    if q.empty:
        return (np.nan, np.nan, 0)
    g = q.groupby(["d", "hour_class"])[outcome].mean().unstack()
    g = g.dropna(subset=["Critical", "Flat"])
    if len(g) < 8:
        return (np.nan, np.nan, len(g))
    gap = (g["Critical"] - g["Flat"]).values
    t = (g.index - g.index.min()).days.values.astype(float)
    X = np.column_stack([np.ones(len(t)), t])
    b, se = clustered_ols(gap, X, np.arange(len(t)).astype(str))  # plain OLS SE
    return (b[1], b[1] / se[1] if se[1] else np.nan, len(g))


def star(t):
    a = abs(t); return "***" if a >= 2.58 else "**" if a >= 1.96 else "*" if a >= 1.645 else ""


def main():
    OUTDIR.mkdir(parents=True, exist_ok=True)
    rows, pre_rows = [], []
    for label, cfg in CELLS.items():
        print(f"\n=== {label}  (delta={cfg['band']:g}) ===", flush=True)
        panel = build(cfg["market"], cfg["win"][0], cfg["win"][3], cfg["band"])
        print(f"  curves: {len(panel):,}   with >=2 in-band prices: "
              f"{panel['hhi_price'].notna().mean()*100:.0f}%")
        for tech in TECHS:
            r = {"cell": label, "tech": tech}
            for outc in ["sigma_p", "hhi_tr", "hhi_price"]:
                b, se, t, n = did(panel, tech, cfg["win"], outc)
                r[outc] = b; r[outc + "_t"] = t; r[outc + "_n"] = n
            ptb, ptt, ng = pretrend(panel, tech, cfg["win"], "hhi_price")
            r["hhi_price_pretrend_slope"] = ptb; r["hhi_price_pretrend_t"] = ptt
            rows.append(r)
            pre_rows.append({"cell": label, "tech": tech, "pretrend_slope_per_day": ptb,
                             "pretrend_t": ptt, "n_pre_days": ng})
            print(f"  {tech:11s}  sigma_p {r['sigma_p']:+7.3f}{star(r['sigma_p_t']):3s}"
                  f"   HHI_tr {r['hhi_tr']:+7.3f}{star(r['hhi_tr_t']):3s}"
                  f"   HHI_price {r['hhi_price']:+7.3f}{star(r['hhi_price_t']):3s}"
                  f"   (n={r['hhi_price_n']:,})  pretrend t={ptt:+.2f}")
    pd.DataFrame(rows).to_csv(OUTDIR / "hhi_price_did.csv", index=False)
    pd.DataFrame(pre_rows).to_csv(OUTDIR / "hhi_price_pretrend.csv", index=False)
    print(f"\nWrote {(OUTDIR / 'hhi_price_did.csv').relative_to(REPO)}")
    print("Reading: HHI_price falls (more, more even price steps) = finer price discrimination,")
    print("so a NEGATIVE critical-flat DiD on HHI_price mirrors a POSITIVE one on sigma_p.")


if __name__ == "__main__":
    main()
