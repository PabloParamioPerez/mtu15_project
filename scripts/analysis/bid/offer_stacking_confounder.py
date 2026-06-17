# STATUS: ALIVE
# LAST-AUDIT: 2026-06-14
# FEEDS: thesis robustness -- the in-band bid-shape metrics (sigma_p, n_eff,
#        n_tranche) are computed on each unit's COMBINED day-ahead curve, i.e.
#        across every offer the unit submits in that period. A unit may stack
#        several offers, so a change in stacking across a reform could confound
#        the critical-flat DiD. Two checks for CCGT day-ahead at both reforms:
#          (1) how prevalent is multi-offer stacking, and does it move pre/post
#              and critical-vs-flat?
#          (2) does the sigma_p / n_eff critical-flat DiD survive when we restrict
#              to SINGLE-OFFER unit-periods (where stacking cannot operate)?
#        If the single-offer DiD matches the full-panel DiD, stacking is not the
#        driver.
#
# OUT: results/regressions/bid/mtu15_critical_flat/offer_stacking_confounder.csv

from pathlib import Path
import sys
import duckdb
import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO / "scripts/analysis/bid"))
from mtu15_critical_flat_did import (  # noqa: E402
    CAB, DET, MPDBC, ICAB, IDET, MPIBC, UNITS, TECHS, tech_bucket, firm_bucket,
    hour_class_label, clustered_ols)

OUT = REPO / "results/regressions/bid/mtu15_critical_flat/offer_stacking_confounder.csv"
BAND = 50.0
# Day-ahead cells (single-offer market) at both reforms; intraday cells (where
# units stack offers) at ID15 -- the per-session IDA CCGT analysis lives here.
WIN = {
    "ID15": ("2024-12-19", "2025-03-18", "2025-03-19", "2025-04-27"),
    "DA15": ("2025-07-01", "2025-09-30", "2025-10-01", "2025-12-31"),
}
IDA_WIN = ("2024-12-19", "2025-03-18", "2025-03-19", "2025-04-27")  # ID15


def build(lo, hi, band=BAND):
    con = duckdb.connect(); con.execute("SET memory_limit='12GB'"); con.execute("SET threads=4")
    units = pd.read_csv(UNITS)
    units["tech"] = units["technology"].apply(tech_bucket)
    units["firm"] = units["owner_agent"].apply(firm_bucket)
    units = units[units["tech"] == "CCGT"][["unit_code", "firm", "tech"]].drop_duplicates("unit_code")
    con.register("u", units)
    sql = f"""
    WITH cab_l AS (
      SELECT CAST(date AS DATE) d, offer_code, unit_code FROM (
        SELECT CAST(date AS DATE) date, offer_code, unit_code,
               ROW_NUMBER() OVER (PARTITION BY CAST(date AS DATE), offer_code, unit_code ORDER BY version DESC) rn
        FROM '{CAB}' WHERE date BETWEEN '{lo}' AND '{hi}' AND buy_sell='V'
      ) WHERE rn=1
    ),
    det AS (
      SELECT CAST(date AS DATE) d, offer_code, period, price_eur_mwh p, quantity_mw q,
             COALESCE(mtu_minutes,60) mtu
      FROM '{DET}' WHERE date BETWEEN '{lo}' AND '{hi}' AND quantity_mw > 0
    ),
    mp AS (
      SELECT CAST(date AS DATE) d, period, price_es_eur_mwh p_clear, COALESCE(mtu_minutes,60) mtu_p
      FROM '{MPDBC}' WHERE date BETWEEN '{lo}' AND '{hi}' AND price_es_eur_mwh IS NOT NULL
    ),
    inband AS (
      SELECT mp.d, mp.period, c.unit_code, c.offer_code, dv.q, dv.p,
             CASE WHEN COALESCE(mp.mtu_p, dv.mtu)=60 THEN mp.period-1
                  ELSE CAST(FLOOR((mp.period-1)/4.0) AS INT) END AS clock_hour
      FROM det dv JOIN cab_l c ON dv.d=c.d AND dv.offer_code=c.offer_code
        JOIN mp ON mp.d=dv.d AND mp.period=dv.period
      WHERE dv.p BETWEEN mp.p_clear-{band} AND mp.p_clear+{band}
    )
    SELECT i.d, i.period, i.clock_hour, i.unit_code, u.firm,
           SUM(i.q) sum_w, SUM(i.q*i.p) sum_wp, SUM(i.q*i.p*i.p) sum_wp2, SUM(i.q*i.q) sum_w2,
           COUNT(*) n_tranche, COUNT(DISTINCT i.offer_code) n_offers
    FROM inband i JOIN u ON i.unit_code=u.unit_code
    GROUP BY 1,2,3,4,5 HAVING SUM(i.q) > 0
    """
    da = con.execute(sql).fetchdf()
    da["d"] = pd.to_datetime(da["d"])
    mp = da["sum_wp"]/da["sum_w"]
    da["sigma_p"] = np.sqrt((da["sum_wp2"]/da["sum_w"] - mp**2).clip(lower=0))
    da["hhi"] = da["sum_w"]**2 / da["sum_w2"]
    da["hhi"] = da["sum_w2"] / da["sum_w"]**2
    da["hour_class"] = da["clock_hour"].map(hour_class_label)
    da["tech"] = "CCGT"
    return da


def build_ida(lo, hi, band=BAND):
    con = duckdb.connect(); con.execute("SET memory_limit='12GB'"); con.execute("SET threads=4")
    units = pd.read_csv(UNITS)
    units["tech"] = units["technology"].apply(tech_bucket)
    units = units[units["tech"].isin(TECHS)][["unit_code", "tech"]].drop_duplicates("unit_code")
    con.register("u", units)
    sql = f"""
    WITH icab_l AS (
      SELECT d, session_number, offer_code, version, unit_code FROM (
        SELECT CAST(date AS DATE) d, session_number, offer_code, version, unit_code,
               ROW_NUMBER() OVER (PARTITION BY CAST(date AS DATE), session_number, offer_code, unit_code ORDER BY version DESC) rn
        FROM '{ICAB}' WHERE date BETWEEN '{lo}' AND '{hi}' AND buy_sell='V') WHERE rn=1
    ),
    idet AS (
      SELECT CAST(date AS DATE) d, session_number, offer_code, version, unit_code, period,
             price_eur_mwh p, quantity_mw q, COALESCE(mtu_minutes,60) mtu
      FROM '{IDET}' WHERE date BETWEEN '{lo}' AND '{hi}' AND quantity_mw>0
    ),
    mp AS (
      SELECT CAST(date AS DATE) d, session_number, period, price_es_eur_mwh p_clear, COALESCE(mtu_minutes,60) mtu_p
      FROM '{MPIBC}' WHERE date BETWEEN '{lo}' AND '{hi}' AND price_es_eur_mwh IS NOT NULL
    ),
    inband AS (
      SELECT mp.d, mp.session_number, mp.period, c.unit_code, c.offer_code, dv.q, dv.p,
             CASE WHEN COALESCE(mp.mtu_p,dv.mtu)=60 THEN mp.period-1 ELSE CAST(FLOOR((mp.period-1)/4.0) AS INT) END AS clock_hour
      FROM idet dv JOIN icab_l c ON dv.d=c.d AND dv.session_number=c.session_number
        AND dv.offer_code=c.offer_code AND dv.version=c.version AND dv.unit_code=c.unit_code
      JOIN mp ON mp.d=dv.d AND mp.session_number=dv.session_number AND mp.period=dv.period
      WHERE dv.p BETWEEN mp.p_clear-{band} AND mp.p_clear+{band}
    )
    SELECT i.d, i.session_number, i.period, i.clock_hour, i.unit_code, u.tech,
           SUM(i.q) sum_w, SUM(i.q*i.p) sum_wp, SUM(i.q*i.p*i.p) sum_wp2, SUM(i.q*i.q) sum_w2,
           COUNT(*) n_tranche, COUNT(DISTINCT i.offer_code) n_offers
    FROM inband i JOIN u ON i.unit_code=u.unit_code GROUP BY 1,2,3,4,5,6 HAVING SUM(i.q)>0
    """
    df = con.execute(sql).fetchdf(); df["d"] = pd.to_datetime(df["d"])
    mp = df["sum_wp"]/df["sum_w"]
    df["sigma_p"] = np.sqrt((df["sum_wp2"]/df["sum_w"] - mp**2).clip(lower=0))
    df["hhi"] = df["sum_w"]**2/df["sum_w2"]
    df["hhi"] = df["sum_w2"] / df["sum_w"]**2
    df["hour_class"] = df["clock_hour"].map(hour_class_label)
    return df


def did(p, pre_lo, pre_hi, post_lo, post_hi, outcome):
    pre_lo, pre_hi, post_lo, post_hi = map(pd.Timestamp, (pre_lo, pre_hi, post_lo, post_hi))
    q = p[((p["d"] >= pre_lo) & (p["d"] <= pre_hi)) | ((p["d"] >= post_lo) & (p["d"] <= post_hi))]
    q = q[q["hour_class"].isin(["Critical", "Flat"])].dropna(subset=[outcome]).copy()
    if len(q) < 50 or q["unit_code"].nunique() < 2:
        return (np.nan, np.nan, np.nan, len(q))
    q["post"] = (q["d"] >= post_lo).astype(int)
    q["crit"] = (q["hour_class"] == "Critical").astype(int)
    q["pc"] = q["post"]*q["crit"]
    q["y_w"] = q[outcome] - q.groupby("unit_code")[outcome].transform("mean")
    for c in ["post", "crit", "pc"]:
        q[c+"_w"] = q[c] - q.groupby("unit_code")[c].transform("mean")
    X = np.column_stack([np.ones(len(q)), q["post_w"], q["crit_w"], q["pc_w"]])
    b, se = clustered_ols(q["y_w"].values, X, q["d"].astype(str).values)
    return (b[3], se[3], b[3]/se[3], len(q))


def star(t):
    a = abs(t); return "***" if a>=2.58 else "**" if a>=1.96 else "*" if a>=1.645 else ""


def report(label, p, win, tech):
    pre_lo, pre_hi, post_lo, post_hi = win
    pt = p[p["tech"] == tech].copy()
    pt = pt[pt["hour_class"].isin(["Critical", "Flat"])]
    if pt.empty:
        return []
    multi = (pt["n_offers"] > 1).mean()
    print(f"\n=== {label} ===  mean_offers={pt['n_offers'].mean():.2f}  "
          f"multi_share={multi:.3f}  max_offers={int(pt['n_offers'].max())}")
    rows = []
    for outcome in ["sigma_p", "hhi"]:
        bf, sf, tf, nf = did(pt, *win, outcome)
        bs, ss, ts, ns = did(pt[pt["n_offers"] == 1], *win, outcome)
        print(f"  DiD {outcome:8s}: FULL {bf:+.3f}{star(tf)} (n={nf})   "
              f"SINGLE-OFFER {bs:+.3f}{star(ts)} (n={ns})")
        rows.append(dict(market=label, outcome=outcome, multi_share=round(multi, 3),
                         did_full=round(bf, 4), t_full=round(tf, 2), n_full=nf,
                         did_single=round(bs, 4), t_single=round(ts, 2), n_single=ns))
    return rows


def main():
    rows = []
    # Day-ahead CCGT (single-offer market) at both reforms
    for reform, win in WIN.items():
        p = build(min(win[0], win[2]), max(win[1], win[3]))
        rows += report(f"{reform} DA CCGT", p, win, "CCGT")
    # Intraday at ID15 -- where stacking happens
    ida = build_ida(min(IDA_WIN[0], IDA_WIN[2]), max(IDA_WIN[1], IDA_WIN[3]))
    for tech in ["CCGT", "Hydro", "Hydro_pump"]:
        rows += report(f"ID15 IDA {tech}", ida, IDA_WIN, tech)
    out = pd.DataFrame(rows)
    OUT.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUT, index=False)
    print(f"\nWrote {OUT.relative_to(REPO)}")


if __name__ == "__main__":
    main()
