# STATUS: ALIVE
# LAST-AUDIT: 2026-06-15
# FEEDS: thesis Appendix B per-firm CCGT decomposition (tab:perfirm-ccgt),
#        recast onto the ROBUST bid-shape metrics sigma_p and HHI_tr (not the
#        drifting alpha/beta/gamma). One critical-flat DiD per dominant CCGT-owning
#        firm on its own fleet (unit FE), for the two own-market headline cells:
#        DA15 day-ahead and ID15 intraday. In-band band +/-50 EUR/MWh.
#
# OUT: results/regressions/bid/mtu15_critical_flat/perfirm_sigma_hhi_did.csv

from pathlib import Path
import sys
import duckdb
import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO / "scripts/analysis/bid"))
from mtu15_critical_flat_did import (  # noqa: E402
    CAB, DET, MPDBC, ICAB, IDET, MPIBC, UNITS,
    tech_bucket, firm_bucket, hour_class_label, clustered_ols)

OUT = REPO / "results/regressions/bid/mtu15_critical_flat/perfirm_sigma_hhi_did.csv"
BAND = 50.0
FIRM_NAME = {"GN": "Naturgy", "IB": "Iberdrola", "GE": "Endesa", "HC": "EDP-HC"}
DA15_DA = ("2025-07-01", "2025-09-30", "2025-10-01", "2025-12-31")
ID15_IDA = ("2024-12-19", "2025-03-18", "2025-03-19", "2025-04-27")


def ccgt_units(con):
    u = pd.read_csv(UNITS)
    u["tech"] = u["technology"].apply(tech_bucket)
    u["firm"] = u["owner_agent"].apply(firm_bucket)
    u = u[(u["tech"] == "CCGT") & (u["firm"].isin(FIRM_NAME))][
        ["unit_code", "firm"]].drop_duplicates("unit_code")
    con.register("u", u)


def build_da(lo, hi):
    con = duckdb.connect(); con.execute("SET memory_limit='12GB'"); con.execute("SET threads=4")
    ccgt_units(con)
    sql = f"""
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
      SELECT mp.d, mp.period, c.unit_code, dv.q, dv.p,
             CASE WHEN COALESCE(mp.mtu_p,dv.mtu)=60 THEN mp.period-1 ELSE CAST(FLOOR((mp.period-1)/4.0) AS INT) END clock_hour
      FROM det dv JOIN cab_l c ON dv.d=c.d AND dv.offer_code=c.offer_code
      JOIN mp ON mp.d=dv.d AND mp.period=dv.period WHERE dv.p BETWEEN mp.pc-{BAND} AND mp.pc+{BAND})
    SELECT i.d, i.period, i.clock_hour, i.unit_code, u.firm,
           SUM(i.q) sum_w, SUM(i.q*i.p) sum_wp, SUM(i.q*i.p*i.p) sum_wp2, SUM(i.q*i.q) sum_w2
    FROM inband i JOIN u ON i.unit_code=u.unit_code GROUP BY 1,2,3,4,5 HAVING SUM(i.q)>0
    """
    return finalize(con.execute(sql).fetchdf())


def build_ida(lo, hi):
    con = duckdb.connect(); con.execute("SET memory_limit='12GB'"); con.execute("SET threads=4")
    ccgt_units(con)
    sql = f"""
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
      SELECT mp.d, mp.session_number, mp.period, c.unit_code, c.offer_code, dv.q, dv.p,
             CASE WHEN COALESCE(mp.mtu_p,dv.mtu)=60 THEN mp.period-1 ELSE CAST(FLOOR((mp.period-1)/4.0) AS INT) END clock_hour
      FROM idet dv JOIN icab_l c ON dv.d=c.d AND dv.session_number=c.session_number
        AND dv.offer_code=c.offer_code AND dv.version=c.version AND dv.unit_code=c.unit_code
      JOIN mp ON mp.d=dv.d AND mp.session_number=dv.session_number AND mp.period=dv.period
      WHERE dv.p BETWEEN mp.pc-{BAND} AND mp.pc+{BAND})
    SELECT i.d, i.session_number, i.period, i.clock_hour, i.unit_code, u.firm,
           SUM(i.q) sum_w, SUM(i.q*i.p) sum_wp, SUM(i.q*i.p*i.p) sum_wp2, SUM(i.q*i.q) sum_w2
    FROM inband i JOIN u ON i.unit_code=u.unit_code GROUP BY 1,2,3,4,5,6 HAVING SUM(i.q)>0
    """
    return finalize(con.execute(sql).fetchdf())


def finalize(df):
    df["d"] = pd.to_datetime(df["d"])
    mp = df["sum_wp"]/df["sum_w"]
    df["sigma_p"] = np.sqrt((df["sum_wp2"]/df["sum_w"] - mp**2).clip(lower=0))
    df["hhi"] = df["sum_w2"]/df["sum_w"]**2          # tranche-HHI (1/n_eff)
    df["hhi"] = df["hhi"] * 100.0                     # report in HHI x100 (readable)
    df["hour_class"] = df["clock_hour"].map(hour_class_label)
    return df


def did(panel, firm, win, outcome):
    pre_lo, pre_hi, post_lo, post_hi = map(pd.Timestamp, win)
    q = panel[panel["firm"] == firm].copy()
    inw = ((q.d >= pre_lo) & (q.d <= pre_hi)) | ((q.d >= post_lo) & (q.d <= post_hi))
    q = q[inw & q["hour_class"].isin(["Critical", "Flat"])].dropna(subset=[outcome])
    if len(q) < 50 or q["unit_code"].nunique() < 2:
        return (np.nan, np.nan, np.nan, len(q))
    q["post"] = (q.d >= post_lo).astype(int); q["crit"] = (q.hour_class == "Critical").astype(int)
    q["pc"] = q["post"]*q["crit"]
    q["y_w"] = q[outcome] - q.groupby("unit_code")[outcome].transform("mean")
    for c in ["post", "crit", "pc"]:
        q[c+"_w"] = q[c] - q.groupby("unit_code")[c].transform("mean")
    X = np.column_stack([np.ones(len(q)), q.post_w, q.crit_w, q.pc_w])
    b, se = clustered_ols(q["y_w"].values, X, q.d.astype(str).values)
    return (b[3], se[3], b[3]/se[3] if se[3] else np.nan, len(q))


def star(t):
    a = abs(t); return "***" if a >= 2.58 else "**" if a >= 1.96 else "*" if a >= 1.645 else ""


def main():
    cells = [("DA15 DA", build_da(DA15_DA[0], DA15_DA[3]), DA15_DA),
             ("ID15 IDA", build_ida(ID15_IDA[0], ID15_IDA[3]), ID15_IDA)]
    rows = []
    for label, panel, win in cells:
        print(f"\n=== {label} ===")
        firms = [f for f in ["GN", "IB", "GE", "HC"] if (panel["firm"] == f).any()]
        for f in firms:
            r = {"cell": label, "firm": FIRM_NAME[f]}
            for outc in ["sigma_p", "hhi"]:
                b, se, t, n = did(panel, f, win, outc)
                r[outc] = b; r[outc+"_t"] = t; r["n"] = n
                print(f"  {FIRM_NAME[f]:10s} {outc:8s} {b:+8.3f}{star(t):3s} (n={n})")
            rows.append(r)
    out = pd.DataFrame(rows)
    OUT.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUT, index=False)
    print(f"\nWrote {OUT.relative_to(REPO)}")


if __name__ == "__main__":
    main()
