# STATUS: ALIVE
# LAST-AUDIT: 2026-06-14
# FEEDS: thesis appendix (pre-trend placebo) -- a same-calendar 2024 placebo for
#        the bid-shape critical-flat DiD on the HEADLINE metrics sigma_p and the
#        tranche-Herfindahl (reported as 1/HHI = n_eff). The existing midpoint
#        placebo table covers the drifting alpha/beta/gamma; this adds the clean
#        headline metrics under the sharper same-calendar test.
#
# Design: shift each reform's pre/post windows back exactly one year, where no
# granularity reform occurred, and re-run the identical critical-flat DiD on the
# DAY-AHEAD market (hourly throughout 2024, so a 2024 cutover is structurally
# clean). A null placebo confirms the real effect is not a recurring seasonal
# critical-vs-flat pattern. Both real (2025) and placebo (2024) are estimated at
# the same +/-50 EUR/MWh band for an apples-to-apples contrast.
#
# OUT: results/regressions/bid/mtu15_critical_flat/spec_c_2024_placebo.csv

from pathlib import Path
import sys
import duckdb
import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO / "scripts/analysis/bid"))
from mtu15_critical_flat_did import (  # noqa: E402
    CAB, DET, MPDBC, UNITS, CRITICAL, FLAT, TECHS, FIRMS,
    tech_bucket, firm_bucket, hour_class_label, clustered_ols)

OUT = REPO / "results/regressions/bid/mtu15_critical_flat/spec_c_2024_placebo.csv"
BAND = 50.0  # +/- EUR/MWh in-band region (near the headline p90; excludes the floor)

# Real (2025) windows and their same-calendar 2024 placebo (one year earlier).
CELLS = {
    "ID15 DA": {
        "real":    ("2024-12-19", "2025-03-18", "2025-03-19", "2025-04-27"),
        "placebo": ("2023-12-19", "2024-03-18", "2024-03-19", "2024-04-27"),
    },
    "DA15 DA": {
        "real":    ("2025-07-01", "2025-09-30", "2025-10-01", "2025-12-31"),
        "placebo": ("2024-07-01", "2024-09-30", "2024-10-01", "2024-12-31"),
    },
}


def build_da_panel(lo, hi, band=BAND):
    con = duckdb.connect()
    con.execute("SET memory_limit='12GB'"); con.execute("SET threads=4")
    units = pd.read_csv(UNITS)
    units["tech"] = units["technology"].apply(tech_bucket)
    units["firm"] = units["owner_agent"].apply(firm_bucket)
    units = units[units["tech"].isin(TECHS)][["unit_code", "firm", "tech"]].drop_duplicates("unit_code")
    con.register("u", units)
    sql = f"""
    WITH cab_l AS (
      SELECT CAST(date AS DATE) d, offer_code, unit_code FROM (
        SELECT CAST(date AS DATE) date, offer_code, unit_code,
               ROW_NUMBER() OVER (PARTITION BY CAST(date AS DATE), offer_code, unit_code
                                  ORDER BY version DESC) rn
        FROM '{CAB}' WHERE date BETWEEN '{lo}' AND '{hi}' AND buy_sell='V'
      ) WHERE rn=1
    ),
    det AS (
      SELECT CAST(date AS DATE) d, offer_code, period,
             price_eur_mwh p, quantity_mw q, COALESCE(mtu_minutes, 60) AS mtu
      FROM '{DET}' WHERE date BETWEEN '{lo}' AND '{hi}' AND quantity_mw > 0
    ),
    mp AS (
      SELECT CAST(date AS DATE) d, period, price_es_eur_mwh p_clear,
             COALESCE(mtu_minutes, 60) mtu_p
      FROM '{MPDBC}' WHERE date BETWEEN '{lo}' AND '{hi}' AND price_es_eur_mwh IS NOT NULL
    ),
    inband AS (
      SELECT mp.d, mp.period, c.unit_code, dv.q, dv.p,
             CASE WHEN COALESCE(mp.mtu_p, dv.mtu) = 60 THEN mp.period - 1
                  ELSE CAST(FLOOR((mp.period - 1) / 4.0) AS INT) END AS clock_hour
      FROM det dv JOIN cab_l c ON dv.d=c.d AND dv.offer_code=c.offer_code
        JOIN mp ON mp.d=dv.d AND mp.period=dv.period
      WHERE dv.p BETWEEN mp.p_clear - {band} AND mp.p_clear + {band}
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
    return da


def did(panel, pre_lo, pre_hi, post_lo, post_hi, tech):
    pre_lo, pre_hi = pd.Timestamp(pre_lo), pd.Timestamp(pre_hi)
    post_lo, post_hi = pd.Timestamp(post_lo), pd.Timestamp(post_hi)
    p = panel[panel["tech"] == tech].copy()
    in_pre = (p["d"] >= pre_lo) & (p["d"] <= pre_hi)
    in_post = (p["d"] >= post_lo) & (p["d"] <= post_hi)
    p = p[(in_pre | in_post) & p["hour_class"].isin(["Critical", "Flat"])].copy()
    p["post"] = (p["d"] >= post_lo).astype(int)
    p["crit"] = (p["hour_class"] == "Critical").astype(int)
    p["post_crit"] = p["post"] * p["crit"]
    res = {}
    for outcome in ["sigma_p", "n_eff"]:
        d = p.dropna(subset=[outcome]).copy()
        if len(d) < 50 or d["post"].nunique() < 2:
            res[outcome] = (np.nan, np.nan, np.nan, len(d)); continue
        gm = d.groupby("unit_code")[outcome].transform("mean")
        d["y_w"] = d[outcome] - gm
        for c in ["post", "crit", "post_crit"]:
            d[c + "_w"] = d[c] - d.groupby("unit_code")[c].transform("mean")
        X = np.column_stack([np.ones(len(d)), d["post_w"], d["crit_w"], d["post_crit_w"]])
        beta, se = clustered_ols(d["y_w"].values, X, d["d"].astype(str).values)
        res[outcome] = (beta[3], se[3], beta[3] / se[3], len(d))
    return res


def star(t):
    a = abs(t)
    return "***" if a >= 2.58 else "**" if a >= 1.96 else "*" if a >= 1.645 else ""


def main():
    rows = []
    for cell, w in CELLS.items():
        for kind in ("real", "placebo"):
            lo = min(w[kind][0], w[kind][2]); hi = max(w[kind][1], w[kind][3])
            print(f"Building DA panel {cell} {kind} ({lo}..{hi}) ...", flush=True)
            panel = build_da_panel(lo, hi)
            r = did(panel, *w[kind], "CCGT")
            for outcome in ("sigma_p", "n_eff"):
                b, se, t, n = r[outcome]
                rows.append(dict(cell=cell, kind=kind, outcome=outcome,
                                 DiD=round(b, 4), se=round(se, 4),
                                 t=round(t, 2) if t == t else np.nan, n=n))
                print(f"  {cell:8s} {kind:7s} {outcome:8s}: DiD={b:+.4f}{star(t)} (SE {se:.4f}, n={n})")
    out = pd.DataFrame(rows)
    OUT.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUT, index=False)
    print(f"\nWrote {OUT.relative_to(REPO)}")


if __name__ == "__main__":
    main()
