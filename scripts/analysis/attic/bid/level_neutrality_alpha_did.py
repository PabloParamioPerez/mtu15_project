# STATUS: ALIVE
# LAST-AUDIT: 2026-06-16
# FEEDS: thesis 6.4 level-neutrality claim (Corollary rem:level). Runs the SAME
#        critical-flat within-day DiD as the headline bid-shape table, but with
#        the MW-weighted mean in-band price (alpha) as the outcome, for every
#        (reform x market x tech) cell. Level-neutrality predicts theta ~ 0
#        everywhere: a granular reform reshapes the ladder (sigma_p, HHI move)
#        without moving where the curve sits, once the common (critical+flat)
#        price-level shift is differenced out by the within-day design.
#
#        This is the cell-by-cell counterpart the 6.4 "untested" box flagged as
#        in reach; alpha = sum_wp/sum_w is already in the per-curve panels.
#
# OUT: results/regressions/bid/mtu15_critical_flat/level_neutrality_alpha_did.csv

from pathlib import Path
import sys

import duckdb
import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO / "scripts/analysis/bid"))
import mtu15_critical_flat_did as base  # noqa: E402
from mtu15_critical_flat_did import (  # noqa: E402
    clustered_ols, hour_class_label, WINDOWS, tech_bucket, firm_bucket,
    TECHS, FIRMS, CAB, DET, MPDBC, UNITS,
)

# Headline per-(reform, market) bandwidths delta = p90 - p50 (Table tab:bandwidths, real row).
DELTA = {("ID15", "da"): 50.0, ("ID15", "ida"): 62.0,
         ("DA15", "da"): 50.0, ("DA15", "ida"): 58.0}
OUT = REPO / "results/regressions/bid/mtu15_critical_flat/level_neutrality_alpha_did.csv"
OUT.parent.mkdir(parents=True, exist_ok=True)


def build_da_panel(lo, hi, h):
    """Per (unit, date, period) DA in-band sums at bandwidth h (mirrors the
    headline DA panel; carries sum_w, sum_wp so alpha = sum_wp/sum_w)."""
    units = pd.read_csv(UNITS)
    units["tech"] = units["technology"].apply(tech_bucket)
    units["firm"] = units["owner_agent"].apply(firm_bucket)
    units = units[units["tech"].isin(TECHS) & units["firm"].isin(FIRMS)][
        ["unit_code", "firm", "tech"]
    ].drop_duplicates("unit_code")
    con = duckdb.connect()
    con.execute("SET memory_limit='12GB'")
    con.execute("SET threads=4")
    con.register("u", units)
    sql = f"""
    WITH cab_l AS (
      SELECT d, offer_code, unit_code FROM (
        SELECT CAST(date AS DATE) d, offer_code, unit_code,
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
      WHERE dv.p BETWEEN mp.p_clear - {h} AND mp.p_clear + {h}
    )
    SELECT i.d, i.period, i.clock_hour, i.unit_code, u.firm, u.tech,
           SUM(i.q) sum_w, SUM(i.q*i.p) sum_wp, COUNT(*) n_tranche
    FROM inband i JOIN u ON i.unit_code = u.unit_code
    GROUP BY 1,2,3,4,5,6
    HAVING SUM(i.q) > 0
    """
    df = con.execute(sql).fetchdf()
    df["d"] = pd.to_datetime(df["d"])
    df["mean_p"] = df["sum_wp"] / df["sum_w"]
    df["hour_class"] = df["clock_hour"].map(hour_class_label)
    return df


def run_did_alpha(panel, reform, tech):
    """Critical-flat within-day DiD with mean in-band price (alpha) as outcome."""
    w = WINDOWS[reform]
    pre_lo, pre_hi = pd.Timestamp(w["pre_lo"]), pd.Timestamp(w["pre_hi"])
    post_lo, post_hi = pd.Timestamp(w["post_lo"]), pd.Timestamp(w["post_hi"])
    p = panel[panel["tech"] == tech].copy()
    in_pre = (p["d"] >= pre_lo) & (p["d"] <= pre_hi)
    in_post = (p["d"] >= post_lo) & (p["d"] <= post_hi)
    p = p[(in_pre | in_post) & p["hour_class"].isin(["Critical", "Flat"])].copy()
    p = p.dropna(subset=["mean_p"])
    if len(p) < 50 or p["d"].nunique() < 5:
        return None
    p["post"] = (p["d"] >= post_lo).astype(int)
    p["crit"] = (p["hour_class"] == "Critical").astype(int)
    p["post_crit"] = p["post"] * p["crit"]
    gm = p.groupby("unit_code")["mean_p"].transform("mean")
    p["y_w"] = p["mean_p"] - gm
    for c in ["post", "crit", "post_crit"]:
        p[c + "_w"] = p[c] - p.groupby("unit_code")[c].transform("mean")
    X = np.column_stack([np.ones(len(p)), p["post_w"], p["crit_w"], p["post_crit_w"]])
    beta, se = clustered_ols(p["y_w"].values, X, p["d"].astype(str).values)
    return {"DiD": beta[3], "se": se[3], "t": beta[3] / se[3] if se[3] > 0 else np.nan,
            "n": len(p), "pre_mean_level": float(p.loc[p["post"] == 0, "mean_p"].mean())}


def star(t):
    a = abs(t)
    return "***" if a >= 2.58 else "**" if a >= 1.96 else "*" if a >= 1.645 else ""


def main():
    rows = []
    for reform in ["ID15", "DA15"]:
        w = WINDOWS[reform]
        print(f"\n=== {reform} ({w['pre_lo']} -> {w['post_hi']}) ===", flush=True)
        base.H = DELTA[(reform, "ida")]
        print(f"  building IDA panel (delta={base.H:g}) ...", flush=True)
        ida = base.build_ida_panel(w["pre_lo"], w["post_hi"])
        ida["mean_p"] = ida["sum_wp"] / ida["sum_w"]
        print(f"  building DA panel (delta={DELTA[(reform,'da')]:g}) ...", flush=True)
        da = build_da_panel(w["pre_lo"], w["post_hi"], DELTA[(reform, "da")])
        for market, panel in [("da", da), ("ida", ida)]:
            for tech in TECHS:
                r = run_did_alpha(panel, reform, tech)
                if r is None:
                    print(f"    {market.upper():3s} {tech:11s}  (insufficient)")
                    continue
                rows.append({"reform": reform, "market": market, "tech": tech, **r})
                print(f"    {market.upper():3s} {tech:11s}  alpha-DiD={r['DiD']:+7.2f}"
                      f"{star(r['t']):3s}  se={r['se']:5.2f}  n={r['n']:,}  "
                      f"(pre level {r['pre_mean_level']:.1f})")
    out = pd.DataFrame(rows)
    out.to_csv(OUT, index=False)
    print(f"\nWrote {OUT.relative_to(REPO)}")
    print("\nLevel-neutrality reading: theta (alpha-DiD) ~ 0 in every cell means a "
          "reform reshapes the ladder without moving the curve level (Corollary rem:level).")


if __name__ == "__main__":
    main()
