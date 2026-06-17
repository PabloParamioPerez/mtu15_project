# STATUS: ALIVE
# LAST-AUDIT: 2026-05-27
# FEEDS: advisor_memo.tex sec 5(e) bandwidth robustness -- per-curve sigma_p
#        and N_eff DiDs at h in {100, 140, 200} for both reforms and the four
#        in-band-eligible techs (CCGT, Hydro, Hydro_pump, Wind). Headline
#        bandwidth h=140 is the upper edge of the competing-cluster mode;
#        wider/narrower bandwidths shift sample size but should leave the
#        critical-flat differential intact if the result is not a band artefact.
#
# OUT: results/regressions/bid/mtu15_critical_flat/bandwidth_robustness.csv

from pathlib import Path
import sys

import duckdb
import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO / "scripts/analysis/bid"))
import mtu15_critical_flat_did as base
from mtu15_critical_flat_did import (  # noqa: E402
    clustered_ols, hour_class_label, WINDOWS, tech_bucket, firm_bucket, TECHS, FIRMS,
)

DET = REPO / "data/processed/omie/mercado_diario/ofertas/det_all.parquet"
CAB = REPO / "data/processed/omie/mercado_diario/ofertas/cab_all.parquet"
MPDBC = REPO / "data/processed/omie/mercado_diario/precios/marginalpdbc_all.parquet"
UNITS = REPO / "data/external/omie_reference/lista_unidades.csv"
OUT = REPO / "results/regressions/bid/mtu15_critical_flat/bandwidth_robustness.csv"
OUT.parent.mkdir(parents=True, exist_ok=True)

BANDWIDTHS = [100.0, 140.0, 200.0]


def build_da_panel(lo, hi, h):
    """Per (unit, date, period) DA in-band sigma_p and N_eff at bandwidth h."""
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
           SUM(i.q) sum_w, SUM(i.q*i.p) sum_wp,
           SUM(i.q*i.p*i.p) sum_wp2, SUM(i.q*i.q) sum_w2, COUNT(*) n_tranche
    FROM inband i JOIN u ON i.unit_code = u.unit_code
    GROUP BY 1,2,3,4,5,6
    HAVING SUM(i.q) > 0
    """
    df = con.execute(sql).fetchdf()
    df["d"] = pd.to_datetime(df["d"])
    mean_p = df["sum_wp"] / df["sum_w"]
    var_p = df["sum_wp2"] / df["sum_w"] - mean_p ** 2
    df["sigma_p"] = np.sqrt(var_p.clip(lower=0))
    df["hhi"] = df["sum_w"] ** 2 / df["sum_w2"]
    df["hhi"] = df["sum_w2"] / df["sum_w"]**2
    df["hour_class"] = df["clock_hour"].map(hour_class_label)
    return df


def run_did(panel, reform, tech_filter, outcome):
    w = WINDOWS[reform]
    pre_lo, pre_hi = pd.Timestamp(w["pre_lo"]), pd.Timestamp(w["pre_hi"])
    post_lo, post_hi = pd.Timestamp(w["post_lo"]), pd.Timestamp(w["post_hi"])
    p = panel.copy()
    if tech_filter is not None:
        p = p[p["tech"] == tech_filter].copy()
    in_pre = (p["d"] >= pre_lo) & (p["d"] <= pre_hi)
    in_post = (p["d"] >= post_lo) & (p["d"] <= post_hi)
    p = p[(in_pre | in_post) & p["hour_class"].isin(["Critical", "Flat"])].copy()
    p = p.dropna(subset=[outcome])
    if len(p) < 50:
        return None
    p["post"] = (p["d"] >= post_lo).astype(int)
    p["crit"] = (p["hour_class"] == "Critical").astype(int)
    p["post_crit"] = p["post"] * p["crit"]
    gm = p.groupby("unit_code")[outcome].transform("mean")
    p["y_w"] = p[outcome] - gm
    for c in ["post", "crit", "post_crit"]:
        gmc = p.groupby("unit_code")[c].transform("mean")
        p[c + "_w"] = p[c] - gmc
    X = np.column_stack([np.ones(len(p)), p["post_w"].values,
                         p["crit_w"].values, p["post_crit_w"].values])
    beta, se = clustered_ols(p["y_w"].values, X, p["d"].astype(str).values)
    return {"DiD": beta[3], "se": se[3], "t": beta[3] / se[3], "n": len(p)}


def main():
    rows = []
    techs = ["CCGT", "Hydro", "Hydro_pump", "Wind"]
    for h in BANDWIDTHS:
        print(f"\n=== bandwidth h = {h:g} EUR/MWh ===")
        # patch the module-level H so build_ida_panel uses this bandwidth
        base.H = h

        # ID15: IDA panel
        wid = WINDOWS["ID15"]
        print(f"  building IDA panel [{wid['pre_lo']} -> {wid['post_hi']}]...")
        ida = base.build_ida_panel(wid["pre_lo"], wid["post_hi"])
        print(f"    {len(ida):,} IDA in-band curves")

        # DA15: DA panel (built inline with parametric h)
        wda = WINDOWS["DA15"]
        print(f"  building DA panel [{wda['pre_lo']} -> {wda['post_hi']}]...")
        da = build_da_panel(wda["pre_lo"], wda["post_hi"], h)
        print(f"    {len(da):,} DA in-band curves")

        for reform, panel in [("ID15", ida), ("DA15", da)]:
            for tech in techs:
                for outcome in ["sigma_p", "hhi"]:
                    r = run_did(panel, reform, tech, outcome)
                    if r is None:
                        continue
                    row = {"h": h, "reform": reform, "tech": tech, "outcome": outcome, **r}
                    rows.append(row)
                    print(f"    {reform}  {tech:10s} {outcome:8s}  "
                          f"DiD={r['DiD']:+8.3f}  se={r['se']:6.3f}  "
                          f"t={r['t']:+6.2f}  n={r['n']:,}")

    pd.DataFrame(rows).to_csv(OUT, index=False)
    print(f"\nWrote {OUT}")


if __name__ == "__main__":
    main()
