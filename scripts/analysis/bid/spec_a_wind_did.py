# STATUS: ALIVE
# LAST-AUDIT: 2026-05-25
# FEEDS: advisor_memo.tex sec 4 results table -- adds a Wind row to Spec A
#        for both reforms. Motivation: Reguant's papers always include wind;
#        we cannot exclude it as a "price-taker" without showing the DiD.
#        Caveat documented in the memo: wind within-hour bid variation is
#        sub-hourly forecast revision, not strategic ladder shaping
#        (descriptive_facts.tex sec 5: 84% of in-band wind curves are a
#        single block; scarcity-response slope is +0.01 EUR/MWh per GW).
#
# OUT: results/regressions/bid/mtu15_critical_flat/spec_a_wind.csv

from pathlib import Path
import sys

import duckdb
import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO / "scripts/analysis/bid"))
from mtu15_critical_flat_did import (  # noqa: E402
    clustered_ols, CRITICAL, FLAT, WINDOWS, hour_class_label,
)

OUT = REPO / "results/regressions/bid/mtu15_critical_flat/spec_a_wind.csv"
OUT.parent.mkdir(parents=True, exist_ok=True)

DET = REPO / "data/processed/omie/mercado_diario/ofertas/det_all.parquet"
CAB = REPO / "data/processed/omie/mercado_diario/ofertas/cab_all.parquet"
IDET = REPO / "data/processed/omie/mercado_intradiario_subastas/ofertas/idet_all.parquet"
ICAB = REPO / "data/processed/omie/mercado_intradiario_subastas/ofertas/icab_all.parquet"
MPDBC = REPO / "data/processed/omie/mercado_diario/precios/marginalpdbc_all.parquet"
MPIBC = REPO / "data/processed/omie/mercado_intradiario_subastas/precios/marginalpibc_all.parquet"
UNITS = REPO / "data/external/omie_reference/lista_unidades.csv"
H = 140.0


def is_wind_tech(t):
    return "eólica" in str(t).lower()


def build_wind_panel(market, lo, hi):
    """Per-curve sigma_p / N_eff panel for all wind sell units."""
    units = pd.read_csv(UNITS)
    units = units[units["technology"].apply(is_wind_tech)][["unit_code"]].drop_duplicates()
    con = duckdb.connect()
    con.execute("SET memory_limit='12GB'")
    con.execute("SET threads=4")
    con.register("u", units)
    if market == "DA":
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
          FROM '{MPDBC}' WHERE date BETWEEN '{lo}' AND '{hi}'
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
        SELECT i.d, CAST(NULL AS INT) session_number, i.period, i.clock_hour,
               i.unit_code, SUM(i.q) sum_w, SUM(i.q*i.p) sum_wp,
               SUM(i.q*i.p*i.p) sum_wp2, SUM(i.q*i.q) sum_w2
        FROM inband i JOIN u ON i.unit_code = u.unit_code
        GROUP BY 1,2,3,4,5 HAVING SUM(i.q) > 0
        """
    else:
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
                 unit_code, period, price_eur_mwh p, quantity_mw q,
                 COALESCE(mtu_minutes, 60) AS mtu
          FROM '{IDET}' WHERE date BETWEEN '{lo}' AND '{hi}' AND quantity_mw > 0
        ),
        mp AS (
          SELECT CAST(date AS DATE) d, session_number, period, price_es_eur_mwh p_clear,
                 COALESCE(mtu_minutes, 60) mtu_p
          FROM '{MPIBC}' WHERE date BETWEEN '{lo}' AND '{hi}'
            AND price_es_eur_mwh IS NOT NULL
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
        SELECT i.d, i.session_number, i.period, i.clock_hour, i.unit_code,
               SUM(i.q) sum_w, SUM(i.q*i.p) sum_wp,
               SUM(i.q*i.p*i.p) sum_wp2, SUM(i.q*i.q) sum_w2
        FROM inband i JOIN u ON i.unit_code = u.unit_code
        GROUP BY 1,2,3,4,5 HAVING SUM(i.q) > 0
        """
    df = con.execute(sql).fetchdf()
    df["d"] = pd.to_datetime(df["d"])
    mean_p = df["sum_wp"] / df["sum_w"]
    var_p = df["sum_wp2"] / df["sum_w"] - mean_p ** 2
    df["sigma_p"] = np.sqrt(var_p.clip(lower=0))
    df["n_eff"] = df["sum_w"] ** 2 / df["sum_w2"]
    df["hour_class"] = df["clock_hour"].map(hour_class_label)
    return df


def run_spec_A(panel, reform):
    w = WINDOWS[reform]
    pre_lo, pre_hi = pd.Timestamp(w["pre_lo"]), pd.Timestamp(w["pre_hi"])
    post_lo, post_hi = pd.Timestamp(w["post_lo"]), pd.Timestamp(w["post_hi"])
    p = panel.copy()
    in_pre = (p["d"] >= pre_lo) & (p["d"] <= pre_hi)
    in_post = (p["d"] >= post_lo) & (p["d"] <= post_hi)
    p = p[(in_pre | in_post) & p["hour_class"].isin(["Critical", "Flat"])].copy()
    if p.empty: return None
    p["post"] = (p["d"] >= post_lo).astype(int)
    p["crit"] = (p["hour_class"] == "Critical").astype(int)
    p["post_crit"] = p["post"] * p["crit"]
    out = []
    for outcome in ["sigma_p", "n_eff"]:
        d = p.dropna(subset=[outcome]).copy()
        if len(d) < 50: continue
        gm = d.groupby("unit_code")[outcome].transform("mean")
        d["y_w"] = d[outcome] - gm
        for c in ["post", "crit", "post_crit"]:
            gmc = d.groupby("unit_code")[c].transform("mean")
            d[c + "_w"] = d[c] - gmc
        X = np.column_stack([np.ones(len(d)), d["post_w"].values,
                             d["crit_w"].values, d["post_crit_w"].values])
        beta, se = clustered_ols(d["y_w"].values, X, d["d"].astype(str).values)
        out.append({"outcome": outcome, "n": len(d), "DiD": beta[3],
                    "se": se[3], "t": beta[3]/se[3]})
    return pd.DataFrame(out)


def main():
    rows = []
    for reform, market in [("ID15", "IDA"), ("DA15", "DA")]:
        w = WINDOWS[reform]
        print(f"=== {reform} ({market}) Wind Spec A ===")
        panel = build_wind_panel(market, w["pre_lo"], w["post_hi"])
        print(f"  {len(panel):,} wind in-band curves")
        r = run_spec_A(panel, reform)
        if r is None: continue
        r.insert(0, "reform", reform); r.insert(1, "tech", "Wind")
        rows.append(r)
        for _, row in r.iterrows():
            print(f"  {row['outcome']:8s}  DiD={row['DiD']:+8.3f}  se={row['se']:6.3f}  t={row['t']:+6.2f}  n={int(row['n']):,}")
    pd.concat(rows, ignore_index=True).to_csv(OUT, index=False)
    print(f"\nWrote {OUT}")


if __name__ == "__main__":
    main()
