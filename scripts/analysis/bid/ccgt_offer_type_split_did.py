# STATUS: ALIVE
# LAST-AUDIT: 2026-05-25
# FEEDS: advisor_memo.tex sec 4 -- CCGT offer-type robustness for Spec A.
# CLAIM: Spec A (sigma_p, N_eff DiD) for CCGT, split by offer type
#        (simple / MIC / block) per the classification in
#        scripts/analysis/bid/offer_type_mix_by_tech.py:
#          MIC    -- cab fixed_term_eur > 0
#          block  -- any det tranche with block_number > 0
#          simple -- neither (priority: MIC > block > simple)
#        Also reports the OFFER-TYPE COMPOSITION pre vs post and critical vs
#        flat, to detect whether the headline CCGT DiD is a composition
#        artefact (i.e., more simple offers post-reform mechanically widen
#        sigma_p) rather than a strategic ladder-shape shift.
#
# OUT: results/regressions/bid/mtu15_critical_flat/ccgt_offer_type_split.csv

from pathlib import Path
import sys

import duckdb
import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO / "scripts/analysis/bid"))
from mtu15_critical_flat_did import (  # noqa: E402
    clustered_ols, WINDOWS, hour_class_label,
)

OUT = REPO / "results/regressions/bid/mtu15_critical_flat/ccgt_offer_type_split.csv"
OUT.parent.mkdir(parents=True, exist_ok=True)

DET = REPO / "data/processed/omie/mercado_diario/ofertas/det_all.parquet"
CAB = REPO / "data/processed/omie/mercado_diario/ofertas/cab_all.parquet"
IDET = REPO / "data/processed/omie/mercado_intradiario_subastas/ofertas/idet_all.parquet"
ICAB = REPO / "data/processed/omie/mercado_intradiario_subastas/ofertas/icab_all.parquet"
MPDBC = REPO / "data/processed/omie/mercado_diario/precios/marginalpdbc_all.parquet"
MPIBC = REPO / "data/processed/omie/mercado_intradiario_subastas/precios/marginalpibc_all.parquet"
UNITS = REPO / "data/external/omie_reference/lista_unidades.csv"
H = 140.0


def is_ccgt_tech(t):
    return "ciclo combinado" in str(t).lower()


def build_ccgt_panel_with_offer_type(market, lo, hi):
    """Per-curve sigma_p / N_eff panel for CCGT sell offers, with offer_type
    column. Classification per (date, offer_code, unit_code, [session_no]) on
    the LAST cab/icab version."""
    units = pd.read_csv(UNITS)
    units = units[units["technology"].apply(is_ccgt_tech)][["unit_code"]].drop_duplicates()
    con = duckdb.connect()
    con.execute("SET memory_limit='12GB'")
    con.execute("SET threads=4")
    con.execute("SET preserve_insertion_order=false")
    con.register("u", units)
    if market == "DA":
        sql = f"""
        WITH cab_l AS (
          SELECT d, offer_code, unit_code, fixed_term_eur FROM (
            SELECT CAST(date AS DATE) d, offer_code, unit_code, fixed_term_eur,
                   ROW_NUMBER() OVER (PARTITION BY CAST(date AS DATE), offer_code, unit_code
                                      ORDER BY version DESC) rn
            FROM '{CAB}' WHERE date BETWEEN '{lo}' AND '{hi}' AND buy_sell='V'
          ) WHERE rn=1
        ),
        det_block AS (
          SELECT CAST(date AS DATE) d, offer_code,
                 MAX((block_number > 0)::INT) AS has_block
          FROM '{DET}' WHERE date BETWEEN '{lo}' AND '{hi}'
          GROUP BY 1, 2
        ),
        cab_typ AS (
          SELECT c.d, c.offer_code, c.unit_code,
                 CASE WHEN c.fixed_term_eur > 0        THEN 'MIC'
                      WHEN COALESCE(b.has_block,0)=1   THEN 'block'
                      ELSE 'simple' END AS offer_type
          FROM cab_l c LEFT JOIN det_block b
            ON c.d=b.d AND c.offer_code=b.offer_code
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
          SELECT mp.d, mp.period, c.unit_code, c.offer_type, dv.q, dv.p,
                 CASE WHEN COALESCE(mp.mtu_p, dv.mtu) = 60 THEN mp.period - 1
                      ELSE CAST(FLOOR((mp.period - 1) / 4.0) AS INT) END AS clock_hour
          FROM det dv JOIN cab_typ c ON dv.d=c.d AND dv.offer_code=c.offer_code
            JOIN mp ON mp.d=dv.d AND mp.period=dv.period
            JOIN u ON c.unit_code = u.unit_code
          WHERE dv.p BETWEEN mp.p_clear - {H} AND mp.p_clear + {H}
        )
        SELECT d, CAST(NULL AS INT) session_number, period, clock_hour,
               unit_code, offer_type,
               SUM(q) sum_w, SUM(q*p) sum_wp, SUM(q*p*p) sum_wp2, SUM(q*q) sum_w2
        FROM inband
        GROUP BY 1,2,3,4,5,6 HAVING SUM(q) > 0
        """
    else:
        sql = f"""
        WITH icab_l AS (
          SELECT d, session_number, offer_code, version, unit_code, fixed_term_eur FROM (
            SELECT CAST(date AS DATE) d, session_number, offer_code, version, unit_code, fixed_term_eur,
                   ROW_NUMBER() OVER (PARTITION BY CAST(date AS DATE), session_number, offer_code, unit_code
                                      ORDER BY version DESC) rn
            FROM '{ICAB}' WHERE date BETWEEN '{lo}' AND '{hi}' AND buy_sell='V'
          ) WHERE rn=1
        ),
        idet_block AS (
          SELECT CAST(date AS DATE) d, session_number, offer_code, version,
                 MAX((block_number > 0)::INT) AS has_block
          FROM '{IDET}' WHERE date BETWEEN '{lo}' AND '{hi}'
          GROUP BY 1, 2, 3, 4
        ),
        icab_typ AS (
          SELECT c.d, c.session_number, c.offer_code, c.version, c.unit_code,
                 CASE WHEN c.fixed_term_eur > 0        THEN 'MIC'
                      WHEN COALESCE(b.has_block,0)=1   THEN 'block'
                      ELSE 'simple' END AS offer_type
          FROM icab_l c LEFT JOIN idet_block b
            ON c.d=b.d AND c.session_number=b.session_number
           AND c.offer_code=b.offer_code AND c.version=b.version
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
          SELECT mp.d, mp.session_number, mp.period, c.unit_code, c.offer_type, dv.q, dv.p,
                 CASE WHEN COALESCE(mp.mtu_p, dv.mtu) = 60 THEN mp.period - 1
                      ELSE CAST(FLOOR((mp.period - 1) / 4.0) AS INT) END AS clock_hour
          FROM idet dv JOIN icab_typ c
            ON dv.d=c.d AND dv.session_number=c.session_number
           AND dv.offer_code=c.offer_code AND dv.version=c.version AND dv.unit_code=c.unit_code
          JOIN mp ON mp.d=dv.d AND mp.session_number=dv.session_number AND mp.period=dv.period
          JOIN u ON c.unit_code = u.unit_code
          WHERE dv.p BETWEEN mp.p_clear - {H} AND mp.p_clear + {H}
        )
        SELECT d, session_number, period, clock_hour, unit_code, offer_type,
               SUM(q) sum_w, SUM(q*p) sum_wp, SUM(q*p*p) sum_wp2, SUM(q*q) sum_w2
        FROM inband
        GROUP BY 1,2,3,4,5,6 HAVING SUM(q) > 0
        """
    df = con.execute(sql).fetchdf()
    df["d"] = pd.to_datetime(df["d"])
    mean_p = df["sum_wp"] / df["sum_w"]
    var_p = df["sum_wp2"] / df["sum_w"] - mean_p ** 2
    df["sigma_p"] = np.sqrt(var_p.clip(lower=0))
    df["n_eff"] = df["sum_w"] ** 2 / df["sum_w2"]
    df["hour_class"] = df["clock_hour"].map(hour_class_label)
    return df


def run_spec_A(panel, reform, label_extra=""):
    w = WINDOWS[reform]
    pre_lo, pre_hi = pd.Timestamp(w["pre_lo"]), pd.Timestamp(w["pre_hi"])
    post_lo, post_hi = pd.Timestamp(w["post_lo"]), pd.Timestamp(w["post_hi"])
    p = panel.copy()
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
        d = p.dropna(subset=[outcome]).copy()
        if len(d) < 50:
            continue
        gm = d.groupby("unit_code")[outcome].transform("mean")
        d["y_w"] = d[outcome] - gm
        for c in ["post", "crit", "post_crit"]:
            gmc = d.groupby("unit_code")[c].transform("mean")
            d[c + "_w"] = d[c] - gmc
        X = np.column_stack([np.ones(len(d)), d["post_w"].values,
                             d["crit_w"].values, d["post_crit_w"].values])
        beta, se = clustered_ols(d["y_w"].values, X, d["d"].astype(str).values)
        out.append({"label": label_extra, "outcome": outcome, "n": len(d),
                    "DiD": beta[3], "se": se[3], "t": beta[3] / se[3]})
    return pd.DataFrame(out)


def composition_table(panel, reform):
    w = WINDOWS[reform]
    pre_lo, pre_hi = pd.Timestamp(w["pre_lo"]), pd.Timestamp(w["pre_hi"])
    post_lo, post_hi = pd.Timestamp(w["post_lo"]), pd.Timestamp(w["post_hi"])
    p = panel.copy()
    in_pre = (p["d"] >= pre_lo) & (p["d"] <= pre_hi)
    in_post = (p["d"] >= post_lo) & (p["d"] <= post_hi)
    p = p[(in_pre | in_post) & p["hour_class"].isin(["Critical", "Flat"])].copy()
    p["arm"] = np.where(p["d"] >= post_lo, "post", "pre")
    tab = (p.groupby(["arm", "hour_class", "offer_type"]).size()
             .unstack(fill_value=0))
    tab = tab.div(tab.sum(axis=1), axis=0) * 100
    return tab.round(1)


def main():
    rows = []
    # Offer-type contamination concern is DA-side only. In IDA, block orders
    # are ~0.1% of offers (per icab.block_order_avg_price_eur) and IDA has no
    # minimum-income-condition field -- ~all IDA CCGT bids are simple.
    reform, market = "DA15", "DA"
    w = WINDOWS[reform]
    print(f"\n=== {reform} ({market}) CCGT Spec A by offer type ===")
    panel = build_ccgt_panel_with_offer_type(market, w["pre_lo"], w["post_hi"])
    print(f"  total {len(panel):,} CCGT in-band curves")
    print("  offer_type breakdown:")
    print(panel["offer_type"].value_counts().to_string())
    print("\n  composition (% of in-band curves per arm x hour_class):")
    print(composition_table(panel, reform).to_string())
    print()
    for ot in ["simple", "MIC", "block"]:
        sub = panel[panel["offer_type"] == ot]
        if len(sub) < 500:
            print(f"  {ot:8s}  n={len(sub):,}  -- too small, skip")
            continue
        r = run_spec_A(sub, reform, f"CCGT {ot}")
        if r is None:
            continue
        r.insert(0, "reform", reform)
        rows.append(r)
        for _, row in r.iterrows():
            print(f"  CCGT {ot:8s}  {row['outcome']:8s}  "
                  f"DiD={row['DiD']:+8.3f}  se={row['se']:6.3f}  "
                  f"t={row['t']:+6.2f}  n={int(row['n']):,}")
    r = run_spec_A(panel, reform, "CCGT all (pooled)")
    if r is not None:
        r.insert(0, "reform", reform)
        rows.append(r)
        for _, row in r.iterrows():
            print(f"  CCGT all      {row['outcome']:8s}  "
                  f"DiD={row['DiD']:+8.3f}  se={row['se']:6.3f}  "
                  f"t={row['t']:+6.2f}  n={int(row['n']):,}")
    pd.concat(rows, ignore_index=True).to_csv(OUT, index=False)
    print(f"\nWrote {OUT}")


if __name__ == "__main__":
    main()
