# STATUS: ALIVE
# LAST-AUDIT: 2026-05-25
# FEEDS: advisor_memo.tex sec 4 -- offer-type robustness for non-CCGT techs
#        (Hydro, Hydro pump, Wind, Nuclear, Solar PV).
# CLAIM: Composition of DA in-band sell offers by offer type for each tech,
#        plus -- where there is meaningful variation across types -- Spec A
#        sigma_p / N_eff DiD by offer type. The user's worry: are the
#        per-tech bid-shape DiDs in section 4 contaminated by offer-type
#        composition shifts at the MTU15-DA cutover?
#
# Classification per (date, offer_code, unit_code), last cab version,
# matching the CCGT script:
#   MIC    -- cab fixed_term_eur > 0
#   block  -- any det tranche with block_number > 0
#   simple -- neither (priority: MIC > block > simple)
#
# OUT: results/regressions/bid/mtu15_critical_flat/all_tech_offer_type.csv

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

OUT = REPO / "results/regressions/bid/mtu15_critical_flat/all_tech_offer_type.csv"
OUT.parent.mkdir(parents=True, exist_ok=True)

DET = REPO / "data/processed/omie/mercado_diario/ofertas/det_all.parquet"
CAB = REPO / "data/processed/omie/mercado_diario/ofertas/cab_all.parquet"
MPDBC = REPO / "data/processed/omie/mercado_diario/precios/marginalpdbc_all.parquet"
UNITS = REPO / "data/external/omie_reference/lista_unidades.csv"
H = 140.0


TECH_PATTERNS = {
    "CCGT":       lambda t: "ciclo combinado" in str(t).lower(),
    "Hydro":      lambda t: ("hidr" in str(t).lower()
                              and "bombeo" not in str(t).lower()),
    "Hydro pump": lambda t: "bombeo" in str(t).lower(),
    "Wind":       lambda t: "eólica" in str(t).lower() or "eolica" in str(t).lower(),
    "Nuclear":    lambda t: "nuclear" in str(t).lower(),
    "Solar PV":   lambda t: "fotovoltaica" in str(t).lower() or "solar pv" in str(t).lower(),
}


def build_panel_with_offer_type(tech_filter, lo, hi):
    """Per-curve sigma_p / N_eff panel for a tech subset with offer_type column.
    DA market only."""
    units = pd.read_csv(UNITS)
    units = units[units["technology"].apply(tech_filter)][["unit_code"]].drop_duplicates()
    if len(units) == 0:
        return pd.DataFrame()
    con = duckdb.connect()
    con.execute("SET memory_limit='12GB'")
    con.execute("SET threads=4")
    con.execute("SET preserve_insertion_order=false")
    con.register("u", units)
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
    SELECT d, period, clock_hour, unit_code, offer_type,
           SUM(q) sum_w, SUM(q*p) sum_wp, SUM(q*p*p) sum_wp2, SUM(q*q) sum_w2
    FROM inband
    GROUP BY 1,2,3,4,5 HAVING SUM(q) > 0
    """
    df = con.execute(sql).fetchdf()
    if df.empty:
        return df
    df["d"] = pd.to_datetime(df["d"])
    mean_p = df["sum_wp"] / df["sum_w"]
    var_p = df["sum_wp2"] / df["sum_w"] - mean_p ** 2
    df["sigma_p"] = np.sqrt(var_p.clip(lower=0))
    df["n_eff"] = df["sum_w"] ** 2 / df["sum_w2"]
    df["hour_class"] = df["clock_hour"].map(hour_class_label)
    return df


def composition(panel, reform):
    w = WINDOWS[reform]
    pre_lo, pre_hi = pd.Timestamp(w["pre_lo"]), pd.Timestamp(w["pre_hi"])
    post_lo, post_hi = pd.Timestamp(w["post_lo"]), pd.Timestamp(w["post_hi"])
    p = panel.copy()
    in_pre = (p["d"] >= pre_lo) & (p["d"] <= pre_hi)
    in_post = (p["d"] >= post_lo) & (p["d"] <= post_hi)
    p = p[(in_pre | in_post) & p["hour_class"].isin(["Critical", "Flat"])].copy()
    p["arm"] = np.where(p["d"] >= post_lo, "post", "pre")
    tab = (p.groupby(["arm", "offer_type"]).size()
             .unstack(fill_value=0))
    tab = tab.div(tab.sum(axis=1), axis=0) * 100
    return tab.round(1)


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
        if len(d) < 100:
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


def main():
    rows = []
    reform = "DA15"
    w = WINDOWS[reform]
    for tech, filt in TECH_PATTERNS.items():
        print(f"\n=== {reform} {tech} ===")
        panel = build_panel_with_offer_type(filt, w["pre_lo"], w["post_hi"])
        if panel.empty:
            print(f"  empty panel, skip")
            continue
        print(f"  total {len(panel):,} in-band curves")
        print(f"  offer_type breakdown:")
        print(panel["offer_type"].value_counts().to_string())
        comp = composition(panel, reform)
        print(f"\n  composition (% by arm):")
        print(comp.to_string())
        # If any offer type has < 5% share both pre and post, skip the split
        # (no meaningful variation to test).
        if {"simple", "MIC", "block"}.issubset(comp.columns):
            shares = comp.max()
            small = [c for c in ["simple", "MIC", "block"] if shares.get(c, 0) < 5]
        else:
            small = ["MIC", "block"]  # likely no fixed_term, no block — pure simple
        if len(small) >= 2:
            print(f"  -- only {[c for c in ['simple','MIC','block'] if c not in small]} is non-trivial -- pooled is the simple-only result; no split needed.")
            r = run_spec_A(panel, reform, f"{tech} all (pooled)")
            if r is not None:
                r.insert(0, "reform", reform)
                r.insert(1, "tech", tech)
                rows.append(r)
                for _, row in r.iterrows():
                    print(f"  {tech} pooled  {row['outcome']:8s}  "
                          f"DiD={row['DiD']:+8.3f}  se={row['se']:6.3f}  "
                          f"t={row['t']:+6.2f}  n={int(row['n']):,}")
            continue
        # Otherwise split by offer type and run
        for ot in ["simple", "MIC", "block"]:
            sub = panel[panel["offer_type"] == ot]
            if len(sub) < 500:
                print(f"  {ot:8s}  n={len(sub):,}  -- too small, skip")
                continue
            r = run_spec_A(sub, reform, f"{tech} {ot}")
            if r is None:
                continue
            r.insert(0, "reform", reform)
            r.insert(1, "tech", tech)
            rows.append(r)
            for _, row in r.iterrows():
                print(f"  {tech:11s} {ot:7s} {row['outcome']:8s}  "
                      f"DiD={row['DiD']:+8.3f}  se={row['se']:6.3f}  "
                      f"t={row['t']:+6.2f}  n={int(row['n']):,}")
        r = run_spec_A(panel, reform, f"{tech} all (pooled)")
        if r is not None:
            r.insert(0, "reform", reform)
            r.insert(1, "tech", tech)
            rows.append(r)
            for _, row in r.iterrows():
                print(f"  {tech:11s} all     {row['outcome']:8s}  "
                      f"DiD={row['DiD']:+8.3f}  se={row['se']:6.3f}  "
                      f"t={row['t']:+6.2f}  n={int(row['n']):,}")
    if rows:
        pd.concat(rows, ignore_index=True).to_csv(OUT, index=False)
        print(f"\nWrote {OUT}")


if __name__ == "__main__":
    main()
