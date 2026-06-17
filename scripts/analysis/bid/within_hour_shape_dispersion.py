# STATUS: ALIVE
# LAST-AUDIT: 2026-05-25
# FEEDS: advisor_memo.tex sec 2 Spec C table -- adds two ladder-shape
#        dispersion outcomes complementing the existing price/qty level
#        ones (D_price = sd_q mean price, D_qty = cv_q MW). The shape-side
#        analogs:
#           D_sigma  = sd_q sigma_p^{(q)}   (does the ladder spread change across quarters?)
#           D_neff   = sd_q N_eff^{(q)}      (does the tranche count change across quarters?)
# Both are post-only and mechanically zero when all 4 quarter curves are
# identical (the MTU60-equivalent strategy), so they share the same
# identification logic as D_price / D_qty.
#
# OUT: results/regressions/bid/mtu15_critical_flat/specC_shape_dispersion.csv

from pathlib import Path
import sys

import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO / "scripts/analysis/bid"))
from mtu15_critical_flat_did import (  # noqa: E402
    build_ida_panel, hour_class_label, clustered_ols, WINDOWS,
)

OUT = REPO / "results/regressions/bid/mtu15_critical_flat/specC_shape_dispersion.csv"
OUT.parent.mkdir(parents=True, exist_ok=True)


def build_shape_dispersion(per_curve):
    """For each (unit, date, [session,] clock_hour) cell with all 4 quarters,
    compute D_sigma = sd across quarters of sigma_p and D_neff = sd across
    quarters of N_eff. Returns DataFrame with one row per cell."""
    pc = per_curve.copy()
    pc["quarter"] = pc["period"].mod(4)
    group_cols = ["unit_code", "d", "clock_hour"]
    if "session_number" in pc.columns and pc["session_number"].notna().any():
        group_cols.append("session_number")
    nq = pc.groupby(group_cols)["quarter"].nunique().rename("nq").reset_index()
    pc = pc.merge(nq, on=group_cols)
    pc = pc[pc["nq"] == 4].copy()
    if pc.empty:
        return pd.DataFrame()
    agg = (pc.groupby(group_cols + ["hour_class"])
             .agg(D_sigma=("sigma_p", lambda x: np.std(x.values, ddof=1)),
                  D_neff=("n_eff", lambda x: np.std(x.values, ddof=1)),
                  mean_sigma=("sigma_p", "mean"),
                  mean_neff=("n_eff", "mean"))
             .reset_index())
    return agg


def run_spec_C(disp, reform, label):
    """Post-only cross-sectional crit-vs-flat on shape-dispersion outcomes
    with date FE (absorbed via within-date demeaning of Y and crit)."""
    w = WINDOWS[reform]
    post_lo, post_hi = pd.Timestamp(w["post_lo"]), pd.Timestamp(w["post_hi"])
    p = disp.copy()
    p["d"] = pd.to_datetime(p["d"])
    p = p[(p["d"] >= post_lo) & (p["d"] <= post_hi)
          & p["hour_class"].isin(["Critical", "Flat"])].copy()
    if p.empty: return None
    p["crit"] = (p["hour_class"] == "Critical").astype(int)
    out = []
    for outcome in ["D_sigma", "D_neff"]:
        d = p.dropna(subset=[outcome]).copy()
        if len(d) < 50: continue
        # Within-date demean
        ym = d.groupby(d["d"].astype(str))[outcome].transform("mean")
        cm = d.groupby(d["d"].astype(str))["crit"].transform("mean")
        d["y_w"] = d[outcome] - ym
        d["c_w"] = d["crit"] - cm
        X = np.column_stack([np.ones(len(d)), d["c_w"].values])
        beta, se = clustered_ols(d["y_w"].values, X.astype(float),
                                  d["d"].astype(str).values)
        mean_flat = d[d["crit"] == 0][outcome].mean()
        mean_crit = d[d["crit"] == 1][outcome].mean()
        out.append({"reform": reform, "label": label, "outcome": outcome,
                    "n": len(d), "theta_crit": beta[1], "se": se[1],
                    "t": beta[1] / se[1],
                    "mean_flat": mean_flat, "mean_crit": mean_crit})
    return pd.DataFrame(out)


def main():
    rows = []

    # ID15 — IDA per-curve panel, per-unit shape dispersion
    print("=== Building ID15 IDA per-curve panel ===")
    ida = build_ida_panel(WINDOWS["ID15"]["pre_lo"], WINDOWS["ID15"]["post_hi"])
    print(f"  {len(ida):,} IDA in-band curves")
    print("  Computing ID15 shape dispersion (post only)...")
    ida_disp = build_shape_dispersion(ida)
    print(f"  {len(ida_disp):,} (unit, date, session, hour) cells with 4 quarters")
    r = run_spec_C(ida_disp, "ID15", "IDA per-unit shape")
    if r is not None:
        rows.append(r)
        for _, row in r.iterrows():
            print(f"  {row['outcome']:8s}  theta_crit={row['theta_crit']:+8.4f}  "
                  f"se={row['se']:7.4f}  t={row['t']:+6.2f}  n={int(row['n']):,}  "
                  f"mean_crit={row['mean_crit']:6.3f}  mean_flat={row['mean_flat']:6.3f}")

    # DA15 — DA per-curve panel needed (rebuild slim)
    print("\n=== Building DA15 DA per-curve panel ===")
    import duckdb
    DET = REPO / "data/processed/omie/mercado_diario/ofertas/det_all.parquet"
    CAB = REPO / "data/processed/omie/mercado_diario/ofertas/cab_all.parquet"
    MPDBC = REPO / "data/processed/omie/mercado_diario/precios/marginalpdbc_all.parquet"
    UNITS = REPO / "data/external/omie_reference/lista_unidades.csv"

    def tech_bucket(t):
        t = str(t).lower()
        if "ciclo combinado" in t: return "CCGT"
        if "hidráulica generación" in t: return "Hydro"
        if "bombeo" in t: return "Hydro_pump"
        return "Other"

    def firm_bucket(o):
        o = str(o).lower()
        if "iberdrola" in o: return "IB"
        if "endesa" in o: return "GE"
        if "naturgy" in o or "gas natural" in o: return "GN"
        if "edp" in o or "hidroel" in o: return "HC"
        return "OTH"

    units = pd.read_csv(UNITS)
    units["tech"] = units["technology"].apply(tech_bucket)
    units["firm"] = units["owner_agent"].apply(firm_bucket)
    units = units[units["tech"].isin(["CCGT", "Hydro", "Hydro_pump"])
                  & units["firm"].isin(["IB", "GE", "GN", "HC"])][
        ["unit_code", "firm", "tech"]].drop_duplicates("unit_code")
    H = 140.0
    da_lo, da_hi = WINDOWS["DA15"]["pre_lo"], WINDOWS["DA15"]["post_hi"]
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
        FROM '{CAB}' WHERE date BETWEEN '{da_lo}' AND '{da_hi}' AND buy_sell='V'
      ) WHERE rn=1
    ),
    det AS (
      SELECT CAST(date AS DATE) d, offer_code, period,
             price_eur_mwh p, quantity_mw q, COALESCE(mtu_minutes, 60) AS mtu
      FROM '{DET}' WHERE date BETWEEN '{da_lo}' AND '{da_hi}' AND quantity_mw > 0
    ),
    mp AS (
      SELECT CAST(date AS DATE) d, period, price_es_eur_mwh p_clear,
             COALESCE(mtu_minutes, 60) mtu_p
      FROM '{MPDBC}' WHERE date BETWEEN '{da_lo}' AND '{da_hi}' AND price_es_eur_mwh IS NOT NULL
    ),
    inband AS (
      SELECT mp.d, mp.period, c.unit_code, dv.q, dv.p,
             CASE WHEN COALESCE(mp.mtu_p, dv.mtu) = 60 THEN mp.period - 1
                  ELSE CAST(FLOOR((mp.period - 1) / 4.0) AS INT) END AS clock_hour
      FROM det dv JOIN cab_l c ON dv.d=c.d AND dv.offer_code=c.offer_code
        JOIN mp ON mp.d=dv.d AND mp.period=dv.period
      WHERE dv.p BETWEEN mp.p_clear - {H} AND mp.p_clear + {H}
    )
    SELECT i.d, i.period, i.clock_hour, i.unit_code, u.firm, u.tech,
           SUM(i.q) sum_w, SUM(i.q*i.p) sum_wp, SUM(i.q*i.p*i.p) sum_wp2,
           SUM(i.q*i.q) sum_w2
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
    print(f"  {len(da):,} DA in-band curves")
    print("  Computing DA15 shape dispersion (post only)...")
    da_disp = build_shape_dispersion(da)
    print(f"  {len(da_disp):,} (unit, date, hour) cells with 4 quarters")
    r = run_spec_C(da_disp, "DA15", "DA per-unit shape")
    if r is not None:
        rows.append(r)
        for _, row in r.iterrows():
            print(f"  {row['outcome']:8s}  theta_crit={row['theta_crit']:+8.4f}  "
                  f"se={row['se']:7.4f}  t={row['t']:+6.2f}  n={int(row['n']):,}  "
                  f"mean_crit={row['mean_crit']:6.3f}  mean_flat={row['mean_flat']:6.3f}")

    pd.concat(rows, ignore_index=True).to_csv(OUT, index=False)
    print(f"\nWrote {OUT}")


if __name__ == "__main__":
    main()
