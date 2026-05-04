# STATUS: ALIVE
# LAST-AUDIT: 2026-05-05
# FEEDS: Conditional parallel trends test for q_2 critical-hours DiD
# CLAIM: After conditioning on Spanish wind+solar generation × hour-of-day and
#        Spanish demand × hour-of-day, the pre-IDA crit_pre stabilizes across
#        baseline windows, supporting conditional parallel trends.
"""Conditional parallel trends test for q_2 critical-hours DiD.

The unconditional design has a parallel-trends instability: with clean
flat-hour controls h{3,4,5}, the pre-IDA crit-flat gap (crit_pre) swings
+73 (excl_crisis) to -33 (recent_2y) — a 106 MWh drift.

Hypothesized confounders driving the drift:
  - Spanish solar capacity grew ~6× from 2018 to 2024. Solar concentrates in
    midday hours, indirectly steepening evening-ramp critical hours h17-18 over
    time. Flat pre-dawn hours h{3,4,5} are untouched by solar growth.
  - Demand level growth (slight) may concentrate in critical hours.

If conditioning on (wind+solar × hour-of-day) and (demand × hour-of-day)
absorbs these trends, the conditional crit_pre should stabilize across
baseline windows, defending conditional parallel trends.

Specifications, all with treated = h{7,8,16,17,18}, control = h{3,4,5}:

  A — sparse:        q_2 ~ crit + post + crit×post + firm FE + DOW FE
  B — renewables:    A + Wind_(d,h) + Solar_(d,h) + Wind×crit + Solar×crit
  C — full controls: B + Demand_(d,h) + Demand×crit

Each estimated under five pre-IDA baseline windows (full / excl_crisis /
recent_2y / recent_1y / recent_6m). Stable crit_pre and DiD δ across baseline
windows under specs B and C → conditional parallel trends defensible.

Outcome: q_2 = signed Big-4 IDA repositioning per firm-day-hour, MWh.

Data: ENTSO-E A75 wind+solar actual generation (B16=solar, B19=wind), ENTSO-E
load_actual hourly Spain demand. Both timestamped UTC; converted to Europe/
Madrid local time to match OMIE/PIBCIE Spanish local hour conventions.
"""
from __future__ import annotations

from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
import statsmodels.api as sm

PROJECT  = Path(__file__).resolve().parents[3]
PIBCIE   = PROJECT / "data" / "processed" / "omie" / "mercado_intradiario_subastas" / "programas" / "pibcie_all.parquet"
WSACT    = PROJECT / "data" / "processed" / "entsoe" / "generation" / "wind_solar_actual_all.parquet"
LOADACT  = PROJECT / "data" / "processed" / "entsoe" / "load" / "load_actual_all.parquet"

OUT_DIR_R = PROJECT / "results" / "regressions"

CRITICAL_HOURS = [7, 8, 16, 17, 18]
FLAT_CONTROL   = [3, 4, 5]
MTU15_IDA_DATE = pd.Timestamp("2025-03-19")
PRE_IDA_END    = pd.Timestamp("2024-06-14")
DA15_START     = pd.Timestamp("2025-10-01")

BASELINES = [
    ("full",        [(pd.Timestamp("2018-01-01"), PRE_IDA_END)]),
    ("excl_crisis", [(pd.Timestamp("2018-01-01"), pd.Timestamp("2022-01-01")),
                     (pd.Timestamp("2024-01-01"), PRE_IDA_END)]),
    ("recent_2y",   [(pd.Timestamp("2022-06-14"), PRE_IDA_END)]),
    ("recent_1y",   [(pd.Timestamp("2023-06-14"), PRE_IDA_END)]),
    ("recent_6m",   [(pd.Timestamp("2023-12-14"), PRE_IDA_END)]),
]


def assign_hour(date_series, period_series, mtu15_cutoff):
    is_post = date_series >= mtu15_cutoff
    h = np.where(is_post,
                 ((period_series - 1) // 4).astype(int),
                 (period_series - 1).astype(int))
    return np.clip(h, 0, 23)


def fit_did(panel, extra_cols=None, outcome="q"):
    """DiD with optional extra control columns."""
    cols = {"const": np.ones(len(panel))}
    cols["critical"] = panel["critical"].values.astype(float)
    cols["post"] = panel["post"].values.astype(float)
    cols["crit×post"] = (panel["critical"] * panel["post"]).values.astype(float)
    if extra_cols:
        for c in extra_cols:
            cols[c] = panel[c].values.astype(float)
    for f in sorted(panel["firm"].unique())[1:]:
        cols[f"firm_{f}"] = (panel["firm"] == f).astype(float).values
    for d_ in range(1, 7):
        cols[f"DOW{d_}"] = (panel["dow"] == d_).astype(float).values
    X = pd.DataFrame(cols, index=panel.index)
    y = panel[outcome].values
    # drop any rows with NaN in X or y
    mask = (~X.isna().any(axis=1)) & (~np.isnan(y))
    X = X[mask]
    y = y[mask]
    cluster = panel.loc[X.index, "date"].dt.strftime("%Y%m%d").astype(np.int64).values
    m = sm.OLS(y, X.values).fit(cov_type="cluster", cov_kwds={"groups": cluster})
    return dict(
        crit_pre=float(m.params[X.columns.get_loc("critical")]),
        crit_pre_se=float(m.bse[X.columns.get_loc("critical")]),
        crit_pre_p=float(m.pvalues[X.columns.get_loc("critical")]),
        did=float(m.params[X.columns.get_loc("crit×post")]),
        did_se=float(m.bse[X.columns.get_loc("crit×post")]),
        did_p=float(m.pvalues[X.columns.get_loc("crit×post")]),
        crit_post=float(m.params[X.columns.get_loc("critical")]
                         + m.params[X.columns.get_loc("crit×post")]),
        n=len(y))


def main() -> None:
    con = duckdb.connect()
    con.execute("SET memory_limit='8GB'")

    # ------------------------------------------------------------------
    # 1. Build q_2 panel (Big-4 firm-day-hour)
    # ------------------------------------------------------------------
    print("[1] Big-4 q_2 panel from PIBCIE…", flush=True)
    q2 = con.execute(f"""
        SELECT CAST(date AS DATE) AS date, period, grupo_empresarial AS firm,
               SUM(assigned_power_mw * mtu_minutes / 60.0) AS q
        FROM '{PIBCIE}'
        WHERE assigned_power_mw IS NOT NULL
          AND grupo_empresarial IN ('IB','GE','GN','HC')
        GROUP BY 1, 2, 3
    """).df()
    q2["date"] = pd.to_datetime(q2["date"])
    q2["hour"] = assign_hour(q2["date"], q2["period"], MTU15_IDA_DATE)
    q2_h = q2.groupby(["date","firm","hour"], as_index=False)["q"].sum()
    q2_h["dow"] = q2_h["date"].dt.dayofweek
    q2_h = q2_h[q2_h["hour"].isin(CRITICAL_HOURS + FLAT_CONTROL)].copy()
    q2_h["critical"] = q2_h["hour"].isin(CRITICAL_HOURS).astype(int)
    print(f"   {len(q2_h):,} firm-day-hour rows (treated + control hours only)")

    # ------------------------------------------------------------------
    # 2. Build hourly Spanish wind+solar+demand controls
    #    UTC → Europe/Madrid local time, aggregate ISP→hour
    # ------------------------------------------------------------------
    print("[2] Hourly Spanish wind+solar+demand controls (UTC → Madrid local)…", flush=True)
    ws = con.execute(f"""
        SELECT isp_start_utc, psr_type, quantity_mw, mtu_minutes
        FROM '{WSACT}'
        WHERE psr_type IN ('B16', 'B19')
    """).df()
    # Convert to Madrid local time
    ws["ts_local"] = (pd.to_datetime(ws["isp_start_utc"], utc=True)
                       .dt.tz_convert("Europe/Madrid").dt.tz_localize(None))
    ws["date"] = ws["ts_local"].dt.date
    ws["hour"] = ws["ts_local"].dt.hour
    ws["energy_mwh"] = ws["quantity_mw"] * ws["mtu_minutes"] / 60.0
    ws_pivot = (ws.groupby(["date","hour","psr_type"])["energy_mwh"].sum().unstack("psr_type")
                  .rename(columns={"B16":"solar_mwh","B19":"wind_mwh"})
                  .fillna(0).reset_index())
    ws_pivot["date"] = pd.to_datetime(ws_pivot["date"])
    print(f"   wind+solar hourly rows: {len(ws_pivot):,}")

    ld = con.execute(f"""
        SELECT isp_start_utc, load_mw, mtu_minutes
        FROM '{LOADACT}'
    """).df()
    ld["ts_local"] = (pd.to_datetime(ld["isp_start_utc"], utc=True)
                       .dt.tz_convert("Europe/Madrid").dt.tz_localize(None))
    ld["date"] = ld["ts_local"].dt.date
    ld["hour"] = ld["ts_local"].dt.hour
    ld["energy_mwh"] = ld["load_mw"] * ld["mtu_minutes"] / 60.0
    ld_h = (ld.groupby(["date","hour"])["energy_mwh"].sum().reset_index()
              .rename(columns={"energy_mwh":"demand_mwh"}))
    ld_h["date"] = pd.to_datetime(ld_h["date"])
    print(f"   demand hourly rows: {len(ld_h):,}")

    controls = ws_pivot.merge(ld_h, on=["date","hour"], how="inner")
    # Convert to GWh for numerical stability of coefficients
    for c in ["wind_mwh","solar_mwh","demand_mwh"]:
        controls[c.replace("_mwh","_GWh")] = controls[c] / 1000.0
    print(f"   merged controls: {len(controls):,} (date, hour) rows")

    # ------------------------------------------------------------------
    # 3. Merge into panel
    # ------------------------------------------------------------------
    panel = q2_h.merge(controls[["date","hour","wind_GWh","solar_GWh","demand_GWh"]],
                       on=["date","hour"], how="inner")
    print(f"   merged panel: {len(panel):,} (lost {len(q2_h)-len(panel):,} rows missing controls)")

    # Use raw controls (GWh) — z-scoring across the full panel creates a moving-
    # target reference point across baseline windows. With raw controls, the
    # "critical" coefficient is the crit-flat gap when controls=0 (physically
    # meaningless but stable across windows). The DiD δ is what we care about.
    for c in ["wind_GWh","solar_GWh","demand_GWh"]:
        panel[c+"_x_crit"] = panel[c] * panel["critical"]

    # ------------------------------------------------------------------
    # 4. Run for each baseline × spec
    # ------------------------------------------------------------------
    SPECS = {
        "A: sparse":           [],
        "B: renewables":       ["wind_GWh","solar_GWh",
                                "wind_GWh_x_crit","solar_GWh_x_crit"],
        "C: ren+demand":       ["wind_GWh","solar_GWh","demand_GWh",
                                "wind_GWh_x_crit","solar_GWh_x_crit","demand_GWh_x_crit"],
    }

    rows = []
    for spec_label, extra in SPECS.items():
        print()
        print("=" * 130)
        print(f"Spec {spec_label}, control = h{{3,4,5}}, treated = h{{7,8,16,17,18}}, outcome = q_2 (Big-4 IDA, MWh/firm-hour)")
        print(f"controls: {extra if extra else '(none)'}")
        print("=" * 130)
        print(f"  {'pre-window':14s} {'mo':>4s} {'N':>9s} | {'crit_pre':>9s} ({'SE':>5s}, p={'p':>7s}) | {'DiD δ':>9s} ({'SE':>5s}, p={'p':>7s}) | {'crit_post':>10s}")

        for bl_label, intervals in BASELINES:
            pre = pd.concat([
                panel[(panel["date"] >= s) & (panel["date"] < e)]
                for s, e in intervals
            ]).assign(post=0)
            post = panel[panel["date"] >= DA15_START].assign(post=1)
            sub = pd.concat([pre, post], ignore_index=True)
            if len(pre) < 100 or len(post) < 100:
                continue
            res = fit_did(sub, extra_cols=extra)
            n_months = sum((e - s).days / 30 for s, e in intervals)
            print(f"  {bl_label:14s} {n_months:>4.1f} {res['n']:>9,} | {res['crit_pre']:>+9.2f} ({res['crit_pre_se']:>5.2f}, p={res['crit_pre_p']:>7.1e}) | {res['did']:>+9.2f} ({res['did_se']:>5.2f}, p={res['did_p']:>7.1e}) | {res['crit_post']:>+10.2f}")
            rows.append({"spec": spec_label, "baseline": bl_label,
                         "n_months": n_months,
                         "crit_pre": res["crit_pre"], "crit_pre_se": res["crit_pre_se"], "crit_pre_p": res["crit_pre_p"],
                         "did": res["did"], "did_se": res["did_se"], "did_p": res["did_p"],
                         "crit_post": res["crit_post"], "n": res["n"]})

    pd.DataFrame(rows).to_csv(OUT_DIR_R / "critical_hours_conditional_parallel_trends.csv", index=False)
    print()
    print(f"wrote {OUT_DIR_R / 'critical_hours_conditional_parallel_trends.csv'}")


if __name__ == "__main__":
    main()
