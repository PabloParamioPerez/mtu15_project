# STATUS: ALIVE
# LAST-AUDIT: 2026-04-28
# FEEDS: S8 robustness (aggregation-discipline check on the wounded claim)
# CLAIM: S8's wounding under monthly aggregation may be an aggregation artefact
#        — the renewable control is collinear with regime dummies at monthly
#        resolution because both are essentially time trends. Daily-level
#        re-analysis with within-regime renewable variation provides separate
#        identification of the regime and renewable channels.
"""S8 daily disaggregated re-analysis: aggregation-artefact check.

The original `s8_renewable_control.py` regression aggregated RZ-61 activations
to monthly totals (n=78 months). At that aggregation level:

  - Spanish renewable installed capacity grew ~6× across 2018-2025.
  - Monthly average wind+solar MW is therefore essentially a smooth time trend.
  - Regime dummies are also essentially time-window dummies (sequential
    calendar windows).
  - The two are highly collinear at monthly resolution; identification of
    the regime coefficient *separate from* the renewable trend is weak.
  - Result: when the renewable control is added, regime coefficients can
    collapse not because the regime effect is null, but because monthly-
    aggregated data lacks the within-regime variation needed to separate
    the two channels.

This re-analysis uses DAILY data (~3000 daily observations vs 78 monthly):

  Outcome:    daily total RZ-61 activations in MWh (qty_up + qty_down)
  Regressor:  regime dummies (5 levels, drop pre-IDA)
  Controls:   daily wind+solar generation (MWh) -- huge within-regime variation
              cal-month FE -- structural seasonality
              day-of-week FE -- weekly structure (workday vs weekend)
              year FE -- long trend
  SEs:        cluster-robust by year-month (~80 clusters, OK)

If the regime coefficients survive the daily-level renewable control, the
S8 wounding was an aggregation artefact and the original alive claim should
be reinstated (with caveats). If they still collapse, the wounding is real.

Output:
    data/derived/results/s8_daily_disaggregated.csv
"""
from __future__ import annotations

from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
import statsmodels.api as sm

PROJECT = Path(__file__).resolve().parents[3]
RP48 = PROJECT / "data" / "processed" / "esios" / "restricciones" / "totalrp48preccierre_all.parquet"
ACTUAL = PROJECT / "data" / "processed" / "entsoe" / "generation" / "wind_solar_actual_all.parquet"
OUT = PROJECT / "data" / "derived" / "results" / "s8_daily_disaggregated.csv"

REGIMES = ["pre-IDA", "3-sess", "ISP15 win", "DA60/ID15", "DA15/ID15"]


def assign_regime(d: pd.Timestamp) -> str:
    if d < pd.Timestamp("2024-06-14"): return "pre-IDA"
    if d < pd.Timestamp("2024-12-01"): return "3-sess"
    if d < pd.Timestamp("2025-03-19"): return "ISP15 win"
    if d < pd.Timestamp("2025-10-01"): return "DA60/ID15"
    return "DA15/ID15"


def build_daily_panel() -> pd.DataFrame:
    con = duckdb.connect()
    con.execute("SET memory_limit='6GB'")
    con.execute("SET threads=4")

    print("[1/3] Building daily RZ-61 activations…")
    rz_daily = con.execute(f"""
        SELECT  CAST(period_start_utc AS DATE) AS date,
                SUM(COALESCE(qty_up_mwh, 0) + COALESCE(qty_down_mwh, 0)) AS rz_mwh
        FROM '{RP48}'
        WHERE tipo_redespacho = '61'
        GROUP BY 1
        ORDER BY 1
    """).df()
    rz_daily["date"] = pd.to_datetime(rz_daily["date"])
    print(f"   daily RZ-61 panel: {len(rz_daily):,} days, "
          f"range {rz_daily.date.min().date()} → {rz_daily.date.max().date()}")

    print("[2/3] Building daily wind+solar generation (B16 solar + B19 wind onshore + B18 wind offshore)…")
    vre_daily = con.execute(f"""
        SELECT  CAST(isp_start_utc AS DATE) AS date,
                SUM(quantity_mw * mtu_minutes / 60.0) AS vre_mwh
        FROM '{ACTUAL}'
        WHERE psr_type IN ('B16', 'B18', 'B19')
        GROUP BY 1
        ORDER BY 1
    """).df()
    vre_daily["date"] = pd.to_datetime(vre_daily["date"])
    print(f"   daily VRE panel: {len(vre_daily):,} days, "
          f"avg {vre_daily.vre_mwh.mean()/1000:.0f} GWh/day, "
          f"range {vre_daily.vre_mwh.min()/1000:.0f}–{vre_daily.vre_mwh.max()/1000:.0f} GWh/day")

    print("[3/3] Joining and assigning regime…")
    df = rz_daily.merge(vre_daily, on="date", how="inner")
    df["regime"] = df["date"].apply(assign_regime)
    df["year"] = df["date"].dt.year
    df["cal_month"] = df["date"].dt.month
    df["dow"] = df["date"].dt.dayofweek
    df["regime_cat"] = pd.Categorical(df["regime"], categories=REGIMES, ordered=False)
    df = df.dropna(subset=["rz_mwh", "vre_mwh"])
    print(f"   joined panel: {len(df):,} daily observations")
    print(f"   regime counts:")
    for r in REGIMES:
        n = (df.regime == r).sum()
        print(f"     {r:<14}  {n:>5}")
    print()
    return df


def fit_spec(df: pd.DataFrame, name: str, controls: list[str], cluster_by_month: bool = True):
    """Fit a regression with the given controls and (optionally) cluster SEs by year-month."""
    # Build design matrix
    cols = {"const": 1.0}
    for r in REGIMES[1:]:
        cols[f"D[{r}]"] = (df["regime"] == r).astype(float).values

    if "vre" in controls:
        cols["vre_gwh"] = (df["vre_mwh"] / 1000.0).values  # in GWh for readability
    if "cal_month" in controls:
        for m in range(2, 13):
            cols[f"M[{m}]"] = (df["cal_month"] == m).astype(float).values
    if "dow" in controls:
        for d_ in range(1, 7):
            cols[f"DOW[{d_}]"] = (df["dow"] == d_).astype(float).values
    if "year" in controls:
        years = sorted(df["year"].unique())
        for yr in years[1:]:
            cols[f"Y[{yr}]"] = (df["year"] == yr).astype(float).values

    X = pd.DataFrame(cols, index=df.index)
    y = df["rz_mwh"].values / 1000.0  # GWh outcome for readability

    if cluster_by_month:
        cluster = (df["year"] * 100 + df["cal_month"]).values
        m = sm.OLS(y, X.values).fit(cov_type="cluster", cov_kwds={"groups": cluster})
    else:
        m = sm.OLS(y, X.values).fit(cov_type="HC3")

    out = {"spec": name, "n": len(df), "r2": float(m.rsquared), "controls": ",".join(controls)}
    for r in REGIMES[1:]:
        j = list(X.columns).index(f"D[{r}]")
        out[f"{r}_beta"] = float(m.params[j])
        out[f"{r}_se"] = float(m.bse[j])
        out[f"{r}_t"] = float(m.params[j] / m.bse[j])
        out[f"{r}_p"] = float(m.pvalues[j])
    if "vre" in controls:
        j = list(X.columns).index("vre_gwh")
        out["vre_beta"] = float(m.params[j])
        out["vre_se"] = float(m.bse[j])
        out["vre_p"] = float(m.pvalues[j])
    return out, m, X


def main() -> None:
    df = build_daily_panel()

    specs = [
        ("Spec 1: regime only (sparse)",                 []),
        ("Spec 2: + cal-month FE",                       ["cal_month"]),
        ("Spec 3: + cal-month FE + DOW FE",              ["cal_month", "dow"]),
        ("Spec 4: + cal-month + DOW + VRE (daily GWh)",  ["cal_month", "dow", "vre"]),
        ("Spec 5: + cal-month + DOW + VRE + year FE",    ["cal_month", "dow", "vre", "year"]),
    ]

    rows = []
    for name, ctrl in specs:
        r, model, X = fit_spec(df, name, ctrl)
        rows.append(r)

    print("=" * 95)
    print("S8 DAILY DISAGGREGATED RE-ANALYSIS")
    print("=" * 95)
    print(f"  Outcome: daily RZ-61 activations (GWh/day)")
    print(f"  N = {rows[0]['n']:,} daily observations  vs  78 monthly in original spec  ({rows[0]['n']/78:.0f}× more data)")
    print(f"  SEs: cluster-robust by year-month")
    print()

    # Side-by-side coefficients across specs
    cols = ['Spec', 'R²', 'β(3-sess)', 'β(ISP15)', 'β(DA60/ID15)', 'β(DA15/ID15)', 'β(VRE)']
    fmt = '{:<48}  {:>5}  {:>13}  {:>13}  {:>13}  {:>13}  {:>10}'
    print(fmt.format(*cols))
    print('-' * 130)
    for r in rows:
        rr = lambda b, se, p: f"{b:+.1f}({p:.2f})" if b is not None else "       — "
        b3   = rr(r.get("3-sess_beta"),    r.get("3-sess_se"),    r.get("3-sess_p"))
        bisp = rr(r.get("ISP15 win_beta"), r.get("ISP15 win_se"), r.get("ISP15 win_p"))
        bda60 = rr(r.get("DA60/ID15_beta"), r.get("DA60/ID15_se"), r.get("DA60/ID15_p"))
        bda15 = rr(r.get("DA15/ID15_beta"), r.get("DA15/ID15_se"), r.get("DA15/ID15_p"))
        bvre  = f"{r.get('vre_beta', 0):+.3f}" if r.get('vre_beta') is not None else "    — "
        print(fmt.format(r['spec'], f"{r['r2']:.3f}", b3, bisp, bda60, bda15, bvre))
    print('  (entries: β(p-value); β in GWh/day for regime dummies, GWh/day per GWh-VRE for VRE)')
    print()

    print("=" * 95)
    print("HEADLINE: comparison of original (monthly) vs disaggregated (daily) under matched specs")
    print("=" * 95)
    print()
    print("   The original wounding regression (s8_renewable_control.py) had NO year FE — controls")
    print("   were: regime + cal-month FE + monthly average renewable MW.  At monthly resolution,")
    print("   that renewable control is essentially a smooth time trend (Spanish capacity grew 6×")
    print("   over the sample), highly collinear with regime dummies.")
    print()
    print("   The CLEANEST DAILY ANALOG of that spec is Spec 4 (no year FE).  Spec 5 adds year FE")
    print("   as a more aggressive trend control, but year FE is collinear with regime by")
    print("   construction (post-IDA regimes are entirely within recent years).  Reporting both.")
    print()

    # original monthly numbers (from ledger for comparison reference)
    original = {
        "3-sess":    (+70,   0.077),  # marginal in original
        "ISP15 win": (+156,  0.022),  # only survivor in original
        "DA60/ID15": (-27,   0.610),  # COLLAPSED in original (original wounding)
        "DA15/ID15": (-43,   0.420),  # COLLAPSED in original (original wounding)
    }

    sp4 = rows[3]  # Spec 4: daily analog of original (no year FE)
    sp5 = rows[4]  # Spec 5: + year FE

    print(f"   {'Regime':<14}  {'monthly orig (Spec 3)':>22}  {'daily Spec 4 (no yr FE)':>26}  {'daily Spec 5 (+yr FE)':>23}")
    print(f"   {'':<14}  {'β (GWh/mo)   p':>22}  {'β (GWh/day)   p':>26}  {'β (GWh/day)   p':>23}")
    print('   ' + '-' * 92)
    for reg in REGIMES[1:]:
        ob, op = original.get(reg, (None, None))
        b4, p4 = sp4.get(f'{reg}_beta'), sp4.get(f'{reg}_p')
        b5, p5 = sp5.get(f'{reg}_beta'), sp5.get(f'{reg}_p')
        cell_orig = f"{ob:+5.0f}  ({op:.3f})" if ob is not None else "      —"
        cell_sp4  = f"{b4:+6.2f}  ({p4:.3f})" if b4 is not None else "      —"
        cell_sp5  = f"{b5:+6.2f}  ({p5:.3f})" if b5 is not None else "      —"
        print(f'   {reg:<14}  {cell_orig:>22}  {cell_sp4:>26}  {cell_sp5:>23}')

    print()
    print("=" * 95)
    print("Honest reading:")
    print("=" * 95)
    print()
    print("  Spec 4 (daily, NO year FE — direct analog of the original wounding spec):")
    for reg in REGIMES[1:]:
        b, p = sp4.get(f'{reg}_beta'), sp4.get(f'{reg}_p')
        sign = '+' if b > 0 else '-'
        sig  = 'sig' if p < 0.05 else 'ns '
        ob = original[reg][0]; op = original[reg][1]
        flip_status = ''
        if ob is not None:
            if ob < 0 and b > 0 and p < 0.05:
                flip_status = '  ←  ORIGINAL WOUNDING REVERSED at daily level'
            elif ob > 0 and b > 0 and p < 0.05:
                flip_status = '  ←  positive effect survives at daily level'
            elif p >= 0.05:
                flip_status = '  ←  daily ns; wounding holds'
        b_monthly_equiv = b * 30
        print(f'    {reg:<14}  β = {b:+6.2f} GWh/day  ({b_monthly_equiv:+5.0f} GWh/mo equiv)  '
              f'p = {p:.3f}  ({sig}){flip_status}')
    print()
    n_pos = sum(1 for r in REGIMES[1:] if sp4.get(f'{r}_beta', 0) > 0 and sp4.get(f'{r}_p', 1) < 0.05)
    n_orig_pos = sum(1 for r in REGIMES[1:] if (original[r][0] or 0) > 0 and (original[r][1] or 1) < 0.05)
    print(f'    Number of regimes with positive significant effect: daily Spec 4 = {n_pos}/4;'
          f'  monthly orig = {n_orig_pos}/4')
    print()
    print("  Spec 5 (daily + year FE — more conservative; year FE is collinear with regime):")
    for reg in REGIMES[1:]:
        b, p = sp5.get(f'{reg}_beta'), sp5.get(f'{reg}_p')
        b_monthly_equiv = b * 30
        sign_str = 'positive' if b > 0 else 'NEGATIVE'
        sig_str = 'sig' if p < 0.05 else 'ns'
        print(f'    {reg:<14}  β = {b:+6.2f} GWh/day  ({b_monthly_equiv:+5.0f} GWh/mo equiv)  '
              f'p = {p:.3f}  ({sign_str}, {sig_str})')
    print()
    print("=" * 95)
    print("Bottom-line interpretation:")
    print("=" * 95)
    if n_pos >= 3 and n_orig_pos < 3:
        print('  Spec 4 (daily, original-equivalent controls) reverses the wounding: regime')
        print('  coefficients are POSITIVE and significant at daily resolution after the same')
        print('  cal-month + renewable controls that monthly aggregation said killed the effect.')
        print('  This is consistent with monthly-aggregation collinearity between regime dummies')
        print('  and the renewable trend; daily within-regime VRE variation separates the channels.')
        print()
        print('  Spec 5 (adding year FE) absorbs the regime trend mechanically because post-IDA')
        print('  regimes are entirely within recent years.  Year FE is a debatable control here:')
        print('  it answers a different question ("are regime effects above the year mean?")')
        print('  rather than the original S8 question ("did RZ activations rise post-IDA?").')
        print()
        print('  RECOMMENDED LEDGER UPDATE: S8 wounding was an aggregation artefact under the')
        print('  original specifications.  At daily disaggregation with the same controls, regime')
        print('  effects survive positive and significant.  Reinstate as ALIVE with the methodology')
        print('  caveat that year FE specs (which absorb the trend mechanically) give different')
        print('  point estimates and reflect a stricter identification benchmark.')
    elif n_pos == 0:
        print('  Even at daily level with original-equivalent controls, post-IDA regime coefficients')
        print('  are NOT positive and significant.  The wounding holds; not an aggregation artefact.')
    else:
        print(f'  Mixed result: {n_pos}/4 regimes survive at daily level with original controls.')
        print('  Partial revival; chapter should report both specs and the methodology choice.')

    OUT.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(OUT, index=False)
    print(f"\nwrote {OUT}")


if __name__ == "__main__":
    main()
