# STATUS: DEAD-KEPT-AS-RECORD
# LAST-AUDIT: 2026-04-29
# RETRACTION-DATE: 2026-04-29
# RETRACTION-REASON: F5 (Allaz-Vila slope β=∂ΔQ_IDA/∂q_DA) is a mechanical accounting
#   identity, not strategic-conduct evidence. Q_actual ≈ q_DA + ΔQ_IDA implies
#   ∂ΔQ_IDA/∂q_DA = ∂Q_actual/∂q_DA − 1; since q_DA explains nearly all within-unit
#   variation in Q_actual, β is mechanically near −1 regardless of strategic conduct.
#   The HDFE absorption that 'restored' F5 was confirming the identity, not testing AV.
#   AV anchor for the thesis is now B9's firm-ISP cross-regime regression
#   (b9_replicated_isp_grain.py), which identifies via cross-regime variation, not
#   within-unit slope.
"""Allaz-Vila portfolio decomposition (modelling-track §2 refinement).

Hypothesis. The Allaz-Vila commitment-value mechanism applies when
firms have strategic-marginal capacity. In the Spanish wholesale market,
that's CCGT (gas turbines) for thermal-portfolio firms. Hydro and
nuclear are inframarginal for most pricing decisions.

If the Allaz-Vila mechanism is the right reading of F5, the slope
beta = d(dq_IDA)/dq_DA should be:
  * MORE negative (stronger commitment-deterrent) in CCGT-margin hours
    (peak demand, high clearing price)
  * LESS negative or zero in hydro/baseload-margin hours
    (off-peak, low clearing price)

If beta has the same sign and magnitude across these partitions,
Allaz-Vila isn't the mechanism (it's just a generic q_DA correlation).

Two cuts:
  (1) Peak (h11-22) vs off-peak (h1-10 + h23-24) — proxy for CCGT margin
  (2) Clearing-price quartile — direct proxy for marginal tech

Sample restricted to MTU15-IDA-onwards regimes (DA60/ID15 + DA15/ID15)
where the F5 sign-flip vs pre-reform was strongest. We pool pre-reform
regimes as 'pre-MTU15-IDA' since within-regime Ns were too small for
the partition cut.
"""
from __future__ import annotations

from pathlib import Path
import duckdb
import numpy as np
import pandas as pd
import statsmodels.api as sm

PROJECT = Path(__file__).resolve().parents[3]
PANEL = PROJECT / "data" / "derived" / "panels" / "allaz_vila_panel.parquet"
PRICES = PROJECT / "data" / "processed" / "omie" / "mercado_diario" / "precios" / "marginalpdbc_all.parquet"

REGIMES = ["pre-IDA", "3-sess", "ISP15 win", "DA60/ID15", "DA15/ID15"]
BIG4 = ("GE", "IB", "GN", "HC")

# Pool the pre-MTU15-IDA regimes for power; F5 already shows the
# regime-by-regime evolution. The portfolio split tests *whether* the
# observed slope changes by tech, not *when* the change happened.
POOLED_REGIMES = {
    "pre-MTU15-IDA": ["pre-IDA", "3-sess", "ISP15 win"],
    "DA60/ID15": ["DA60/ID15"],
    "DA15/ID15": ["DA15/ID15"],
}


def load_panel_with_price() -> pd.DataFrame:
    df = pd.read_parquet(PANEL)
    df["date"] = pd.to_datetime(df["date"])

    # Hourly DA clearing price (mean across mtu_minutes within an hour-of-day)
    con = duckdb.connect()
    prices = con.sql(f"""
        SELECT date,
               CASE WHEN mtu_minutes = 15 THEN CEIL(period / 4.0)::INTEGER
                    ELSE period END AS hour,
               AVG(price_es_eur_mwh) AS p_da
        FROM '{PRICES}'
        WHERE price_es_eur_mwh IS NOT NULL
        GROUP BY 1, 2
    """).df()
    prices["date"] = pd.to_datetime(prices["date"])

    out = df.merge(prices, on=["date", "hour"], how="left")
    return out


def fit(sub: pd.DataFrame) -> dict:
    if len(sub) < 200:
        return {"n": len(sub), "beta": np.nan, "se": np.nan, "p": np.nan}
    X = sm.add_constant(sub[["q_da"]].astype(float))
    y = sub["dq_ida"].astype(float)
    res = sm.OLS(y, X).fit(
        cov_type="cluster",
        cov_kwds={"groups": sub["date"].values},
    )
    return {
        "n": len(sub),
        "beta": float(res.params["q_da"]),
        "se": float(res.bse["q_da"]),
        "p": float(res.pvalues["q_da"]),
    }


def main() -> None:
    df = load_panel_with_price()
    df["pooled_regime"] = "(other)"
    for label, regs in POOLED_REGIMES.items():
        df.loc[df["regime"].isin(regs), "pooled_regime"] = label
    df = df[df["pooled_regime"] != "(other)"].copy()

    # Peak/off-peak by hour-of-day
    df["peak"] = df["hour"].between(11, 22)

    # Clearing-price quartile (computed pooled, so partition is exogenous to firm activity within each cell)
    qs = df["p_da"].quantile([0.25, 0.50, 0.75]).values
    def price_q(p):
        if pd.isna(p): return np.nan
        if p <= qs[0]: return "Q1 low"
        if p <= qs[1]: return "Q2"
        if p <= qs[2]: return "Q3"
        return "Q4 high"
    df["price_q"] = df["p_da"].apply(price_q)

    print(f"Panel rows: {len(df):,}")
    print(f"Price quartile cuts: q25={qs[0]:.1f}, q50={qs[1]:.1f}, q75={qs[2]:.1f} EUR/MWh")
    print()

    # ============================================================
    # CUT 1: peak vs off-peak
    # ============================================================
    print("=" * 90)
    print("CUT 1: Allaz-Vila slope by peak (h11-22) vs off-peak hours")
    print("(peak hours = CCGT-margin proxy)")
    print("=" * 90)
    rows = []
    for firm in BIG4:
        for r in ["pre-MTU15-IDA", "DA60/ID15", "DA15/ID15"]:
            for is_peak, label in [(True, "peak"), (False, "off-peak")]:
                sub = df[(df["firm"] == firm) & (df["pooled_regime"] == r) & (df["peak"] == is_peak)]
                f = fit(sub)
                rows.append({"firm": firm, "regime": r, "partition": label, **f})
    tab = pd.DataFrame(rows)

    for firm in BIG4:
        print(f"\n{firm}:")
        print(f"  {'regime':<14} {'partition':<10}  {'n':>8}  {'beta':>10}  {'se':>9}  {'p':>7}")
        sub = tab[tab["firm"] == firm]
        for _, row in sub.iterrows():
            sig = "***" if row["p"] < 0.001 else ("**" if row["p"] < 0.01 else (" *" if row["p"] < 0.05 else "  "))
            print(f"  {row['regime']:<14} {row['partition']:<10}  {row['n']:>8,}  {row['beta']:>+10.4f}{sig} {row['se']:>9.4f}  {row['p']:>7.3f}")

    # Headline test: does the (DA15/ID15 - pre) sign-flip concentrate in peak hours for thermal firms?
    print()
    print("Headline test: change in slope from pre-MTU15-IDA to DA15/ID15, by peak vs off-peak")
    print("(Allaz-Vila prediction: change should be larger in peak hours for CCGT-heavy firms)")
    print(f"  {'firm':<5}  {'peak Δβ':>10}  {'off-peak Δβ':>14}  {'peak signal stronger?'}")
    for firm in BIG4:
        sub = tab[tab["firm"] == firm].set_index(["regime", "partition"])
        try:
            d_peak = sub.loc[("DA15/ID15", "peak"), "beta"] - sub.loc[("pre-MTU15-IDA", "peak"), "beta"]
            d_off  = sub.loc[("DA15/ID15", "off-peak"), "beta"] - sub.loc[("pre-MTU15-IDA", "off-peak"), "beta"]
        except KeyError:
            continue
        verdict = "YES" if abs(d_peak) > abs(d_off) else "no"
        print(f"  {firm:<5}  {d_peak:>+10.4f}  {d_off:>+14.4f}  {verdict}")

    # ============================================================
    # CUT 2: by clearing-price quartile (direct CCGT-margin proxy)
    # ============================================================
    print()
    print("=" * 90)
    print("CUT 2: Allaz-Vila slope by clearing-price quartile (CCGT-margin proxy)")
    print("(Q1 low = hydro/wind margin; Q4 high = CCGT margin)")
    print("=" * 90)

    rows2 = []
    for firm in BIG4:
        for r in ["pre-MTU15-IDA", "DA60/ID15", "DA15/ID15"]:
            for q in ["Q1 low", "Q2", "Q3", "Q4 high"]:
                sub = df[(df["firm"] == firm) & (df["pooled_regime"] == r) & (df["price_q"] == q)]
                f = fit(sub)
                rows2.append({"firm": firm, "regime": r, "price_q": q, **f})
    tab2 = pd.DataFrame(rows2)

    print()
    print("Δβ (DA15/ID15 - pre-MTU15-IDA), by price quartile:")
    print(f"  {'firm':<5}  {'Q1 low':>10}  {'Q2':>10}  {'Q3':>10}  {'Q4 high':>10}")
    for firm in BIG4:
        sub = tab2[tab2["firm"] == firm].set_index(["regime", "price_q"])
        deltas = []
        for q in ["Q1 low", "Q2", "Q3", "Q4 high"]:
            try:
                d = sub.loc[("DA15/ID15", q), "beta"] - sub.loc[("pre-MTU15-IDA", q), "beta"]
                deltas.append(d)
            except KeyError:
                deltas.append(np.nan)
        line = f"  {firm:<5}  " + "  ".join(f"{d:>+10.4f}" if not pd.isna(d) else f"{'(NA)':>10}" for d in deltas)
        print(line)

    # Save tables
    out1 = PROJECT / "results" / "regressions" / "allaz_vila_peak_offpeak.csv"
    out2 = PROJECT / "results" / "regressions" / "allaz_vila_price_quartile.csv"
    tab.to_csv(out1, index=False)
    tab2.to_csv(out2, index=False)
    print(f"\nwrote {out1}")
    print(f"wrote {out2}")


if __name__ == "__main__":
    main()
