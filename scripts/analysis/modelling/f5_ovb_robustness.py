# STATUS: ALIVE
# LAST-AUDIT: 2026-04-27
# FEEDS: F5 OVB-robustness — does the IB peak-hour sign-flip survive richer controls?
# CLAIM: F5's IB peak-hour Δβ = +0.049 (sign-flip from -0.025 to +0.024 across MTU15-DA) survives OVB controls.

"""F5 OVB robustness check — Allaz-Vila peak-hour signal for IB.

F5's headline (currently wounded): IB's peak-hour Allaz-Vila slope
β = ∂ΔQ_IDA / ∂q_DA flips sign at MTU15-DA (-0.025 → +0.024, Δβ = +0.049,
p<0.05). The off-peak partition shows no such flip — within-firm placebo.

Concern: the original spec is just ΔQ_IDA = α + β·q_DA (clustered SE by
date), with the peak (h11-22) partition as the only structural control.
Omitted variables that could drive both q_DA and ΔQ_IDA:
  - Hourly DA price (high prices → both larger q_DA and larger |ΔQ_IDA|)
  - VRE generation (low VRE → high prices → both signals correlate)
  - Hour-of-day within peak/off-peak (h11 differs from h22)
  - Day-of-week (weekday vs weekend cycle in load)

Test progressively richer specs, isolating the (regime × partition × firm)
β coefficient:
  Spec 1 (original): ΔQ_IDA ~ q_DA
  Spec 2: + p_da
  Spec 3: + p_da + p_da²
  Spec 4: + VRE
  Spec 5: + hour FE within peak/off-peak
  Spec 6: + day-of-week FE

If the IB peak-hour Δβ stays around +0.05 across specs, F5 promotes from
wounded to alive. If collapses or flips, F5 stays wounded or dies.
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
VRE = PROJECT / "data" / "processed" / "entsoe" / "generation" / "wind_solar_actual_all.parquet"

BIG4 = ["GE", "IB", "GN", "HC"]
POOLED_REGIMES = {
    "pre-MTU15-IDA": ["pre-IDA", "3-sess", "ISP15-win"],
    "DA60/ID15": ["DA60/ID15"],
    "DA15/ID15": ["DA15/ID15"],
}


def load_panel_with_controls() -> pd.DataFrame:
    df = pd.read_parquet(PANEL)
    df["date"] = pd.to_datetime(df["date"])

    con = duckdb.connect()
    prices = con.sql(f"""
        WITH hp AS (
            SELECT date,
                   CASE WHEN mtu_minutes = 15 THEN CEIL(period / 4.0)::INTEGER
                        ELSE period END AS hour,
                   price_es_eur_mwh AS p
            FROM '{PRICES}'
            WHERE price_es_eur_mwh IS NOT NULL
        )
        SELECT date, hour, AVG(p) AS p_da FROM hp GROUP BY 1, 2
    """).df()
    prices["date"] = pd.to_datetime(prices["date"])

    # Hourly Spanish wind+solar
    vre = pd.read_parquet(VRE, columns=["isp_start_utc", "psr_type", "quantity_mw", "mtu_minutes"])
    vre = vre[vre["psr_type"].isin(["B01", "B16", "B18", "B19"])].copy()
    vre["isp_start"] = pd.to_datetime(vre["isp_start_utc"]).dt.tz_localize(None)
    vre["date"] = vre["isp_start"].dt.normalize()
    vre["hour"] = vre["isp_start"].dt.hour + 1
    vre["mwh"] = vre["quantity_mw"] * (vre["mtu_minutes"] / 60.0)
    vre_h = vre.groupby(["date", "hour"], as_index=False)["mwh"].sum().rename(columns={"mwh": "vre_mw"})

    out = df.merge(prices, on=["date", "hour"], how="left")
    out = out.merge(vre_h, on=["date", "hour"], how="left")
    out["vre_mw"] = out["vre_mw"].fillna(out["vre_mw"].mean())
    out["dow"] = out["date"].dt.dayofweek
    return out


def fit_with_controls(sub: pd.DataFrame, controls: list[str], add_hour_fe: bool, add_dow_fe: bool) -> dict:
    if len(sub) < 200:
        return {"n": len(sub), "beta": np.nan, "se": np.nan, "p": np.nan}
    X_parts = [sub[["q_da"]].astype(float)]
    if controls:
        X_parts.append(sub[controls].astype(float))
    if add_hour_fe:
        h = pd.get_dummies(sub["hour"], prefix="h", drop_first=True, dtype=float)
        X_parts.append(h)
    if add_dow_fe:
        d = pd.get_dummies(sub["dow"], prefix="d", drop_first=True, dtype=float)
        X_parts.append(d)
    X = pd.concat(X_parts, axis=1)
    X = sm.add_constant(X)
    y = sub["dq_ida"].astype(float)
    try:
        res = sm.OLS(y, X).fit(cov_type="cluster", cov_kwds={"groups": sub["date"].values})
    except Exception as e:
        return {"n": len(sub), "beta": np.nan, "se": np.nan, "p": np.nan, "error": str(e)}
    return {
        "n": len(sub),
        "beta": float(res.params["q_da"]),
        "se": float(res.bse["q_da"]),
        "p": float(res.pvalues["q_da"]),
    }


def main() -> None:
    df = load_panel_with_controls()
    df["pooled_regime"] = "(other)"
    for label, regs in POOLED_REGIMES.items():
        df.loc[df["regime"].isin(regs), "pooled_regime"] = label
    df = df[df["pooled_regime"] != "(other)"].copy()
    df["peak"] = df["hour"].between(11, 22)
    df["p_da_sq"] = df["p_da"] ** 2
    print(f"Panel rows: {len(df):,}")

    # All controls below are valid: VRE is exogenous (weather), p_da is
    # predetermined relative to ΔQ_IDA (DA clears BEFORE IDA bidding, so p_da
    # is observed at the time of IDA repositioning — NOT jointly determined
    # with the outcome). p_da is correlated with q_DA (multicollinearity) but
    # that's not bad-control bias on β(q_DA → ΔQ_IDA).
    specs = [
        ("Spec 1 (original sparse)",                  [],                              False, False),
        ("Spec 2 (+ VRE only)",                       ["vre_mw"],                      False, False),
        ("Spec 3 (+ VRE + hour FE)",                  ["vre_mw"],                      True,  False),
        ("Spec 4 (+ VRE + hour FE + DOW FE)",         ["vre_mw"],                      True,  True),
        ("Spec 5 (+ p_da)",                           ["p_da"],                        False, False),
        ("Spec 6 (full: VRE + p_da + p_da² + FE)",    ["p_da", "p_da_sq", "vre_mw"],   True,  True),
    ]

    print()
    print("Headline test: peak-hour Δβ from pre-MTU15-IDA to DA15/ID15 across specs (per firm)")
    print("Allaz-Vila prediction: Δβ_peak > 0 (slope flips from negative toward positive in peak hours)")
    print("Within-firm placebo: Δβ_off < Δβ_peak (off-peak should NOT show the same sign-flip)")
    print()
    print(f"{'Spec':<28} {'firm':<3}  {'pre peak':>9}  {'post peak':>10}  {'Δβ_peak':>9}  {'pre off':>8}  {'post off':>9}  {'Δβ_off':>8}  {'placebo OK?'}")
    print("-" * 130)

    for spec_name, controls, hr_fe, dw_fe in specs:
        for firm in BIG4:
            cells = {}
            for partition, is_peak in [("peak", True), ("off-peak", False)]:
                for regime in ["pre-MTU15-IDA", "DA15/ID15"]:
                    sub = df[(df["firm"] == firm) & (df["pooled_regime"] == regime) & (df["peak"] == is_peak)]
                    f = fit_with_controls(sub, controls, hr_fe, dw_fe)
                    cells[(regime, partition)] = f["beta"]
            pre_peak = cells.get(("pre-MTU15-IDA", "peak"), np.nan)
            post_peak = cells.get(("DA15/ID15", "peak"), np.nan)
            pre_off = cells.get(("pre-MTU15-IDA", "off-peak"), np.nan)
            post_off = cells.get(("DA15/ID15", "off-peak"), np.nan)
            d_peak = post_peak - pre_peak
            d_off = post_off - pre_off
            placebo_ok = "YES" if abs(d_peak) > abs(d_off) else "no"
            print(f"{spec_name:<28} {firm:<3}  {pre_peak:>+9.4f}  {post_peak:>+10.4f}  {d_peak:>+9.4f}  {pre_off:>+8.4f}  {post_off:>+9.4f}  {d_off:>+8.4f}  {placebo_ok}")

    print()
    print("Reading: stable IB Δβ_peak ≈ +0.05 ⇒ F5 OVB-robust → promote wounded to alive.")
    print("         IB Δβ_peak collapses or flips ⇒ F5 stays wounded.")


if __name__ == "__main__":
    main()
