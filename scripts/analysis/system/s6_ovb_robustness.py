# STATUS: ALIVE
# LAST-AUDIT: 2026-04-27
# FEEDS: S6 OVB-robustness — does the asymmetric-window cumulative excess survive monthly controls?
# CLAIM: S6's +€1,094.9M asymmetric-window BRP→TSO transfer is robust to omitted-variable controls.

"""S6 OVB robustness check.

S6 currently estimates monthly A87 NET excess vs same-calendar pre-IDA
baseline using OLS:

    y_m = alpha + sum_r beta_r * 1{m in regime r} + gamma_cm * 1{cal_month=cm} + eps_m

with regime ∈ {pre-IDA, 3-sess, ISP15 win, DA60/ID15, DA15/ID15} and pre-IDA
as reference. The asymmetric-window cumulative = beta(ISP15) × 4 +
beta(DA60/ID15) × 6 ≈ +€1,094.9M.

OVB risk: pre-IDA window includes 2022-2023 gas-price crisis, RES capacity
growth was ~80% pre-IDA alone, post-IDA windows cover post-blackout
operación reforzada. Each could be a confounder driving the regime
coefficients.

Controls to test:
  M1: monthly mean Spanish wind+solar (B01+B16+B18+B19) — captures RES growth
  M2: monthly mean DA price level — captures fuel cost cycles + demand effects
  M3: monthly RZ activation volume — captures redispatch escalation
  M4: monthly mean load proxy (sum of all A75 generation) — captures demand growth

Compare regime coefficients across:
  Spec 1 (current S6 spec):           regime + cal-month FE
  Spec 2 (+ RES growth):              + monthly_vre_twh
  Spec 3 (+ price level):             + monthly_p_da
  Spec 4 (+ redispatch):              + monthly_rz_gwh
  Spec 5 (full):                       + monthly_load_proxy

If beta(ISP15 win) and beta(DA60/ID15) hold their sign and >50% of magnitude
across all specs, S6 is OVB-robust. If they collapse or flip, S6 needs to
be wounded.
"""
from __future__ import annotations

from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
import statsmodels.api as sm

PROJECT = Path(__file__).resolve().parents[3]
A87 = PROJECT / "data" / "processed" / "entsoe" / "balancing" / "financial_balance_all.parquet"
PRICE = PROJECT / "data" / "processed" / "omie" / "mercado_diario" / "precios" / "marginalpdbc_all.parquet"
VRE = PROJECT / "data" / "processed" / "entsoe" / "generation" / "wind_solar_actual_all.parquet"
RP48 = PROJECT / "data" / "processed" / "esios" / "restricciones" / "totalrp48preccierre_all.parquet"

REGIME_ORDER = ["pre-IDA", "3-sess", "ISP15 win", "DA60/ID15", "DA15/ID15"]


def assign_regime(d) -> str:
    if d < pd.Timestamp("2024-06-14"):
        return "pre-IDA"
    if d < pd.Timestamp("2024-12-01"):
        return "3-sess"
    if d < pd.Timestamp("2025-03-19"):
        return "ISP15 win"
    if d < pd.Timestamp("2025-10-01"):
        return "DA60/ID15"
    return "DA15/ID15"


def main() -> None:
    print("[1/3] Build monthly panel (A87 NET, RES, price, RZ, load proxy)...")
    con = duckdb.connect()
    con.execute("SET memory_limit='2GB'")

    # A87 NET monthly (direction_code A02 = BRP-paid; A01 = TSO-paid)
    a87 = pd.read_parquet(A87)
    a87["month"] = pd.to_datetime(a87["month"])
    a02 = a87[a87["direction_code"] == "A02"].groupby("month", as_index=False)["amount_eur"].sum().rename(columns={"amount_eur": "a02"})
    a01 = a87[a87["direction_code"] == "A01"].groupby("month", as_index=False)["amount_eur"].sum().rename(columns={"amount_eur": "a01"})
    a_panel = a02.merge(a01, on="month", how="outer").fillna(0)
    a_panel["a87_net"] = a_panel["a02"] - a_panel["a01"]

    # Monthly Spain DA price
    px = con.sql(f"""
        SELECT DATE_TRUNC('month', CAST(date AS DATE)) AS month,
               AVG(price_es_eur_mwh) AS p_da
        FROM '{PRICE}'
        WHERE price_es_eur_mwh IS NOT NULL
        GROUP BY 1
    """).df()
    px["month"] = pd.to_datetime(px["month"])

    # Monthly Spanish wind+solar TWh
    vre = pd.read_parquet(VRE, columns=["isp_start_utc", "psr_type", "quantity_mw", "mtu_minutes"])
    vre = vre[vre["psr_type"].isin(["B01", "B16", "B18", "B19"])].copy()
    vre["isp_start"] = pd.to_datetime(vre["isp_start_utc"]).dt.tz_localize(None)
    vre["month"] = vre["isp_start"].dt.to_period("M").dt.to_timestamp()
    vre["mwh"] = vre["quantity_mw"] * (vre["mtu_minutes"] / 60.0)
    vre_m = vre.groupby("month", as_index=False)["mwh"].sum().rename(columns={"mwh": "vre_twh"})
    vre_m["vre_twh"] = vre_m["vre_twh"] / 1e6

    # Monthly load proxy: total Spanish generation sum across ALL PSR types from A75
    print("   computing total-generation load proxy (sum of all PSR types)...")
    load = pd.read_parquet(VRE, columns=["isp_start_utc", "quantity_mw", "mtu_minutes"])
    load["isp_start"] = pd.to_datetime(load["isp_start_utc"]).dt.tz_localize(None)
    load["month"] = load["isp_start"].dt.to_period("M").dt.to_timestamp()
    load["mwh"] = load["quantity_mw"] * (load["mtu_minutes"] / 60.0)
    load_m = load.groupby("month", as_index=False)["mwh"].sum().rename(columns={"mwh": "total_gen_twh"})
    load_m["total_gen_twh"] = load_m["total_gen_twh"] / 1e6

    # Monthly RZ activation volume from totalrp48preccierre (TipoRedespacho 61 = system-security RZ)
    rp48 = con.sql(f"""
        SELECT DATE_TRUNC('month', CAST(date AS DATE)) AS month,
               SUM(COALESCE(qty_up_mwh, 0) + COALESCE(qty_down_mwh, 0)) / 1000.0 AS rz_gwh
        FROM '{RP48}'
        WHERE tipo_redespacho = 61
        GROUP BY 1
    """).df()
    rp48["month"] = pd.to_datetime(rp48["month"])
    rz_m = rp48

    panel = a_panel[["month", "a87_net"]].merge(px, on="month", how="left") \
                                          .merge(vre_m, on="month", how="left") \
                                          .merge(load_m, on="month", how="left") \
                                          .merge(rz_m, on="month", how="left")
    panel["regime"] = panel["month"].apply(assign_regime)
    panel["cal_month"] = panel["month"].dt.month
    panel["a87_net_M"] = panel["a87_net"] / 1e6
    panel = panel[panel["month"] <= pd.Timestamp("2025-12-31")].copy()
    print(f"   monthly panel: {len(panel)} months, {panel.month.min().date()} → {panel.month.max().date()}")
    print(f"   regime counts: {panel.groupby('regime').size().to_dict()}")
    print(f"   means: a87_net €{panel.a87_net_M.mean():.1f}M; p_da €{panel.p_da.mean():.1f}; vre {panel.vre_twh.mean():.2f} TWh; rz {panel.rz_gwh.mean():.0f} GWh; total_gen {panel.total_gen_twh.mean():.2f} TWh")

    # Pairwise correlations
    print()
    print("Pairwise correlations among regime indicators and controls (post-IDA only):")
    post = panel[panel["regime"] != "pre-IDA"].copy()
    post["asym_window"] = post["regime"].isin(["ISP15 win", "DA60/ID15"]).astype(int)
    print(post[["asym_window", "vre_twh", "p_da", "rz_gwh", "total_gen_twh"]].corr().iloc[0].round(3).to_string())

    print()
    print("[2/3] Run progressively richer specifications (HC3 SE):")

    # Build dummies
    rd = pd.get_dummies(pd.Categorical(panel["regime"], categories=REGIME_ORDER, ordered=False), prefix="rg", dtype=float)
    rd = rd.drop(columns="rg_pre-IDA")
    cm = pd.get_dummies(panel["cal_month"], prefix="cm", drop_first=True, dtype=float)

    y = panel["a87_net_M"].astype(float)

    # Drop NaN
    panel_full = pd.concat([panel[["month", "regime", "cal_month", "a87_net_M", "vre_twh", "p_da", "rz_gwh", "total_gen_twh"]], rd, cm], axis=1)
    panel_full = panel_full.dropna(subset=["vre_twh", "p_da", "rz_gwh", "total_gen_twh"])
    print(f"   after dropping NaN controls: n={len(panel_full)} months")

    rd_cols = [c for c in panel_full.columns if c.startswith("rg_")]
    cm_cols = [c for c in panel_full.columns if c.startswith("cm_")]

    specs = [
        ("Spec 1 (current S6: regime + cal-month FE)",            rd_cols + cm_cols),
        ("Spec 2 (+ vre_twh)",                                     rd_cols + cm_cols + ["vre_twh"]),
        ("Spec 3 (+ p_da)",                                        rd_cols + cm_cols + ["vre_twh", "p_da"]),
        ("Spec 4 (+ rz_gwh)",                                      rd_cols + cm_cols + ["vre_twh", "p_da", "rz_gwh"]),
        ("Spec 5 (+ total_gen_twh)",                               rd_cols + cm_cols + ["vre_twh", "p_da", "rz_gwh", "total_gen_twh"]),
    ]

    # Compute cumulative asymmetric window for each spec
    isp15_n = (panel["regime"] == "ISP15 win").sum()
    da60_n = (panel["regime"] == "DA60/ID15").sum()
    print(f"   ISP15 win months: {isp15_n}, DA60/ID15 months: {da60_n}")
    print()
    print(f"{'Spec':<55} {'β(3sess)':>10} {'β(ISP15)':>10} {'β(DA60)':>10} {'β(DA15)':>10} {'β(VRE)':>10} {'β(p_da)':>10} {'β(RZ)':>10} {'β(load)':>10} {'cum asym M€':>14} {'R²':>6}")
    print("-" * 175)

    y2 = panel_full["a87_net_M"].astype(float)
    for name, regs in specs:
        X = panel_full[regs].astype(float)
        X = sm.add_constant(X)
        res = sm.OLS(y2, X).fit(cov_type="HC3")
        b_3sess = res.params.get("rg_3-sess", float("nan"))
        b_isp15 = res.params.get("rg_ISP15 win", float("nan"))
        b_da60 = res.params.get("rg_DA60/ID15", float("nan"))
        b_da15 = res.params.get("rg_DA15/ID15", float("nan"))
        b_vre = res.params.get("vre_twh", float("nan"))
        b_pda = res.params.get("p_da", float("nan"))
        b_rz = res.params.get("rz_gwh", float("nan"))
        b_load = res.params.get("total_gen_twh", float("nan"))
        cum_asym = b_isp15 * isp15_n + b_da60 * da60_n if not (np.isnan(b_isp15) or np.isnan(b_da60)) else float("nan")
        r2 = res.rsquared
        print(f"{name:<55} {b_3sess:>+10.1f} {b_isp15:>+10.1f} {b_da60:>+10.1f} {b_da15:>+10.1f} "
              f"{b_vre:>+10.2f} {b_pda:>+10.3f} {b_rz:>+10.4f} {b_load:>+10.2f} "
              f"{cum_asym:>+14.1f} {r2:>6.3f}")

    print()
    print("[3/3] Reading:")
    print("  Stable β(ISP15 win) and β(DA60/ID15) ⇒ S6 OVB-robust")
    print("  Sign-flip or magnitude collapse ⇒ S6 wound or kill")


if __name__ == "__main__":
    main()
