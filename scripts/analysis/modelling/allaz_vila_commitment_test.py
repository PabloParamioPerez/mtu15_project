# STATUS: ALIVE
# LAST-AUDIT: 2026-04-25
# FEEDS: F5 (wounded); modelling-track §2 Allaz-Vila Run 2026-04-25
# CLAIM: Per-regime slope of IDA repositioning on DA cleared MWh; Allaz-Vila commitment-value test
"""Allaz-Vila commitment-value test (Phase 2 modelling-track §2).

Theory. In Allaz-Vila (1993), forward sales soften IDA competition by
serving as commitment devices. The strategic value of the commitment
depends on how granularly firms can re-trade in IDA: when IDA is coarse
relative to ISP, forward commitment is more valuable; when IDA matches
ISP granularity, the commitment value weakens because firms can fine-tune
all the way to delivery.

Test. For each Big-4 firm i and each (date, hour) cell, compute:
    q_DA_{i,d,h}    — firm i's DA-cleared MWh at hour h on day d
    DeltaQ_IDA_{i,d,h} — firm i's signed net IDA repositioning at the
                        same (d,h) (sells minus buys, summed across all
                        IDA sessions covering that hour)

Regression by regime r:
    DeltaQ_IDA = alpha_i + beta_r * q_DA + epsilon
with firm FE (alpha_i) and SE clustered by date.

The Allaz-Vila prediction is that beta_r changes across reform regimes
that alter IDA-vs-ISP granularity. Specifically:
  * 3-sess (2024-06-14 to 2024-12-01): IDA coarsened to 3 sessions;
    forward commitment value rises -> beta should be more negative
    (DA position deters IDA re-trade)
  * MTU15-IDA (2025-03-19 onwards): IDA matches ISP at 15-min granularity;
    commitment value weakens -> beta moves toward zero
  * MTU15-DA (2025-10-01 onwards): DA also at 15-min, full symmetry restored;
    beta should be close to its pre-IDA baseline if granularity-asymmetry
    was the driver.

A flat (regime-invariant) slope would be evidence against the granularity-
mediated commitment-value channel. A monotone evolution pre -> 3-sess ->
DA60/ID15 -> DA15/ID15 with sign flip or attenuation at MTU15-IDA would
support it.

Output: prints a coefficient table per (firm, regime). Persists the
regression panel so re-runs are fast.
"""
from __future__ import annotations

import time
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
import statsmodels.api as sm

PROJECT = Path(__file__).resolve().parents[3]
PANEL_PATH = PROJECT / "data" / "derived" / "panels" / "allaz_vila_panel.parquet"
PDBCE = PROJECT / "data" / "processed" / "omie" / "mercado_diario" / "programas" / "pdbce_all.parquet"
PIBCIE = PROJECT / "data" / "processed" / "omie" / "mercado_intradiario_subastas" / "programas" / "pibcie_all.parquet"

BIG4 = ("GE", "IB", "GN", "HC")

# Five regimes per the project's reform timeline.
def regime_label(d: pd.Timestamp) -> str:
    d = pd.Timestamp(d)
    if d < pd.Timestamp("2024-06-14"):
        return "pre-IDA"
    if d < pd.Timestamp("2024-12-01"):
        return "3-sess"
    if d < pd.Timestamp("2025-03-19"):
        return "ISP15 win"
    if d < pd.Timestamp("2025-10-01"):
        return "DA60/ID15"
    return "DA15/ID15"


REGIMES = ("pre-IDA", "3-sess", "ISP15 win", "DA60/ID15", "DA15/ID15")


def build_panel() -> pd.DataFrame:
    """Build per-(firm, date, hour) panel of DA cleared and IDA delta."""
    con = duckdb.connect()
    con.execute("SET memory_limit='8GB'")

    print("[1/3] DA cleared MWh per (firm, date, hour) from pdbce_all...")
    t0 = time.time()
    con.execute(f"""
        CREATE TEMP VIEW da_clr AS
        SELECT date,
               CASE WHEN mtu_minutes = 15 THEN CEIL(period / 4.0)::INTEGER
                    ELSE period END AS hour,
               grupo_empresarial AS firm,
               SUM(assigned_power_mw)
                 / CASE WHEN mtu_minutes = 15 THEN 4.0 ELSE 1.0 END AS q_da
        FROM '{PDBCE}'
        WHERE offer_type = 1
          AND assigned_power_mw IS NOT NULL
          AND grupo_empresarial IN ('GE','IB','GN','HC')
        GROUP BY date, hour, firm, mtu_minutes
    """)
    print(f"   done in {time.time()-t0:.1f}s")

    print("[2/3] IDA signed delta per (firm, date, hour) from pibcie_all (sells - buys, all sessions)...")
    t1 = time.time()
    # offer_type 1 = sell, offer_type 2 = buy. Sign convention: net delivery
    # change = sell - buy. In IDA, a buy is a buyback of a DA sell position.
    con.execute(f"""
        CREATE TEMP VIEW ida_signed AS
        SELECT date,
               CASE WHEN mtu_minutes = 15 THEN CEIL(period / 4.0)::INTEGER
                    ELSE period END AS hour,
               grupo_empresarial AS firm,
               SUM(CASE WHEN offer_type = 1 THEN assigned_power_mw
                        WHEN offer_type = 2 THEN -assigned_power_mw
                        ELSE 0 END)
                 / CASE WHEN mtu_minutes = 15 THEN 4.0 ELSE 1.0 END AS dq_ida
        FROM '{PIBCIE}'
        WHERE assigned_power_mw IS NOT NULL
          AND grupo_empresarial IN ('GE','IB','GN','HC')
        GROUP BY date, hour, firm, mtu_minutes
    """)
    print(f"   done in {time.time()-t1:.1f}s")

    print("[3/3] Join + persist...")
    t2 = time.time()
    df = con.sql("""
        SELECT da.date::DATE AS date,
               da.hour,
               da.firm,
               da.q_da,
               COALESCE(ida.dq_ida, 0) AS dq_ida
        FROM da_clr da
        LEFT JOIN ida_signed ida USING (date, hour, firm)
        WHERE da.q_da IS NOT NULL
    """).df()
    print(f"   panel: {len(df):,} rows in {time.time()-t2:.1f}s")

    df["date"] = pd.to_datetime(df["date"])
    df["regime"] = df["date"].apply(regime_label)
    PANEL_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(PANEL_PATH, index=False)
    print(f"   wrote {PANEL_PATH}")
    return df


def regression_table(df: pd.DataFrame) -> pd.DataFrame:
    """Per (firm, regime), OLS of dq_ida on q_da with intercept; cluster SE by date."""
    rows = []
    for firm in BIG4:
        for r in REGIMES:
            sub = df[(df["firm"] == firm) & (df["regime"] == r)].copy()
            if len(sub) < 200:
                rows.append({
                    "firm": firm, "regime": r,
                    "n": len(sub),
                    "beta": np.nan, "se": np.nan, "p": np.nan, "r2": np.nan,
                    "mean_q_da": sub["q_da"].mean() if len(sub) else np.nan,
                    "mean_dq_ida": sub["dq_ida"].mean() if len(sub) else np.nan,
                })
                continue
            X = sm.add_constant(sub[["q_da"]].astype(float))
            y = sub["dq_ida"].astype(float)
            # Cluster-robust SE by date
            res = sm.OLS(y, X).fit(
                cov_type="cluster",
                cov_kwds={"groups": sub["date"].values},
            )
            rows.append({
                "firm": firm, "regime": r,
                "n": len(sub),
                "beta": float(res.params["q_da"]),
                "se": float(res.bse["q_da"]),
                "p": float(res.pvalues["q_da"]),
                "r2": float(res.rsquared),
                "mean_q_da": float(sub["q_da"].mean()),
                "mean_dq_ida": float(sub["dq_ida"].mean()),
            })
    return pd.DataFrame(rows)


def pretty_print(tab: pd.DataFrame) -> None:
    print()
    print("=" * 88)
    print("Allaz-Vila commitment-value test")
    print("=" * 88)
    print("Spec: dq_ida = const + beta * q_da, by (firm, regime). Cluster SE by date.")
    print("Panel grain: (firm, date, hour). Big-4 only (GE, IB, GN, HC).")
    print()
    for firm in BIG4:
        sub = tab[tab["firm"] == firm]
        print(f"--- {firm} ---")
        print(f"  {'regime':<13} {'n':>8} {'beta':>10} {'se':>9} {'p':>8} {'R2':>6} {'<q_da>':>10} {'<dq_ida>':>10}")
        for _, r in sub.iterrows():
            if pd.isna(r["beta"]):
                print(f"  {r['regime']:<13} {r['n']:>8,} {'(too few)':>10}")
                continue
            sig = "***" if r["p"] < 0.001 else ("**" if r["p"] < 0.01 else (" *" if r["p"] < 0.05 else "  "))
            print(
                f"  {r['regime']:<13} {r['n']:>8,} {r['beta']:>+10.4f}{sig}"
                f" {r['se']:>9.4f} {r['p']:>8.3f} {r['r2']:>6.3f}"
                f" {r['mean_q_da']:>10.1f} {r['mean_dq_ida']:>+10.1f}"
            )
        print()
    # Cross-firm contrast: (3-sess - pre-IDA) and (DA60/ID15 - 3-sess) and (DA15/ID15 - DA60/ID15)
    print("Regime-to-regime slope changes (delta beta):")
    print(f"  {'firm':<6} {'pre->3-sess':>14} {'3sess->ISP15':>14} {'ISP15->DA60/15':>16} {'DA60->DA15/15':>14}")
    for firm in BIG4:
        sub = tab[tab["firm"] == firm].set_index("regime")
        if not all(r in sub.index for r in REGIMES):
            print(f"  {firm:<6} (insufficient regime coverage)")
            continue
        b = {r: sub.loc[r, "beta"] for r in REGIMES}
        if any(pd.isna(v) for v in b.values()):
            print(f"  {firm:<6} (NaN beta in some regime)")
            continue
        d1 = b["3-sess"] - b["pre-IDA"]
        d2 = b["ISP15 win"] - b["3-sess"]
        d3 = b["DA60/ID15"] - b["ISP15 win"]
        d4 = b["DA15/ID15"] - b["DA60/ID15"]
        print(f"  {firm:<6} {d1:>+14.4f} {d2:>+14.4f} {d3:>+16.4f} {d4:>+14.4f}")


def main() -> None:
    if PANEL_PATH.exists():
        print(f"Loading existing panel: {PANEL_PATH}")
        df = pd.read_parquet(PANEL_PATH)
        df["date"] = pd.to_datetime(df["date"])
    else:
        df = build_panel()

    # Quick sanity print
    print()
    print(f"Panel rows: {len(df):,}")
    print(f"Regime coverage:")
    print(df.groupby("regime", observed=True).size().reindex(REGIMES))

    tab = regression_table(df)
    pretty_print(tab)

    out = PROJECT / "data" / "derived" / "results" / "allaz_vila_results.csv"
    tab.to_csv(out, index=False)
    print(f"\nwrote {out}")


if __name__ == "__main__":
    main()
