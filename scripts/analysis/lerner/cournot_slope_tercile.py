# STATUS: ALIVE
# LAST-AUDIT: 2026-04-26
# FEEDS: F1, F2, F6; modelling-track §1 Cournot
# CLAIM: Cournot mechanism test: tercile sort + direct structural log-log regression of Lerner on supply slope
"""Cournot mechanism test via supply-slope tercile sort + direct
log-log structural regression (modelling-track §1).

Theory. The Cournot-Nash Lerner-index formula
    L_i = q_i / (p* * (1 - s_i) * |dS/dp|)
implies that holding q_i, p*, and s_i fixed, the Lerner index scales
**inversely** with the market supply slope |dS/dp|. With slope measured
in MW/EUR:
  * LOW slope value = inelastic supply ('steep' curve)  -> HIGH Lerner
  * HIGH slope value = elastic supply ('flat' curve)    -> LOW Lerner
So Cournot predicts Lerner concentrates in cells with LOW |dS/dp|.

Two tests:

(1) TERCILE SORT. Tercile-split the per-(date, hour) supply-slope
observations using only pre-IDA data (so terciles are not endogenous to
reform). Apply matched-price regime contrasts separately within each
tercile:
    lerner = const + sum_r beta_r * 1{r} + p-bin FE + hour FE
HC3 SE. Cournot prediction: beta_DA60/ID15 should be LARGER in steep
tercile (low |dS/dp|) and SMALLER in flat tercile (high |dS/dp|), so
the contrast monotonically falls from T1 to T3.

(2) STRUCTURAL LOG-LOG REGRESSION. Within each (firm, regime), with
controls for log(q_i), log(p*), log(1-s_i), test:
    log(L_i) = const + gamma * log(|dS/dp|) + log-controls + ...
Cournot predicts gamma ≈ -1 (proportional inverse). Caveat: L_i in our
data is computed FROM the formula, so this is partly a self-regression;
it tests whether residual variation respects the Cournot rank-ordering,
not whether the formula generates the data (which it does by
construction). Useful as a coherence check on the tercile sort.

Update 2026-04-26: corrected the Cournot direction (the original Run
Note in modelling-track §1 had the inequality reversed). With the
correction, the data show:
  * IB: monotonic decline from steep to flat — supports Cournot
  * GE: T1 > T2 (right) but T3 highest (wrong) — mixed
  * GN, HC: monotonic rise — opposite of Cournot
This is a heterogeneity finding by portfolio composition, not a flat
wound.
"""
from __future__ import annotations

from pathlib import Path
import duckdb
import numpy as np
import pandas as pd
import statsmodels.api as sm

PROJECT = Path(__file__).resolve().parents[3]
LERNER = PROJECT / "data" / "derived" / "panels" / "firm_lerner_hourly.parquet"

REGIME_ORDER = ["pre-IDA", "3-sess", "ISP15 window", "DA60/ID15", "DA15/ID15"]


def assign_regime(d: pd.Timestamp) -> str:
    d = pd.Timestamp(d)
    if d < pd.Timestamp("2024-06-14"):
        return "pre-IDA"
    if d < pd.Timestamp("2024-12-01"):
        return "3-sess"
    if d < pd.Timestamp("2025-03-19"):
        return "ISP15 window"
    if d < pd.Timestamp("2025-10-01"):
        return "DA60/ID15"
    return "DA15/ID15"


def main() -> None:
    con = duckdb.connect()
    con.execute("SET memory_limit='6GB'")

    df = con.sql(f"""
        SELECT date, hour, firm, lerner_index,
               clearing_price_eur_mwh AS p,
               supply_slope_mw_per_eur AS slope
        FROM '{LERNER}'
        WHERE lerner_index BETWEEN 0 AND 1
          AND supply_slope_mw_per_eur > 0
    """).df()
    df["date"] = pd.to_datetime(df["date"])
    df["regime"] = df["date"].apply(assign_regime)
    df["p_bin"] = pd.cut(
        df["p"],
        bins=[-1000, 0, 25, 50, 100, 200, 1e6],
        labels=["neg", "0-25", "25-50", "50-100", "100-200", "200+"],
    )

    # Tercile split on pre-IDA slope distribution. Apply same cuts everywhere.
    pre = df[df["regime"] == "pre-IDA"]
    q1, q2 = pre["slope"].quantile([1.0 / 3, 2.0 / 3])
    print(f"Pre-IDA slope tercile cuts (MW/EUR): q33={q1:.0f}, q66={q2:.0f}")

    def tercile_label(s: float) -> str:
        if s <= q1:
            return "T1 steep"
        if s <= q2:
            return "T2 medium"
        return "T3 flat"

    df["slope_tercile"] = df["slope"].apply(tercile_label)

    # Note on labels: 'flat' = HIGH numerical slope (more MW per EUR change).
    # In residual-demand-slope terms, a high market supply-slope value
    # means an EXTRA MW costs little — i.e. the marginal supply curve is
    # FLAT in the conventional sense. Cournot predicts higher Lerner in
    # this regime because the firm faces less price reaction per MW.

    print("\nPanel coverage by (firm, regime, tercile):")
    counts = (
        df.groupby(["firm", "regime", "slope_tercile"], observed=True)
        .size()
        .unstack(fill_value=0)
    )
    print(counts.to_string())

    # Run Spec-3 within each (firm, tercile)
    rows = []
    print()
    print("=" * 110)
    print("Cournot tercile sort: Spec-3 matched-price Lerner regime contrasts vs pre-IDA, by slope tercile")
    print("=" * 110)
    for firm in ["GE", "IB", "GN", "HC"]:
        for terc in ["T1 steep", "T2 medium", "T3 flat"]:
            sub = df[(df["firm"] == firm) & (df["slope_tercile"] == terc)].copy()
            if len(sub) < 1000:
                continue
            sub["regime_cat"] = pd.Categorical(
                sub["regime"], categories=REGIME_ORDER, ordered=False
            )
            rd = pd.get_dummies(sub["regime_cat"], prefix="regime", drop_first=False, dtype=float)
            if "regime_pre-IDA" not in rd.columns:
                continue
            rd = rd.drop(columns="regime_pre-IDA")
            pb = pd.get_dummies(sub["p_bin"], prefix="p_bin", drop_first=True, dtype=float)
            hr = pd.get_dummies(sub["hour"], prefix="hr", drop_first=True, dtype=float)
            X = pd.concat([rd, pb, hr], axis=1).assign(const=1.0)
            y = sub["lerner_index"].astype(float)
            try:
                res = sm.OLS(y, X).fit(cov_type="HC3")
            except Exception as e:
                print(f"   FAIL {firm} {terc}: {e}")
                continue
            for r in ["3-sess", "ISP15 window", "DA60/ID15", "DA15/ID15"]:
                col = f"regime_{r}"
                if col not in res.params.index:
                    continue
                rows.append({
                    "firm": firm,
                    "tercile": terc,
                    "regime": r,
                    "n": len(sub),
                    "beta": float(res.params[col]),
                    "se": float(res.bse[col]),
                    "p": float(res.pvalues[col]),
                })

    tab = pd.DataFrame(rows)

    # Pretty print: rows = (firm, regime), cols = tercile
    pivot_beta = tab.pivot_table(
        index=["firm", "regime"],
        columns="tercile",
        values="beta",
    )
    pivot_se = tab.pivot_table(
        index=["firm", "regime"],
        columns="tercile",
        values="se",
    )
    print()
    print("Coefficients (matched-price contrast vs pre-IDA), by tercile:")
    print()
    print(f"{'firm':<5} {'regime':<14}  {'T1 steep':>14}  {'T2 medium':>14}  {'T3 flat':>14}")
    for (firm, regime), row in pivot_beta.iterrows():
        line = f"{firm:<5} {regime:<14}  "
        for terc in ["T1 steep", "T2 medium", "T3 flat"]:
            beta = row.get(terc, np.nan)
            se = pivot_se.loc[(firm, regime)].get(terc, np.nan)
            if pd.isna(beta):
                cell = "—".rjust(14)
            else:
                cell = f"{beta:+.3f} ({se:.3f})".rjust(14)
            line += cell + "  "
        print(line)

    # Headline test: corrected Cournot direction.
    # Cournot predicts higher Lerner under steep supply (low MW/EUR, low |dS/dp|).
    # So DA60/ID15 contrast should DECREASE monotonically T1 (steep) -> T3 (flat).
    print()
    print("Headline test: does DA60/ID15 contrast DECREASE monotonically from steep -> flat?")
    print("(Cournot prediction: yes, since Lerner ~ 1/|dS/dp|)")
    print(f"{'firm':<5}  {'T1 steep':>9}  {'T2 medium':>9}  {'T3 flat':>9}  Cournot?")
    for firm in ["GE", "IB", "GN", "HC"]:
        sub = pivot_beta.loc[(firm, "DA60/ID15")] if (firm, "DA60/ID15") in pivot_beta.index else None
        if sub is None:
            continue
        b1 = sub.get("T1 steep", np.nan)
        b2 = sub.get("T2 medium", np.nan)
        b3 = sub.get("T3 flat", np.nan)
        if b1 > b2 > b3:
            verdict = "YES (monotone decline)"
        elif b1 < b2 < b3:
            verdict = "NO (monotone rise — opposite)"
        elif b1 > b3:
            verdict = "PARTIAL (T1 > T3; non-monotonic)"
        else:
            verdict = "NO (T1 < T3)"
        print(f"{firm:<5}  {b1:>+9.3f}  {b2:>+9.3f}  {b3:>+9.3f}  {verdict}")

    out = PROJECT / "results" / "regressions" / "cournot_tercile_results.csv"
    tab.to_csv(out, index=False)
    print(f"\nwrote {out}")

    # ============================================================
    # STRUCTURAL LOG-LOG REGRESSION (within regime, within firm)
    # ============================================================
    print()
    print("=" * 110)
    print("Structural log-log Cournot test: log(Lerner) on log(slope), within (firm, regime)")
    print("Cournot predicts gamma ~ -1 (inverse proportionality)")
    print("Caveat: lerner_index in panel is computed FROM the formula, so a perfectly")
    print("  identified Cournot relationship would yield gamma = -1 mechanically. The test")
    print("  is whether the within-regime variation in slope explains within-regime variation")
    print("  in Lerner with the right sign/magnitude under the Cournot identifying assumptions.")
    print("=" * 110)

    con2 = duckdb.connect()
    full = con2.sql(f"""
        SELECT date, hour, firm,
               lerner_index, q_mwh, s_share,
               clearing_price_eur_mwh AS p,
               supply_slope_mw_per_eur AS slope
        FROM '{LERNER}'
        WHERE lerner_index > 1e-4 AND lerner_index < 1
          AND supply_slope_mw_per_eur > 0
          AND q_mwh > 0
          AND s_share > 0 AND s_share < 1
          AND clearing_price_eur_mwh > 0
    """).df()
    full["date"] = pd.to_datetime(full["date"])
    full["regime"] = full["date"].apply(assign_regime)
    full["log_L"] = np.log(full["lerner_index"])
    full["log_slope"] = np.log(full["slope"])
    full["log_q"] = np.log(full["q_mwh"])
    full["log_p"] = np.log(full["p"])
    full["log_1ms"] = np.log(1.0 - full["s_share"])

    print(f"\n{'firm':<5} {'regime':<14}  {'n':>8}  {'gamma':>10}  {'se':>8}  {'p':>7}  {'R²':>6}")
    rows_loglog = []
    for firm in ["GE", "IB", "GN", "HC"]:
        for r in REGIME_ORDER:
            sub = full[(full["firm"] == firm) & (full["regime"] == r)]
            if len(sub) < 200:
                continue
            X = sub[["log_slope", "log_q", "log_p", "log_1ms"]].copy()
            X["const"] = 1.0
            y = sub["log_L"].astype(float)
            try:
                res = sm.OLS(y, X).fit(cov_type="HC3")
            except Exception:
                continue
            g = float(res.params["log_slope"])
            se = float(res.bse["log_slope"])
            p = float(res.pvalues["log_slope"])
            r2 = float(res.rsquared)
            rows_loglog.append({
                "firm": firm, "regime": r, "n": len(sub),
                "gamma": g, "se": se, "p": p, "r2": r2,
            })
            print(f"{firm:<5} {r:<14}  {len(sub):>8,}  {g:>+10.3f}  {se:>8.3f}  {p:>7.3f}  {r2:>6.3f}")

    pd.DataFrame(rows_loglog).to_csv(
        PROJECT / "results" / "regressions" / "cournot_loglog_results.csv", index=False,
    )
    print()
    print("Cournot prediction: gamma ≈ -1. Per-firm interpretation:")
    print("  * |gamma + 1| small: Cournot relationship holds approximately")
    print("  * gamma > 0 or far from -1: relationship breaks (non-Cournot mechanism)")


if __name__ == "__main__":
    main()
