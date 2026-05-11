# STATUS: ALIVE
# LAST-AUDIT: 2026-05-08
# FEEDS: thesis paper.tex §5.2 (DA-IDA price wedge as supporting outcome)
# CLAIM: Within-day DiD on (DA price − IDA price), critical h{18-22} vs
#        flat h{3-5}, same-cal-month Oct-Dec 2024 vs Oct-Dec 2025.
#
# The wedge spiked in critical hours during reforzada (Apr-Sep 2025) but
# our same-cal-month window is OUTSIDE that period (autumn-winter only).
# So we test whether MTU15-DA structurally changed the within-day wedge.
# Expectation: smaller β₃ than B1; signs informative.
#
# Companion: B2.1 — full window 2024-01 to 2025-12 with reforzada dummy.

from __future__ import annotations

from pathlib import Path
import sys

import duckdb
import numpy as np
import pandas as pd
from statsmodels.api import OLS, add_constant
from statsmodels.stats.sandwich_covariance import cov_cluster
from scipy.stats import norm

REPO = Path(__file__).resolve().parents[3]

OUTDIR = REPO / "results" / "regressions" / "firm" / "critical_hours_thesis"
OUTDIR.mkdir(parents=True, exist_ok=True)

MARGPDBC = REPO / "data" / "processed" / "omie" / "mercado_diario" / "precios" / "marginalpdbc_all.parquet"
MARGPIBC = REPO / "data" / "processed" / "omie" / "mercado_intradiario_subastas" / "precios" / "marginalpibc_all.parquet"

CRITICAL_HOURS = (18, 19, 20, 21, 22)
FLAT_HOURS = (3, 4, 5)

# Same-cal-month window
PRE_START, PRE_END = "2024-10-01", "2025-01-01"
POST_START, POST_END = "2025-10-01", "2026-01-01"

# Full window for B2.1 with reforzada dummy
FULL_START, FULL_END = "2024-01-01", "2026-01-01"
REFORZADA_START = "2025-04-28"
MTU15_IDA_DATE = "2025-03-19"
MTU15_DA_DATE = "2025-10-01"


def hour_class(h: int) -> str:
    if h in CRITICAL_HOURS: return "critical_h18_22"
    if h in FLAT_HOURS:     return "flat_h3_5"
    return "other"


def build_wedge_panel(start: str, end: str) -> pd.DataFrame:
    con = duckdb.connect()
    con.execute("PRAGMA threads = 4")
    print(f"Building wedge panel {start} to {end}...")
    df = con.execute(
        f"""
        WITH da AS (
            SELECT date::DATE AS d, period, mtu_minutes, price_es_eur_mwh AS da_p
            FROM '{MARGPDBC}'
            WHERE date::DATE >= DATE '{start}' AND date::DATE < DATE '{end}'
        ),
        da_h AS (
            SELECT d,
                   CASE WHEN mtu_minutes = 60 THEN period - 1
                        ELSE (period - 1) // 4 END AS hour,
                   AVG(da_p) AS da_p_h
            FROM da WHERE period IS NOT NULL
            GROUP BY 1,2 HAVING hour BETWEEN 0 AND 23
        ),
        ida AS (
            SELECT date::DATE AS d, period, mtu_minutes, session_number,
                   price_es_eur_mwh AS ida_p
            FROM '{MARGPIBC}'
            WHERE date::DATE >= DATE '{start}' AND date::DATE < DATE '{end}'
        ),
        ida_h AS (
            SELECT d,
                   CASE WHEN mtu_minutes = 60 THEN period - 1
                        ELSE (period - 1) // 4 END AS hour,
                   AVG(ida_p) AS ida_p_h
            FROM ida WHERE period IS NOT NULL
            GROUP BY 1,2 HAVING hour BETWEEN 0 AND 23
        )
        SELECT da_h.d, da_h.hour, da_h.da_p_h, ida_h.ida_p_h,
               (da_h.da_p_h - ida_h.ida_p_h) AS wedge
        FROM da_h JOIN ida_h USING (d, hour)
        """
    ).df()
    df["d"] = pd.to_datetime(df["d"])
    df["hour_class"] = df["hour"].astype(int).apply(hour_class)
    df["crit"] = (df["hour_class"] == "critical_h18_22").astype(int)
    df["dow"] = df["d"].dt.dayofweek
    return df


def run_did(panel: pd.DataFrame, label: str, extra_controls: list = None) -> dict:
    df = panel[panel["hour_class"].isin(["critical_h18_22","flat_h3_5"])].copy()
    if len(df) < 30 or df["d"].nunique() < 5:
        return {"label": label, "n": len(df), "beta_3": np.nan}
    df["crit_x_post"] = df["crit"] * df["post"]
    cols_main = ["crit", "post", "crit_x_post"]
    if extra_controls:
        for c in extra_controls:
            cols_main.append(c)
    X_main = df[cols_main].copy().astype(float)
    dow_dummies = pd.get_dummies(df["dow"], prefix="dow", drop_first=True).astype(float)
    X = pd.concat([X_main, dow_dummies], axis=1)
    X = add_constant(X, has_constant='add')
    y = df["wedge"].astype(float).values
    cluster = df["d"].astype(str).values
    try:
        model = OLS(y, X)
        result = model.fit()
        cov = cov_cluster(result, cluster)
        se_cluster = np.sqrt(np.diag(cov))
        cols = list(X.columns)
        idx = cols.index("crit_x_post")
        beta_3 = result.params.iloc[idx]
        se = se_cluster[idx]
        p = 2 * (1 - norm.cdf(abs(beta_3 / se))) if se > 0 else np.nan
        return {
            "label": label,
            "n": len(df),
            "n_clusters": df["d"].nunique(),
            "beta_3": beta_3,
            "se": se,
            "p": p,
            "beta_1_crit": result.params.iloc[cols.index("crit")],
            "beta_2_post": result.params.iloc[cols.index("post")],
            "y_mean_pre_flat": float(df.loc[(df["post"]==0)&(df["crit"]==0), "wedge"].mean()),
            "y_mean_pre_crit": float(df.loc[(df["post"]==0)&(df["crit"]==1), "wedge"].mean()),
            "y_mean_post_flat": float(df.loc[(df["post"]==1)&(df["crit"]==0), "wedge"].mean()),
            "y_mean_post_crit": float(df.loc[(df["post"]==1)&(df["crit"]==1), "wedge"].mean()),
        }
    except Exception as e:
        return {"label": label, "n": len(df), "error": str(e)}


def print_result(r: dict) -> None:
    if "error" in r:
        print(f"  {r['label']:32s}  ERROR: {r['error']}"); return
    if pd.isna(r.get("beta_3", np.nan)):
        print(f"  {r['label']:32s}  n={r['n']:5d}  (insufficient)"); return
    sig = "***" if r["p"] < 0.001 else ("**" if r["p"] < 0.01 else ("*" if r["p"] < 0.05 else ""))
    print(f"  {r['label']:32s}  n={r['n']:5d}  G={r['n_clusters']:3d}  "
          f"β₃={r['beta_3']:+7.3f}  SE={r['se']:5.3f}  p={r['p']:.4f}{sig}  "
          f"means(pre-flat,pre-crit,post-flat,post-crit)=({r['y_mean_pre_flat']:5.2f},{r['y_mean_pre_crit']:5.2f},{r['y_mean_post_flat']:5.2f},{r['y_mean_post_crit']:5.2f})")


def main() -> None:
    results = []

    # B2 — same-cal-month
    print("\n=== B2 — DA-IDA wedge DiD: same-cal-month Oct-Dec 2024 vs Oct-Dec 2025 ===")
    pre = build_wedge_panel(PRE_START, PRE_END)
    post = build_wedge_panel(POST_START, POST_END)
    pre["post"] = 0; post["post"] = 1
    panel = pd.concat([pre, post], ignore_index=True)
    print(f"Panel rows: {len(panel)}")
    r = run_did(panel, "samecal")
    results.append(r); print_result(r)

    # B2.1 — full window with reforzada and MTU15-IDA dummies
    print("\n=== B2.1 — DA-IDA wedge DiD: full window 2024-2025, with reforzada control ===")
    full = build_wedge_panel(FULL_START, FULL_END)
    # post = post-MTU15-DA
    full["post"] = (full["d"] >= pd.Timestamp(MTU15_DA_DATE)).astype(int)
    full["reforzada"] = (full["d"] >= pd.Timestamp(REFORZADA_START)).astype(int)
    full["mtu15_ida"] = (full["d"] >= pd.Timestamp(MTU15_IDA_DATE)).astype(int)
    full["crit_x_reforzada"] = (full["hour_class"] == "critical_h18_22").astype(int) * full["reforzada"]
    full["crit_x_mtu15_ida"] = (full["hour_class"] == "critical_h18_22").astype(int) * full["mtu15_ida"]

    print("\n  no controls:")
    r = run_did(full, "full_no_ctrl")
    results.append(r); print_result(r)

    print("\n  with reforzada × crit interaction:")
    r = run_did(full, "full_reforzada_ctrl", extra_controls=["reforzada", "crit_x_reforzada"])
    results.append(r); print_result(r)

    print("\n  with mtu15_ida × crit + reforzada × crit interactions (both):")
    r = run_did(full, "full_both_ctrls",
                extra_controls=["mtu15_ida","reforzada","crit_x_mtu15_ida","crit_x_reforzada"])
    results.append(r); print_result(r)

    df_results = pd.DataFrame(results)
    df_results.to_csv(OUTDIR / "B2_wedge_did.csv", index=False)
    print(f"\nSaved: {OUTDIR / 'B2_wedge_did.csv'}")


if __name__ == "__main__":
    main()
