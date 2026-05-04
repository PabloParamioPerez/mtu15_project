# STATUS: ALIVE
# LAST-AUDIT: 2026-05-04
# FEEDS: Critical-hours supply decomposition + composition check
# CLAIM: Track where Big-4 supply goes in critical hours under each regime —
#        auction (PDBC) vs bilateral (PDBF) vs IDA (PIBCIE) vs continuous
#        (PIBCICE). Tests whether the q1 + q2 V-shape reflects total-supply
#        reduction or substitution into other channels.
"""Critical-hours supply decomposition.

For each Big-4 firm-day-hour, compute:
  - q_DA_auction:  PDBCE auction-cleared sell (offer_type=1)
  - q_DA_bilat:    PDBF bilateral commitments (offer_type=4) per unit, summed to firm
  - q_IDA:         PIBCIE signed IDA repositioning
  - q_CID:         PIBCICE per-firm continuous-market repositioning

Run the same DiD framework on each component AND on the sum.
If the sum is roughly constant across regimes (only allocation shifts),
then we have substitution between channels. If the sum follows the same
V-shape as q_DA, then critical-hour supply is genuinely lower under
asymmetric clocks.

Plus a composition check: which Big-4 units are active in critical hours
across regimes? If the active set differs, the q1 V-shape may be partly
compositional.

Output:
  results/regressions/critical_hours_supply_decomp.csv
  figures/working/critical_hours_supply_decomp.png
"""
from __future__ import annotations

from pathlib import Path

import duckdb
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import statsmodels.api as sm

PROJECT  = Path(__file__).resolve().parents[3]
PIBCIE   = PROJECT / "data" / "processed" / "omie" / "mercado_intradiario_subastas" / "programas" / "pibcie_all.parquet"
PDBCE    = PROJECT / "data" / "processed" / "omie" / "mercado_diario" / "programas" / "pdbce_all.parquet"
PDBF     = PROJECT / "data" / "processed" / "omie" / "mercado_diario" / "programas" / "pdbf_all.parquet"
PIBCICE  = PROJECT / "data" / "processed" / "omie" / "mercado_intradiario_continuo" / "programas" / "pibcice_all.parquet"

OUT_DIR_R = PROJECT / "results" / "regressions"
OUT_DIR_F = PROJECT / "figures" / "working"

REGIMES_6 = ["pre-IDA", "3-sess", "ISP15-win", "DA60/ID15-pre-blackout",
             "DA60/ID15-reforzada", "DA15/ID15"]
BIG4 = ["IB", "GE", "GN", "HC"]
MTU15_IDA_DATE = pd.Timestamp("2025-03-19")
MTU15_DA_DATE  = pd.Timestamp("2025-10-01")
BLACKOUT_DATE  = pd.Timestamp("2025-04-28")
CRITICAL_HOURS = [7, 8, 16, 17, 18]


def assign_regime6(d) -> str:
    d = pd.Timestamp(d)
    if d < pd.Timestamp("2024-06-14"): return "pre-IDA"
    if d < pd.Timestamp("2024-12-01"): return "3-sess"
    if d < pd.Timestamp("2025-03-19"): return "ISP15-win"
    if d < pd.Timestamp("2025-04-28"): return "DA60/ID15-pre-blackout"
    if d < pd.Timestamp("2025-10-01"): return "DA60/ID15-reforzada"
    return "DA15/ID15"


def fit_did(panel, regimes, group_for_dummies=None, outcome="q_mwh"):
    cols = {"const": np.ones(len(panel))}
    cols["critical"] = panel["critical"].values.astype(float)
    for r in regimes[1:]:
        cols[f"R_{r}"] = (panel["regime"] == r).astype(float).values
    for r in regimes[1:]:
        cols[f"crit×R_{r}"] = ((panel["critical"]) * (panel["regime"] == r)).astype(float).values
    if group_for_dummies is not None:
        for g in group_for_dummies:
            for v in sorted(panel[g].unique())[1:]:
                cols[f"{g}_{v}"] = (panel[g] == v).astype(float).values
    for m_ in range(2, 13):
        cols[f"M{m_}"] = (panel["month"] == m_).astype(float).values
    years = sorted(panel["year"].unique())
    for yr in years[1:]:
        cols[f"Y{yr}"] = (panel["year"] == yr).astype(float).values
    for d_ in range(1, 7):
        cols[f"DOW{d_}"] = (panel["dow"] == d_).astype(float).values

    X = pd.DataFrame(cols, index=panel.index)
    y = panel[outcome].values
    cluster = panel["date"].dt.strftime("%Y%m%d").astype(np.int64).values
    m = sm.OLS(y, X.values).fit(cov_type="cluster", cov_kwds={"groups": cluster})
    return dict(coefs=pd.Series(m.params, index=X.columns),
                ses=pd.Series(m.bse, index=X.columns),
                pvals=pd.Series(m.pvalues, index=X.columns),
                n=len(panel), n_clusters=len(np.unique(cluster)),
                rsq=m.rsquared)


def assign_hour(date_series, period_series, mtu15_cutoff):
    """Map period→hour: post-MTU15 cutoff → period//4; pre → period-1."""
    is_post = date_series >= mtu15_cutoff
    h = np.where(is_post,
                 ((period_series - 1) // 4).astype(int),
                 (period_series - 1).astype(int))
    return np.clip(h, 0, 23)


def main() -> None:
    con = duckdb.connect()
    con.execute("SET memory_limit='8GB'")
    con.execute("SET threads=4")

    print("[setup] unit → firm mapping…", flush=True)
    firms = con.execute(f"""
        SELECT unit_code, grupo_empresarial AS firm FROM (
          SELECT unit_code, grupo_empresarial,
                 ROW_NUMBER() OVER (PARTITION BY unit_code ORDER BY date DESC) AS rn
          FROM '{PDBCE}' WHERE grupo_empresarial IS NOT NULL) WHERE rn = 1
    """).df()
    con.register("uf", firms[["unit_code","firm"]])

    # ---------------------------------------------------------------
    # 1. q_DA_auction (PDBCE, sell-side, per firm-day-period)
    # ---------------------------------------------------------------
    print("[1/4] q_DA_auction from PDBCE…", flush=True)
    q_da = con.execute(f"""
        SELECT CAST(date AS DATE) AS date, period, grupo_empresarial AS firm,
               SUM(CASE WHEN offer_type = 1 AND assigned_power_mw > 0
                        THEN assigned_power_mw * mtu_minutes / 60.0 ELSE 0 END) AS q_da_auction_mwh
        FROM '{PDBCE}'
        WHERE grupo_empresarial IN ('IB','GE','GN','HC')
        GROUP BY 1, 2, 3
    """).df()
    q_da["date"] = pd.to_datetime(q_da["date"])
    q_da["hour"] = assign_hour(q_da["date"], q_da["period"], MTU15_DA_DATE)

    # ---------------------------------------------------------------
    # 2. q_DA_bilat (PDBF offer_type=4, per unit → aggregate to firm)
    # ---------------------------------------------------------------
    print("[2/4] q_DA_bilat from PDBF (per unit)…", flush=True)
    q_bilat = con.execute(f"""
        SELECT CAST(p.date AS DATE) AS date, p.period, uf.firm,
               SUM(CASE WHEN p.offer_type = 4 AND p.assigned_power_mw > 0
                        THEN p.assigned_power_mw * p.mtu_minutes / 60.0 ELSE 0 END) AS q_da_bilat_mwh
        FROM '{PDBF}' p JOIN uf USING (unit_code)
        WHERE uf.firm IN ('IB','GE','GN','HC')
        GROUP BY 1, 2, 3
    """).df()
    q_bilat["date"] = pd.to_datetime(q_bilat["date"])
    # PDBF went 15-min at MTU15-IDA (2025-03-19), NOT at MTU15-DA (2025-10-01)
    q_bilat["hour"] = assign_hour(q_bilat["date"], q_bilat["period"], MTU15_IDA_DATE)

    # ---------------------------------------------------------------
    # 3. q_IDA (PIBCIE per firm-day-period)
    # ---------------------------------------------------------------
    print("[3/4] q_IDA from PIBCIE…", flush=True)
    q_ida = con.execute(f"""
        SELECT CAST(date AS DATE) AS date, period, grupo_empresarial AS firm,
               SUM(assigned_power_mw * mtu_minutes / 60.0) AS q_ida_mwh
        FROM '{PIBCIE}'
        WHERE assigned_power_mw IS NOT NULL
          AND grupo_empresarial IN ('IB','GE','GN','HC')
        GROUP BY 1, 2, 3
    """).df()
    q_ida["date"] = pd.to_datetime(q_ida["date"])
    q_ida["hour"] = assign_hour(q_ida["date"], q_ida["period"], MTU15_IDA_DATE)

    # ---------------------------------------------------------------
    # 4. q_CID (PIBCICE per firm-day-period — continuous market, signed)
    # ---------------------------------------------------------------
    print("[4/4] q_CID from PIBCICE…", flush=True)
    try:
        q_cid = con.execute(f"""
            SELECT CAST(date AS DATE) AS date, period, grupo_empresarial AS firm,
                   SUM(assigned_power_mw * mtu_minutes / 60.0) AS q_cid_mwh
            FROM '{PIBCICE}'
            WHERE assigned_power_mw IS NOT NULL
              AND grupo_empresarial IN ('IB','GE','GN','HC')
            GROUP BY 1, 2, 3
        """).df()
        q_cid["date"] = pd.to_datetime(q_cid["date"])
        # PIBCICE went 15-min at MTU15-IDA (continuous market); use that cutoff
        q_cid["hour"] = assign_hour(q_cid["date"], q_cid["period"], MTU15_IDA_DATE)
        has_cid = True
    except Exception as e:
        print(f"   (PIBCICE not available: {e}; skipping continuous channel)", flush=True)
        q_cid = pd.DataFrame(columns=["date","period","firm","q_cid_mwh","hour"])
        has_cid = False

    # ---------------------------------------------------------------
    # Aggregate to firm-day-hour and merge
    # ---------------------------------------------------------------
    print("\n[merge] aggregating to firm-day-hour…", flush=True)
    q_da_h    = q_da.groupby(["date","firm","hour"], as_index=False)["q_da_auction_mwh"].sum()
    q_bilat_h = q_bilat.groupby(["date","firm","hour"], as_index=False)["q_da_bilat_mwh"].sum()
    q_ida_h   = q_ida.groupby(["date","firm","hour"], as_index=False)["q_ida_mwh"].sum()
    if has_cid and len(q_cid) > 0:
        q_cid_h = q_cid.groupby(["date","firm","hour"], as_index=False)["q_cid_mwh"].sum()
    else:
        q_cid_h = pd.DataFrame(columns=["date","firm","hour","q_cid_mwh"])

    panel = q_da_h.merge(q_bilat_h, on=["date","firm","hour"], how="outer")
    panel = panel.merge(q_ida_h,   on=["date","firm","hour"], how="outer")
    panel = panel.merge(q_cid_h,   on=["date","firm","hour"], how="outer")
    for c in ["q_da_auction_mwh","q_da_bilat_mwh","q_ida_mwh","q_cid_mwh"]:
        if c not in panel.columns: panel[c] = 0.0
        panel[c] = pd.to_numeric(panel[c], errors="coerce").fillna(0.0).astype(float)
    panel["q_total"] = (panel["q_da_auction_mwh"] + panel["q_da_bilat_mwh"]
                       + panel["q_ida_mwh"] + panel["q_cid_mwh"]).astype(float)
    panel["regime"] = panel["date"].apply(assign_regime6)
    panel["critical"] = panel["hour"].isin(CRITICAL_HOURS).astype(int)
    panel["dow"] = panel["date"].dt.dayofweek
    panel["month"] = panel["date"].dt.month
    panel["year"] = panel["date"].dt.year
    print(f"   panel size: {len(panel):,} firm-day-hour rows", flush=True)
    print(f"   has_cid: {has_cid}", flush=True)

    # ---------------------------------------------------------------
    # Run DiD for each component
    # ---------------------------------------------------------------
    components = [
        ("q_da_auction_mwh",   "q_DA_auction (PDBC)"),
        ("q_da_bilat_mwh",     "q_DA_bilateral (PDBF)"),
        ("q_ida_mwh",          "q_IDA (PIBCIE)"),
        ("q_cid_mwh",          "q_CID (PIBCICE)"),
        ("q_total",            "TOTAL (DA + bilat + IDA + CID)"),
    ]
    rows = []
    print()
    print("=" * 110)
    print("Critical-hours DiD by SUPPLY COMPONENT (Big-4 firm-day-hour)")
    print("=" * 110)
    for col, label in components:
        # Skip CID if the data isn't there
        if col == "q_cid_mwh" and panel["q_cid_mwh"].abs().sum() == 0:
            print(f"\n  {label:35s}: (no CID data)")
            continue
        res = fit_did(panel.assign(q_mwh=panel[col]).rename(columns={col: "_drop"}),
                      REGIMES_6, group_for_dummies=["firm"], outcome="q_mwh")
        print()
        print(f"  {label}  (R²={res['rsq']:.3f}, N={res['n']:,}, G={res['n_clusters']:,})")
        print(f"    crit main effect (pre-IDA baseline): β = {res['coefs']['critical']:+.1f}")
        for r in REGIMES_6[1:]:
            k = f"crit×R_{r}"
            b, se, p = res["coefs"][k], res["ses"][k], res["pvals"][k]
            print(f"    {r:30s}: δ = {b:+8.1f}  (SE {se:5.1f}, p={p:.2e})")
            rows.append({"component": label, "regime": r,
                         "delta_did": b, "se": se, "p": p,
                         "crit_main_effect": res["coefs"]["critical"]})
    pd.DataFrame(rows).to_csv(OUT_DIR_R / "critical_hours_supply_decomp.csv", index=False)

    # Figure: stacked DiD bars by component × regime
    OUT_DIR_F.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(rows)
    fig, ax = plt.subplots(figsize=(13, 5.5))
    width = 0.18
    x = np.arange(len(REGIMES_6[1:]))
    components_in_data = [(c,l) for c,l in components if l in df.component.unique()]
    colors = ["tab:blue","tab:green","tab:red","tab:orange","tab:purple"]
    for i, (col, label) in enumerate(components_in_data):
        sub = df[df.component == label].set_index("regime")
        d = [sub.loc[r, "delta_did"] if r in sub.index else 0 for r in REGIMES_6[1:]]
        s = [sub.loc[r, "se"]        if r in sub.index else 0 for r in REGIMES_6[1:]]
        ax.bar(x + (i-1.5)*width, d, width, yerr=[1.96*v for v in s], capsize=3,
               label=label, color=colors[i % len(colors)])
    ax.axhline(0, color="black", lw=0.6)
    labels_x = ["3-sess","ISP15-win","DA60/ID15\npre-blackout","DA60/ID15\nreforzada","DA15/ID15"]
    ax.set_xticks(x)
    ax.set_xticklabels(labels_x, fontsize=9)
    ax.set_ylabel(r"DiD δ$_R$ (MWh / firm-hour)")
    ax.set_title("Critical-hours DiD by supply component — Big-4 firm-day-hour\n"
                 "Tracks where supply moves in critical hours across regimes (95% CI)")
    ax.legend(fontsize=9, ncol=2)
    plt.tight_layout()
    plt.savefig(OUT_DIR_F / "critical_hours_supply_decomp.png", dpi=110, bbox_inches="tight")
    plt.close()

    # ---------------------------------------------------------------
    # Composition check: how many distinct Big-4 units are active in critical hours
    # ---------------------------------------------------------------
    print()
    print("=" * 110)
    print("Composition check: number of unique Big-4 units active in critical vs flat hours, by regime")
    print("=" * 110)
    # PDBC unit-level activity (auction)
    unit_active = con.execute(f"""
        SELECT CAST(p.date AS DATE) AS date, p.period, p.unit_code, uf.firm
        FROM '{PDBCE}' p JOIN uf USING (unit_code)
        WHERE uf.firm IN ('IB','GE','GN','HC')
          AND p.offer_type = 1
          AND p.assigned_power_mw > 0
        GROUP BY 1, 2, 3, 4
    """).df()
    unit_active["date"] = pd.to_datetime(unit_active["date"])
    unit_active["hour"] = assign_hour(unit_active["date"], unit_active["period"], MTU15_DA_DATE)
    unit_active["regime"] = unit_active["date"].apply(assign_regime6)
    unit_active["critical"] = unit_active["hour"].isin(CRITICAL_HOURS).astype(int)
    comp = (unit_active.groupby(["regime","critical"])["unit_code"].nunique()
                       .reset_index().rename(columns={"unit_code":"n_unique_units"}))
    comp_pivot = comp.pivot(index="regime", columns="critical", values="n_unique_units")
    comp_pivot.columns = ["flat", "critical"]
    comp_pivot = comp_pivot.reindex(REGIMES_6)
    print()
    print(comp_pivot.to_string())

    print()
    print(f"wrote {OUT_DIR_F / 'critical_hours_supply_decomp.png'}")
    print(f"wrote {OUT_DIR_R / 'critical_hours_supply_decomp.csv'}")
    print("Done.")


if __name__ == "__main__":
    main()
