# STATUS: ALIVE
# LAST-AUDIT: 2026-05-02
# FEEDS: F26 mechanism diagnostic — did contracts terminate or shrink at IDA reform?
# CLAIM: F26's 31.6pp nuclear bilateral drop at 2024-06-14 reflects contract
#        churn (different contract IDs active before vs after) or volume
#        compression (same contracts, less volume).
"""F26 mechanism diagnostic — bilateral contract churn at IDA reform.

For each Big-4 nuclear unit, profile bilateral contracts active in:
  PRE  = 2024-01-01 → 2024-06-13 (last 5.5 mo pre-IDA)
  POST = 2024-06-14 → 2024-12-01 (3-sess regime, 5.5 mo post-IDA)

Then compute:
  T1 — # of distinct contract IDs active per unit, pre vs post
  T2 — set diff: contracts terminated (in PRE only), continuing (in both),
       new (in POST only)
  T3 — total bilateral GWh per unit pre vs post; share attributable to
       continuing-vs-terminated contracts

Diagnostic logic:
  - If POST has FEWER contracts than PRE and total GWh dropped: contract
    churn (a commercial/regulatory event ended specific contracts).
  - If POST has SIMILAR contracts but lower GWh-per-contract: existing
    contracts still in force, less volume utilized.
  - If POST has MORE contracts but lower mean volume: contract proliferation
    with smaller agreements (bilateral channel restructured).

Output:
  results/regressions/pdbf_f26_contract_churn.csv
  + console pivot tables for each test
"""
from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd

PROJECT = Path(__file__).resolve().parents[3]
PDBF    = PROJECT / "data" / "processed" / "omie" / "mercado_diario" / "programas" / "pdbf_all.parquet"
PDBCE   = PROJECT / "data" / "processed" / "omie" / "mercado_diario" / "programas" / "pdbce_all.parquet"
LISTA   = PROJECT / "data" / "external" / "omie_reference" / "lista_unidades.csv"
OUT     = PROJECT / "results" / "regressions" / "pdbf_f26_contract_churn.csv"

PRE_START  = "2024-01-01"
PRE_END    = "2024-06-13"   # last day pre-IDA
POST_START = "2024-06-14"
POST_END   = "2024-12-01"   # last day 3-sess


def main() -> None:
    con = duckdb.connect()
    con.execute("SET memory_limit='8GB'")
    con.execute("SET threads=4")

    print("[setup] firm + nuclear-tech mapping…", flush=True)
    firms = con.execute(f"""
        SELECT unit_code, grupo_empresarial AS firm FROM (
          SELECT unit_code, grupo_empresarial,
                 ROW_NUMBER() OVER (PARTITION BY unit_code ORDER BY date DESC) AS rn
          FROM '{PDBCE}' WHERE grupo_empresarial IS NOT NULL) WHERE rn = 1
    """).df()
    lista = pd.read_csv(LISTA)[["unit_code", "technology"]]
    map_uf = firms.merge(lista, on="unit_code", how="left")
    nuclear_units = map_uf[
        map_uf.firm.isin(["IB","GE","GN","HC"])
        & map_uf.technology.fillna("").str.lower().str.contains("nuclear")
    ][["unit_code","firm"]]
    con.register("uf", nuclear_units)
    print(f"   nuclear Big-4 units: {len(nuclear_units)}; "
          f"{nuclear_units.unit_code.tolist()}", flush=True)

    # Per (unit, period, contract_id) bilateral volume in PRE and POST
    print("[panel] bilateral contracts × unit × window…", flush=True)
    panel = con.execute(f"""
        WITH d AS (
          SELECT CAST(p.date AS DATE) AS date, p.unit_code,
                 uf.firm, p.bilateral_contract_id,
                 p.assigned_power_mw, p.mtu_minutes
          FROM '{PDBF}' p JOIN uf USING (unit_code)
          WHERE p.offer_type = 4 AND p.assigned_power_mw > 0
            AND p.bilateral_contract_id IS NOT NULL
            AND CAST(p.date AS DATE) BETWEEN DATE '{PRE_START}' AND DATE '{POST_END}'
        )
        SELECT unit_code, firm, bilateral_contract_id,
               CASE WHEN date <= DATE '{PRE_END}' THEN 'PRE' ELSE 'POST' END AS window,
               SUM(assigned_power_mw * mtu_minutes / 60.0) AS gwh,
               COUNT(*) AS n_periods,
               COUNT(DISTINCT date) AS n_days
        FROM d GROUP BY 1, 2, 3, 4
    """).df()
    panel["gwh"] = panel["gwh"] / 1000  # MWh → GWh

    # ----------------------------------------------------------------------
    # T1 — # of distinct contract IDs active per unit, pre vs post
    # ----------------------------------------------------------------------
    print("\n=== T1 — # of distinct contract IDs active per nuclear unit ===\n", flush=True)
    t1 = (panel.groupby(["unit_code", "firm", "window"]).agg(
              n_contracts=("bilateral_contract_id", "nunique"),
              total_gwh=("gwh", "sum"),
              n_days=("n_days", "max")
          ).reset_index())
    t1_w = t1.pivot_table(index=["firm","unit_code"], columns="window",
                          values=["n_contracts","total_gwh","n_days"]).reset_index()
    t1_w.columns = [f"{a}_{b}" if b else a for a,b in t1_w.columns]
    t1_w["Δn_contracts"] = t1_w["n_contracts_POST"].fillna(0) - t1_w["n_contracts_PRE"].fillna(0)
    t1_w["Δgwh"]         = t1_w["total_gwh_POST"].fillna(0) - t1_w["total_gwh_PRE"].fillna(0)
    t1_w["pct_gwh_change"] = (t1_w["total_gwh_POST"].fillna(0) - t1_w["total_gwh_PRE"].fillna(0)) / t1_w["total_gwh_PRE"] * 100
    print(t1_w.to_string(index=False, float_format=lambda x: f"{x:.1f}"))

    # ----------------------------------------------------------------------
    # T2 — set diff per unit: terminated, continuing, new
    # ----------------------------------------------------------------------
    print("\n=== T2 — contract set-diff per unit (terminated / continuing / new) ===\n", flush=True)
    rows_t2 = []
    for u, sub in panel.groupby("unit_code"):
        firm = sub.firm.iloc[0]
        pre_ids  = set(sub[sub.window == "PRE"].bilateral_contract_id)
        post_ids = set(sub[sub.window == "POST"].bilateral_contract_id)
        terminated = pre_ids - post_ids
        continuing = pre_ids & post_ids
        new        = post_ids - pre_ids
        # GWh attributable to each group, in their respective window
        gwh_term_pre  = sub[(sub.window == "PRE")  & sub.bilateral_contract_id.isin(terminated)].gwh.sum()
        gwh_cont_pre  = sub[(sub.window == "PRE")  & sub.bilateral_contract_id.isin(continuing)].gwh.sum()
        gwh_cont_post = sub[(sub.window == "POST") & sub.bilateral_contract_id.isin(continuing)].gwh.sum()
        gwh_new_post  = sub[(sub.window == "POST") & sub.bilateral_contract_id.isin(new)].gwh.sum()
        rows_t2.append({
            "firm": firm, "unit_code": u,
            "n_pre":  len(pre_ids), "n_post": len(post_ids),
            "n_term": len(terminated), "n_cont": len(continuing), "n_new": len(new),
            "gwh_term_pre":  gwh_term_pre,
            "gwh_cont_pre":  gwh_cont_pre, "gwh_cont_post": gwh_cont_post,
            "gwh_new_post":  gwh_new_post,
            "cont_gwh_pct_change": (gwh_cont_post - gwh_cont_pre) / gwh_cont_pre * 100
                                   if gwh_cont_pre > 0 else float("nan"),
        })
    t2 = pd.DataFrame(rows_t2).sort_values(["firm","unit_code"])
    print(t2.to_string(index=False, float_format=lambda x: f"{x:.1f}"))

    # ----------------------------------------------------------------------
    # T3 — Big-4 nuclear aggregate: how much of the F26 drop is each channel?
    # ----------------------------------------------------------------------
    print("\n=== T3 — aggregate nuclear bilateral GWh decomposition (Big-4 nuclear) ===\n", flush=True)
    pre_total_gwh   = t2.gwh_term_pre.sum() + t2.gwh_cont_pre.sum()
    post_total_gwh  = t2.gwh_cont_post.sum() + t2.gwh_new_post.sum()
    drop_total_gwh  = post_total_gwh - pre_total_gwh
    drop_term       = -t2.gwh_term_pre.sum()        # terminated contracts contribute -gwh_term_pre
    drop_cont       = (t2.gwh_cont_post.sum() - t2.gwh_cont_pre.sum())
    drop_new        = t2.gwh_new_post.sum()
    print(f"  PRE  total nuclear bilateral GWh: {pre_total_gwh:8,.0f}")
    print(f"  POST total nuclear bilateral GWh: {post_total_gwh:8,.0f}")
    print(f"  Δ total: {drop_total_gwh:+,.0f} GWh ({drop_total_gwh/pre_total_gwh*100:+.1f}%)")
    print()
    print(f"  Decomposition:")
    print(f"    Terminated contracts ({t2.n_term.sum()} contracts dropped): {drop_term:+,.0f} GWh")
    print(f"    Continuing contracts ({t2.n_cont.sum()} contracts persist): {drop_cont:+,.0f} GWh")
    print(f"    New contracts        ({t2.n_new.sum()} contracts added):   {drop_new:+,.0f} GWh")
    print()
    pct_term = drop_term / drop_total_gwh * 100 if drop_total_gwh != 0 else 0
    pct_cont = drop_cont / drop_total_gwh * 100 if drop_total_gwh != 0 else 0
    pct_new  = drop_new  / drop_total_gwh * 100 if drop_total_gwh != 0 else 0
    print(f"    → Terminated explains {pct_term:.0f}% of the change")
    print(f"    → Continuing explains {pct_cont:.0f}% of the change")
    print(f"    → New        explains {pct_new:.0f}% of the change")
    print()
    if abs(pct_term) > 60:
        print("  → DOMINANT CHANNEL: contract TERMINATIONS at IDA reform")
    elif abs(pct_cont) > 60:
        print("  → DOMINANT CHANNEL: existing contracts continue but at lower volume")
    else:
        print("  → MIXED: both contract churn and volume compression")

    OUT.parent.mkdir(parents=True, exist_ok=True)
    t2.to_csv(OUT, index=False)
    print(f"\nwrote {OUT}")


if __name__ == "__main__":
    main()
