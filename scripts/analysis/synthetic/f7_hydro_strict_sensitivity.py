# STATUS: ALIVE
# LAST-AUDIT: 2026-04-27
# FEEDS: F7 hydro sensitivity (red-team audit item 7)
# CLAIM: Sensitivity test of F7 IB-hydro attribution under STRICTER plant-pair matching (split reservoir vs RoR; capacity tolerance ≤3×). Bounds the matching-artifact concern from _red_team_audit.md A1.
"""F7 hydro plant-pair matching sensitivity — STRICT mode.

Red-team audit item 7. The baseline F7 matching (synthetic_firm_matching.py)
lumps all hydro into a single "Hydro" bucket and matches by closest absolute
capacity, no tolerance. This is the source of the A1 attack: a Big-4
reservoir-hydro plant of 1000 MW could be matched to a 30 MW Fringe
run-of-river — the resulting "synthetic" supply is dominated by
operational asymmetry, not strategic markup.

This script re-runs the per-IB-hydro-unit decomposition under STRICTER
matching:

  1. Hydro is split into "Hydro-Reservoir" and "Hydro-RoR" technology
     subtypes (matching only within subtype):
       - Hydro-Reservoir = "Hidráulica Generación" + "Hidráulica de Bombeo Puro"
       - Hydro-RoR       = "RE Mercado Hidráulica" + "RE Tar. CUR Hidráulica"
  2. Capacity tolerance: K_ratio (capacity_L / capacity_S) must be in
     [1/3, 3]. Plants outside this band are dropped (no match).
  3. Compare per-unit IB-hydro attribution under strict matching to the
     baseline (data/derived/results/synthetic_firm_per_unit_ib.csv).

Output:
    data/derived/results/f7_hydro_strict_sensitivity.csv

The headline number to track: what fraction of the baseline ~€530M
IB-hydro attribution survives strict matching? If a substantial fraction
survives (e.g. >50%), the F7 hydro story is robust. If most disappears
(no valid match for IB reservoir hydro, or huge K-ratio drift), the F7
hydro attribution is flagged as matching-artifact.
"""
from __future__ import annotations

import sys
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

PROJECT = Path(__file__).resolve().parents[3]
DET = PROJECT / "data" / "processed" / "omie" / "mercado_diario" / "ofertas" / "det_all.parquet"
CAB = PROJECT / "data" / "processed" / "omie" / "mercado_diario" / "ofertas" / "cab_all.parquet"
PDBCE = PROJECT / "data" / "processed" / "omie" / "mercado_diario" / "programas" / "pdbce_all.parquet"
REF = PROJECT / "data" / "external" / "omie_reference" / "lista_unidades.csv"
BASELINE = PROJECT / "data" / "derived" / "results" / "synthetic_firm_per_unit_ib.csv"
OUT = PROJECT / "data" / "derived" / "results" / "f7_hydro_strict_sensitivity.csv"

K_RATIO_MIN = 1.0 / 3.0
K_RATIO_MAX = 3.0


def bucket_tech_strict(t) -> str:
    """Strict tech bucket: split hydro into reservoir vs run-of-river."""
    if pd.isna(t):
        return "Other"
    t = str(t).lower()
    if "ciclo combinado" in t:
        return "CCGT"
    if "nuclear" in t:
        return "Nuclear"
    if "hidrá" in t or "hidra" in t or "hidr" in t:
        # Split: reservoir-style vs run-of-river/small
        if "re mercado hidr" in t or "re tar. cur hidr" in t:
            return "Hydro-RoR"
        # Hidráulica Generación and Hidráulica de Bombeo Puro
        return "Hydro-Reservoir"
    if "bombeo" in t or "bomba" in t:
        return "Hydro-Reservoir"   # pumped storage with reservoir
    return "Other"


def build_strict_match_table() -> pd.DataFrame:
    """Replicate the matching logic with stricter constraints; return the
    IB-hydro slice (we only need IB hydro for this sensitivity)."""
    print("[1/3] Building strict matching table (hydro split + ±3× capacity tolerance)...")
    ref = pd.read_csv(REF, encoding="latin1")
    ref["tech"] = ref["technology"].apply(bucket_tech_strict)

    con = duckdb.connect()
    con.execute("SET memory_limit='2GB'")
    con.execute("SET threads=4")
    cap_df = con.sql(f"""
        SELECT unit_code,
               grupo_empresarial AS firm,
               COUNT(*) AS n_obs,
               QUANTILE_CONT(assigned_power_mw, 0.99) AS p99_mw
        FROM '{PDBCE}'
        WHERE offer_type = 1
          AND assigned_power_mw IS NOT NULL
          AND assigned_power_mw > 0
          AND CAST(date AS DATE) >= DATE '2024-01-01'
        GROUP BY unit_code, grupo_empresarial
    """).df()
    cap_df = cap_df.merge(ref[["unit_code", "tech"]], on="unit_code", how="left")
    cap_df["tech"] = cap_df["tech"].fillna("Other")
    cap_df["capacity_mw"] = cap_df["p99_mw"]

    BIG4 = ["GE", "IB", "GN", "HC"]
    cap_df["firm_group"] = cap_df["firm"].where(cap_df["firm"].isin(BIG4), "Fringe")

    # Restrict to hydro for this sensitivity
    hydro = cap_df[cap_df["tech"].isin(["Hydro-Reservoir", "Hydro-RoR"])].copy()
    print()
    print(f"   Hydro inventory by firm × subtype:")
    inv = hydro.groupby(["firm_group", "tech"], observed=True).agg(
        n_units=("unit_code", "count"),
        total_cap=("capacity_mw", "sum"),
        median_cap=("capacity_mw", "median"),
    ).round(1)
    print(inv.to_string())
    print()

    big4_hy = hydro[hydro["firm_group"].isin(BIG4)].copy()
    fringe_hy = hydro[hydro["firm_group"] == "Fringe"].copy()

    rows = []
    for _, L in big4_hy.iterrows():
        cands = fringe_hy[fringe_hy["tech"] == L["tech"]].copy()
        if len(cands) == 0:
            rows.append({
                "unit_L": L["unit_code"], "firm_L": L["firm"], "tech": L["tech"],
                "capacity_L": L["capacity_mw"], "unit_S": None, "firm_S": None,
                "capacity_S": np.nan, "K_ratio": np.nan, "match_distance": np.nan,
                "match_status": "no_subtype_candidate",
            })
            continue
        # Apply capacity tolerance
        cands["K_ratio"] = L["capacity_mw"] / cands["capacity_mw"].replace(0, np.nan)
        valid = cands[(cands["K_ratio"] >= K_RATIO_MIN) & (cands["K_ratio"] <= K_RATIO_MAX)]
        if len(valid) == 0:
            rows.append({
                "unit_L": L["unit_code"], "firm_L": L["firm"], "tech": L["tech"],
                "capacity_L": L["capacity_mw"], "unit_S": None, "firm_S": None,
                "capacity_S": np.nan, "K_ratio": np.nan, "match_distance": np.nan,
                "match_status": "capacity_out_of_band",
            })
            continue
        valid = valid.copy()
        valid["distance"] = (valid["capacity_mw"] - L["capacity_mw"]).abs()
        S = valid.sort_values("distance").iloc[0]
        rows.append({
            "unit_L": L["unit_code"], "firm_L": L["firm"], "tech": L["tech"],
            "capacity_L": L["capacity_mw"], "unit_S": S["unit_code"], "firm_S": S["firm"],
            "capacity_S": S["capacity_mw"],
            "K_ratio": L["capacity_mw"] / S["capacity_mw"],
            "match_distance": S["distance"],
            "match_status": "matched",
        })
    return pd.DataFrame(rows)


def assign_regime(d):
    d = pd.Timestamp(d)
    if d < pd.Timestamp("2025-03-19"):
        return "pre-MTU15-IDA"
    if d < pd.Timestamp("2025-10-01"):
        return "DA60/ID15"
    return "DA15/ID15"


def process_month(con, year: int, month: int, ib_match: pd.DataFrame) -> pd.DataFrame:
    """Re-clear month with each IB unit individually substituted via the
    in-memory ib_match table."""
    start = f"{year:04d}-{month:02d}-01"
    end = f"{year+1:04d}-01-01" if month == 12 else f"{year:04d}-{month+1:02d}-01"

    con.execute("DROP TABLE IF EXISTS match_tbl_strict")
    con.register("match_df", ib_match)
    con.execute("CREATE TEMP TABLE match_tbl_strict AS SELECT * FROM match_df")
    con.unregister("match_df")

    con.execute("DROP TABLE IF EXISTS sell_raw")
    con.execute(f"""
        CREATE TEMP TABLE sell_raw AS
        SELECT d.date, d.period, d.price_eur_mwh AS price,
               d.quantity_mw AS qty, c.unit_code
        FROM '{DET}' d JOIN '{CAB}' c
          ON c.date = d.date AND c.offer_code = d.offer_code AND c.version = d.version
        WHERE c.buy_sell = 'V' AND d.quantity_mw > 0 AND d.price_eur_mwh IS NOT NULL
          AND CAST(d.date AS DATE) >= DATE '{start}'
          AND CAST(d.date AS DATE) <  DATE '{end}'
    """)
    if con.sql("SELECT COUNT(*) FROM sell_raw").fetchone()[0] == 0:
        return pd.DataFrame()
    con.execute("DROP TABLE IF EXISTS buy_raw")
    con.execute(f"""
        CREATE TEMP TABLE buy_raw AS
        SELECT d.date, d.period, d.price_eur_mwh AS price, d.quantity_mw AS qty
        FROM '{DET}' d JOIN '{CAB}' c
          ON c.date = d.date AND c.offer_code = d.offer_code AND c.version = d.version
        WHERE c.buy_sell = 'C' AND d.quantity_mw > 0 AND d.price_eur_mwh IS NOT NULL
          AND CAST(d.date AS DATE) >= DATE '{start}'
          AND CAST(d.date AS DATE) <  DATE '{end}'
    """)
    con.execute("DROP TABLE IF EXISTS supply_actual")
    con.execute("CREATE TEMP TABLE supply_actual AS SELECT date, period, price, SUM(qty) AS qty FROM sell_raw GROUP BY date, period, price")
    con.execute("DROP TABLE IF EXISTS demand")
    con.execute("CREATE TEMP TABLE demand AS SELECT date, period, price, SUM(qty) AS qty FROM buy_raw GROUP BY date, period, price")
    con.execute("DROP TABLE IF EXISTS d_cum")
    con.execute("""
        CREATE TEMP TABLE d_cum AS
        SELECT date, period, price, qty,
               SUM(qty) OVER (PARTITION BY date, period
                              ORDER BY price DESC
                              ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_demand
        FROM demand
    """)
    actual_clear = con.sql("""
        WITH sa AS (
            SELECT date, period, price,
                   SUM(qty) OVER (PARTITION BY date, period
                                  ORDER BY price
                                  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_supply
            FROM supply_actual
        ),
        sa_join AS (
            SELECT s.date, s.period, s.price, s.cum_supply,
                   COALESCE(MAX(d.cum_demand), 0) AS cum_demand_at_or_above
            FROM sa s LEFT JOIN d_cum d
              ON d.date = s.date AND d.period = s.period AND d.price >= s.price
            GROUP BY s.date, s.period, s.price, s.cum_supply
        )
        SELECT date, period, MIN(price) AS p_actual
        FROM sa_join WHERE cum_supply >= cum_demand_at_or_above
        GROUP BY date, period
    """).df()

    out_rows = []
    for _, m in ib_match.iterrows():
        unit = m["unit_L"]
        if pd.isna(m["unit_S"]):
            continue
        con.execute("DROP TABLE IF EXISTS sell_synth_u")
        con.execute(f"""
            CREATE TEMP TABLE sell_synth_u AS
            SELECT date, period, price, qty FROM sell_raw WHERE unit_code <> '{unit}'
            UNION ALL
            SELECT s.date, s.period, s.price, s.qty * m.K_ratio AS qty
            FROM sell_raw s JOIN match_tbl_strict m
              ON m.unit_S = s.unit_code AND m.unit_L = '{unit}'
        """)
        clear_u = con.sql("""
            WITH agg AS (SELECT date, period, price, SUM(qty) AS qty FROM sell_synth_u GROUP BY date, period, price),
            cum AS (
                SELECT date, period, price,
                       SUM(qty) OVER (PARTITION BY date, period ORDER BY price ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_supply
                FROM agg
            ),
            jn AS (
                SELECT c.date, c.period, c.price, c.cum_supply,
                       COALESCE(MAX(d.cum_demand), 0) AS cum_demand_at_or_above
                FROM cum c LEFT JOIN d_cum d ON d.date = c.date AND d.period = c.period AND d.price >= c.price
                GROUP BY c.date, c.period, c.price, c.cum_supply
            )
            SELECT date, period, MIN(price) AS p_synth
            FROM jn WHERE cum_supply >= cum_demand_at_or_above
            GROUP BY date, period
        """).df()
        merged = actual_clear.merge(clear_u, on=["date", "period"], how="inner")
        merged["unit_L"] = unit
        merged["mp"] = merged["p_actual"] - merged["p_synth"]
        out_rows.append(merged)

    return pd.concat(out_rows, ignore_index=True) if out_rows else pd.DataFrame()


def main() -> None:
    match_strict = build_strict_match_table()
    print()
    print("[2/3] Strict match table — IB hydro:")
    ib = match_strict[match_strict["firm_L"] == "IB"].copy()
    print(ib[["unit_L", "tech", "capacity_L", "unit_S", "capacity_S", "K_ratio", "match_status"]].to_string(index=False))
    print()

    n_matched = (ib["match_status"] == "matched").sum()
    n_total = len(ib)
    print(f"   IB hydro matched under strict: {n_matched}/{n_total}")

    if n_matched == 0:
        print("ERROR: no IB hydro plants matched under strict criteria. Cannot run sensitivity.")
        return

    ib_matched_strict = ib[ib["match_status"] == "matched"][
        ["unit_L", "firm_L", "tech", "capacity_L", "unit_S", "capacity_S", "K_ratio"]
    ].copy()

    print()
    print("[3/3] Re-clearing post-MTU15-IDA months with strict-matched IB hydro substitution...")
    months = [(2025, m) for m in range(3, 13)] + [(2026, 1)]
    con = duckdb.connect()
    con.execute("SET memory_limit='3GB'")
    con.execute("SET threads=4")

    all_results = []
    for y, m in months:
        print(f"   {y:04d}-{m:02d}...", flush=True)
        try:
            df = process_month(con, y, m, ib_matched_strict)
        except Exception as e:
            print(f"     FAIL: {e}")
            continue
        if len(df) == 0:
            continue
        all_results.append(df)

    if not all_results:
        print("No results.")
        return

    full = pd.concat(all_results, ignore_index=True)
    full["date"] = pd.to_datetime(full["date"])
    full["regime"] = full["date"].apply(assign_regime)

    print()
    print("=" * 110)
    print("STRICT-MATCH per-IB-hydro-unit attribution (post-MTU15-IDA)")
    print("=" * 110)
    print(f"{'unit':<10}  {'subtype':<18}  {'n ISPs':>8}  {'mean MP €/MWh':>14}  {'~transfer M€':>14}")

    rows = []
    for unit in ib_matched_strict["unit_L"]:
        sub = full[full["unit_L"] == unit]
        if len(sub) == 0:
            continue
        mean_mp = sub["mp"].mean()
        transfer_eur = (sub["mp"] * 25_000 / 4).sum() / 1e6
        tech = ib_matched_strict[ib_matched_strict["unit_L"] == unit]["tech"].iloc[0]
        rows.append({
            "unit": unit, "tech": tech, "n_isps": len(sub),
            "mean_mp": float(mean_mp), "transfer_eur_M": float(transfer_eur),
        })

    rdf = pd.DataFrame(rows).sort_values("transfer_eur_M", ascending=False)
    for _, r in rdf.iterrows():
        print(f"{r['unit']:<10}  {r['tech']:<18}  {r['n_isps']:>8,}  {r['mean_mp']:>+14.3f}  {r['transfer_eur_M']:>+14.1f}")
    strict_total = rdf["transfer_eur_M"].sum()
    print()
    print(f"  STRICT total IB-hydro: €{strict_total:+.1f}M")

    # Compare to baseline
    print()
    print("=" * 110)
    print("Comparison: BASELINE (loose matching) vs STRICT (subtype + ±3× capacity)")
    print("=" * 110)
    if BASELINE.exists():
        base = pd.read_csv(BASELINE)
        base_hydro = base[base["tech"] == "Hydro"].copy()
        baseline_total = base_hydro["transfer_eur_M"].sum()
        print(f"  BASELINE IB hydro total:  €{baseline_total:+.1f}M  (n_units={len(base_hydro)})")
        print(f"  STRICT IB hydro total:    €{strict_total:+.1f}M  (n_units={len(rdf)})")
        survival_pct = (strict_total / baseline_total * 100) if baseline_total != 0 else float('nan')
        print(f"  Survival ratio:           {survival_pct:.1f}%")
        print()

        merge = base_hydro[["unit", "transfer_eur_M"]].rename(columns={"transfer_eur_M": "transfer_BASELINE"})
        merge = merge.merge(rdf[["unit", "transfer_eur_M", "tech"]].rename(columns={"transfer_eur_M": "transfer_STRICT"}), on="unit", how="outer")
        merge["delta"] = merge["transfer_STRICT"] - merge["transfer_BASELINE"]
        merge = merge.sort_values("transfer_BASELINE", ascending=False, na_position="last")
        print("  Per-unit comparison (sorted by BASELINE):")
        print(merge.to_string(index=False))
    else:
        print(f"  (baseline file {BASELINE} not found; comparison skipped)")

    # Save
    OUT.parent.mkdir(parents=True, exist_ok=True)
    rdf.to_csv(OUT, index=False)
    print(f"\nwrote {OUT}")
    match_out = OUT.parent / "f7_hydro_strict_matchtable.csv"
    match_strict.to_csv(match_out, index=False)
    print(f"wrote {match_out}")


if __name__ == "__main__":
    main()
