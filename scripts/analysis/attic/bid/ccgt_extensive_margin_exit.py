# STATUS: ALIVE
# LAST-AUDIT: 2026-04-27
# FEEDS: D4 (Fringe extensive-margin exit at MTU15-IDA: 23 small Fringe + 3 HC units, ~6.8% of pre-reform DA volume; Big-4 zero exits)
# CLAIM: Extensive-margin Fringe exit at MTU15-IDA. Note: original W3 framing (CCGT bid simplification) is dead (X14) — this script's exit-audit finding survived as D4.
"""Extensive-margin participation response to MTU15-IDA (W3 reframe).

Hypothesis. The aggregate CCGT bid-granularity drop at MTU15-IDA
(W3 in CLAIMS_LEDGER) is mostly composition: complex-bidder CCGT units
EXIT the DA market entirely. The within-unit behavioural shift (W3's
original framing) is small; the participation-margin response is large.

Reframing. This is not a wound on the original within-unit story —
it's a *separate IO finding*: MTU15-IDA induced extensive-margin exit
of complex-bidder CCGT units from DA. Reform-induced participation
changes are a textbook IO topic (entry/exit, market participation
under shifting market design) and worth elevating.

Spec. Per (unit_code, year_month), sum DA cleared MWh (sell-side,
offer_type=1) from pdbce. Compute pre vs post MTU15-IDA totals per
unit. Identify 'exiters' = units active pre, near-zero post. List
them; quantify aggregate share of pre-reform DA volume they
represented; cross-reference with the named complex-bidder list
(TAPOWER, SRI4R, ARCOS1, CTN4) from earlier nb09 work.

Memory-conscious: DuckDB streams the parquet, returns only an
aggregated panel (~1k unit-month rows for CCGT-only). No full pandas
load.
"""
from __future__ import annotations

from pathlib import Path
import duckdb
import pandas as pd

PROJECT = Path(__file__).resolve().parents[3]
PDBCE = PROJECT / "data" / "processed" / "omie" / "mercado_diario" / "programas" / "pdbce_all.parquet"

# Reform date: MTU15-IDA = 2025-03-19.
PRE_START = "2024-01-01"
PRE_END = "2025-03-18"
POST_START = "2025-03-19"
POST_END = "2026-04-30"

# Named complex-bidders from nb09 audit
NAMED_COMPLEX = {"TAPOWER", "SRI4R", "ARCOS1", "CTN4"}


def main() -> None:
    con = duckdb.connect()
    con.execute("SET memory_limit='2GB'")
    con.execute("SET threads=4")

    # Per-unit DA cleared MWh in pre and post windows (sell-side only)
    print("[1/3] Per-unit DA cleared MWh, pre vs post MTU15-IDA...")
    df = con.sql(f"""
        SELECT unit_code,
               grupo_empresarial AS firm,
               SUM(CASE WHEN CAST(date AS DATE) BETWEEN DATE '{PRE_START}' AND DATE '{PRE_END}'
                        THEN assigned_power_mw ELSE 0 END) AS pre_mwh,
               SUM(CASE WHEN CAST(date AS DATE) BETWEEN DATE '{POST_START}' AND DATE '{POST_END}'
                        THEN assigned_power_mw ELSE 0 END) AS post_mwh,
               SUM(CASE WHEN CAST(date AS DATE) BETWEEN DATE '{PRE_START}' AND DATE '{PRE_END}'
                        AND assigned_power_mw > 0 THEN 1 ELSE 0 END) AS pre_active_obs,
               SUM(CASE WHEN CAST(date AS DATE) BETWEEN DATE '{POST_START}' AND DATE '{POST_END}'
                        AND assigned_power_mw > 0 THEN 1 ELSE 0 END) AS post_active_obs
        FROM '{PDBCE}'
        WHERE offer_type = 1
          AND assigned_power_mw IS NOT NULL
          AND assigned_power_mw >= 0
        GROUP BY unit_code, grupo_empresarial
    """).df()
    print(f"   {len(df):,} unit-firm rows")

    # Activity-rate metric: post-reform days active / pre-reform days active.
    # Avoid div-by-zero — units with pre_active_obs = 0 are out of scope.
    df = df[df["pre_active_obs"] > 0].copy()
    df["post_share"] = df["post_active_obs"] / df["pre_active_obs"]
    df["mwh_drop_pct"] = 100.0 * (df["pre_mwh"] - df["post_mwh"]) / df["pre_mwh"].clip(lower=1)

    # Identify "exiter" = active pre, post_active_obs / pre_active_obs < 0.10
    # (post-reform participation rate <10% of pre-reform).
    EXIT_THRESHOLD = 0.10
    MWH_THRESHOLD_PRE = 1000.0  # need substantial pre-reform DA presence to count
    df["exited"] = (df["post_share"] < EXIT_THRESHOLD) & (df["pre_mwh"] > MWH_THRESHOLD_PRE)

    # Restrict to Big-4 + Fringe with non-trivial pre-reform DA volume
    big4 = ["GE", "IB", "GN", "HC"]
    df["firm_group"] = df["firm"].where(df["firm"].isin(big4), "Fringe")

    print()
    print("=" * 90)
    print(f"EXTENSIVE-MARGIN EXIT AUDIT")
    print(f"  Pre window:  {PRE_START} to {PRE_END}")
    print(f"  Post window: {POST_START} to {POST_END}")
    print(f"  Exit criterion: pre cleared >{MWH_THRESHOLD_PRE:.0f} MWh AND post-active-rate <{EXIT_THRESHOLD:.0%}")
    print("=" * 90)
    print()

    # Headline: how many units exited? What share of pre-reform DA volume?
    pre_total = df["pre_mwh"].sum()
    exiters = df[df["exited"]].copy()
    exit_pre_share = exiters["pre_mwh"].sum() / pre_total * 100
    print(f"Total units with active pre-reform DA: {len(df):,}")
    print(f"  of which 'exiters' (pre-active, post-near-zero): {len(exiters):,}")
    print(f"  exiters' share of total pre-reform DA cleared volume: {exit_pre_share:.1f}%")
    print()

    # Per-firm breakdown of exit count
    print("Exit count by firm group:")
    print(f"  {'firm':<10}  {'units active pre':>18}  {'exiters':>10}  {'exit rate':>11}  {'exiters pre-MWh':>18}  {'exiters % of group pre':>22}")
    for fg in big4 + ["Fringe"]:
        sub = df[df["firm_group"] == fg]
        sub_exit = sub[sub["exited"]]
        if len(sub) == 0:
            continue
        rate = len(sub_exit) / len(sub) * 100
        pre_g = sub["pre_mwh"].sum()
        exit_pre_g = sub_exit["pre_mwh"].sum()
        share_g = (exit_pre_g / pre_g * 100) if pre_g > 0 else 0
        print(f"  {fg:<10}  {len(sub):>18,}  {len(sub_exit):>10,}  {rate:>10.1f}%  {exit_pre_g:>18,.0f}  {share_g:>21.1f}%")

    print()
    print("Top 25 exiters by pre-reform DA cleared volume:")
    print(f"  {'unit_code':<14}  {'firm':<8}  {'pre MWh':>14}  {'post MWh':>14}  {'pre active obs':>16}  {'post active obs':>17}  {'named complex?'}")
    top = exiters.sort_values("pre_mwh", ascending=False).head(25)
    for _, r in top.iterrows():
        named = "**YES**" if r["unit_code"] in NAMED_COMPLEX else ""
        print(
            f"  {r['unit_code']:<14}  {r['firm_group']:<8}  {r['pre_mwh']:>14,.0f}  {r['post_mwh']:>14,.0f}"
            f"  {r['pre_active_obs']:>16,}  {r['post_active_obs']:>17,}  {named}"
        )

    # Check named complex-bidders specifically
    print()
    print("Named complex-bidder CCGT units (TAPOWER, SRI4R, ARCOS1, CTN4):")
    named = df[df["unit_code"].isin(NAMED_COMPLEX)]
    if len(named):
        print(f"  {'unit_code':<14}  {'firm':<8}  {'pre MWh':>14}  {'post MWh':>14}  {'post share':>12}  {'flagged exited?':>16}")
        for _, r in named.iterrows():
            flag = "YES" if r["exited"] else "no"
            print(
                f"  {r['unit_code']:<14}  {r['firm_group']:<8}  {r['pre_mwh']:>14,.0f}  {r['post_mwh']:>14,.0f}"
                f"  {r['post_share']:>11.2%}  {flag:>16}"
            )
    else:
        print("  (none of the named units found in pdbce — check unit-code spelling)")

    # Save
    out = PROJECT / "results" / "regressions" / "ccgt_extensive_margin_exit.csv"
    df.to_csv(out, index=False)
    print(f"\nwrote {out}")


if __name__ == "__main__":
    main()
