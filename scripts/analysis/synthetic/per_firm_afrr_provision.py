# STATUS: ALIVE
# LAST-AUDIT: 2026-04-27
# FEEDS: F9 candidate (per-firm aFRR provision concentration)
# CLAIM: IB dominance in DA market (F7) extends to aFRR provision — cross-market robustness check
"""Per-BSP × per-firm decomposition of aFRR provision (F9 candidate).

Tests whether the IB-canonical thesis (F7: IB carries ~98% of joint
Big-4 DA cleared-price-difference transfer) extends to a market that
is NOT directly affected by the MTU15-IDA / MTU15-DA reform: the
secondary-regulation (aFRR) market.

ESIOS `liquicierre` (id=17, 2015 → 2024-12-03) and `liquicierresrs`
(id=203, 2024-11-22 → present) provide per-BSP aFRR settlement detail
publicly. ~23 BSPs in total; we map to the OMIE Big-4 firm taxonomy
under two assumptions:

LIBERAL mapping (best-guess, magnitude-suggestive):
    IB ← {IMA, IGR, IGN}
    GE ← {END}
    GN ← {GN}
    HC ← {HC}
    EV ← {EV, EVM}
    Fringe ← all others

CONSERVATIVE mapping (high-confidence only):
    IB ← {IGN}                  (IGN ↔ OMIE IGNU = IBERDROLA GENERACION NUCLEAR)
    GE ← {END}                  (END ↔ OMIE ENDG = ENDESA GENERACIÓN)
    GN ← {GN}                   (exact OMIE match)
    HC ← {HC}                   (exact OMIE match)
    EV ← {EV}                   (exact OMIE match)
    Fringe ← all others incl. IMA, IGR

Both readings are reported. The thesis can present whichever is
defensible; under either, the per-firm decomposition is meaningful.

Outcomes computed (per regime: pre-IDA / 3-sess / ISP15-win / DA60-ID15
/ DA15-ID15):

  share_RMRSP_pct  — % of total system aFRR-up provision (RMRSP)
  share_RMRSN_pct  — % of total system aFRR-down provision (|RMRSN|)
  share_COEFPAR_pct — % of total participation coefficient
  share_combined   — geometric mean of the three shares

Output:
    data/derived/results/per_firm_afrr_provision.csv

For each (firm, regime, mapping) combination, one row with the share
metrics. The thesis-relevant headline is the IB share under both
mappings, contrasted with GE / GN / HC.
"""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

PROJECT = Path(__file__).resolve().parents[3]
LIQUI_ALL = PROJECT / "data" / "processed" / "esios" / "reservas" / "liquicierre_all.parquet"
OUT = PROJECT / "data" / "derived" / "results" / "per_firm_afrr_provision.csv"

IDA_REFORM   = pd.Timestamp("2024-06-14")
ISP15_REFORM = pd.Timestamp("2024-12-01")
MTU15_IDA    = pd.Timestamp("2025-03-19")
MTU15_DA     = pd.Timestamp("2025-10-01")

MAPPINGS = {
    "liberal": {
        "IB": {"IMA", "IGR", "IGN"},
        "GE": {"END"},
        "GN": {"GN"},
        "HC": {"HC"},
        "EV": {"EV", "EVM"},
    },
    "conservative": {
        "IB": {"IGN"},
        "GE": {"END"},
        "GN": {"GN"},
        "HC": {"HC"},
        "EV": {"EV"},
    },
}


def assign_regime(d: pd.Timestamp) -> str:
    d = pd.Timestamp(d)
    if d < IDA_REFORM:
        return "pre-IDA"
    if d < ISP15_REFORM:
        return "3-sess"
    if d < MTU15_IDA:
        return "ISP15 win"
    if d < MTU15_DA:
        return "DA60/ID15"
    return "DA15/ID15"


def assign_firm(bsp: str, mapping: dict[str, set[str]]) -> str:
    for firm, codes in mapping.items():
        if bsp in codes:
            return firm
    return "Fringe"


def main() -> None:
    if not LIQUI_ALL.exists():
        print(f"Missing {LIQUI_ALL}. Run 20_build_liquicierre_all.py first.")
        return

    print(f"[load] {LIQUI_ALL}")
    df = pd.read_parquet(LIQUI_ALL)
    df["date"] = pd.to_datetime(df["date"])
    df["regime"] = df["date"].apply(assign_regime)

    # We focus on three Info codes that capture aFRR provision and
    # exclude system-only blocks (REE / RESNUP / RESNDW etc.).
    PROVISION_INFOS = ["RMRSP", "RMRSN", "COEFPAR"]
    df = df[df["info"].isin(PROVISION_INFOS)].copy()
    df["abs_ctd"] = df["ctd"].abs()

    rows = []
    for mapping_name, mapping in MAPPINGS.items():
        firms_universe = list(mapping.keys()) + ["Fringe"]
        df["firm"] = df["bsp"].apply(lambda b: assign_firm(b, mapping))

        # System totals per (regime, info)
        sys_tot = (
            df.groupby(["regime", "info"], as_index=False)["abs_ctd"]
            .sum()
            .rename(columns={"abs_ctd": "system_total"})
        )

        # Firm sums per (regime, firm, info)
        firm_sum = (
            df.groupby(["regime", "firm", "info"], as_index=False)["abs_ctd"]
            .sum()
            .rename(columns={"abs_ctd": "firm_total"})
        )

        m = firm_sum.merge(sys_tot, on=["regime", "info"], how="left")
        m["share_pct"] = m["firm_total"] / m["system_total"] * 100

        # Pivot: one row per (regime, firm); cols are share for each info
        wide = m.pivot_table(
            index=["regime", "firm"],
            columns="info",
            values="share_pct",
            aggfunc="first",
        ).reset_index()
        wide.columns.name = None
        wide = wide.rename(
            columns={
                "RMRSP":  "share_RMRSP_pct",
                "RMRSN":  "share_RMRSN_pct",
                "COEFPAR": "share_COEFPAR_pct",
            }
        )
        # Combined (geometric mean) — ignore zeros gracefully
        for c in ("share_RMRSP_pct", "share_RMRSN_pct", "share_COEFPAR_pct"):
            if c not in wide.columns:
                wide[c] = np.nan
        wide["share_combined_pct"] = (
            wide["share_RMRSP_pct"].fillna(0)
            * wide["share_RMRSN_pct"].fillna(0)
            * wide["share_COEFPAR_pct"].fillna(0)
        ).pow(1 / 3)
        wide["mapping"] = mapping_name
        rows.append(wide)

    out = pd.concat(rows, ignore_index=True)
    out = out[
        [
            "mapping",
            "regime",
            "firm",
            "share_RMRSP_pct",
            "share_RMRSN_pct",
            "share_COEFPAR_pct",
            "share_combined_pct",
        ]
    ]

    OUT.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUT, index=False)

    # Print summary tables — Big-4 + Fringe per regime per mapping
    REGIME_ORDER = ["pre-IDA", "3-sess", "ISP15 win", "DA60/ID15", "DA15/ID15"]
    FIRM_ORDER = ["IB", "GE", "GN", "HC", "EV", "Fringe"]
    for mapping_name in MAPPINGS:
        sub = out[out["mapping"] == mapping_name]
        print(f"\n=== {mapping_name.upper()} mapping ===")
        for regime in REGIME_ORDER:
            r = sub[sub["regime"] == regime]
            if r.empty:
                continue
            r = r.set_index("firm").reindex(FIRM_ORDER).reset_index()
            print(f"\n  Regime: {regime}")
            print(
                "    "
                + r.to_string(
                    index=False,
                    columns=[
                        "firm",
                        "share_RMRSP_pct",
                        "share_RMRSN_pct",
                        "share_COEFPAR_pct",
                        "share_combined_pct",
                    ],
                    float_format=lambda x: f"{x:6.2f}" if pd.notna(x) else "  -   ",
                ).replace("\n", "\n    ")
            )

    print(f"\nWrote {OUT}")


if __name__ == "__main__":
    main()
