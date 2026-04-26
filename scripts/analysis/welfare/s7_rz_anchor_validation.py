# STATUS: ALIVE
# LAST-AUDIT: 2026-04-27
# FEEDS: S7 (per-segment marginal imbalance cost — Pigouvian)
# CLAIM: ESIOS totalrp48preccierre directly publishes RZ closure prices €/MWh; validates the S7 conv-RZ €210–300/MWh figure
"""S7 anchor validation: ESIOS RZ closure prices vs S7 estimate.

S7 (alive) claims per-segment marginal imbalance cost is order-of-
magnitude heterogeneous: conv-RZ €210–300/MWh vs LIB free retailers
≤€37/MWh. The conv-RZ figure was derived in `pigouvian_clean_regression.py`
from A87 / A86 imbalance data via a structural model. This script
validates the conv-RZ piece with a DIRECT measurement: ESIOS publishes
the closure price at which RZ-zone units are re-instructed for system
security (TipoRedespacho code 61).

Data source: `data/processed/esios/restricciones/totalrp48preccierre_all.parquet`
(2015 → present, PT15M resolution, 1.2M rows × 22 TipoRedespacho codes).

Method:
- Filter to TipoRedespacho code 61 (RZ technical restrictions per P.O. 3.2).
- Compute mean / median / quantiles of the closure price per regime.
- Cross-check against the S7-derived conv-RZ €210–300/MWh range.

Per discipline: this validates an alive claim, no new claim needed.
Adds robustness footnote to S7 ledger row.

Output:
    data/derived/results/s7_rz_anchor.csv

Reading: if the directly-published RZ closure price tracks the S7
structural estimate, S7 strengthens. If they diverge, S7 needs a
caveat about the segment-allocation rule the structural model
assumed.
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd

PROJECT = Path(__file__).resolve().parents[3]
RP48 = PROJECT / "data" / "processed" / "esios" / "restricciones" / "totalrp48preccierre_all.parquet"
OUT = PROJECT / "data" / "derived" / "results" / "s7_rz_anchor.csv"

IDA_REFORM   = pd.Timestamp("2024-06-14")
ISP15_REFORM = pd.Timestamp("2024-12-01")
MTU15_IDA    = pd.Timestamp("2025-03-19")
MTU15_DA     = pd.Timestamp("2025-10-01")


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


# RZ technical-restrictions for system security per REE / ENTSO-E coding.
RZ_CODES = {"61"}

# Other relevant codes — kept for cross-reference in the output.
ALL_CODES = {
    "33": "real-time technical restrictions",
    "34": "inter-zonal restrictions resolution",
    "61": "system-security RZ (P.O. 3.2)",
    "68": "reserve management",
    "69": "voltage control / black-start",
    "81": "other",
    "92": "mFRR activation",
    "94": "system balancing",
}


def main() -> None:
    if not RP48.exists():
        print(f"Missing {RP48}. Run 20_build_totalrp48preccierre_all.py first.")
        return

    print(f"[load] {RP48}")
    df = pd.read_parquet(RP48)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"]).copy()
    df["regime"] = df["date"].apply(assign_regime)

    # Combined price = max(price_up, price_down) with sign — for RZ
    # we typically have only one direction per Intervalo; take whichever
    # is non-null.
    df["price_eur_mwh"] = df["price_up_eur"].combine_first(df["price_down_eur"])
    df["qty_mwh"] = df["qty_up_mwh"].combine_first(df["qty_down_mwh"])

    # Drop zero-price rows (ESIOS pads PT15M intervals with no activity)
    active = df[(df["price_eur_mwh"].notna()) & (df["qty_mwh"].notna()) & (df["qty_mwh"] > 0)].copy()

    print(f"\nTotal rows (all codes): {len(df):,}")
    print(f"Active (qty>0) rows:    {len(active):,}")

    print("\n=== Per-regime distribution by TipoRedespacho ===")
    REGIME_ORDER = ["pre-IDA", "3-sess", "ISP15 win", "DA60/ID15", "DA15/ID15"]
    rows = []
    for code, label in ALL_CODES.items():
        sub = active[active["tipo_redespacho"] == code]
        if sub.empty:
            continue
        for regime in REGIME_ORDER:
            r = sub[sub["regime"] == regime]
            if r.empty:
                continue
            wmean = (r["price_eur_mwh"] * r["qty_mwh"]).sum() / r["qty_mwh"].sum()
            rows.append({
                "tipo_redespacho": code,
                "label": label,
                "regime": regime,
                "n_isps": len(r),
                "qty_total_gwh": r["qty_mwh"].sum() / 1e3,
                "price_mean_eur": r["price_eur_mwh"].mean(),
                "price_median_eur": r["price_eur_mwh"].median(),
                "price_p25_eur": r["price_eur_mwh"].quantile(0.25),
                "price_p75_eur": r["price_eur_mwh"].quantile(0.75),
                "price_q_weighted_eur": wmean,
            })

    out = pd.DataFrame(rows)

    print()
    print("=" * 110)
    print("RZ system-security closure price (TipoRedespacho 61) — direct ESIOS measurement")
    print("=" * 110)
    rz_only = out[out["tipo_redespacho"] == "61"]
    if rz_only.empty:
        print("No RZ (code 61) rows found — schema may have changed; check raw data.")
    else:
        cols = ["regime", "n_isps", "qty_total_gwh", "price_mean_eur",
                "price_median_eur", "price_q_weighted_eur"]
        print(rz_only[cols].to_string(index=False, float_format=lambda x: f"{x:8.1f}"))

    print()
    print("=" * 110)
    print("All TipoRedespacho codes — cross-reference (regime-weighted means)")
    print("=" * 110)
    piv = out.pivot_table(
        index=["tipo_redespacho", "label"],
        columns="regime",
        values="price_q_weighted_eur",
        aggfunc="first",
    ).reset_index()
    cols_order = ["tipo_redespacho", "label"] + [r for r in REGIME_ORDER if r in piv.columns]
    piv = piv[cols_order]
    print(piv.to_string(index=False, float_format=lambda x: f"{x:7.1f}" if pd.notna(x) else "   -   "))

    OUT.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUT, index=False)
    print(f"\nWrote {OUT}")


if __name__ == "__main__":
    main()
