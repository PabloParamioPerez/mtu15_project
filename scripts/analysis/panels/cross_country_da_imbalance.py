# STATUS: ALIVE
# LAST-AUDIT: 2026-04-27
# FEEDS: F11 (cross-border) and B6 (imbalance regime structure)
# CLAIM: Cross-country DA + imbalance price comparison ES vs DE/PT/FR by reform window
"""Cross-country DA + imbalance price comparison.

DA prices: ES (OMIE marginalpdbc, ES side), FR / DE / PT (ENTSO-E A44).
Imbalance prices (A04 up direction): ES (A85 controlArea_Domain=ES) and
FR (A85 controlArea_Domain=FR).

Outputs:
  - Monthly mean DA price by country (time-weighted)
  - Reform-window means + ES gap to each neighbour
  - Reform-window means of A04 imbalance prices ES vs FR

Findings (run 2026-04-27):
  DA price:
    - ES is BELOW DE by €30 in pre-IDA, gap WIDENS to €40-45 in DA60/DA15
      (German prices stay high; Spanish prices fall with renewable expansion)
    - ES vs PT is essentially zero throughout (MIBEL price coupling tight)
    - ES vs FR is volatile across regimes (gas + nuclear France-specific shocks)
  Imbalance price (A04 up direction):
    - ES is BELOW FR by €20-30 in most regimes
    - Single regime where the gap CLOSES: DA60/ID15 POST-blackout (gap -1)
    - Operación reforzada raised Spanish balancing costs to French parity;
      post-MTU15-DA the structural ES<FR gap re-opens

Output: data/derived/results/cross_country_prices.csv
"""
from __future__ import annotations

from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

PROJECT = Path(__file__).resolve().parents[3]

WINDOWS = [
    ("pre-IDA",                  "2024-01", "2024-05"),
    ("3-sess",                   "2024-06", "2024-11"),
    ("ISP15-win",                "2024-12", "2025-02"),
    ("DA60/ID15 PRE-blackout",   "2025-03", "2025-04"),
    ("DA60/ID15 POST-blackout",  "2025-05", "2025-09"),
    ("DA15/ID15",                "2025-10", "2026-04"),
]


def wmean(df: pd.DataFrame, val_col: str) -> pd.Series:
    """Time-weighted monthly mean (weights = mtu_minutes per row)."""
    return df.groupby("month").apply(
        lambda g: np.average(g[val_col], weights=g["weight"]) if g["weight"].sum() > 0 else np.nan,
        include_groups=False,
    )


def main() -> None:
    proc = PROJECT / "data/processed/entsoe"

    # DA prices: DE, PT, FR (A44)
    paths = {"DE": proc / "prices/de_da_all.parquet",
             "PT": proc / "prices/pt_da_all.parquet",
             "FR": proc / "prices/fr_da_all.parquet"}
    dfs = {}
    for k, p in paths.items():
        df = pd.read_parquet(p)
        df["ts"] = pd.to_datetime(df["isp_start_utc"])
        df["month"] = df["ts"].dt.to_period("M")
        df["weight"] = df["mtu_minutes"]
        dfs[k] = df

    # ES via OMIE marginal_pdbc
    con = duckdb.connect()
    res = con.execute(f"""
        SELECT date, period, price_es_eur_mwh, mtu_minutes
        FROM '{PROJECT}/data/processed/omie/mercado_diario/precios/marginalpdbc_all.parquet'
    """).df()
    res["ts"] = pd.to_datetime(res["date"]) + pd.to_timedelta(res["period"].astype(int) - 1, unit="h")
    res["month"] = res["ts"].dt.to_period("M")
    res["weight"] = res["mtu_minutes"]

    monthly = pd.DataFrame({
        "ES": wmean(res, "price_es_eur_mwh"),
        "FR": wmean(dfs["FR"], "price_eur_per_mwh"),
        "DE": wmean(dfs["DE"], "price_eur_per_mwh"),
        "PT": wmean(dfs["PT"], "price_eur_per_mwh"),
    }).round(1)

    print("Reform-window time-weighted DA price means (€/MWh):")
    print(f"{'Window':30} {'ES':>7} {'FR':>7} {'DE':>7} {'PT':>7}  | {'ES-FR':>7} {'ES-DE':>7} {'ES-PT':>7}")
    rows = []
    for name, lo, hi in WINDOWS:
        lo_p, hi_p = pd.Period(lo, "M"), pd.Period(hi, "M")
        sub = monthly[(monthly.index >= lo_p) & (monthly.index <= hi_p)].mean()
        es, fr, de, pt = sub["ES"], sub["FR"], sub["DE"], sub["PT"]
        print(f"{name:30} {es:>7.1f} {fr:>7.1f} {de:>7.1f} {pt:>7.1f}  | {es - fr:>7.1f} {es - de:>7.1f} {es - pt:>7.1f}")
        rows.append({"window": name, "ES_da": es, "FR_da": fr, "DE_da": de, "PT_da": pt,
                     "ES_FR_gap": es - fr, "ES_DE_gap": es - de, "ES_PT_gap": es - pt})

    # Imbalance prices ES vs FR (A85, A04 = up direction)
    es_imb = pd.read_parquet(proc / "balancing/imbalance_prices_all.parquet")
    es_imb_a04 = es_imb[es_imb["imbalance_flag"] == "A04"].copy()
    es_imb_a04["ts"] = pd.to_datetime(es_imb_a04["isp_start_utc"])
    es_a04 = es_imb_a04.set_index("ts")["price_eur_per_mwh"].dropna().resample("ME").mean()

    fr_imb = pd.read_parquet(proc / "balancing/imbalance_prices_fr_all.parquet")
    fr_imb_a04 = fr_imb[fr_imb["flag"] == "A04"].copy()
    fr_imb_a04["ts"] = pd.to_datetime(fr_imb_a04["isp_start_utc"])
    fr_a04 = fr_imb_a04.set_index("ts")["price_eur_mwh"].dropna().resample("ME").mean()

    imb_m = pd.DataFrame({"ES_imb": es_a04.round(0), "FR_imb": fr_a04.round(0)})
    imb_m["gap_ES_FR"] = (imb_m["ES_imb"] - imb_m["FR_imb"]).round(0)

    print()
    print("Reform-window mean imbalance prices (A04 up direction, €/MWh):")

    def regime_of(ts: pd.Timestamp) -> str:
        if ts < pd.Timestamp("2024-06-14"): return "pre-IDA"
        if ts < pd.Timestamp("2024-12-01"): return "3-sess"
        if ts < pd.Timestamp("2025-03-19"): return "ISP15-win"
        if ts < pd.Timestamp("2025-04-28"): return "DA60/ID15 PRE-blackout"
        if ts < pd.Timestamp("2025-10-01"): return "DA60/ID15 POST-blackout"
        return "DA15/ID15"

    imb_m["reg"] = imb_m.index.to_series().apply(regime_of)
    imb_summary = imb_m.groupby("reg")[["ES_imb", "FR_imb", "gap_ES_FR"]].mean().round(0)
    print(imb_summary.to_string())

    out_dir = PROJECT / "data/derived/results"
    out_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(out_dir / "cross_country_da_prices.csv", index=False)
    imb_summary.to_csv(out_dir / "cross_country_imbalance_prices.csv")
    print(f"\nwrote {out_dir / 'cross_country_da_prices.csv'}")
    print(f"wrote {out_dir / 'cross_country_imbalance_prices.csv'}")


if __name__ == "__main__":
    main()
