"""Parse ENTSO-E File Library 'Balancing Energy Bids' (12.3.B&C) for Spain.

Source files: data/raw/entsoe/fms_balancing_energy_bids/*.csv
  Daily files, all-Europe, ~25 MB each. ~46k Spanish bids per day.

Per EU GL EB Article 12.3.B and the data-view spec in entsoe v3r4 page 83-85,
each row is one balancing-energy bid (anonymised — no Balancing Service
Provider identity exposed). Columns:
  - BidID, DeliveryPeriodStart/End, ValidityPeriodStart/End, ISP
  - AreaCode, MapCode (ES)
  - ReserveType (mFRR / aFRR / RR), TypeOfProduct (Standard mFRR SA / SA,DA / etc.)
  - Direction (Up / Down)
  - Volume[MW], Price[Currency/MWh], Currency
  - Status (Available / Unavailable / Activated)
  - Complexity (linked / exclusive / multipart, or NaN)
  - ReasonForUnavailability, ActivationPurpose

Output: data/processed/entsoe/balancing/individual_bids_es_all.parquet
  Filtered to MapCode=='ES'.
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[4]


def main() -> None:
    raw = PROJECT_ROOT / "data/raw/entsoe/fms_balancing_energy_bids"
    out = PROJECT_ROOT / "data/processed/entsoe/balancing/individual_bids_es_all.parquet"
    out.parent.mkdir(parents=True, exist_ok=True)

    files = sorted(raw.glob("*.csv"))
    if not files:
        print(f"no files in {raw}")
        return

    keep_cols = [
        "BidID", "DeliveryPeriodStart(UTC)", "DeliveryPeriodEnd(UTC)",
        "ValidityPeriodStart(UTC)", "ValidityPeriodEnd(UTC)", "ISP(UTC)",
        "ResolutionCode", "MapCode", "ReserveType", "TypeOfProduct",
        "Direction", "Complexity", "Status",
        "Volume[MW]", "Price[Currency/MWh]", "Currency",
        "ReasonForUnavailability", "ActivationPurpose",
    ]

    dfs = []
    for f in files:
        try:
            df = pd.read_csv(f, sep="\t", low_memory=False)
        except Exception as e:
            print(f"  FAIL {f.name}: {type(e).__name__}")
            continue
        # Filter to ES at read-time to keep memory low
        if "MapCode" in df.columns:
            df = df[df["MapCode"].astype(str) == "ES"]
        cols = [c for c in keep_cols if c in df.columns]
        df = df[cols]
        dfs.append(df)

    if not dfs:
        print("no ES rows found")
        return

    big = pd.concat(dfs, ignore_index=True)
    # Parse timestamps
    for col in ["DeliveryPeriodStart(UTC)", "DeliveryPeriodEnd(UTC)",
                "ValidityPeriodStart(UTC)", "ValidityPeriodEnd(UTC)", "ISP(UTC)"]:
        if col in big.columns:
            big[col] = pd.to_datetime(big[col], errors="coerce")

    # Rename for python-friendly column names
    big = big.rename(columns={
        "DeliveryPeriodStart(UTC)": "delivery_start",
        "DeliveryPeriodEnd(UTC)": "delivery_end",
        "ValidityPeriodStart(UTC)": "validity_start",
        "ValidityPeriodEnd(UTC)": "validity_end",
        "ISP(UTC)": "isp_start",
        "Volume[MW]": "volume_mw",
        "Price[Currency/MWh]": "price_eur_mwh",
    })
    big.to_parquet(out, index=False)
    print(f"{len(big):,} ES rows from {len(dfs)} daily files")
    print(f"date range: {big['isp_start'].min()} → {big['isp_start'].max()}")
    print(f"reserve types: {big['ReserveType'].value_counts().to_dict()}")
    print(f"products: {big['TypeOfProduct'].value_counts().head(8).to_dict()}")
    print(f"status: {big['Status'].value_counts().to_dict()}")
    print(f"direction: {big['Direction'].value_counts().to_dict()}")
    print(f"wrote {out}")


if __name__ == "__main__":
    main()
