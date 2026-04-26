"""Parse REE_BalancingEnerBids per-ISP CSVs into long format.

Schema (id=181 archive, 2022-05-24 → 2024-12-10):

Each daily ZIP contains 96 CSVs (one per ISP), named:
    REE_BalancingEnerBids_YYYYMMDDHHII.csv
where HH=hour (01-24), II=quarter-within-hour (01-04).

CSV format (semicolon-separated, decimal-comma):
    line 1:   ;DD/MM/YYYY HH:MM:SS                 (header label)
    line 2:   ID;NAME;DATETIME_START;DATETIME_END;MW;EURO/MWH;
    line 3+:  bid tranches

ID is the BID-TYPE identifier (679 = Oferta Regulación Terciaria
Subir [mFRR up]; 678 = Oferta Regulación Terciaria Bajar [mFRR
down]). Each row is one tranche of the SYSTEM-AGGREGATE offer
curve for that ISP × direction. **No per-unit/per-firm identifier**
is included in this archive.

Output schema (long format):
    date              ISO date (from filename)
    period_start_utc  UTC timestamp of ISP start (DATETIME_START)
    period_end_utc    UTC timestamp of ISP end
    hour              1..24 (Spain CET local hour, from filename suffix)
    isp_in_hour       1..4 (quarter within hour)
    bid_type_id       int (679 / 678 / etc)
    bid_type_name     str ("Oferta Regulación Terciaria Subir" etc)
    mw                float (offer quantity, MW)
    price_eur_mwh     float (offer price, EUR/MWh)
    source_file       inner CSV filename
"""
from __future__ import annotations

import re
from pathlib import Path

import pandas as pd


_FILENAME_RE = re.compile(r"REE_BalancingEnerBids_(\d{8})(\d{2})(\d{2})\.csv$")


def parse_balancing_bids_csv(path: Path) -> pd.DataFrame:
    """Parse one ISP CSV. Returns long-format DataFrame."""
    m = _FILENAME_RE.search(path.name)
    if m is None:
        return pd.DataFrame()
    yyyymmdd, hh, ii = m.group(1), m.group(2), m.group(3)
    date = pd.to_datetime(yyyymmdd, format="%Y%m%d").date()

    # Skip the first label row, use row 2 as header.
    try:
        df = pd.read_csv(
            path,
            sep=";",
            skiprows=1,
            decimal=",",
            engine="python",
            on_bad_lines="skip",
        )
    except Exception:
        return pd.DataFrame()

    # Drop unnamed trailing column from the trailing-semicolon delimiter.
    df = df.loc[:, ~df.columns.astype(str).str.startswith("Unnamed")]
    df.columns = [c.strip() for c in df.columns]

    expected = {"ID", "NAME", "DATETIME_START", "DATETIME_END", "MW", "EURO/MWH"}
    if not expected.issubset(df.columns):
        return pd.DataFrame()

    out = pd.DataFrame({
        "date": date,
        "period_start_utc": pd.to_datetime(df["DATETIME_START"], errors="coerce", utc=True),
        "period_end_utc": pd.to_datetime(df["DATETIME_END"], errors="coerce", utc=True),
        "hour": int(hh),
        "isp_in_hour": int(ii),
        "bid_type_id": pd.to_numeric(df["ID"], errors="coerce").astype("Int64"),
        "bid_type_name": df["NAME"].astype(str).str.strip(),
        "mw": pd.to_numeric(df["MW"], errors="coerce"),
        "price_eur_mwh": pd.to_numeric(df["EURO/MWH"], errors="coerce"),
        "source_file": path.name,
    })
    return out


def parse_balancing_bids_dir(extracted_dir: Path) -> pd.DataFrame:
    """Parse all CSVs in an extracted/ day directory and concatenate."""
    parts = []
    for p in sorted(extracted_dir.glob("REE_BalancingEnerBids_*.csv")):
        try:
            sub = parse_balancing_bids_csv(p)
            if not sub.empty:
                parts.append(sub)
        except Exception as e:
            print(f"[WARN parse] {p.name}: {e}")
    if not parts:
        return pd.DataFrame()
    return pd.concat(parts, ignore_index=True)
