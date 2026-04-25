"""Parse ESIOS A2_liquicomun monthly settlement archive inner files.

A2_liquicomun is a monthly ZIP archive containing 500+ inner files,
one per settlement concept. The format is a fixed set of CSV-like
text files described in `A2__modelcom_*.pdf` data dictionary.

This parser handles the **thesis-relevant** subset of inner files
(see explore/_modelable_patterns.md and ref_post_blackout_regulation.md):

| Inner file (prefix) | Schema | Unit | Description |
|---|---|---|---|
| `A2_impdsvqh_` | date;hour;quarter;eur | EUR/ISP | Total system imbalance amount per ISP |
| `A2_cdvbrp_` | date;hour;quarter;eur_mwh | EUR/MWh | Avg deviation cost per MWh across all BRPs |
| `A2_cdsvbrp_` | date;hour;eur_mwh | EUR/MWh | Hourly avg deviation cost across all BRPs |
| `A2_codsvbaqh_` | date;hour;quarter;eur_mwh | EUR/MWh | Cost of down-deviation per ISP |
| `A2_codsvsuqh_` | date;hour;quarter;eur_mwh | EUR/MWh | Cost of up-deviation per ISP |
| `A2_cosdsvqh_` | date;hour;quarter;eur_mwh | EUR/MWh | Cost of contrary deviations |
| `A2_endsvqh_` | date;hour;quarter;mwh | MWh | Net imbalance volume per ISP |
| `A2_endvBRPqh_` | date;hour;quarter;mwh | MWh | Energy of deviations for cost allocation |
| `A2_endrozrqh_` | date;hour;quarter;mwh | MWh | Deviation MWh in regulation zones (big plants) |
| `A2_endronzqh_` | date;hour;quarter;mwh | MWh | Deviation MWh outside regulation zones |
| `A2_endreeoqh_` | date;hour;quarter;mwh | MWh | Deviation MWh of RE wind |
| `A2_endrehiqh_` | date;hour;quarter;mwh | MWh | Deviation MWh of RE hydro |
| `A2_endretqh_` | date;hour;quarter;mwh | MWh | Deviation MWh of RE thermal |
| `A2_endcurqh_` | date;hour;quarter;mwh | MWh | Deviation MWh of CUR retailers |
| `A2_endlibqh_` | date;hour;quarter;mwh | MWh | Deviation MWh of free-market retailers |
| `A2_endexpqh_` | date;hour;quarter;mwh | MWh | Deviation MWh of export units |
| `A2_endimpqh_` | date;hour;quarter;mwh | MWh | Deviation MWh of import units |

Inner-file format (per the spec): semicolon-separated values, with a
2-line header (file family name + emission timestamp) followed by data
rows. Numeric values use a `.` decimal separator.

Output schema (one DataFrame per inner file):
    date              first-of-period as date
    hour              1..24 (Spain CET local hour)
    quarter           1..4 (15-min position within hour) — NULL for hourly files
    value             numeric value (EUR or MWh per the file family)
    family            inner-file prefix (e.g. 'impdsvqh', 'endrozrqh')
    source_file       the inner filename (snapshot identity)

Multiple inner files are concatenated into a single long-format
DataFrame with `family` as the discriminator column.
"""

from __future__ import annotations

import re
from pathlib import Path

import pandas as pd

# Inner-file prefixes we currently parse. Extend as needs expand.
PARSED_FAMILIES_QH = (
    "impdsvqh",
    "cdvbrp",
    "codsvbaqh",
    "codsvsuqh",
    "cosdsvqh",
    "endsvqh",
    "endvBRPqh",
    "endrozrqh",
    "endronzqh",
    "endreeoqh",
    "endrehiqh",
    "endretqh",
    "endcurqh",
    "endlibqh",
    "endexpqh",
    "endimpqh",
)

# Hourly-resolution families (date;hour;value, no quarter column).
PARSED_FAMILIES_H = (
    "cdsvbrp",
)


def _identify_family(filename: str) -> str | None:
    """Return the recognised family prefix for an A2_liquicomun inner
    filename, or None if not in our parsed set."""
    name = Path(filename).name
    # Filenames look like 'A2_impdsvqh_20260401_20260430' (no extension)
    m = re.match(r"^A2_([A-Za-z0-9]+)_\d{8}_\d{8}$", name)
    if not m:
        return None
    fam = m.group(1)
    if fam in PARSED_FAMILIES_QH or fam in PARSED_FAMILIES_H:
        return fam
    return None


def _parse_qh_file(path: Path, family: str) -> pd.DataFrame:
    """Parse a quarter-hourly inner file.

    Format:
        line 1: family name + ;
        line 2: emission timestamp 'YYYY;MM;DD;HH;MM;SS;'
        line 3+: 'DD/MM/YYYY;hour;quarter;value;'

    `value` is the float in EUR or MWh depending on family.
    """
    rows: list[dict] = []
    with path.open("r", encoding="latin1") as f:
        for i, line in enumerate(f):
            if i < 2:
                continue  # skip header lines
            parts = line.strip().rstrip(";").split(";")
            if len(parts) < 4:
                continue
            try:
                d = pd.to_datetime(parts[0], format="%d/%m/%Y").date()
                h = int(parts[1])
                q = int(parts[2])
                v = float(parts[3])
            except (ValueError, TypeError):
                continue
            rows.append({
                "date": d,
                "hour": h,
                "quarter": q,
                "value": v,
                "family": family,
                "source_file": path.name,
            })
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"])
    return df


def _parse_h_file(path: Path, family: str) -> pd.DataFrame:
    """Parse an hourly inner file (no quarter column)."""
    rows: list[dict] = []
    with path.open("r", encoding="latin1") as f:
        for i, line in enumerate(f):
            if i < 2:
                continue
            parts = line.strip().rstrip(";").split(";")
            if len(parts) < 3:
                continue
            try:
                d = pd.to_datetime(parts[0], format="%d/%m/%Y").date()
                h = int(parts[1])
                v = float(parts[2])
            except (ValueError, TypeError):
                continue
            rows.append({
                "date": d,
                "hour": h,
                "quarter": pd.NA,
                "value": v,
                "family": family,
                "source_file": path.name,
            })
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"])
    return df


def parse_inner_file(path: Path) -> pd.DataFrame:
    """Parse a single A2_liquicomun inner file. Returns empty DataFrame
    if the file is not in our recognised parse set."""
    family = _identify_family(path.name)
    if family is None:
        return pd.DataFrame()
    if family in PARSED_FAMILIES_QH:
        return _parse_qh_file(path, family)
    if family in PARSED_FAMILIES_H:
        return _parse_h_file(path, family)
    return pd.DataFrame()


def parse_extracted_dir(extracted_dir: Path) -> pd.DataFrame:
    """Parse all recognised inner files in an extracted A2_liquicomun
    directory. Returns a long-format DataFrame with `family` column.
    """
    frames: list[pd.DataFrame] = []
    for path in sorted(extracted_dir.iterdir()):
        if not path.is_file():
            continue
        df = parse_inner_file(path)
        if not df.empty:
            frames.append(df)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)
