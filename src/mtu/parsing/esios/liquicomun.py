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
    # Reserve-cost streams added 2026-04-25 to test the A87/ESIOS gap:
    "imresecqh",   # secondary-regulation reserve cost per ISP (EUR)
)

# Hourly-resolution families (one row per day, 24 hourly columns).
PARSED_FAMILIES_H = (
    "cdsvbrp",
    "imexdedv",   # excess/deficit of deviations per hour (EUR)
)


def _identify_family(filename: str) -> str | None:
    """Return the recognised family prefix for a liquicomun inner
    filename, or None if not in our parsed set.

    Filenames look like:
        A2_impdsvqh_20260401_20260430  (provisional settlement, archive 3)
        C2_impdsvqh_20240101_20240131  (definitive settlement,  archive 8)

    The two share the same inner-file format; only the leading vintage
    code differs.
    """
    name = Path(filename).name
    m = re.match(r"^(?:A2|C2)_([A-Za-z0-9]+)_\d{8}_\d{8}$", name)
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
    """Parse an hourly inner file in the WIDE day-x-24-hour format used
    by cdsvbrp, cdvbrp, etc.

    Format example (cdsvbrp):
        cdsvbrp;
        2024;02;09;12;10;17;
        L 01;7.13;26.24;...;28.46;;     ← Lunes 01, then 24 hourly values
        M 02;29.67;...;31.43;;          ← Martes 02
        ...

    The leading day code is `<day-of-week-letter> <day-of-month>` where
    the letter is L/M/X/J/V/S/D (Spanish weekday initials). Day-of-month
    is the 2-digit number. The month + year come from the filename
    (`A2/C2_<family>_YYYYMMDD_YYYYMMDD`) or the second header line.

    Some hourly files publish the data in long format (one row per
    (date, hour, value) triple). Detect the format from the first
    data line and parse accordingly.
    """
    # Pull year/month from filename: <prefix>_<family>_<startYYYYMMDD>_<endYYYYMMDD>
    fn_match = re.match(
        r"^(?:A2|C2)_[A-Za-z0-9]+_(\d{4})(\d{2})(\d{2})_\d{8}$",
        path.name,
    )
    if fn_match is None:
        return pd.DataFrame()
    yr_int = int(fn_match.group(1))
    mo_int = int(fn_match.group(2))

    rows: list[dict] = []
    with path.open("r", encoding="latin1") as f:
        lines = list(f)
    if len(lines) < 3:
        return pd.DataFrame()

    # Detect format from the first non-header line
    sample = lines[2].strip().rstrip(";")
    sample_parts = sample.split(";")

    # Long-format detection: first field is dd/mm/yyyy
    is_long = bool(re.match(r"^\d{2}/\d{2}/\d{4}$", sample_parts[0]))

    if is_long:
        # date;hour;value;
        for line in lines[2:]:
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
                "date": d, "hour": h, "quarter": pd.NA, "value": v,
                "family": family, "source_file": path.name,
            })
    else:
        # Wide format: '<L|M|X|J|V|S|D> DD;v1;v2;...;v24;'
        for line in lines[2:]:
            parts = line.strip().rstrip(";").split(";")
            if len(parts) < 25:
                continue
            day_code = parts[0].strip()
            # Day of month is the trailing integer in day_code
            dom_match = re.search(r"(\d{1,2})$", day_code)
            if not dom_match:
                continue
            dom = int(dom_match.group(1))
            try:
                d = pd.Timestamp(year=yr_int, month=mo_int, day=dom).date()
            except (ValueError, TypeError):
                continue
            for h_idx in range(24):
                raw = parts[1 + h_idx].strip()
                if raw == "":
                    continue
                try:
                    v = float(raw.replace(",", "."))
                except ValueError:
                    continue
                rows.append({
                    "date": d, "hour": h_idx + 1, "quarter": pd.NA, "value": v,
                    "family": family, "source_file": path.name,
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
