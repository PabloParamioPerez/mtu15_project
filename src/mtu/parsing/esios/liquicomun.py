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
#
# Schema applies to ALL families listed: each row carries
# `[date, hour, quarter, value, family, source_file]`. Quarter is
# populated for QH families and NULL for H families. Pure metadata,
# regulated-tariff, regional-island-system, and per-tarif-zone families
# need a different schema and are NOT parsed here (see EXCLUDED at end).
#
# The 162 added 2026-04-28 cover all peninsular-Spain, single-value-per-
# (date, hour, quarter)/family content extracted from A2/C2_liquicomun
# in `data/raw/esios/liquidaciones/*/extracted/`. After this extension
# the parser handles 181/234 extracted families.

PARSED_FAMILIES_QH = (
    # ── Original 17 (pre-2026-04-28) ───────────────────────────────────
    "impdsvqh",     # Total system imbalance amount per ISP (EUR)
    "cdvbrp",       # Avg deviation cost per MWh across BRPs (EUR/MWh)
    "codsvbaqh",    # Cost of down-deviation per ISP (EUR/MWh)
    "codsvsuqh",    # Cost of up-deviation per ISP (EUR/MWh)
    "cosdsvqh",     # Cost of contrary deviations per ISP (EUR/MWh)
    "endsvqh",      # Net imbalance volume per ISP (MWh)
    "endvBRPqh",    # Energy of deviations for cost allocation per ISP (MWh)
    "endrozrqh",    # Deviation MWh in regulation zones (big plants)
    "endronzqh",    # Deviation MWh outside regulation zones
    "endreeoqh",    # Deviation MWh of RE wind
    "endrehiqh",    # Deviation MWh of RE hydro
    "endretqh",     # Deviation MWh of RE thermal
    "endcurqh",     # Deviation MWh of CUR retailers
    "endlibqh",     # Deviation MWh of LIB free-market retailers
    "endexpqh",     # Deviation MWh of export units
    "endimpqh",     # Deviation MWh of import units
    "imresecqh",    # Secondary-regulation reserve cost per ISP (EUR)
    # ── Added 2026-04-28: reserves & balancing per-ISP families (auto-classified) ──
    "RRsalqh",          # RR (replacement reserve) salida per ISP
    "afrrbaj",          # aFRR down per ISP (post-ISP15)
    "afrrsub",          # aFRR up per ISP (post-ISP15)
    "ccbbrp",           # BRP-cost component per ISP
    "ecf2bpbf",         # PBF correction-down energy per ISP
    "ecrebpbf",         # PBF reserve-down energy per ISP
    "ecrespbf",         # PBF reserve-up energy per ISP
    "enINqhba",         # Inflow energy down per ISP
    "enINqhsu",         # Inflow energy up per ISP
    "enRRqhba",         # RR energy down per ISP
    "enRRqhsu",         # RR energy up per ISP
    "enacbaqh",         # ACB (actual deviations) energy per ISP
    "encom",            # Commercial energy per ISP
    "encor",            # Correction energy per ISP
    "endcodqh",         # Code-deviation energy per ISP
    "endcomqh",         # Common-deviation energy per ISP
    "endem",            # Demand energy per ISP
    "endfrpoqh",        # Power foreign-flows energy per ISP
    "endireqh",         # Direct deviation per ISP
    "enduadqh",         # UAD (additional dispatch unit) energy per ISP
    "endvlbqh",         # LIB-block deviation energy per ISP
    "enitbqhba",        # Intra-block down per ISP
    "enitbqhsu",        # Intra-block up per ISP
    "enlib",            # Free-market energy per ISP
    "enperdiqh",        # Loss energy per ISP
    "enpertpqh",        # Transmission loss energy per ISP
    "enrepscqh",        # Reservoir energy per ISP
    "enrttrba",         # RT trans down per ISP
    "enrttrsu",         # RT trans up per ISP
    "ensecqhba",        # Secondary down per ISP
    "ensecqhsu",        # Secondary up per ISP
    "enterqhba",        # Tertiary down per ISP
    "enterqhsu",        # Tertiary up per ISP
    "entod",            # Total dispatched energy per ISP
    "itafrrsu",         # aFRR up settlement per ISP
    "mfrrdirsu",        # mFRR direct up per ISP
    "mfrrprosu",        # mFRR programmed up per ISP
    "pcrespbf",         # PBF reserve cost per ISP
    "pcrespbfsi",       # PBF reserve cost (system) per ISP
    "pfcom",            # Commercial PF per ISP
    "pfcur",            # CUR PF per ISP
    "pfdem",            # Demand PF per ISP
    "pflib",            # LIB PF per ISP
    "phlqh",            # Half-hour PHL per ISP
    "pmdiario",         # Daily-market price per ISP
    "prRRqh",           # RR price per ISP
    "prcrtf2b",         # Correction down price per ISP
    "prdvbaqh",         # Deviation down price per ISP
    "prdvsuqh",         # Deviation up price per ISP
    "prrseqhba",        # Reserve down price per ISP
    "prrseqhsu",        # Reserve up price per ISP
    "prsecqhba",        # Secondary down price per ISP
    "prsecqhsu",        # Secondary up price per ISP
    "prterqhba",        # Tertiary down price per ISP
    "prterqhsu",        # Tertiary up price per ISP
    "resecqhba",        # Secondary regulation down volume per ISP
    "resecqhsu",        # Secondary regulation up volume per ISP
    "saldoeneqh",       # Net energy balance per ISP
)

# Hourly-resolution families (one row per day, 24 hourly columns).
PARSED_FAMILIES_H = (
    # ── Original 2 (pre-2026-04-28) ───────────────────────────────────
    "cdsvbrp",      # Hourly avg deviation cost across BRPs (EUR/MWh)
    "imexdedv",     # Excess/deficit of deviations per hour (EUR)
    # ── Added 2026-04-28: hourly wide-24 families (auto-classified) ──
    "Scdsvdem",         # Demand-deviation cost per day x hour
    "SphgenfCE",        # Coal-generation profile per day x hour
    "SphgenfFL",        # Fuel/oil-generation profile
    "SphgenfGC",        # Gas-cycle profile
    "SphgenfHI",        # Hydro profile
    "SphgenfLG",        # Light-gas (CCGT) profile
    "SphgenfML",        # ML profile
    "SphgenfPA",        # PA profile
    "SphgenfSB",        # SB profile
    "SphgenfTF",        # TF profile
    "Spmdmirp",         # MD/MIRP price per hour
    "SprecorOS",        # OS-correction price per hour
    "Sprecure",         # Regulation price per hour
    "Srecdvca",         # Deviation reception A per hour
    "Srecdvcb",         # Deviation reception B per hour
    "Srecdvcc",         # Deviation reception C per hour
    "bandabaj",         # Band down per hour
    "bandasub",         # Band up per hour
    "cbdmcocd",         # Demand-coverage cost per hour
    "ccbrpbs3",         # BRP-cost subsidy 3 per hour
    "ccbrprad3",        # BRP-cost rad 3 per hour
    "codsvbaj",         # Cost of down-deviation per hour
    "codsvsub",         # Cost of up-deviation per hour
    "costedsv",         # Imbalance cost per hour
    "dsvcontr",         # Imbalance contribution per hour
    "enP48dem",         # P48 demand per hour
    "enacbala",         # ACB band per hour
    "enajosde",         # Adjustment-down energy per hour
    "encomcon",         # Commercial-consumption energy per hour
    "encomcur",         # Commercial-CUR energy per hour
    "endesvlb",         # LIB imbalance volume per hour
    "endmcocur",        # CUR-cocur deviation per hour
    "endmcome",         # Common-deviation energy per hour
    "endmcons",         # Consumption-deviation per hour
    "endmexpo",         # Export-deviation per hour
    "endmreeo",         # Wind-deviation per hour
    "endmrehi",         # Hydro-deviation per hour
    "endmrete",         # Thermal-RE deviation per hour
    "endmronz",         # Out-RZ deviation per hour
    "endmrozr",         # In-RZ deviation per hour
    "endsv",            # Net deviation energy per hour
    "endvBRP",          # BRP deviation energy per hour
    "enecd",            # Code-energy per hour
    "eneddire",         # Direct-deviation energy per hour
    "enedesca",         # Discount energy per hour
    "eneincbb",         # Incentive band-down per hour
    "eneincbs",         # Incentive band-up per hour
    "enenocur",         # Non-CUR energy per hour
    "enerespbf",        # PBF reserve energy per hour
    "eneuadq",          # UAD energy per hour
    "enf2bpbf",         # PBF F2-down energy per hour
    "engsttot",         # Total settled-energy per hour
    "enrebpbf",         # PBF reserve-band per hour
    "enrepscf",         # SCF reserve per hour
    "imacbala",         # ACB amount per hour
    "imcfband",         # Band cost amount per hour
    "imctboa",          # Daily total amount
    "imctdcom",         # Commercial daily amount
    "imdemcad",         # Demand-charge amount per hour
    "imeddire",         # Direct deviation amount per hour
    "imexdedvcur",      # CUR excess/deficit per hour
    "imexdedvncur",     # Non-CUR excess/deficit per hour
    "imincbal",         # Incentive band amount per hour
    "impdcfp",          # PDCFP amount per hour
    "impdsv",           # Imbalance amount per hour
    "imrad",            # Rad amount per hour
    "imradcom",         # Commercial rad amount per hour
    "imradx",           # Rad-X amount per hour
    "imscrpbf",         # PBF SCR amount per hour
    "imscrpbfcur",      # PBF SCR CUR amount per hour
    "imscrpbfncur",     # PBF SCR non-CUR amount per hour
    "imscrtre",         # Tertiary SCR amount per hour
    "imscrtrecur",      # Tertiary SCR CUR amount per hour
    "imscrtrencur",     # Tertiary SCR non-CUR amount per hour
    "pesrad",           # Rad weighting per hour
    "porcbalx",         # Band-X percentage per hour
    "prcfbans",         # Band-CFB price per hour
    "prdemcad",         # Demand-charge price per hour
    "prdsvcos",         # Imbalance cost price per hour
    "prdvbamd",         # Deviation down avg price per hour
    "prdvpeba",         # Deviation peak down per hour
    "prdvpesu",         # Deviation peak up per hour
    "prdvsumd",         # Deviation up avg price per hour
    "precband",         # Band price per hour
    "precioRR",         # RR price per hour
    "presobre",         # Surcharge per hour
    "prexcdsv",         # Excess imbalance price per hour
    "prmcom",           # Commercial price per hour
    "prmdiari",         # Daily mean price per hour
    "prmecur",          # CUR mean price per hour
    "prmelch",          # Mean clearing price per hour
    "prmncur",          # Non-CUR mean price per hour
    "prmtod",           # Total mean price per hour
    "prresmi1",         # MI session 1 reserve price per hour
    "prresmi2",         # MI session 2 reserve price per hour
    "prresmi3",         # MI session 3 reserve price per hour
    "prresmi4",         # MI session 4 reserve price per hour
    "prresmi5",         # MI session 5 reserve price per hour
    "prresmi6",         # MI session 6 reserve price per hour
    "prresmi7",         # MI session 7 reserve price per hour
    "prrestpbfsi",      # PBF reserve restriction (system) price per hour
    "prrtf2bj",         # F2bj reserve transfer price per hour
    "prrtp48c",         # P48 reserve transfer price per hour
    "prrtpdbf",         # PDBF reserve transfer price per hour
)

# Multi-region/multi-segment-id formats not auto-parseable into the
# `[date, hour, quarter, value, family]` schema; skipped for now.
# Many require an extra `region` or `segment_id` column.
EXCLUDED_NEEDS_CUSTOM_PARSER = (
    # Multi-zone / multi-segment tables
    "Sbaprdem",     # ↳ row per BALEARES/CANARIAS × segment-name
    "baprdeme",     # ↳ daily row per category
    "baprodem",     # ↳ multi-column per-zone
    "bamerupg",     # ↳ aggregate-update format with multi-region
    "comppfre",     # ↳ row per generation tech ("Eólica", ...)
    "grdesvio",     # ↳ "grupos de desvío" — multi-column daily
    "grpbfmed",     # ↳ multi-column daily
    "grpreolr",     # ↳ multi-column daily
    "grpresfh",     # ↳ multi-column daily
    "grpresol",     # ↳ multi-column daily
    "imctd",        # ↳ daily summary, < 24 hourly cols
    "imrrttuad",    # ↳ wide but with mixed-emptiness
    "imsajuad",     # ↳ per-zone × per-hour (different schema)
    "penalpmd",     # ↳ daily aggregate "% horas"
    # Daily-or-different-resolution series
    "SPpeninOSD",   # ↳ daily Iberian penin OSD value (single-col)
    "SparamDD",     # ↳ daily 2-column parameter
    "imsajco",      # ↳ daily single-row
    "imsajli",      # ↳ daily single-row
    "imsecxqh",     # ↳ unparseable single-row sample
    "pdbaqhmd",     # ↳ unparseable single-row sample
    "pdsuqhmd",     # ↳ unparseable single-row sample
    "ecf2spbf",     # ↳ unparseable single-row sample
    "enerad",       # ↳ unparseable single-row sample
    "itafrrba",     # ↳ schema mismatch with itafrrsu (3 cols vs 4)
    # Wide-format files where data line is empty / single-col (no values)
    "endmimpo", "enf2spbf", "eniappre", "imeradx", "imgpcd",
    "imgpncur", "imgpocom", "imgpocur", "imiappre",
)

# Non-thesis-relevant families excluded by family pattern (skipped silently)
EXCLUDED_TARIFF_RX = re.compile(
    r"^(coper|perd|petar|porc(rad|rt|in|bs|t)|prpcap|perdqh|"
    r"prqhmi|porcrad|porcsecx|porcfpo|porcexd|kestmedio|"
    r"K(estimado|estimqh|realqh)|pcpotufi|gaprdema|inceinve|"
    r"penalpmd|pmhfdccp|cilraipre|iimpmcfpo|festgppe|paraliq|"
    r"caleliqui|tipoliqu|ficheliq|listfich|segmento|modelcom|"
    r"pvpc|compodem|liqsegme|unifisic|uprogram|uproufis|"
    r"diasinha|prrtp48c|prrestpbf$|prresmi[1-7]$)",
    re.I,
)
# (prresmi1-7 already in PARSED_FAMILIES_H above; the regex match prevents
#  EXCLUDED_TARIFF_RX from claiming them — order matters in _identify_family.)

# Regional/island-system suffix patterns — not relevant for peninsular Spain
ISLAND_SUFFIXES = (
    "BALEARES", "CANARIAS", "CEUTA", "MELILLA",
    "EL_HIERRO", "GCANARIA", "LA_GOMERA", "LA_PALMA",
    "LZ_FV", "TENERIFE",
)


def _identify_family(filename: str) -> str | None:
    """Return the recognised family prefix for a liquicomun inner
    filename, or None if not in our parsed set.

    Filenames look like:
        A2_impdsvqh_20260401_20260430                   (peninsula)
        C2_impdsvqh_20240101_20240131                   (peninsula)
        C2_SapunhDD_BALEARES_20240501_20240531          (regional, skipped)
        C2_modelcom1_V2-P1-A1_SEPE_20240501_20240531    (settlement metadata, skipped)

    The leading A2/C2 vintage code is the only difference for canonical
    peninsular families. Regional (island) variants and per-tariff-zone
    files use suffix tokens between family and date — those are excluded
    here because they need a richer schema (region/zone/segment_id column).
    """
    name = Path(filename).name
    m = re.match(r"^(?:A2|C2)_([A-Za-z0-9]+)_\d{8}_\d{8}$", name)
    if not m:
        return None  # has a non-date suffix (regional/zone variants)
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
