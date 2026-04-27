"""Build EIC -> OMIE-code -> firm mapping for Spanish CCGT plants.

Joins ENTSO-E A73 per-unit CCGT EIC codes (B04 PSR type) to OMIE
unit_code via a regex extraction from the EIC structure (18W<CODE>...),
then attaches owner_agent and firm group from the OMIE reference list.

Manual overrides handle three EIC patterns the regex cannot match:
  - 18WTARRAG-123-03  -> TAPOWER (Tarragona Power, IB)
  - 18WCAMGI20--1-0D  -> CAMG20R (Campo Gibraltar 20)
  - 18W18WSRI4--123V  -> SRI4R (the double-18W prefix is a known typo)

Output:
  data/external/omie_reference/ccgt_eic_to_omie.csv

Schema: entsoe_eic, omie_code, description, owner_agent, firm
where firm in {IB, GE, GN, HC, EDP_PT, OTHER}.
"""
from __future__ import annotations

import re
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[4]

OWN_TO_FIRM = {
    "IBERDROLA ENERGÍA ESPAÑA S..A.": "IB",
    "GAS NATURAL COMERCIALIZADORA": "GN",
    "ENDESA GENERACIÓN, S.A.": "GE",
    "EDP ESPAÑA, S.A.U. (GENERACIÓN)": "HC",
    "EDP GEM PORTUGAL S.A.": "EDP_PT",
}

MANUAL_OVERRIDES = {
    "18WCAMGI20--1-0D": "CAMG20R",
    "18WTARRAG-123-03": "TAPOWER",
    "18WABO2-12345-0N": None,
    "UNKNOWN": None,
    "18W000000000T2JX": None,
}


def extract_omie(eic: str, omie_codes: set[str], omie_norm: dict) -> str | None:
    if eic in MANUAL_OVERRIDES:
        return MANUAL_OVERRIDES[eic]
    m = re.match(r"^18W(18W)?([A-Z0-9]+?)(?:-|$)", eic)
    if not m:
        return None
    cand = m.group(2)
    if cand in omie_codes:
        return cand
    if cand in omie_norm:
        return omie_norm[cand]
    for n in range(min(8, len(cand)), 2, -1):
        if cand[:n] in omie_codes:
            return cand[:n]
        if cand[:n] in omie_norm:
            return omie_norm[cand[:n]]
    return None


def main() -> None:
    panel_path = PROJECT_ROOT / "data/processed/entsoe/generation/a73_per_unit_all.parquet"
    ref_path = PROJECT_ROOT / "data/external/omie_reference/lista_unidades.csv"
    out_path = PROJECT_ROOT / "data/external/omie_reference/ccgt_eic_to_omie.csv"

    df = pd.read_parquet(panel_path)
    ccgt_eics = sorted(df[df["psr_type"] == "B04"]["unit_eic"].unique())

    ref = pd.read_csv(ref_path)
    ccgt_ref = ref[ref["technology"] == "Ciclo Combinado"].copy()
    omie_codes = set(ccgt_ref["unit_code"].str.upper())
    # Strip-R reverse: e.g. ARRU1 -> ARRU1R
    omie_norm: dict[str, str] = {}
    for code in omie_codes:
        if code.endswith("R"):
            omie_norm[code[:-1]] = code

    own = ccgt_ref.set_index("unit_code")["owner_agent"].to_dict()
    desc = ccgt_ref.set_index("unit_code")["description"].to_dict()

    rows = []
    for eic in ccgt_eics:
        code = extract_omie(eic, omie_codes, omie_norm)
        if not code:
            continue
        rows.append({
            "entsoe_eic": eic,
            "omie_code": code,
            "description": desc.get(code, ""),
            "owner_agent": own.get(code, ""),
            "firm": OWN_TO_FIRM.get(own.get(code, ""), "OTHER"),
        })

    out = pd.DataFrame(rows).sort_values("entsoe_eic")
    out.to_csv(out_path, index=False)
    print(f"mapped {len(out)}/{len(ccgt_eics)} CCGT EICs -> {out_path}")
    print(f"firm counts: {out['firm'].value_counts().to_dict()}")


if __name__ == "__main__":
    main()
