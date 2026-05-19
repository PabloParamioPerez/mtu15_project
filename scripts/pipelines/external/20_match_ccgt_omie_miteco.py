# STATUS: ALIVE
# LAST-AUDIT: 2026-05-19
# CLAIM: Best-effort auto-match between the 57 OMIE CCGT unit_code entries
#        (from data/external/omie_reference/lista_unidades.csv) and the
#        MITECO ORDINARIO installations (from data/external/miteco_productores/).
#        Writes a starter CCGT bridge CSV the user can hand-verify.
#
# Strategy: for each OMIE CCGT unit_code, derive a plant-name stem (everything
# before any trailing digit) and the trailing group number (if any). Then
# search the ORDINARIO MITECO names for a substring match on the stem AND
# on the group number when present. Multi-match candidates are kept; the
# user reviews the output CSV.
#
# Output:
#   data/external/miteco_productores/omie_ccgt_to_miteco.csv
#     Columns: omie_unit_code, omie_owner, omie_zone, miteco_candidates
#       (semicolon-separated list of MITECO installations),
#       potencia_neta_mw_sum, fecha_puesta_servicio_min, autonomia_first,
#       n_phases, match_confidence ('auto' | 'review' | 'no_match').

from __future__ import annotations
from pathlib import Path
import re
import unicodedata

import pandas as pd

REPO = Path(__file__).resolve().parents[3]
MITECO_INST = REPO / "data/external/miteco_productores/installations.parquet"
MITECO_PHASES = REPO / "data/external/miteco_productores/phases.parquet"
OMIE_UNITS = REPO / "data/external/omie_reference/lista_unidades.csv"
OUT = REPO / "data/external/miteco_productores/omie_ccgt_to_miteco.csv"

# Hand-curated stems for the OMIE CCGT codes whose mnemonic isn't obvious.
# Maps unit_code prefix → list of name keywords to search MITECO for.
KNOWN_STEMS = {
    "ACE":     ["ACECA"],
    "ALG":     ["ALGECIRAS"],
    "AMBIETA": ["AMOREBIETA"],
    "ARCOS":   ["ARCOS"],
    "ARRU":    ["ARRUBAL"],
    "BAHIAB":  ["ZIERBENA"],            # Bahía Bizkaia plant is in Zierbena
    "BES":     ["BESÓS", "BESOS"],
    "CAMG":    ["CAMPO DE GIBRALTAR"],
    "CAS":     ["CASTEJÓN", "CASTEJON"],
    "CART":    ["CARTAGENA"],
    "COL":     ["CRISTOBAL COLON", "PALOS DE LA FRONTERA"],   # Endesa Colón is at Palos (Huelva)
    "CTGN":    ["CASTEJÓN", "CASTEJON"],
    "CTJON":   ["CASTEJÓN", "CASTEJON"],
    "CTN":     ["CASTELNOU"],
    "CTNU":    ["CASTELNOU"],
    "ECT":     ["ESCATRÓN", "ESCATRON"],
    "ESC":     ["ESCOMBRERAS"],
    "GRA":     ["GRANADILLA"],
    "MAL":     ["MÁLAGA", "MALAGA", "MALA"],
    "PALOS":   ["PALOS DE LA FRONTERA"],
    "PBCN":    ["PLANA DEL VENT", "PUERTO DE BARCELONA", "PORT DE BARCELONA"],
    "PGR":     ["PUENTES GR", "AS PONTES"],
    "PLA":     ["PLANA DEL VENT"],
    "PVENT":   ["PLANA DEL VENT"],
    "PSP":     ["PALOS DE LA FRONTERA"],
    "SAG":     ["SAGUNTO"],
    "SAB":     ["SABÓN", "SABON"],
    "SBO":     ["SABÓN", "SABON"],
    "SCB":     ["S.C.B.", "SAN CIPRIAN"],
    "SCOL":    ["CRISTOBAL COLON"],
    "SOTR":    ["SOTO DE RIBERA"],
    "SOT":     ["SOTO DE RIBERA"],
    "SRI":     ["SOTO DE RIBERA"],
    "SROQ":    ["SAN ROQUE"],
    "STC":     ["SANTURCE"],
    "TAR":     ["TARRAGONA"],
    "TLG":     ["AS PONTES"],
}


def _norm(s: str) -> str:
    # Strip accents, collapse whitespace, uppercase.
    s = str(s)
    nfkd = unicodedata.normalize("NFKD", s)
    no_accents = "".join(c for c in nfkd if not unicodedata.combining(c))
    return re.sub(r"\s+", " ", no_accents).upper().strip()


def main() -> None:
    units = pd.read_csv(OMIE_UNITS)
    ccgt = units[units["technology"].astype(str).str.contains("Ciclo combinado", case=False, na=False)].copy()
    print(f"OMIE CCGT units: {len(ccgt)}")

    inst = pd.read_parquet(MITECO_INST)
    inst_ord = inst[inst["regimen"] == "ORDINARIO"].copy()
    inst_ord["installation_norm"] = inst_ord["installation"].apply(_norm)
    print(f"MITECO ORDINARIO entries: {len(inst_ord)}")

    phases = pd.read_parquet(MITECO_PHASES)
    # phase rows are keyed by 'installation' name; aggregate per name
    phase_agg = (
        phases.groupby("installation", dropna=True)
        .agg(
            potencia_neta_mw_sum=("potencia_neta_mw", "sum"),
            potencia_bruta_mw_sum=("potencia_bruta_mw", "sum"),
            fecha_puesta_servicio_min=("fecha_puesta_servicio", "min"),
            fecha_baja_max=("fecha_baja", "max"),
            n_phases=("numero_fase", "count"),
        )
        .reset_index()
    )
    phase_agg["installation_norm"] = phase_agg["installation"].apply(_norm)
    inst_phase = inst_ord.merge(phase_agg, on=["installation_norm"], how="left", suffixes=("", "_p"))

    rows = []
    for _, row in ccgt.iterrows():
        code = row["unit_code"]
        owner = row.get("owner_agent", "")
        zone = row.get("zone", "")

        # Find name stem in known mapping
        stems = []
        for prefix, kws in KNOWN_STEMS.items():
            if code.startswith(prefix):
                stems = kws
                break

        candidates_df = pd.DataFrame()
        if stems:
            mask = inst_phase["installation_norm"].apply(
                lambda s: any(_norm(k) in s for k in stems)
            )
            candidates_df = inst_phase[mask]

            # If multiple candidates and OMIE code ends with a digit, prefer
            # the MITECO name that also ends with that digit or contains
            # GRUPO <digit>.
            if len(candidates_df) > 1:
                m = re.search(r"(\d+)R?$", code)
                if m:
                    g = m.group(1)
                    digit_mask = candidates_df["installation_norm"].apply(
                        lambda s: bool(re.search(rf"(GRUPO\s*{g}|\b{g}\b)$", s))
                    )
                    if digit_mask.any():
                        candidates_df = candidates_df[digit_mask]

        if len(candidates_df) == 0:
            rows.append({
                "omie_unit_code": code,
                "omie_owner": owner,
                "omie_zone": zone,
                "miteco_candidates": "",
                "potencia_neta_mw_sum": None,
                "fecha_puesta_servicio_min": None,
                "autonomia_first": None,
                "n_phases": 0,
                "match_confidence": "no_match",
            })
            continue

        names = "; ".join(sorted(candidates_df["installation"].dropna().unique()))
        confidence = "auto" if len(candidates_df) == 1 else "review"
        rows.append({
            "omie_unit_code": code,
            "omie_owner": owner,
            "omie_zone": zone,
            "miteco_candidates": names,
            "potencia_neta_mw_sum": candidates_df["potencia_neta_mw_sum"].sum(),
            "fecha_puesta_servicio_min": candidates_df["fecha_puesta_servicio_min"].min(),
            "autonomia_first": candidates_df["autonomia"].iloc[0] if len(candidates_df) else None,
            "n_phases": int(candidates_df["n_phases"].sum()),
            "match_confidence": confidence,
        })

    out = pd.DataFrame(rows).sort_values("omie_unit_code")
    out.to_csv(OUT, index=False)
    print(f"wrote {OUT}: {len(out)} rows")
    print("\n=== summary by confidence ===")
    print(out["match_confidence"].value_counts().to_string())
    print("\n=== sample auto-matches ===")
    print(out[out["match_confidence"] == "auto"].head(8).to_string(index=False))
    print("\n=== sample no-matches (need manual entry) ===")
    print(out[out["match_confidence"] == "no_match"].head(10).to_string(index=False))


if __name__ == "__main__":
    main()
