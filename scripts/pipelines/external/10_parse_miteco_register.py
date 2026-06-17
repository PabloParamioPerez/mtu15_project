# STATUS: ALIVE
# LAST-AUDIT: 2026-05-19
# CLAIM: Parse the MITECO registry XLSX into two clean parquet tables.
#        Stdlib-only (zipfile + xml.etree) since openpyxl is not in the
#        project venv (no dependency adds without asking).
#
# Input: latest data/external/miteco_productores/registro_<YYYYMMDD>.xlsx
# Output:
#   data/external/miteco_productores/installations.parquet
#     One row per installation. Cols: id_instalacion, regimen (ORDINARIO/ESPECIAL),
#     installation (display name), autonomia.
#   data/external/miteco_productores/phases.parquet
#     One row per installation-phase (multi-phase plants get multiple rows).
#     Cols: installation, potencia_neta_mw, potencia_bruta_mw,
#     fecha_puesta_servicio, fecha_alta_instalacion, fecha_baja,
#     fecha_alta_provisional, alta_registro_opendata, numero_fase,
#     numero_registro, autonomia.

from __future__ import annotations
from datetime import datetime
from pathlib import Path
import re
import xml.etree.ElementTree as ET
import zipfile

import pandas as pd

REPO = Path(__file__).resolve().parents[3]
OUT_DIR = REPO / "data" / "external" / "miteco_productores"
NS = {"x": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}


def _latest_xlsx() -> Path:
    files = sorted(OUT_DIR.glob("registro_*.xlsx"))
    if not files:
        raise FileNotFoundError("No registro_*.xlsx — run 00_sync_miteco_register.py first.")
    return files[-1]


def _load_shared_strings(z: zipfile.ZipFile) -> list[str]:
    tree = ET.parse(z.open("xl/sharedStrings.xml"))
    out = []
    for si in tree.getroot().findall("x:si", NS):
        t = si.find("x:t", NS)
        out.append(t.text if t is not None and t.text is not None else "")
    return out


def _col_letter(cell_ref: str) -> str:
    return re.sub(r"\d+", "", cell_ref)


def _parse_sheet(z: zipfile.ZipFile, sheet_xml: str, strings: list[str]) -> list[dict]:
    """Stream-parse a worksheet into a list of {col_letter: value} dicts (one per row)."""
    rows = []
    for _, elem in ET.iterparse(z.open(sheet_xml), events=("end",)):
        if not elem.tag.endswith("}row"):
            continue
        row_dict = {}
        for c in elem.findall("x:c", NS):
            v_el = c.find("x:v", NS)
            if v_el is None or v_el.text is None:
                continue
            val = v_el.text
            if c.get("t") == "s":
                val = strings[int(val)]
            row_dict[_col_letter(c.get("r"))] = val
        rows.append(row_dict)
        elem.clear()
    return rows


def _to_date(val):
    if val is None or val == "":
        return None
    # Try ISO first
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%d/%m/%Y %H:%M:%S", "%d/%m/%Y"):
        try:
            return datetime.strptime(val, fmt)
        except (ValueError, TypeError):
            continue
    return None


def _to_float(val):
    if val is None or val == "":
        return None
    try:
        return float(str(val).replace(",", "."))
    except (ValueError, TypeError):
        return None


def main() -> None:
    src = _latest_xlsx()
    print(f"parsing {src.name}")
    with zipfile.ZipFile(src) as z:
        strings = _load_shared_strings(z)
        s1 = _parse_sheet(z, "xl/worksheets/sheet1.xml", strings)
        s2 = _parse_sheet(z, "xl/worksheets/sheet2.xml", strings)

    # Sheet 1 — header in row[0]: A=IdInstalacion, B=regimen, C=Instalacion, D=Autonomia
    header_s1 = s1[0]
    print(f"  sheet1 header: {header_s1}")
    inst_rows = [
        {
            "id_instalacion": r.get("A"),
            "regimen":        r.get("B"),
            "installation":   r.get("C"),
            "autonomia":      r.get("D"),
        }
        for r in s1[1:]
    ]
    df_inst = pd.DataFrame(inst_rows)
    print(f"  installations: {len(df_inst):,}")

    # Sheet 2 — header columns A..K: INSTALACION, POTENCIANETA, POTENCIABRUTA,
    # FECHAPUESTASERVICIO, FECHAALTAINSTALACION, FECHABAJA, FECHAALTAPROVISIONAL,
    # ALTAREGISTROOPENDATA, NumeroFase, NumeroRegistro, Autonomia
    header_s2 = s2[0]
    print(f"  sheet2 header: {header_s2}")
    # POTENCIANETA / POTENCIABRUTA are in kW per the MITECO source. Convert to MW.
    def _kw_to_mw(val):
        f = _to_float(val)
        return f / 1000.0 if f is not None else None

    phase_rows = [
        {
            "installation":              r.get("A"),
            "potencia_neta_mw":          _kw_to_mw(r.get("B")),
            "potencia_bruta_mw":         _kw_to_mw(r.get("C")),
            "fecha_puesta_servicio":     _to_date(r.get("D")),
            "fecha_alta_instalacion":    _to_date(r.get("E")),
            "fecha_baja":                _to_date(r.get("F")),
            "fecha_alta_provisional":    _to_date(r.get("G")),
            "alta_registro_opendata":    _to_date(r.get("H")),
            "numero_fase":               r.get("I"),
            "numero_registro":           r.get("J"),
            "autonomia":                 r.get("K"),
        }
        for r in s2[1:]
    ]
    df_phase = pd.DataFrame(phase_rows)
    print(f"  phases:        {len(df_phase):,}")

    inst_out = OUT_DIR / "installations.parquet"
    phase_out = OUT_DIR / "phases.parquet"
    df_inst.to_parquet(inst_out, index=False)
    df_phase.to_parquet(phase_out, index=False)
    print(f"wrote {inst_out} ({inst_out.stat().st_size/1e6:.2f} MB)")
    print(f"wrote {phase_out} ({phase_out.stat().st_size/1e6:.2f} MB)")

    # Quick sanity summary
    print("\nRegimen breakdown:")
    print(df_inst["regimen"].value_counts().to_string())
    print(f"\nPhase capacity sum (POTENCIANETA): {df_phase['potencia_neta_mw'].sum() / 1000:.1f} GW total registered")
    print(f"Commissioning date range: {df_phase['fecha_puesta_servicio'].min()} → {df_phase['fecha_puesta_servicio'].max()}")
    print(f"Phases with FECHABAJA (deactivated): {df_phase['fecha_baja'].notna().sum():,}")


if __name__ == "__main__":
    main()
