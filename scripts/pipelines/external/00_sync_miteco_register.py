# STATUS: ALIVE
# LAST-AUDIT: 2026-05-19
# CLAIM: Download the daily MITECO registry of electricity producers
#        (Registro de Productores de Energía Eléctrica) as a dated XLSX
#        snapshot. The endpoint serves the current state of the register;
#        we save one snapshot per sync, named by the download date.
#
# Source: datos.gob.es catalogue entry e05068001-registro-de-productores-de-
#         energia-electrica
# Publisher: Ministerio para la Transición Ecológica y el Reto Demográfico
# Endpoint: https://energia.serviciosmin.gob.es/Electra/descargarExcelProduccion.aspx
#
# Output: data/external/miteco_productores/registro_<YYYYMMDD>.xlsx

from __future__ import annotations
from datetime import date
from pathlib import Path
import urllib.request

REPO = Path(__file__).resolve().parents[3]
OUT_DIR = REPO / "data" / "external" / "miteco_productores"
URL = "https://energia.serviciosmin.gob.es/Electra/descargarExcelProduccion.aspx"


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    today = date.today().strftime("%Y%m%d")
    out_path = OUT_DIR / f"registro_{today}.xlsx"
    if out_path.exists():
        print(f"skip (already exists): {out_path}")
        return
    print(f"downloading {URL}")
    req = urllib.request.Request(URL, headers={"User-Agent": "Mozilla/5.0"})
    # The MITECO endpoint sometimes takes 30-90 s to respond; give it plenty.
    with urllib.request.urlopen(req, timeout=300) as resp:
        data = resp.read()
    out_path.write_bytes(data)
    print(f"wrote {out_path}: {len(data) / 1e6:.1f} MB")


if __name__ == "__main__":
    main()
