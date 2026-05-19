# MITECO Registry of Electricity Producers

**Source**: Ministerio para la Transición Ecológica y el Reto Demográfico (MITECO), via the open-data portal at
<https://datos.gob.es/es/catalogo/e05068001-registro-de-productores-de-energia-electrica>.

**Endpoint**: `https://energia.serviciosmin.gob.es/Electra/descargarExcelProduccion.aspx` (single XLSX, daily updated, ~5 MB).

**Coverage**: All Spanish electricity-producing installations from 1993-06-22 onward (ordinary + special regime, registered + provisional + deactivated).

## Files

- `registro_<YYYYMMDD>.xlsx` — raw snapshot. One per `00_sync_miteco_register.py` run.
- `installations.parquet` — Sheet 1, cleaned. One row per installation. Cols: `id_instalacion`, `regimen` (ORDINARIO / ESPECIAL), `installation`, `autonomia`.
- `phases.parquet` — Sheet 2, cleaned. One row per (installation, phase). Cols: `installation`, `potencia_neta_mw`, `potencia_bruta_mw` (converted from kW), `fecha_puesta_servicio` (commissioning), `fecha_baja` (deactivation), `numero_fase`, `numero_registro`, `autonomia`.
- `omie_ccgt_to_miteco.csv` — bridge from the 57 OMIE CCGT `unit_code` entries (in `data/external/omie_reference/lista_unidades.csv`) to MITECO installation names. Auto-matched where possible, with `match_confidence` ∈ {`auto`, `review`, `no_match`}.

## Unit convention

MITECO publishes `POTENCIANETA` / `POTENCIABRUTA` in **kilowatts** (kW), with the comma as Spanish-locale decimal separator (e.g., `820536,000` = 820,536 kW = 820.536 MW for the CTCC Bahía de Algeciras). The parser converts to **megawatts (MW)** for the parquet output.

## What this gives the project

1. **Per-installation commissioning + deactivation dates** — useful for fleet-evolution panels (capacity-growth covariate for the apuntamiento story, fleet attrition).
2. **Per-installation capacity in MW** — sanity-check the OMIE roster.
3. **Bridge to OMIE CCGT codes** (via `omie_ccgt_to_miteco.csv`) — pairs each OMIE `unit_code` with its installation name, commissioning date, and *autonomía* (region). Useful for the zonal-concentration / RT2 strategic-bidding analysis in `thesis/provisional/bidding_internal.tex` §§5–7.

## Limitations

- **No owner / firm column** — still need OMIE `lista_unidades.csv` for ownership.
- **No technology / fuel column** — infer from installation name keywords (`CTCC`, `CICLO COMBINADO`, `EOL`, `FV`, etc.).
- **Geographic precision is at the *autonomía* (region) level only** — not municipality or coordinates.
- **No direct OMIE unit-code field** — the `omie_ccgt_to_miteco.csv` bridge is hand-curated (40/57 auto, 9 review, 8 no-match — of which 7 are Portuguese plants not in the Spanish register).

## Refresh

```bash
uv run python scripts/pipelines/external/00_sync_miteco_register.py
uv run python scripts/pipelines/external/10_parse_miteco_register.py
uv run python scripts/pipelines/external/20_match_ccgt_omie_miteco.py
```

The sync writes `registro_<today>.xlsx`; the parser picks the newest file; the matcher rebuilds the bridge CSV.
