# `data/external/esios/` — ESIOS reference taxonomy exports (CSV)

Authoritative ESIOS reference lists for Spanish-market participants and units, downloaded from the ESIOS public download page (no API token). Used to map BSP / SM / UP / UF / EIC codes across data sources.

For the **API-sourced** master data (richer JSON dumps with `Production Type`, `Trade`, `Scope`, `Regulation Zone`, etc.), see [`../esios_master/`](../esios_master/) — which complements rather than replaces this directory.

## Source

> https://www.esios.ree.es/es/descargas?date_type=publicacion&start_date=26-04-2026&end_date=26-04-2026

**Check at the bottom of the page** — the participant/unit reference downloads sit below the day-of-publication archive list. Retrieved **2026-04-26 19:22**.

## Files

| File | Rows | Content |
|---|---:|---|
| `export_sujetos-del-mercado_*.csv` | 753 | Market subjects (BSPs / BRPs). Cols: `Nombre`, `Código de sujeto`, `Código EIC`, `Tipo de SM`. |
| `export_unidades-de-programacion_*.csv` | 3,745 | Scheduling units (UP). Cols include `Código de UP`, `Código EIC`, `Potencia máxima MW`, `Tipo de producción`, `Sujeto del Mercado`, `Zona de Regulación`. |
| `export_unidades-fisicas_*.csv` | 6,461 | Physical units (UF). Cols: `Código de UF`, `Código EIC`, `Vinculación con SM`, `Vinculación con UP`. |
| `export_participantes-habilitados-en-subastas-de-capacidad_*.csv` | 31 | Cross-border explicit-auction participants. |

## Cross-reference notes

- `Sujeto del Mercado` (5-letter SM codes like `IBGES`, `ENDG`, `IGNU`) is the canonical firm-level identifier in ESIOS.
- `Código de UP` and `Código de UF` link to OMIE `unit_code` for most generation units (sometimes with a leading-character difference; check via `Código EIC` for authoritative match).
- The 3-letter `BSP` field in `liquicierre` / `liquicierresrs` (archives 17 / 203) is a SEPARATE REE legacy taxonomy and does NOT appear in these exports. Mapping BSP → SM requires either REE settlement documentation or empirical magnitude inference (see `data/external/esios_reference/bsp_to_firm.csv` and `notebooks/memos/_esios_archive_catalog.md` § "Pivotal-firm BRP codes").

## Related directories

- [`../esios_master/`](../esios_master/) — API-sourced JSON master-data dumps (archives 110, 111, 112, 113). More fields per record, programmatic schema.
- [`../esios_reference/bsp_to_firm.csv`](../esios_reference/bsp_to_firm.csv) — hand-curated BSP → firm mapping inferred from `liquicierre` settlement magnitudes.
- [`../esios_indicator_catalog.yaml`](../esios_indicator_catalog.yaml) — 139-indicator curated catalog driving the `/indicators` backfill pipeline.
