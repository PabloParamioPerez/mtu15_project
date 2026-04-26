# ESIOS reference taxonomy exports

Authoritative ESIOS reference lists for Spanish-market participants and units. Used to map BSP / SM / UP / UF / EIC codes across data sources.

## Source

Downloaded from the ESIOS public-descargas page:

> https://www.esios.ree.es/es/descargas?date_type=publicacion&start_date=26-04-2026&end_date=26-04-2026

**Check at the bottom of the page** — the participant/unit reference downloads sit below the day-of-publication archive list.

Retrieved **April 26, 2026 (19:22)**.

## Files

| File | Rows | Content |
|---|---|---|
| `export_sujetos-del-mercado_*.csv` | 753 | Market subjects (BSPs/BRPs). Cols: `Nombre`, `Código de sujeto`, `Código EIC`, `Tipo de SM`. |
| `export_unidades-de-programacion_*.csv` | 3,745 | Scheduling units (UP). Cols include `Código de UP`, `Código EIC`, `Potencia máxima MW`, `Tipo de producción`, `Sujeto del Mercado`, `Zona de Regulación`. |
| `export_unidades-fisicas_*.csv` | 6,461 | Physical units (UF). Cols: `Código de UF`, `Código EIC`, `Vinculación con SM`, `Vinculación con UP`. |
| `export_participantes-habilitados-en-subastas-de-capacidad_*.csv` | 31 | Cross-border explicit-auction participants. |

## Cross-reference notes (added 2026-04-27)

- `Sujeto del Mercado` (5-letter SM codes like `IBGES`, `ENDG`, `IGNU`) is the canonical firm-level identifier in ESIOS.
- `Código de UP` and `Código de UF` link to OMIE `unit_code` for most generation units (sometimes with a leading-character difference; check via `Código EIC` for authoritative match).
- The 3-letter `BSP` field in `liquicierre` / `liquicierresrs` (id=17 / 203) is a SEPARATE REE legacy taxonomy and does NOT appear in these exports. Mapping BSP → SM requires either REE settlement documentation or empirical magnitude inference (see `scripts/analysis/synthetic/per_firm_afrr_provision.py`).
