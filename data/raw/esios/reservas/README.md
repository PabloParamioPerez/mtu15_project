# `data/raw/esios/reservas/` — REE secondary regulation (aFRR) data

**Status: SCAFFOLDED, not yet synced.** Tier 2 in the ESIOS data plan.

## ESIOS sources to add when needed

| Document | ESIOS area | Frequency | Description |
|---|---|---:|---|
| `Curvas_Ofertas_aFRR` | Regulación Secundaria | per ISP | aFRR offer curves submitted by BSPs (per ISP step quantities and prices) |
| `totalasigsec` | Regulación Secundaria | daily/per-ISP | Aggregate secondary-regulation assignments to BSPs |

## Why Tier 2

These are the cleanest data for testing the **H3 reserve-substitution
hypothesis** (does the reform redirect Big-4 capacity from DA into
secondary reserves?). nb04 §8 ran a weak aggregate version using only
A84 activated prices; per-BSP aFRR offers + assignments would give a
direct firm-level test.

## Pipeline scaffolding

When ready to sync:

1. Add archive_id constants in `src/mtu/ingestion/esios_common.py`
2. Add a parser in `src/mtu/parsing/esios/reservas.py`
3. Mirror the liquicomun pipeline triple at
   `scripts/pipelines/esios/reservas/{00_sync,10_parse,20_build}_*.py`

Look up archive IDs at <https://www.esios.ree.es/es/descargas>.
