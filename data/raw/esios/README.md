# `data/raw/esios/` — REE ESIOS public-archive bulk files

ESIOS (Sistema de Información del Operador del Sistema) is REE's
data platform. This folder holds raw downloads of public archives —
bulk historical files served without authentication via the ESIOS
public archives endpoint.

**This is REE source data, NOT to be confused with ENTSO-E.** The two
operators publish overlapping but different objects:

- **ENTSO-E** (`data/raw/entsoe/`) — pan-European Transparency Platform
  with control-area-aggregated balancing data (A85, A86, A87, A75, etc.)
  Spain control area = `10YES-REE------0`.
- **ESIOS / REE** (`data/raw/esios/`) — Spanish national operator data
  with finer Spanish detail: settlement breakdown by concept (C1-C8),
  technical-restrictions prices, per-BSP aFRR offers, unit outages, etc.

The two should be cross-checked but never mixed without flagging.

## Subfolder map

| Folder | Status | Tier | Content |
|---|---|---|---|
| `liquidaciones/` | active | 1 | A2_liquicomun monthly settlement bundles |
| `reservas/` | scaffolded | 2 | aFRR offer curves + secondary-regulation assignments |
| `restricciones/` | scaffolded | 2 | Technical-restrictions schedules with prices (operación reforzada) |
| `indisponibilidades/` | scaffolded | 2 | Unit outages + maintenance plans |

Tier 1 = pipeline built and at least one month synced + parsed.
Tier 2 = folder scaffolded; pipeline to be built when needed.

## What's deliberately NOT here

- **Programas** (`p48cierre`, `totalp48*`, `totalpdbf`, `totalpdvp`):
  REE-side schedules. We have these via OMIE (`pdbc`, `pdbce`,
  `pibci`, `pibcic`, `phf`, `phfc`) at finer granularity. Don't
  duplicate.
- **PVPC** (regulated retail tariff): not thesis-relevant; thesis
  is about wholesale-market behaviour.
- **Subastas explícitas** (`Espec_Subasta_*`, `Resultado_Subasta_*`):
  cross-border explicit-capacity auctions. Different topic.
- **REE_ActualGen / REE_AggGenOutput**: actual generation. We have
  via ENTSO-E A75 already.
- **Descargos_FRA / Descargos_POR**: cross-border outage windows.
  Narrow use case; defer.

## Authentication

Public archives served without auth. Per-subject (per-BSP) archives
require BRP-role registration which we do not have. For per-firm
balancing settlement at the unit level, the public liquicomun gives
control-area aggregates and segment decomposition; per-BRP detail
would require a research agreement with REE. See memory note
`ref_post_blackout_regulation.md` for further context.

## Pipeline conventions

ESIOS pipelines mirror the ENTSO-E layout:

```
scripts/pipelines/esios/
├── liquidaciones/
│   ├── 00_sync_liquicomun.py
│   ├── 10_parse_liquicomun.py
│   └── 20_build_liquicomun_all.py
├── reservas/             (TODO)
├── restricciones/        (TODO)
└── indisponibilidades/   (TODO)
```

Common helpers in `src/mtu/ingestion/esios_common.py`. Parsers in
`src/mtu/parsing/esios/<family>.py`.

## File naming

Raw ZIP archives are stored as

```
data/raw/esios/<family>/<yyyymm>/<original-filename>
```

When the source archive is itself a ZIP (e.g. liquicomun), the ZIP
is preserved at `data/raw/esios/<family>/<yyyymm>/<source-zip>.zip`
AND its contents extracted to a sibling `extracted/` subfolder.

This mirrors how OMIE archives are kept: source files preserved,
extracted contents alongside for direct parsing.
