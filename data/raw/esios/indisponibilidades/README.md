# `data/raw/esios/indisponibilidades/` — REE unit outages and maintenance

**Status: SCAFFOLDED, not yet synced.** Tier 2.

## ESIOS sources to add when needed

| Document | ESIOS area | Frequency | Description |
|---|---|---:|---|
| `Indisponibilidades` | Indisponibilidades | daily | Unit forced + planned unavailabilities |
| `PlanesMantenimiento` | Indisponibilidades | monthly | Maintenance plans (forward-looking) |

## Why this matters for the thesis

Per-unit availability is a **confound control** for the ΔQ regressions.
If a Big-4 unit is on planned maintenance during a regime window, the
"reduced cleared MW" looks like withholding but is just outage. Same
issue for unplanned outages.

`Indisponibilidades` gives the actual outage windows (start/end timestamps,
unit, reason). Joining with the firm-level cleared volumes lets us
EXCLUDE outage hours from strategic-bidding analyses.

## Pipeline scaffolding

When ready to sync:

1. Look up archive IDs at <https://www.esios.ree.es/es/descargas>
2. Add to `src/mtu/ingestion/esios_common.py`
3. Add parser in `src/mtu/parsing/esios/indisponibilidades.py`
4. Pipeline at `scripts/pipelines/esios/indisponibilidades/`
