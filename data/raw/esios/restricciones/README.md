# `data/raw/esios/restricciones/` — REE technical-restrictions prices

**Status: SCAFFOLDED, not yet synced.** Tier 2.

## ESIOS sources to add when needed

| Document | ESIOS area | Frequency | Description |
|---|---|---:|---|
| `totalrp48prec` | Restricciones | daily | P48 technical-restrictions schedule WITH prices (RT1) |
| `totalrp48preccierre` | Restricciones | daily | P48 closing technical-restrictions with prices |
| `totalrpdvpprec` | Restricciones | daily | PDVP technical-restrictions with prices |

## Why this matters for the thesis

This is the dataset that quantifies the post-blackout **operación
reforzada** (per `ref_post_blackout_regulation.md` memory). REE's
technical-restrictions dispatch with prices is the financial flow
underlying the ~€666M cumulative cost figure cited in REE press
releases. OMIE doesn't publish technical-restrictions prices.

For the thesis: this dataset would let us decompose the post-blackout
~€100M/month "extra" balancing spending into specific units and
prices, separating reform effects from blackout effects.

## Pipeline scaffolding

When ready to sync:

1. Add archive_id constants in `src/mtu/ingestion/esios_common.py`
2. Add a parser in `src/mtu/parsing/esios/restricciones.py`
3. Mirror the liquicomun pipeline triple
