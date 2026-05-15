# `data/external/esios_master/` — ESIOS API master-data dumps

One-shot JSON dumps from the ESIOS `/archives` endpoint for archives 110–113. Provides the **richest** view of the UF → UP → BRP → EIC → Participant chain (more fields than the public CSV exports in [`../esios/`](../esios/)).

Retrieved **2026-05-14 13:36:54** (see `_downloaded_at.txt`).

## Files

| File | Archive ID | Records | Top fields |
|---|---:|---:|---|
| `generation_units.json` | 110 | 6,472 UFs | `UF Code`, `EIC Code`, `Short / Large Description`, `Production Type`, **`BRP Code`**, **`UP Code`**, `Maximum Power Capacity MW` |
| `programming_units.json` | 111 | 3,782 UPs | `UP Code`, `EIC Code`, `Production Type`, `Trade` (Buy / Sale), `Scope` (Spain / Portugal), `Regulation Zone`, `UP Type`, `BRP Code` |
| `balance_responsible_parties.json` | 112 | 739 BRPs | `BRP Code`, `Name`, `EIC Code`, `BRP Type` (Producer / Trader / Direct market consumer / Reference trader / System operator) |
| `entitled_participants.json` | 113 | 30 firms | `Participant`, `EIC Code` |

## Why both `esios/` (CSV) and `esios_master/` (JSON)?

- **`esios/`** is the public-download CSV export — works without an API token; thinner schema (no `Production Type` on UFs, no `Trade` on UPs).
- **`esios_master/`** is the API JSON dump — richer schema, machine-friendly, but requires `ESIOS_TOKEN`.

For new analyses prefer `esios_master/`. The CSV `esios/` exports remain useful for cross-validation and as the snapshot pinned to 2026-04-26.

## Pivotal-firm BRP code map

See [`../../../notebooks/memos/_esios_archive_catalog.md`](../../../notebooks/memos/_esios_archive_catalog.md) § "Pivotal-firm BRP codes (high-confidence map)" for the firm-by-firm BRP and UP-count breakdown derived from these files. Example:

| Project firm | BRP code(s) | BRP type | UP count |
|---|---|---|---:|
| Iberdrola | IBEG | Producer | 14 |
| Iberdrola Generación España | IBGES | Trader | 56 |
| Endesa | ENDG | Producer | 35 |
| Naturgy Ciclos Combinados | GNCC | Producer | 18 |
| EDP-Spain (HidroCantábrico) | HC G | Producer | 25 |

## How to consume

```python
import json
from pathlib import Path

P = Path("data/external/esios_master")
gen = json.loads((P / "generation_units.json").read_text())["GenerationUnits"]
ups = json.loads((P / "programming_units.json").read_text())["ProgrammingUnits"]
brps = json.loads((P / "balance_responsible_parties.json").read_text())["BalanceResponsibleParties"]
participants = json.loads((P / "entitled_participants.json").read_text())["EntitledParticipants"]
```

## Re-downloading

```bash
# Each archive (110-113) is a one-shot dump. Re-pull only if REE publishes a refresh.
uv run scripts/pipelines/esios/00_download_indicator.py \
  --indicator-id <id> ...  # Note: archives use a different endpoint
```

These archives are NOT periodic time series; they're snapshots of REE's master registry. Refresh cadence is determined by REE (typically when new units register).
