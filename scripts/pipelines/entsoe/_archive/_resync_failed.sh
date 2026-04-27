#!/usr/bin/env bash
# Sequential re-run of failed ENTSO-E syncs. Throttled by 5s between calls
# at the script level (the entsoe_common.py layer also retries on 429).
set -e
cd "$(dirname "$0")/../../.."

echo "Waiting 90s for token rate limit to reset..."
sleep 90

# A73 per-tech (CCGT, B12 reservoir, B11 run-of-river, B10 pumped) — most failed
for psr in B04 B12 B11 B10; do
  echo "=== A73 $psr ==="
  uv run python scripts/pipelines/entsoe/generation/00_sync_a73_nuclear.py \
    --start-month 2018-01 --end-month 2026-04 --psr-type "$psr"
done

# A44 DE day-ahead prices (DE-LU bidding zone)
echo "=== A44 DE-LU ==="
uv run python scripts/pipelines/entsoe/_generic_sync.py \
  --doc-type A44 --start-month 2018-01 --end-month 2026-04 \
  --subdir prices_da_de --in-domain "10Y1001A1001A82H" \
  --out-domain "10Y1001A1001A82H" --quiet

# A44 PT day-ahead prices (Portuguese bidding zone)
echo "=== A44 PT ==="
uv run python scripts/pipelines/entsoe/_generic_sync.py \
  --doc-type A44 --start-month 2018-01 --end-month 2026-04 \
  --subdir prices_da_pt --in-domain "10YPT-REN------W" \
  --out-domain "10YPT-REN------W" --quiet

# A65 load — re-try the 6 that failed
echo "=== A65 load (retry remaining) ==="
uv run python scripts/pipelines/entsoe/_generic_sync.py \
  --doc-type A65 --process-type A16 --start-month 2018-01 --end-month 2026-04 \
  --subdir load_actual --out-biz-domain "10YES-REE------0" --quiet

# A11 ES→FR (3 failed)
echo "=== A11 ES→FR (retry) ==="
uv run python scripts/pipelines/entsoe/_generic_sync.py \
  --doc-type A11 --start-month 2018-01 --end-month 2026-04 \
  --subdir flows_physical_es_to_fr --in-domain "10YES-REE------0" \
  --out-domain "10YFR-RTE------C" --quiet

# A09 ES→FR scheduled
echo "=== A09 ES→FR scheduled ==="
uv run python scripts/pipelines/entsoe/_generic_sync.py \
  --doc-type A09 --start-month 2018-01 --end-month 2026-04 \
  --subdir flows_scheduled_es_to_fr --in-domain "10YES-REE------0" \
  --out-domain "10YFR-RTE------C" --extra-param contract_MarketAgreement.Type=A01 \
  --quiet

# A61 NTC ES→FR
echo "=== A61 NTC ES→FR ==="
uv run python scripts/pipelines/entsoe/_generic_sync.py \
  --doc-type A61 --start-month 2018-01 --end-month 2026-04 \
  --subdir ntc_es_to_fr --in-domain "10YES-REE------0" \
  --out-domain "10YFR-RTE------C" --extra-param contract_MarketAgreement.Type=A01 \
  --quiet

# A80 forced outages — retry the 72 that failed (might have been rate-limit not data-empty)
echo "=== A80 forced outages (retry) ==="
uv run python scripts/pipelines/entsoe/_generic_sync.py \
  --doc-type A80 --start-month 2018-01 --end-month 2026-04 \
  --subdir outages_generation_forced --biz-domain "10YES-REE------0" \
  --extra-param businessType=A54 --quiet

# A71 generation forecast per type — try with biddingZone instead of in_Domain
echo "=== A71 generation forecast ==="
uv run python scripts/pipelines/entsoe/_generic_sync.py \
  --doc-type A71 --process-type A01 --start-month 2018-01 --end-month 2026-04 \
  --subdir generation_forecast_per_type --in-domain "10YES-REE------0" \
  --quiet

# A89 scheduled generation total
echo "=== A89 scheduled generation total ==="
uv run python scripts/pipelines/entsoe/_generic_sync.py \
  --doc-type A89 --process-type A01 --start-month 2018-01 --end-month 2026-04 \
  --subdir scheduled_generation_total --biz-domain "10YES-REE------0" \
  --quiet

echo "All sequential re-syncs complete"
