#!/usr/bin/env bash
# Sequential download — uses throttled generic_sync to stay under rate limits.
set -e
cd "$(dirname "$0")/../../.."

GS="uv run python scripts/pipelines/entsoe/_generic_sync.py"
START="2018-01"
END="2026-04"
ES="10YES-REE------0"
FR="10YFR-RTE------C"
DE="10Y1001A1001A82H"
PT="10YPT-REN------W"

# A73 per-tech (psrType filter via --extra-param) — using the EXISTING per-tech sync script
echo "=== A73 B04 CCGT ==="
uv run python scripts/pipelines/entsoe/generation/00_sync_a73_nuclear.py --start-month $START --end-month $END --psr-type B04
echo "=== A73 B12 hydro reservoir ==="
uv run python scripts/pipelines/entsoe/generation/00_sync_a73_nuclear.py --start-month $START --end-month $END --psr-type B12
echo "=== A73 B11 run-of-river ==="
uv run python scripts/pipelines/entsoe/generation/00_sync_a73_nuclear.py --start-month $START --end-month $END --psr-type B11
echo "=== A73 B10 pumped storage ==="
uv run python scripts/pipelines/entsoe/generation/00_sync_a73_nuclear.py --start-month $START --end-month $END --psr-type B10

# A44 DE day-ahead prices (DE-LU bidding zone)
echo "=== A44 DE-LU ==="
$GS --doc-type A44 --start-month $START --end-month $END --subdir prices_da_de --in-domain "$DE" --out-domain "$DE" --quiet

# A44 PT day-ahead prices
echo "=== A44 PT ==="
$GS --doc-type A44 --start-month $START --end-month $END --subdir prices_da_pt --in-domain "$PT" --out-domain "$PT" --quiet

# A65 load — retry remaining 6
echo "=== A65 load (retry remaining) ==="
$GS --doc-type A65 --process-type A16 --start-month $START --end-month $END --subdir load_actual --out-biz-domain "$ES" --quiet

# A11 ES→FR (retry the 3 missing)
echo "=== A11 ES→FR (retry) ==="
$GS --doc-type A11 --start-month $START --end-month $END --subdir flows_physical_es_to_fr --in-domain "$ES" --out-domain "$FR" --quiet

# A09 ES→FR scheduled
echo "=== A09 ES→FR scheduled ==="
$GS --doc-type A09 --start-month $START --end-month $END --subdir flows_scheduled_es_to_fr --in-domain "$ES" --out-domain "$FR" --extra-param contract_MarketAgreement.Type=A01 --quiet

# A61 NTC ES→FR
echo "=== A61 NTC ES→FR ==="
$GS --doc-type A61 --start-month $START --end-month $END --subdir ntc_es_to_fr --in-domain "$ES" --out-domain "$FR" --extra-param contract_MarketAgreement.Type=A01 --quiet

# A80 forced outages — retry the 72 that failed
echo "=== A80 forced outages (retry) ==="
$GS --doc-type A80 --start-month $START --end-month $END --subdir outages_generation_forced --biz-domain "$ES" --extra-param businessType=A54 --quiet

echo "All sequential re-syncs complete"
