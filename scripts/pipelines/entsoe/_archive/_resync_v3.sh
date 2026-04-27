#!/usr/bin/env bash
# Stage-2 download — economically-valuable ENTSO-E gaps not in v2.
# Run sequentially via _generic_sync to stay under the rate limit.
# Targets: forecast load (A65 D-1 process), day-ahead gen forecast (A71),
# aggregated balancing bids (A24), system actual gen per PSR type (A75
# without psrType filter), and ES<->PT cross-border flows.
set -e
cd "$(dirname "$0")/../../.."

GS="uv run python scripts/pipelines/entsoe/_generic_sync.py"
START="2018-01"
END="2026-04"
ES="10YES-REE------0"
PT="10YPT-REN------W"
FR="10YFR-RTE------C"

# A65 day-ahead forecast load (process A01 = D-1 forecast)
echo "=== A65 forecast load (D-1) ==="
$GS --doc-type A65 --process-type A01 --start-month $START --end-month $END --subdir load_forecast_da --out-biz-domain "$ES" --quiet

# A65 week-ahead forecast load (process A31 = year-ahead? actually A31=week-ahead)
# Actually A65 has multiple process types. Try A31 (week-ahead) and A33 (year-ahead).
echo "=== A65 forecast load (week-ahead, A31) ==="
$GS --doc-type A65 --process-type A31 --start-month $START --end-month $END --subdir load_forecast_wa --out-biz-domain "$ES" --quiet

# A71 generation forecast (D-1, system aggregate)
echo "=== A71 system gen forecast D-1 ==="
$GS --doc-type A71 --process-type A01 --start-month $START --end-month $END --subdir generation_forecast_da --in-domain "$ES" --quiet

# A75 actual generation per type (NO psrType filter — full breakdown)
echo "=== A75 actual gen all PSR types ==="
$GS --doc-type A75 --process-type A16 --start-month $START --end-month $END --subdir generation_actual_all --in-domain "$ES" --quiet

# A24 aggregated balancing bids (mFRR / aFRR / RR aggregated bid stack)
# Spain control area; processType A47 = scheduled balancing? Try without first.
# Actually A24 typically requires controlArea_Domain.
echo "=== A24 aggregated balancing bids ==="
$GS --doc-type A24 --start-month $START --end-month $END --subdir balancing_aggregated_bids --biz-domain "$ES" --quiet

# A11 ES <-> PT physical flows (both directions)
echo "=== A11 ES->PT physical flows ==="
$GS --doc-type A11 --start-month $START --end-month $END --subdir flows_physical_es_to_pt --in-domain "$ES" --out-domain "$PT" --quiet

echo "=== A11 PT->ES physical flows ==="
$GS --doc-type A11 --start-month $START --end-month $END --subdir flows_physical_pt_to_es --in-domain "$PT" --out-domain "$ES" --quiet

# A09 ES <-> PT scheduled flows (DA)
echo "=== A09 ES->PT scheduled DA ==="
$GS --doc-type A09 --start-month $START --end-month $END --subdir flows_scheduled_es_to_pt --in-domain "$ES" --out-domain "$PT" --extra-param contract_MarketAgreement.Type=A01 --quiet

echo "=== A09 PT->ES scheduled DA ==="
$GS --doc-type A09 --start-month $START --end-month $END --subdir flows_scheduled_pt_to_es --in-domain "$PT" --out-domain "$ES" --extra-param contract_MarketAgreement.Type=A01 --quiet

# A61 NTC ES <-> PT
echo "=== A61 NTC ES->PT ==="
$GS --doc-type A61 --start-month $START --end-month $END --subdir ntc_es_to_pt --in-domain "$ES" --out-domain "$PT" --extra-param contract_MarketAgreement.Type=A01 --quiet

echo "=== A61 NTC PT->ES ==="
$GS --doc-type A61 --start-month $START --end-month $END --subdir ntc_pt_to_es --in-domain "$PT" --out-domain "$ES" --extra-param contract_MarketAgreement.Type=A01 --quiet

# A85 imbalance prices for FR — useful for cross-country comparison
echo "=== A85 imbalance prices FR ==="
$GS --doc-type A85 --start-month $START --end-month $END --subdir imbalance_prices_fr --extra-param controlArea_Domain="$FR" --quiet

echo "All v3 sequential re-syncs complete"
