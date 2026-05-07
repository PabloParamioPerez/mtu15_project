#!/usr/bin/env bash
# overnight_icab_idet.sh — Download, parse, build icab+idet, then commit.
# Usage: caffeinate -i -s bash scripts/admin/overnight_icab_idet.sh

set -euo pipefail

REPO="$(cd "$(dirname "$0")/../.." && pwd)"
LOG="$REPO/scripts/admin/overnight_icab_idet.log"
START_MONTH="2018-01"
END_MONTH="2026-01"

exec > >(tee -a "$LOG") 2>&1

echo "====== overnight_icab_idet.sh started at $(date -u +%Y-%m-%dT%H:%M:%SZ) ======"
echo "Repo:  $REPO"
echo "Log:   $LOG"
echo ""

cd "$REPO"

# ── 1. Download ──────────────────────────────────────────────────────────────
echo "--- [1/6] Downloading icab ZIPs ($START_MONTH → $END_MONTH) ---"
uv run scripts/pipelines/omie/mercado_intradiario_subastas/00_sync_icab_zips.py \
    --start-month "$START_MONTH" --end-month "$END_MONTH" --timeout 120

echo ""
echo "--- [2/6] Downloading idet ZIPs ($START_MONTH → $END_MONTH) ---"
uv run scripts/pipelines/omie/mercado_intradiario_subastas/00_sync_idet_zips.py \
    --start-month "$START_MONTH" --end-month "$END_MONTH" --timeout 120

# ── 2. Parse ─────────────────────────────────────────────────────────────────
echo ""
echo "--- [3/6] Parsing icab raw files ---"
uv run scripts/pipelines/omie/mercado_intradiario_subastas/10_parse_icab.py

echo ""
echo "--- [4/6] Parsing idet raw files ---"
uv run scripts/pipelines/omie/mercado_intradiario_subastas/10_parse_idet.py

# ── 3. Build consolidated parquets ───────────────────────────────────────────
echo ""
echo "--- [5/6] Building icab_all.parquet ---"
uv run scripts/pipelines/omie/mercado_intradiario_subastas/20_build_icab_all.py

echo ""
echo "--- [6/6] Building idet_all.parquet ---"
uv run scripts/pipelines/omie/mercado_intradiario_subastas/20_build_idet_all.py

# ── 4. Commit skipped (intentional — review parse results before committing) ──
echo ""
echo "--- Git commit skipped ---"

echo ""
echo "====== overnight_icab_idet.sh finished at $(date -u +%Y-%m-%dT%H:%M:%SZ) ======"
