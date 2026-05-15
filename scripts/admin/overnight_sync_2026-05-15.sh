#!/usr/bin/env bash
# Overnight sync queue — 2026-05-15
#
# Idempotent. Logs per-stage to logs/overnight_*.log. Each stage prints a
# header line; failures don't stop the queue (we want as much done as
# possible by morning).
#
# Wait for in-flight ESIOS reforzada backfill (PID 77282) first, then run:
#   1. Parse + build indicators_all (picks up new reforzada series)
#   2. OMIE recent sync: marginalpdbc, marginalpibc, det, idet, pdbf, phf, trades
#   3. ESIOS recent sync: RT2, curvas_ofertas_afrr, indisponibilidades, liquicierre
#   4. ENTSO-E _sync_all (idempotent — adds whatever's missing)
#   5. Final rebuilds + a summary line

set -u
cd "/Users/pabloparamio/Desktop/CEMFI/2nd Year/Master Thesis/mtu15_project"
LOG_DIR="logs"
TS=$(date +%Y%m%d_%H%M%S)
SUMMARY="$LOG_DIR/overnight_summary_${TS}.txt"
mkdir -p "$LOG_DIR"

log() {
  echo "[$(date '+%H:%M:%S')] $*" | tee -a "$SUMMARY"
}

stage() {
  local title="$1"; shift
  local logfile="$LOG_DIR/overnight_${title//[\/ ]/_}_${TS}.log"
  log ">>> $title"
  "$@" >"$logfile" 2>&1
  local rc=$?
  if [ $rc -ne 0 ]; then
    log "  [FAIL rc=$rc] see $logfile"
  else
    log "  [OK] see $logfile"
  fi
}

# ===== 0. Wait for the in-flight reforzada backfill =====
log "Waiting for ESIOS reforzada backfill (PID 77282)..."
while ps -p 77282 >/dev/null 2>&1; do
  sleep 60
done
log "  ESIOS reforzada backfill exited."

# ===== 1. Parse + build indicators_all (now with reforzada) =====
stage "indicators_parse"  uv run python scripts/pipelines/esios/indicators/10_parse_indicators.py
stage "indicators_build"  uv run python scripts/pipelines/esios/indicators/20_build_indicators_all.py

# ===== 2. OMIE recent sync =====
# Daily-download families (marginalpdbc, marginalpibc): --recent-days 60 covers the gap
stage "omie_marginalpdbc_dl" uv run python scripts/pipelines/omie/mercado_diario/00_download_marginalpdbc.py --recent-days 60
stage "omie_marginalpdbc_parse" uv run python scripts/pipelines/omie/mercado_diario/10_parse_marginalpdbc.py
stage "omie_marginalpdbc_build" uv run python scripts/pipelines/omie/mercado_diario/20_build_marginalpdbc_all.py

stage "omie_marginalpibc_dl" uv run python scripts/pipelines/omie/mercado_intradiario_subastas/00_download_marginalpibc.py --recent-days 60
stage "omie_marginalpibc_parse" uv run python scripts/pipelines/omie/mercado_intradiario_subastas/10_parse_marginalpibc.py
stage "omie_marginalpibc_build" uv run python scripts/pipelines/omie/mercado_intradiario_subastas/20_build_marginalpibc_all.py

# Monthly-ZIP families: sync 2026-01 .. 2026-05 (caught up coverage gap)
stage "omie_det_sync"   uv run python scripts/pipelines/omie/mercado_diario/00_sync_det_zips.py --start-month 2026-01 --end-month 2026-05
stage "omie_det_parse"  uv run python scripts/pipelines/omie/mercado_diario/10_parse_det.py
stage "omie_det_build"  uv run python scripts/pipelines/omie/mercado_diario/20_build_det_all.py

stage "omie_idet_sync"  uv run python scripts/pipelines/omie/mercado_intradiario_subastas/00_sync_idet_zips.py --start-month 2026-01 --end-month 2026-05
stage "omie_idet_parse" uv run python scripts/pipelines/omie/mercado_intradiario_subastas/10_parse_idet.py
stage "omie_idet_build" uv run python scripts/pipelines/omie/mercado_intradiario_subastas/20_build_idet_all.py

stage "omie_pdbf_sync"  uv run python scripts/pipelines/omie/mercado_diario/00_sync_pdbf_zips.py --start-month 2026-02 --end-month 2026-05
stage "omie_pdbf_parse" uv run python scripts/pipelines/omie/mercado_diario/10_parse_pdbf.py
stage "omie_pdbf_build" uv run python scripts/pipelines/omie/mercado_diario/20_build_pdbf_all.py

stage "omie_phf_sync"   uv run python scripts/pipelines/omie/mercado_intradiario_subastas/00_sync_phf_zips.py --start-month 2026-01 --end-month 2026-05
stage "omie_phf_parse"  uv run python scripts/pipelines/omie/mercado_intradiario_subastas/10_parse_phf.py
stage "omie_phf_build"  uv run python scripts/pipelines/omie/mercado_intradiario_subastas/20_build_phf_all.py

stage "omie_trades_sync"  uv run python scripts/pipelines/omie/mercado_intradiario_continuo/00_sync_trades_zips.py --start-month 2018-06 --end-month 2026-05
stage "omie_trades_parse" uv run python scripts/pipelines/omie/mercado_intradiario_continuo/10_parse_trades.py
stage "omie_trades_build" uv run python scripts/pipelines/omie/mercado_intradiario_continuo/20_build_trades_all.py

# ===== 3. ESIOS recent sync =====
stage "esios_rt2_sync"  uv run python scripts/pipelines/esios/restricciones/00_sync_totalrp48preccierre.py --start-month 2026-04 --end-month 2026-05
stage "esios_rt2_parse" uv run python scripts/pipelines/esios/restricciones/10_parse_totalrp48preccierre.py
stage "esios_rt2_build" uv run python scripts/pipelines/esios/restricciones/20_build_totalrp48preccierre_all.py

stage "esios_curvas_afrr_sync"  uv run python scripts/pipelines/esios/reservas/00_sync_curvas_ofertas_afrr.py --start-date 2026-04-01 --end-date 2026-05-15
stage "esios_curvas_afrr_parse" uv run python scripts/pipelines/esios/reservas/10_parse_curvas_ofertas_afrr.py
stage "esios_curvas_afrr_build" uv run python scripts/pipelines/esios/reservas/20_build_curvas_ofertas_afrr_all.py

stage "esios_indisp_sync"  uv run python scripts/pipelines/esios/indisponibilidades/00_sync_indisponibilidades.py --start-date 2026-05-01 --end-date 2026-05-15 --cadence week --sleep 1
stage "esios_indisp_parse" uv run python scripts/pipelines/esios/indisponibilidades/10_parse_indisponibilidades.py
stage "esios_indisp_build" uv run python scripts/pipelines/esios/indisponibilidades/20_build_indisponibilidades_all.py

# liquicierre — ESIOS has ~2-month settlement lag, but try anyway (idempotent)
stage "esios_liquicierre_sync"  uv run python scripts/pipelines/esios/liquidaciones/00_sync_liquicierre.py --start-month 2026-03 --end-month 2026-04
stage "esios_liquicierre_parse" uv run python scripts/pipelines/esios/liquidaciones/10_parse_liquicierre.py
stage "esios_liquicierre_build" uv run python scripts/pipelines/esios/liquidaciones/20_build_liquicierre_all.py

# ===== 4. ENTSO-E full sync (idempotent) =====
stage "entsoe_sync_all" uv run python scripts/pipelines/entsoe/_sync_all.py --start 2024-01 --end 2026-05

# ===== 5. Summary =====
log "=== Overnight queue complete ==="
log "Summary: $SUMMARY"
log "Per-stage logs in $LOG_DIR/overnight_*_${TS}.log"
