"""Canonical ENTSO-E full-window sync for the mtu15 thesis project.

Idempotent — running this on a fresh machine reproduces every raw file
under data/raw/entsoe/. Any file already present is skipped (size > 0
check), so re-running after a partial sync resumes cleanly.

Replaces the one-shot _resync_failed.sh, _resync_v2.sh, _resync_v3.sh
that were used during a one-off catch-up download (now in
_archive/). All datasets here are downloaded by composing _generic_sync.py
or the existing per-script helpers, with the rate-limit pacing built in.

Coverage (~24 datasets):
  - A65 actual load + D-1 forecast + week-ahead forecast
  - A71 system gen forecast (D-1)
  - A75 actual gen per type — system aggregate (all 20 PSR types)
  - A75 wind+solar via existing 00_sync_wind_solar_actual.py
  - A73 per-unit gen for 5 PSR types: B04 CCGT, B10 pumped, B11 RoR,
    B12 reservoir, B14 nuclear
  - A44 day-ahead prices: ES (OMIE — separate pipeline), FR/DE/PT
  - A85 imbalance prices ES + FR
  - A86 imbalance volumes ES (existing 00_sync_imbalance.py)
  - A24 aggregated balancing bids ES (mFRR processType A47)
  - A11/A09/A61 cross-border physical/scheduled/NTC ES↔FR + ES↔PT
  - A80 generation unavailability — planned (B53) + forced (B54);
    forced is sparse, ENTSO-E publishes only ~28 of 100 months for ES
  - A68 installed capacity (existing 00_sync_installed_capacity.py)

Usage:
  uv run python scripts/pipelines/entsoe/_sync_all.py
  uv run python scripts/pipelines/entsoe/_sync_all.py --start 2024-01 --end 2025-12

Each stage prints a one-line summary. Skip a stage by editing STAGES below.
"""
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[3]
GS = ["uv", "run", "python", str(PROJECT_ROOT / "scripts/pipelines/entsoe/_generic_sync.py")]

ES = "10YES-REE------0"
FR = "10YFR-RTE------C"
DE = "10Y1001A1001A82H"
PT = "10YPT-REN------W"


def run_generic(args: list[str], label: str) -> None:
    print(f"=== {label} ===", flush=True)
    cmd = GS + args + ["--quiet"]
    subprocess.run(cmd, check=False, cwd=PROJECT_ROOT)


def run_script(rel_path: str, extra: list[str], label: str) -> None:
    print(f"=== {label} ===", flush=True)
    cmd = ["uv", "run", "python", str(PROJECT_ROOT / rel_path)] + extra
    subprocess.run(cmd, check=False, cwd=PROJECT_ROOT)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--start", default="2018-01")
    p.add_argument("--end", default="2026-04")
    p.add_argument("--only", default=None,
                   help="Comma-separated stage labels to run (e.g. 'A24,A85_FR'). Default: all.")
    args = p.parse_args()

    s, e = args.start, args.end
    only = set(args.only.split(",")) if args.only else None

    stages = [
        # === Load (A65) ===
        ("A65_actual",       lambda: run_generic(
            ["--doc-type", "A65", "--process-type", "A16",
             "--start-month", s, "--end-month", e,
             "--subdir", "load_actual",
             "--out-biz-domain", ES],
            "A65 actual load")),
        ("A65_forecast_da",  lambda: run_generic(
            ["--doc-type", "A65", "--process-type", "A01",
             "--start-month", s, "--end-month", e,
             "--subdir", "load_forecast_da",
             "--out-biz-domain", ES],
            "A65 D-1 forecast load")),
        ("A65_forecast_wa",  lambda: run_generic(
            ["--doc-type", "A65", "--process-type", "A31",
             "--start-month", s, "--end-month", e,
             "--subdir", "load_forecast_wa",
             "--out-biz-domain", ES],
            "A65 week-ahead forecast load")),

        # === Generation forecast / actual (system) ===
        ("A71",              lambda: run_generic(
            ["--doc-type", "A71", "--process-type", "A01",
             "--start-month", s, "--end-month", e,
             "--subdir", "generation_forecast_da",
             "--in-domain", ES],
            "A71 D-1 generation forecast")),
        ("A75_system",       lambda: run_generic(
            ["--doc-type", "A75", "--process-type", "A16",
             "--start-month", s, "--end-month", e,
             "--subdir", "generation_actual_all",
             "--in-domain", ES],
            "A75 system actual gen by type (all 20 PSR)")),

        # === A73 per-unit gen by PSR type ===
        ("A73_B04_CCGT",     lambda: run_script(
            "scripts/pipelines/entsoe/generation/00_sync_a73_nuclear.py",
            ["--start-month", s, "--end-month", e, "--psr-type", "B04"],
            "A73 CCGT per-unit (B04)")),
        ("A73_B10_PS",       lambda: run_script(
            "scripts/pipelines/entsoe/generation/00_sync_a73_nuclear.py",
            ["--start-month", s, "--end-month", e, "--psr-type", "B10"],
            "A73 pumped storage per-unit (B10)")),
        ("A73_B11_RoR",      lambda: run_script(
            "scripts/pipelines/entsoe/generation/00_sync_a73_nuclear.py",
            ["--start-month", s, "--end-month", e, "--psr-type", "B11"],
            "A73 run-of-river per-unit (B11)")),
        ("A73_B12_RES",      lambda: run_script(
            "scripts/pipelines/entsoe/generation/00_sync_a73_nuclear.py",
            ["--start-month", s, "--end-month", e, "--psr-type", "B12"],
            "A73 hydro reservoir per-unit (B12)")),
        ("A73_B14_NUC",      lambda: run_script(
            "scripts/pipelines/entsoe/generation/00_sync_a73_nuclear.py",
            ["--start-month", s, "--end-month", e, "--psr-type", "B14"],
            "A73 nuclear per-unit (B14)")),

        # === Existing wind/solar pipelines ===
        ("WS_actual",        lambda: run_script(
            "scripts/pipelines/entsoe/generation/00_sync_wind_solar_actual.py",
            ["--start-month", s, "--end-month", e],
            "Wind+solar actual (A75 B16+B19)")),
        ("WS_forecast_da",   lambda: run_script(
            "scripts/pipelines/entsoe/generation/00_sync_wind_solar_forecast.py",
            ["--start-month", s, "--end-month", e],
            "Wind+solar forecast D-1 (A69 process A01)")),
        ("WS_forecast_id",   lambda: run_script(
            "scripts/pipelines/entsoe/generation/00_sync_wind_solar_intraday_forecast.py",
            ["--start-month", s, "--end-month", e],
            "Wind+solar intraday forecast (A69 process A18)")),

        # === Day-ahead prices (FR/DE/PT) ===
        ("A44_FR",           lambda: run_generic(
            ["--doc-type", "A44",
             "--start-month", s, "--end-month", e,
             "--subdir", "prices/fr_da",
             "--in-domain", FR, "--out-domain", FR],
            "A44 FR day-ahead prices")),
        ("A44_DE",           lambda: run_generic(
            ["--doc-type", "A44",
             "--start-month", s, "--end-month", e,
             "--subdir", "prices_da_de",
             "--in-domain", DE, "--out-domain", DE],
            "A44 DE day-ahead prices")),
        ("A44_PT",           lambda: run_generic(
            ["--doc-type", "A44",
             "--start-month", s, "--end-month", e,
             "--subdir", "prices_da_pt",
             "--in-domain", PT, "--out-domain", PT],
            "A44 PT day-ahead prices")),

        # === Balancing (A85, A86, A24) ===
        ("A85_ES_prices",    lambda: run_script(
            "scripts/pipelines/entsoe/balancing/00_sync_imbalance.py",
            ["--kind", "prices", "--start-month", s, "--end-month", e],
            "A85 ES imbalance prices")),
        ("A85_FR_prices",    lambda: run_generic(
            ["--doc-type", "A85",
             "--start-month", s, "--end-month", e,
             "--subdir", "imbalance_prices_fr",
             "--control-domain", FR],
            "A85 FR imbalance prices")),
        ("A86_ES_volumes",   lambda: run_script(
            "scripts/pipelines/entsoe/balancing/00_sync_imbalance.py",
            ["--kind", "volumes", "--start-month", s, "--end-month", e],
            "A86 ES imbalance volumes")),
        ("A24_ES_bids",      lambda: run_generic(
            ["--doc-type", "A24", "--process-type", "A47",
             "--start-month", s, "--end-month", e,
             "--subdir", "balancing_aggregated_bids",
             "--area-domain", ES],
            "A24 ES aggregated balancing bids (mFRR A47)")),
        ("A37_ES_activations", lambda: run_script(
            "scripts/pipelines/entsoe/balancing/00_sync_activations.py",
            ["--start-month", s, "--end-month", e],
            "A37/A88 activations + activated prices ES")),
        ("A87_ES_financial", lambda: run_script(
            "scripts/pipelines/entsoe/balancing/00_sync_financial_balance.py",
            ["--start-month", s, "--end-month", e],
            "A87 financial balance ES")),

        # === Cross-border (A11 physical, A09 scheduled, A61 NTC) ===
        ("A11_FR_to_ES",     lambda: run_generic(
            ["--doc-type", "A11", "--start-month", s, "--end-month", e,
             "--subdir", "flows_physical_fr_to_es",
             "--in-domain", FR, "--out-domain", ES],
            "A11 FR->ES physical")),
        ("A11_ES_to_FR",     lambda: run_generic(
            ["--doc-type", "A11", "--start-month", s, "--end-month", e,
             "--subdir", "flows_physical_es_to_fr",
             "--in-domain", ES, "--out-domain", FR],
            "A11 ES->FR physical")),
        ("A11_PT_to_ES",     lambda: run_generic(
            ["--doc-type", "A11", "--start-month", s, "--end-month", e,
             "--subdir", "flows_physical_pt_to_es",
             "--in-domain", PT, "--out-domain", ES],
            "A11 PT->ES physical")),
        ("A11_ES_to_PT",     lambda: run_generic(
            ["--doc-type", "A11", "--start-month", s, "--end-month", e,
             "--subdir", "flows_physical_es_to_pt",
             "--in-domain", ES, "--out-domain", PT],
            "A11 ES->PT physical")),
        ("A09_FR_to_ES",     lambda: run_generic(
            ["--doc-type", "A09", "--start-month", s, "--end-month", e,
             "--subdir", "flows_scheduled_fr_to_es",
             "--in-domain", FR, "--out-domain", ES,
             "--extra-param", "contract_MarketAgreement.Type=A01"],
            "A09 FR->ES scheduled DA")),
        ("A09_ES_to_FR",     lambda: run_generic(
            ["--doc-type", "A09", "--start-month", s, "--end-month", e,
             "--subdir", "flows_scheduled_es_to_fr",
             "--in-domain", ES, "--out-domain", FR,
             "--extra-param", "contract_MarketAgreement.Type=A01"],
            "A09 ES->FR scheduled DA")),
        ("A09_PT_to_ES",     lambda: run_generic(
            ["--doc-type", "A09", "--start-month", s, "--end-month", e,
             "--subdir", "flows_scheduled_pt_to_es",
             "--in-domain", PT, "--out-domain", ES,
             "--extra-param", "contract_MarketAgreement.Type=A01"],
            "A09 PT->ES scheduled DA")),
        ("A09_ES_to_PT",     lambda: run_generic(
            ["--doc-type", "A09", "--start-month", s, "--end-month", e,
             "--subdir", "flows_scheduled_es_to_pt",
             "--in-domain", ES, "--out-domain", PT,
             "--extra-param", "contract_MarketAgreement.Type=A01"],
            "A09 ES->PT scheduled DA")),
        ("A61_FR_to_ES",     lambda: run_generic(
            ["--doc-type", "A61", "--start-month", s, "--end-month", e,
             "--subdir", "ntc_fr_to_es",
             "--in-domain", FR, "--out-domain", ES,
             "--extra-param", "contract_MarketAgreement.Type=A01"],
            "A61 NTC FR->ES")),
        ("A61_ES_to_FR",     lambda: run_generic(
            ["--doc-type", "A61", "--start-month", s, "--end-month", e,
             "--subdir", "ntc_es_to_fr",
             "--in-domain", ES, "--out-domain", FR,
             "--extra-param", "contract_MarketAgreement.Type=A01"],
            "A61 NTC ES->FR")),
        ("A61_PT_to_ES",     lambda: run_generic(
            ["--doc-type", "A61", "--start-month", s, "--end-month", e,
             "--subdir", "ntc_pt_to_es",
             "--in-domain", PT, "--out-domain", ES,
             "--extra-param", "contract_MarketAgreement.Type=A01"],
            "A61 NTC PT->ES")),
        ("A61_ES_to_PT",     lambda: run_generic(
            ["--doc-type", "A61", "--start-month", s, "--end-month", e,
             "--subdir", "ntc_es_to_pt",
             "--in-domain", ES, "--out-domain", PT,
             "--extra-param", "contract_MarketAgreement.Type=A01"],
            "A61 NTC ES->PT")),

        # === Outages (A80 planned + forced) ===
        ("A80_planned",      lambda: run_generic(
            ["--doc-type", "A80",
             "--start-month", s, "--end-month", e,
             "--subdir", "outages_generation_planned",
             "--biz-domain", ES,
             "--extra-param", "businessType=A53"],
            "A80 planned outages")),
        ("A80_forced",       lambda: run_generic(
            ["--doc-type", "A80",
             "--start-month", s, "--end-month", e,
             "--subdir", "outages_generation_forced",
             "--biz-domain", ES,
             "--extra-param", "businessType=A54"],
            "A80 forced outages (sparse — ENTSO-E only publishes ~28% for ES)")),

        # === Capacity (A68) ===
        ("A68_capacity",     lambda: run_script(
            "scripts/pipelines/entsoe/generation/00_sync_installed_capacity.py",
            ["--start-year", s.split("-")[0], "--end-year", e.split("-")[0]],
            "A68 installed capacity (annual)")),
    ]

    n_run = 0
    for label, fn in stages:
        if only and label not in only:
            continue
        fn()
        n_run += 1

    print(f"\nDone. {n_run} stages executed.")


if __name__ == "__main__":
    sys.exit(main() or 0)
