#!/usr/bin/env python3
"""
Replication driver --- MTU15/ISP15 granularity reforms thesis (Pablo Paramio Perez, CEMFI 2026).

Reproduces the ANALYTICAL RESULTS (regression CSVs under results/regressions/ and
figures under figures/) end-to-end. Those numbers are then entered into
thesis/paper/thesis.tex by hand (the paper has no auto-generated \\input tables).

PIPELINE STAGES
  0  Data download      OMIE (public) + ESIOS/ENTSO-E (need API keys in .env)
  1  Parse + build      raw -> data/processed/*.parquet
  2  Derived panels     processed -> data/derived/panels/*.parquet
  3  Analyses           panels -> results/regressions/*.csv  + figures/

  Stages 0-1 are AUTOMATICALLY SKIPPED when the processed tables already exist on
  disk (the data/raw, data/processed symlinks point at an external SSD). So a
  replicator who already has the processed parquet runs only stages 2-3, and
  re-running is idempotent and cheap. Stage 0 is only attempted when processed
  data is missing, and the ESIOS/ENTSO-E downloads then require credentials
  (copy .env.example -> .env and fill in). OMIE downloads need no key.

USAGE
  uv run python replication/run_replication.py            # run, skipping data stages if processed data is present
  uv run python replication/run_replication.py --list     # print the plan, run nothing
  uv run python replication/run_replication.py --from 2   # start at stage 2 (panels)
  uv run python replication/run_replication.py --only 3   # run only stage 3 (analyses)
  uv run python replication/run_replication.py --force-download   # re-download even if processed data exists

R scripts (BSTS / OLS) are invoked with `Rscript`; see replication/README.md for the
required R packages. Everything else runs under `uv run python`.
"""
from __future__ import annotations
import argparse
import glob
import os
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]

# A processed table that exists iff the heavy data stages have already run.
# (Used purely as the skip-if-present sentinel for stages 0-1.)
PROCESSED_SENTINELS = [
    "data/processed/omie/mercado_diario/ofertas/det_all.parquet",
    "data/processed/omie/mercado_diario/precios/marginalpdbc_all.parquet",
]

# Pipeline families, in run order. Each is self-contained through its 20_ step.
PIPELINE_FAMILIES = [
    "scripts/pipelines/omie/mercado_diario",
    "scripts/pipelines/omie/mercado_intradiario_subastas",
    "scripts/pipelines/omie/mercado_intradiario_continuo",
    "scripts/pipelines/esios/liquidaciones",
    "scripts/pipelines/esios/restricciones",
    "scripts/pipelines/esios/reservas",
    "scripts/pipelines/esios/indisponibilidades",
    "scripts/pipelines/esios/indicators",
    "scripts/pipelines/entsoe/generation",
    "scripts/pipelines/entsoe/load",
    "scripts/pipelines/entsoe/balancing",
    "scripts/pipelines/entsoe/prices",
    "scripts/pipelines/entsoe/transmission",
    "scripts/pipelines/entsoe/outages",
    "scripts/pipelines/external",
]

# Stage 3: the analysis scripts that produce the thesis's reported numbers/figures
# (the `% Source:` provenance recorded in thesis.tex). Run after panels (stage 2).
# Order: descriptive/builders that other steps read, then the headline estimators.
ANALYSIS_SCRIPTS = [
    # --- prices (Q1) ---
    "scripts/analysis/bid/ols_price_full_controls.R",
    "scripts/analysis/bid/ols_price_hourly.R",
    "scripts/analysis/bid/bsts_daily_year_interactions.R",
    "scripts/analysis/bid/bsts_daily_quadratic.R",
    # --- margin channel (residual-demand slope, markup) ---
    "scripts/analysis/bid/build_per_firm_residual_demand_slope.py",
    "scripts/analysis/bid/build_per_firm_strategic_markup.py",
    "scripts/analysis/bid/build_per_session_bmt_robustness.py",
    "scripts/analysis/bid/residual_demand_pre_vs_post.py",
    "scripts/analysis/bid/slope_change_critical_flat.py",
    "scripts/analysis/bid/ols_per_firm_b_residual.R",
    "scripts/analysis/bid/bsts_per_firm_b.R",
    # --- within-hour wedge dispersion (Q3) ---
    "scripts/analysis/bid/bsts_wedge_hour_class.R",
    # --- bid shape DiD (sigma_p / HHI / slope) ---
    "scripts/analysis/bid/mtu15_critical_flat_did.py",
    "scripts/analysis/bid/perfirm_sigma_hhi_did.py",
    "scripts/analysis/bid/spec_c_2024_placebo.py",
    "scripts/analysis/bid/spec_c_long_pre_ra_did.py",
    "scripts/analysis/bid/run_pre_only_placebo_p90.py",
    "scripts/analysis/bid/run_spec_c_did_p90_midday.py",
    "scripts/analysis/bid/bandwidth_robustness_did.py",
    "scripts/analysis/bid/offer_stacking_confounder.py",
    "scripts/analysis/bid/within_hour_dispersion_per_tech.py",
    "scripts/analysis/bid/claim_C_per_session_bid_shape.py",
    # --- imbalance (Q2) ---
    "scripts/analysis/bid/ols_imbalance.py",
    "scripts/analysis/bid/bsts_imbalance_penalty.R",
    "scripts/analysis/balancing/efficiency_gains_timeseries.py",
    # --- migration / continuous market ---
    "scripts/analysis/bid/bsts_ida_sell_share.R",
    "scripts/analysis/bid/bsts_continuous.R",
    "scripts/analysis/bid/claim_D_ida_activity_post_da15.py",
    # --- reforzada / ancillary layer ---
    "scripts/analysis/bid/bsts_ajuste_costs.R",
    # --- solar-trend robustness ---
    "scripts/analysis/bid/bsts_solar_year_coefficients.R",
    "scripts/analysis/bid/bsts_calibrated_solar.R",
    # --- figures ---
    "scripts/analysis/bid/bid_shape_parallel_trends_fig.py",
    "scripts/analysis/bid/fig_ccgt_pt_main.py",
    "scripts/analysis/bid/fig_bid_curves_by_tech.py",
    "scripts/analysis/bid/fig_buy_vs_sell_curves.py",
    "scripts/analysis/bid/fig_ccgt_offer_type.py",
    "scripts/analysis/bid/fig_parallel_trends_sigma_p_per_session.py",
    "scripts/analysis/firm/programs_weekly_by_tech.py",
    "scripts/analysis/firm/programs_weekly_by_tech_other.py",
]


def processed_present() -> bool:
    """True iff the processed-data sentinels resolve to non-empty files."""
    for rel in PROCESSED_SENTINELS:
        p = REPO / rel
        try:
            if not (p.exists() and p.stat().st_size > 0):
                return False
        except OSError:
            return False
    return True


def numbered(family_dir: Path, prefix: str) -> list[Path]:
    return sorted(family_dir.glob(f"{prefix}_*.py"))


def cmd_for(script: Path) -> list[str]:
    if script.suffix == ".R":
        return ["Rscript", str(script)]
    return ["uv", "run", "python", str(script)]


def run(script: Path, dry: bool) -> bool:
    rel = script.relative_to(REPO)
    print(f"  -> {rel}", flush=True)
    if dry:
        return True
    try:
        subprocess.run(cmd_for(script), cwd=REPO, check=True)
        return True
    except subprocess.CalledProcessError as e:
        print(f"     !! FAILED ({e.returncode}); continuing. Re-run this step manually to debug.", flush=True)
        return False


def plan_stage_0_1() -> list[tuple[str, Path]]:
    steps: list[tuple[str, Path]] = []
    for fam in PIPELINE_FAMILIES:
        d = REPO / fam
        if not d.is_dir():
            continue
        for s in numbered(d, "00"):
            steps.append(("0", s))
    for fam in PIPELINE_FAMILIES:
        d = REPO / fam
        if not d.is_dir():
            continue
        for s in numbered(d, "10"):
            steps.append(("1", s))
        for s in numbered(d, "20"):
            steps.append(("1", s))
    return steps


def plan_stage_2() -> list[Path]:
    builders: list[Path] = []
    for sub in ("panels", "bid"):
        builders += sorted((REPO / "scripts/analysis" / sub).glob("build_*.py"))
    return builders


def plan_stage_3() -> list[Path]:
    return [REPO / s for s in ANALYSIS_SCRIPTS]


def main() -> int:
    ap = argparse.ArgumentParser(description="Replication driver (MTU15 thesis).")
    ap.add_argument("--list", action="store_true", help="print the plan and exit")
    ap.add_argument("--from", dest="from_stage", type=int, default=0, choices=[0, 1, 2, 3])
    ap.add_argument("--only", type=int, default=None, choices=[0, 1, 2, 3])
    ap.add_argument("--force-download", action="store_true",
                    help="run stages 0-1 even if processed data is already present")
    args = ap.parse_args()

    have_data = processed_present()
    skip_data = have_data and not args.force_download

    def active(stage: int) -> bool:
        if args.only is not None:
            return stage == args.only
        return stage >= args.from_stage

    # Assemble the plan.
    stage01 = plan_stage_0_1()
    stage2 = plan_stage_2()
    stage3 = plan_stage_3()

    print("=" * 72)
    print("MTU15 thesis --- replication plan")
    print(f"  repo: {REPO}")
    print(f"  processed data present: {have_data}  ->  data stages (0-1) "
          f"{'SKIPPED' if skip_data else 'WILL RUN'}")
    print("=" * 72)

    if args.list:
        print("\n[stage 0-1] data download + parse/build (skipped if data present):")
        for st, s in stage01:
            print(f"  ({st}) {s.relative_to(REPO)}")
        print("\n[stage 2] derived panels:")
        for s in stage2:
            print(f"  {s.relative_to(REPO)}")
        print("\n[stage 3] analyses -> results + figures:")
        for s in stage3:
            print(f"  {s.relative_to(REPO)}")
        return 0

    failures: list[str] = []

    # Stage 0-1
    if (active(0) or active(1)):
        if skip_data:
            print("\n[stage 0-1] processed data present -> skipping download + parse/build.")
        else:
            if not (REPO / ".env").exists():
                print("\n[stage 0-1] processed data missing AND no .env found.")
                print("            ESIOS/ENTSO-E downloads need credentials:")
                print("            cp replication/.env.example .env  &&  edit .env")
                print("            (OMIE is public; you can still run the OMIE 00_ steps.)")
            print("\n[stage 0-1] data download + parse/build:")
            for st, s in stage01:
                if not active(int(st)):
                    continue
                if not run(s, dry=False):
                    failures.append(str(s.relative_to(REPO)))

    # Stage 2
    if active(2):
        print("\n[stage 2] derived panels:")
        for s in stage2:
            if not run(s, dry=False):
                failures.append(str(s.relative_to(REPO)))

    # Stage 3
    if active(3):
        print("\n[stage 3] analyses -> results + figures:")
        for s in stage3:
            if not s.exists():
                print(f"  (missing, skipped) {s.relative_to(REPO)}")
                continue
            if not run(s, dry=False):
                failures.append(str(s.relative_to(REPO)))

    print("\n" + "=" * 72)
    if failures:
        print(f"Completed with {len(failures)} failed step(s):")
        for f in failures:
            print(f"  - {f}")
        print("Re-run any failed step directly to see its error.")
        return 1
    print("Done. Results in results/regressions/ and figures/; enter the headline")
    print("numbers into thesis/paper/thesis.tex by hand.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
