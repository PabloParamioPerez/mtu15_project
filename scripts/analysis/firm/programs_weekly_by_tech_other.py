# STATUS: ALIVE
# LAST-AUDIT: 2026-06-09
# FEEDS: thesis/presentations/.../slides.tex --- appendix "other technologies"
#        program-cascade figure. Mirror layout: up-redispatch above 0,
#        down-redispatch / pumping load below 0. Same logic as
#        programs_weekly_by_tech.py but for the technologies NOT shown in the
#        body slide: Cogen, Coal, Biomass, small Hydro RES, Pump load,
#        Hybrid RES+storage.
#
# OUT: figures/thesis/fig_programs_by_tech_other_weekly.{pdf,png}

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))
from programs_weekly_by_tech import build_weekly, render, OUTDIR  # noqa: E402

import duckdb  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "src"))
from mtu.classification.units import firm_unit_panel  # noqa: E402

UNITS_CSV = Path(__file__).resolve().parents[3] / "data/external/omie_reference/lista_unidades.csv"

TECHS = ("Cogen", "Coal", "Biomass", "Hydro_RES", "Pump_load", "Hybrid_RES_storage")
TECH_LABELS = {
    "Cogen":              "Cogeneration (CHP)",
    "Coal":               "Coal",
    "Biomass":            "Biomass",
    "Hydro_RES":          "Small hydro (RES)",
    "Pump_load":          "Pump-storage load",
    "Hybrid_RES_storage": "Hybrid (renewable + storage)",
}


def main():
    units = firm_unit_panel(csv_path=str(UNITS_CSV), scheme="short", mode="primary_owner")
    keep = units[units["tech_group"].isin(list(TECHS))][["unit_code", "tech_group"]].rename(
        columns={"tech_group": "tech"})
    print(f"Unit coverage: {len(keep):,} units; per tech: "
          f"{keep.groupby('tech').size().to_dict()}", flush=True)

    con = duckdb.connect()
    con.execute("PRAGMA threads=4"); con.execute("SET memory_limit='5GB'")
    con.execute("SET preserve_insertion_order=false")

    weekly = build_weekly(con, keep, TECHS)
    OUTDIR.mkdir(parents=True, exist_ok=True)
    weekly.to_csv(OUTDIR / "fig_programs_by_tech_other_weekly.csv", index=False)
    render(weekly, TECHS, TECH_LABELS,
            fname="fig_programs_by_tech_other_weekly",
            suptitle="Other technologies --- weekly program cascade (supply above 0, consumption / down-redispatch below 0). "
                     "Pump-storage load is purely consumption (all below 0). Black line $=$ PHF net.")


if __name__ == "__main__":
    main()
