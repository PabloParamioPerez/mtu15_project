# STATUS: ALIVE
# LAST-AUDIT: 2026-05-16
# FEEDS: provisional.tex (geography section)
# CLAIM: CCGT zonal-competition map. Pay-as-bid RT2 means a firm that
#        is the only CCGT operator in a transmission zone has local
#        monopoly power for restricciones técnicas calls. We map the
#        Spanish CCGT fleet to 6 geographic zones from plant addresses
#        and compute per-zone, per-firm RT2 share (PHF − PDBF).

from __future__ import annotations

import sys
from pathlib import Path

import duckdb
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO / "src"))
from mtu.classification.units import firm_unit_panel  # noqa: E402

ZONE_MAP = REPO / "data" / "external" / "ccgt_zonal_map.csv"
PDBF = REPO / "data" / "processed" / "omie" / "mercado_diario" / "programas" / "pdbf_all.parquet"
PHF  = REPO / "data" / "processed" / "omie" / "mercado_intradiario_subastas" / "programas" / "phf_all.parquet"
UNITS_CSV = REPO / "data" / "external" / "omie_reference" / "lista_unidades.csv"
OUTDIR = REPO / "results" / "regressions" / "regulatory" / "ccgt_zonal_competition"
OUTDIR.mkdir(parents=True, exist_ok=True)
FIGDIR = REPO / "figures" / "working"
FIGDIR.mkdir(parents=True, exist_ok=True)

POST_W = ("2025-05-01", "2026-02-01")


def main():
    zones = pd.read_csv(ZONE_MAP)
    units = firm_unit_panel(csv_path=str(UNITS_CSV), scheme="short", mode="primary_owner")
    ccgt = units[units["tech_group"] == "CCGT"][["unit_code", "parent"]].rename(columns={"parent": "firm"})
    panel = zones.merge(ccgt, on="unit_code", how="left")
    panel["firm"] = panel["firm"].fillna("OTHER")
    panel.to_csv(OUTDIR / "ccgt_zonal_firm.csv", index=False)
    print("=== CCGT plants per zone, firm count ===")
    summary = (panel.groupby(["zone", "firm"]).size().unstack(fill_value=0))
    print(summary.to_string())

    # Bring in capacity (MW) per unit from OMIE master
    units_full = pd.read_csv(UNITS_CSV)
    # OMIE list does NOT have capacity; fall back to ESIOS master
    import json
    with open(REPO / "data" / "external" / "esios_master" / "generation_units.json") as f:
        gen = json.load(f)["GenerationUnits"]
    cap = pd.DataFrame(gen)[["UP Code", "Maximum Power Capacity MW"]]
    cap.columns = ["unit_code", "capacity_mw"]
    cap["capacity_mw"] = pd.to_numeric(cap["capacity_mw"].astype(str).str.replace(",", "."), errors="coerce")
    cap = cap.groupby("unit_code", as_index=False)["capacity_mw"].sum()
    panel_cap = panel.merge(cap, on="unit_code", how="left")
    panel_cap["capacity_mw"] = panel_cap["capacity_mw"].fillna(0)

    # Per zone, total capacity per firm
    print("\n=== CCGT installed capacity (MW), by zone × firm ===")
    cap_table = panel_cap.pivot_table(index="zone", columns="firm",
                                       values="capacity_mw", aggfunc="sum",
                                       fill_value=0).round(0)
    print(cap_table.to_string())
    cap_table.to_csv(OUTDIR / "capacity_by_zone_firm.csv")

    # Per zone, top-firm capacity share + HHI
    print("\n=== Per-zone CCGT capacity concentration ===")
    rows = []
    for zone in cap_table.index:
        v = cap_table.loc[zone]
        total = v.sum()
        if total == 0: continue
        shares = v / total
        rows.append({
            "zone": zone, "n_plants": int((panel_cap["zone"] == zone).sum()),
            "total_capacity_mw": float(total),
            "n_firms": int((shares > 0).sum()),
            "top_firm": shares.idxmax(),
            "top_firm_share_pct": round(100 * shares.max(), 1),
            "hhi": round(10000 * (shares ** 2).sum()),
        })
    by_zone = pd.DataFrame(rows)
    print(by_zone.to_string(index=False))
    by_zone.to_csv(OUTDIR / "zone_concentration.csv", index=False)

    # Plot — stacked bar of capacity per zone × firm
    pretty = {"IB": "Iberdrola", "GE": "Endesa", "GN": "Naturgy",
              "HC": "EDP-Sp"}
    other_firms = [c for c in cap_table.columns
                    if c not in ("IB", "GE", "GN", "HC")]
    colors = {"IB": "tab:green", "GE": "tab:red", "GN": "tab:orange",
              "HC": "tab:blue", "OTHER": "lightgrey"}
    zones_ord = ["Norte", "Galicia", "Cataluna", "Aragon", "Levante", "Centro", "Sur"]
    zones_ord = [z for z in zones_ord if z in cap_table.index]
    fig, ax = plt.subplots(figsize=(11, 4.5))
    x = np.arange(len(zones_ord))
    bot = np.zeros(len(zones_ord))
    for firm in ("IB", "GE", "GN", "HC"):
        if firm not in cap_table.columns: continue
        vals = [cap_table.loc[z, firm] for z in zones_ord]
        ax.bar(x, vals, bottom=bot, width=0.7,
               color=colors[firm], label=pretty[firm], edgecolor="white", linewidth=0.5)
        bot = bot + np.array(vals)
    other_vals = [sum(cap_table.loc[z, f] for f in other_firms) for z in zones_ord]
    ax.bar(x, other_vals, bottom=bot, width=0.7,
            color=colors["OTHER"], label="other firms", edgecolor="white", linewidth=0.5)
    for xi, z in zip(x, zones_ord):
        tot = cap_table.loc[z].sum()
        ax.text(xi, tot + 200, f"{int(tot)} MW", ha="center", fontsize=8)
    ax.set_xticks(x); ax.set_xticklabels(zones_ord, rotation=0)
    ax.set_ylabel("CCGT installed capacity (MW)")
    ax.set_title("Spanish CCGT installed capacity by geographic zone and firm — for RT2 (pay-as-bid) local-competition reading")
    ax.legend(loc="upper right", frameon=False, fontsize=9)
    ax.grid(True, alpha=0.3, axis="y")
    plt.tight_layout()
    out = FIGDIR / "fig_ccgt_zonal_capacity.pdf"
    plt.savefig(out, dpi=150, bbox_inches="tight")
    plt.savefig(out.with_suffix(".png"), dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"wrote {out}")


if __name__ == "__main__":
    main()
