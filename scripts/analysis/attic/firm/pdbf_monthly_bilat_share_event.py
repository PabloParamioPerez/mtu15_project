# STATUS: ALIVE
# LAST-AUDIT: 2026-05-02
# FEEDS: F26 mechanism event-study — when do bilateral-share structural breaks happen?
# CLAIM: F26's auction-channel growth is sharp at 2024-06-14 IDA reform AND
#        not at other reform dates → SIDC/IDA-reform-specific.
"""Monthly volume-weighted bilat_share event study, Big-4 × tech.

For Big-4 nuclear/CCGT/hydro, compute monthly volume-weighted bilat_share:
  bilat_share_m = SUM(bilat_mwh) / SUM(total_mwh)  (firm-month aggregate)

Then plot the monthly series with reform-date verticals:
  - 2024-06-14: SIDC/IDA reform (3-sess starts)
  - 2024-12-01: ISP15-win starts
  - 2025-03-19: MTU15-IDA (Rule 28.8 ends)
  - 2025-04-28: blackout / reforzada starts
  - 2025-10-01: MTU15-DA

Output:
  results/regressions/pdbf_monthly_bilat_share.csv
  figures/working/pdbf_monthly_bilat_share.png
"""
from __future__ import annotations

from pathlib import Path

import duckdb
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd

PROJECT = Path(__file__).resolve().parents[3]
PDBF    = PROJECT / "data" / "processed" / "omie" / "mercado_diario" / "programas" / "pdbf_all.parquet"
PDBCE   = PROJECT / "data" / "processed" / "omie" / "mercado_diario" / "programas" / "pdbce_all.parquet"
LISTA   = PROJECT / "data" / "external" / "omie_reference" / "lista_unidades.csv"

OUT_CSV = PROJECT / "results" / "regressions" / "pdbf_monthly_bilat_share.csv"
OUT_PNG = PROJECT / "figures" / "working" / "pdbf_monthly_bilat_share.png"

REFORM_DATES = {
    "IDA reform\n(SIDC, 3-sess)": "2024-06-14",
    "ISP15-win":                  "2024-12-01",
    "MTU15-IDA\n(Rule 28.8 end)": "2025-03-19",
    "Blackout\nreforzada":         "2025-04-28",
    "MTU15-DA":                   "2025-10-01",
}


def main() -> None:
    con = duckdb.connect()
    con.execute("SET memory_limit='8GB'")
    con.execute("SET threads=4")

    print("[setup] firm + tech mapping…", flush=True)
    firms = con.execute(f"""
        SELECT unit_code, grupo_empresarial AS firm FROM (
          SELECT unit_code, grupo_empresarial,
                 ROW_NUMBER() OVER (PARTITION BY unit_code ORDER BY date DESC) AS rn
          FROM '{PDBCE}' WHERE grupo_empresarial IS NOT NULL) WHERE rn = 1
    """).df()
    lista = pd.read_csv(LISTA)[["unit_code", "technology"]]
    map_uf = firms.merge(lista, on="unit_code", how="left")

    def tech_group(t):
        if not isinstance(t, str): return "Other"
        tl = t.lower()
        if "gas" in tl or "ciclo" in tl: return "CCGT"
        if "nuclear" in tl: return "Nuclear"
        if "ombeo" in tl or "idráulica" in tl: return "Hydro"
        return "Other"

    map_uf["tech_group"] = map_uf["technology"].apply(tech_group)
    con.register("uf", map_uf[["unit_code", "firm", "tech_group"]])

    print("[panel] monthly Big-4 × tech volume-weighted bilat_share…", flush=True)
    monthly = con.execute(f"""
        SELECT date_trunc('month', CAST(p.date AS DATE)) AS month,
               uf.tech_group,
               SUM(CASE WHEN p.offer_type = 1 AND p.assigned_power_mw > 0
                        THEN p.assigned_power_mw * p.mtu_minutes / 60.0
                        ELSE 0 END) / 1000.0 AS auction_GWh,
               SUM(CASE WHEN p.offer_type = 4 AND p.assigned_power_mw > 0
                        THEN p.assigned_power_mw * p.mtu_minutes / 60.0
                        ELSE 0 END) / 1000.0 AS bilat_GWh
        FROM '{PDBF}' p JOIN uf USING (unit_code)
        WHERE uf.firm IN ('IB','GE','GN','HC')
          AND uf.tech_group IN ('Nuclear','Hydro','CCGT')
        GROUP BY 1, 2
        ORDER BY 1, 2
    """).df()
    monthly["month"] = pd.to_datetime(monthly["month"])
    monthly["total_GWh"] = monthly["auction_GWh"] + monthly["bilat_GWh"]
    monthly = monthly[monthly["total_GWh"] > 0].copy()
    monthly["bilat_share"] = monthly["bilat_GWh"] / monthly["total_GWh"]
    print(f"   monthly rows: {len(monthly):,}; "
          f"months: {monthly.month.min().date()} → {monthly.month.max().date()}",
          flush=True)

    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    monthly.to_csv(OUT_CSV, index=False)

    # Plot
    fig, axes = plt.subplots(3, 1, figsize=(12, 10), sharex=True)
    techs = ["Nuclear", "Hydro", "CCGT"]
    colors = {"Nuclear": "tab:blue", "Hydro": "tab:green", "CCGT": "tab:red"}
    for ax, tech in zip(axes, techs):
        sub = monthly[monthly.tech_group == tech].sort_values("month")
        ax.plot(sub["month"], sub["bilat_share"], "-o", color=colors[tech],
                ms=3, lw=1.2, label=tech)
        ax.set_ylabel(f"{tech}\nbilat_share")
        ax.set_ylim(-0.05, 1.05)
        ax.grid(alpha=0.3)
        for label, d in REFORM_DATES.items():
            ax.axvline(pd.Timestamp(d), color="black", lw=0.8, ls="--", alpha=0.5)
        # Top panel: annotate reform labels
        if tech == "Nuclear":
            for label, d in REFORM_DATES.items():
                ax.text(pd.Timestamp(d), 1.08, label, rotation=0,
                        ha="center", va="bottom", fontsize=7)
    axes[-1].set_xlabel("Month")
    axes[-1].xaxis.set_major_locator(mdates.YearLocator())
    axes[-1].xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
    fig.suptitle(
        "Big-4 monthly volume-weighted bilat_share by tech (PDBF, 2018–2026)\n"
        "Vertical lines = reform dates",
        fontsize=11)
    plt.tight_layout()
    OUT_PNG.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(OUT_PNG, dpi=110, bbox_inches="tight")
    plt.close()
    print(f"wrote {OUT_PNG}")

    # Console summary: pre/post each reform date for nuclear
    print("\n=== Nuclear monthly volume-weighted bilat_share, ±3 months around each reform ===")
    nuc = monthly[monthly.tech_group == "Nuclear"].set_index("month").sort_index()
    for label, d in REFORM_DATES.items():
        d_ts = pd.Timestamp(d)
        pre = nuc.loc[(nuc.index < d_ts) & (nuc.index >= d_ts - pd.DateOffset(months=3)),
                      "bilat_share"]
        post = nuc.loc[(nuc.index >= d_ts) & (nuc.index < d_ts + pd.DateOffset(months=3)),
                       "bilat_share"]
        if len(pre) and len(post):
            print(f"  {label.replace(chr(10),' '):35s} pre 3M mean = {pre.mean():.3f}  "
                  f"post 3M mean = {post.mean():.3f}  Δ = {(post.mean()-pre.mean())*100:+.1f}pp")

    print("\nDone.")


if __name__ == "__main__":
    main()
