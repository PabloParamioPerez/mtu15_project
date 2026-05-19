# STATUS: ALIVE
# LAST-AUDIT: 2026-05-19
# CLAIM: Per-technology within-hour generation variance (sigma_within MW)
#        from ENTSO-E A75 (15-min actual generation per PSR type). Under
#        MTU60 settlement these within-hour deviations averaged out and
#        produced no imbalance cost; under MTU15 settlement each 15-min
#        cell is settled separately and the per-MW variance becomes a
#        direct imbalance-exposure proxy. We multiply sigma_within by
#        the dual-price spread per regime to get an implied per-hour
#        imbalance cost per tech.
# FEEDS: descriptive_facts.tex (ISP15 differential exposure across techs)
# OUT:
#   results/regressions/balancing/within_hour_variance/per_tech_sigma.csv
#   results/regressions/balancing/within_hour_variance/per_tech_sigma.tex
#
# Data: ENTSO-E A75 by PSR type, 15-min ISP resolution, Jan 2024 onward.

from __future__ import annotations
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
A75 = REPO / "data/processed/entsoe/generation/gen_actual_per_type_all.parquet"
OUT_DIR = REPO / "results/regressions/balancing/within_hour_variance"

PSR_LABEL = {
    "B16": "Solar PV",
    "B19": "Wind onshore",
    "B10": "Hydro pumped storage",
    "B11": "Hydro run-of-river",
    "B12": "Hydro reservoir",
    "B04": "Fossil gas (CCGT)",
    "B14": "Nuclear",
    "B01": "Biomass",
    "B17": "Waste",
    "B05": "Coal",
    "B06": "Oil",
}

# Per-regime dual-price spread (cobro-up, pago-dn), from ESIOS 686/687 as
# documented in descriptive_facts.tex Sec efficiency-gains. Spread = pago-dn - cobro-up
# (= per-MWh wedge between selling deficit and buying surplus, the asymmetric exposure
# being on the wrong side of imbalance).
REGIME_DUAL_SPREAD = {
    "pre-ISP15 (2024 Jan-Nov)": (40.0, 85.0),
    "ISP15-win (Dec24-Mar25)":  (70.0, 110.0),
    "MTU15-IDA postblk":        (95.0, 130.0),
    "MTU15-DA (DA15/ID15)":     (90.0, 120.0),
}


def regime_of(date) -> str:
    d = pd.to_datetime(date).date()
    if d < pd.Timestamp("2024-12-01").date():
        return "pre-ISP15 (2024 Jan-Nov)"
    if d < pd.Timestamp("2025-04-28").date():
        return "ISP15-win (Dec24-Mar25)"
    if d < pd.Timestamp("2025-10-01").date():
        return "MTU15-IDA postblk"
    return "MTU15-DA (DA15/ID15)"


REGIME_ORDER = list(REGIME_DUAL_SPREAD.keys())


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()
    con.execute("SET memory_limit='8GB'")
    con.execute("SET threads=4")

    sql = f"""
    SELECT
      CAST(isp_start_utc AS DATE) AS d,
      EXTRACT(hour FROM isp_start_utc) AS h,
      psr_type,
      quantity_mw,
      mtu_minutes
    FROM '{A75}'
    WHERE isp_start_utc BETWEEN '2024-01-01' AND '2026-05-15'
      AND psr_type IN ({", ".join(f"'{p}'" for p in PSR_LABEL)})
      AND mtu_minutes = 15
    """
    df = con.execute(sql).fetchdf()
    print(f"loaded A75: {len(df):,} rows, {df['psr_type'].nunique()} PSR types")

    # For each (d, h, psr_type) compute sigma_within = std of 4 quarter values
    # (or fewer if data is missing). Also compute mean MW for normalisation.
    g = df.groupby(["d", "h", "psr_type"], observed=True)["quantity_mw"]
    hourly = pd.DataFrame({
        "sigma_within_mw": g.std(ddof=0),  # population std across 4 ISPs in the hour
        "mean_mw": g.mean(),
        "n_isp": g.count(),
    }).reset_index()
    # Require full hour (4 ISPs)
    hourly = hourly[hourly["n_isp"] == 4].copy()

    hourly["regime"] = pd.to_datetime(hourly["d"]).apply(regime_of)
    hourly["psr_label"] = hourly["psr_type"].map(PSR_LABEL)

    # Per (regime, tech), mean sigma_within and mean MW
    per_regime = (
        hourly.groupby(["regime", "psr_label"], observed=True)
        .agg(
            sigma_within_mw=("sigma_within_mw", "mean"),
            mean_mw=("mean_mw", "mean"),
            n_hours=("d", "count"),
        )
        .reset_index()
    )
    per_regime["regime"] = pd.Categorical(per_regime["regime"], categories=REGIME_ORDER, ordered=True)
    per_regime = per_regime.sort_values(["regime", "psr_label"]).reset_index(drop=True)
    per_regime["sigma_pct"] = per_regime["sigma_within_mw"] / per_regime["mean_mw"] * 100

    # Imbalance-cost equivalent per hour per tech:
    #   cost = sigma_within (MW) * 1 hour * dual_spread_avg (EUR/MWh).
    spread_map = {k: (v[0] + v[1]) / 2 for k, v in REGIME_DUAL_SPREAD.items()}
    per_regime["spread_eur_mwh"] = per_regime["regime"].astype(str).map(spread_map).astype(float)
    # Cost per hour per MW-installed = sigma * spread.
    # Monthly cost = sigma * spread * 24 * 30 = sigma * spread * 720
    per_regime["monthly_cost_per_mwh_var_eur_m"] = (
        per_regime["sigma_within_mw"] * per_regime["spread_eur_mwh"] * 720 / 1e6
    )

    out_csv = OUT_DIR / "per_tech_sigma.csv"
    per_regime.to_csv(out_csv, index=False)
    print(f"wrote {out_csv}: {len(per_regime)} rows")

    p_sigma = per_regime.pivot_table(
        index="psr_label", columns="regime", values="sigma_within_mw", observed=True
    ).reindex(columns=REGIME_ORDER)
    p_pct = per_regime.pivot_table(
        index="psr_label", columns="regime", values="sigma_pct", observed=True
    ).reindex(columns=REGIME_ORDER)
    p_mean = per_regime.pivot_table(
        index="psr_label", columns="regime", values="mean_mw", observed=True
    ).reindex(columns=REGIME_ORDER)
    p_cost = per_regime.pivot_table(
        index="psr_label", columns="regime", values="monthly_cost_per_mwh_var_eur_m", observed=True
    ).reindex(columns=REGIME_ORDER)

    # Order rows by mean sigma_within (largest first) for the headline tech.
    order = (
        per_regime.groupby("psr_label", observed=True)["sigma_within_mw"]
        .mean().sort_values(ascending=False).index.tolist()
    )
    p_sigma = p_sigma.reindex(index=order)
    p_pct = p_pct.reindex(index=order)
    p_mean = p_mean.reindex(index=order)
    p_cost = p_cost.reindex(index=order)

    print("\n--- sigma_within_mw (MW per hour, std of within-hour 4-ISP values) ---")
    print(p_sigma.round(1).to_string())
    print("\n--- sigma_within as % of mean MW ---")
    print(p_pct.round(2).to_string())
    print("\n--- mean MW per tech per regime ---")
    print(p_mean.round(0).to_string())
    print("\n--- implied monthly imbalance-exposure cost (M EUR/month, sigma_within * spread * 720h) ---")
    print(p_cost.round(2).to_string())

    short_labels = {
        "pre-ISP15 (2024 Jan-Nov)": "pre-ISP15",
        "ISP15-win (Dec24-Mar25)":  "ISP15-win",
        "MTU15-IDA postblk":        "MTU15 postblk",
        "MTU15-DA (DA15/ID15)":     "DA15/ID15",
    }
    # Build one combined LaTeX table: sigma_within (MW), sigma %, monthly cost
    p_sigma_short = p_sigma.rename(columns=short_labels)
    p_pct_short = p_pct.rename(columns=short_labels)
    p_cost_short = p_cost.rename(columns=short_labels)

    tex_rows = []
    tex_rows.append(r"\begin{tabular}{l r r r r r}")
    tex_rows.append(r"\toprule")
    tex_rows.append(r" & \textbf{Mean MW} & \textbf{$\sigma_{w}$ (MW)} & \textbf{$\sigma_{w}/\bar{Q}$} & \multicolumn{2}{c}{\textbf{Implied monthly cost (M EUR)}} \\")
    tex_rows.append(r"\cmidrule(lr){5-6}")
    tex_rows.append(r"Tech & (DA15/ID15) & (DA15/ID15) & (DA15/ID15) & pre-ISP15 & DA15/ID15 \\")
    tex_rows.append(r"\midrule")
    last_col = "DA15/ID15"
    for tech in order:
        mean = p_mean.loc[tech, "MTU15-DA (DA15/ID15)"]
        sig = p_sigma.loc[tech, "MTU15-DA (DA15/ID15)"]
        pct = p_pct.loc[tech, "MTU15-DA (DA15/ID15)"]
        cost_pre = p_cost.loc[tech, "pre-ISP15 (2024 Jan-Nov)"]
        cost_post = p_cost.loc[tech, "MTU15-DA (DA15/ID15)"]
        def fmt(v, prec=1):
            return "--" if pd.isna(v) else f"{v:.{prec}f}"
        tex_rows.append(
            f"{tech} & {fmt(mean,0)} & {fmt(sig)} & {fmt(pct,1)}\\% & "
            f"{fmt(cost_pre,1)} & {fmt(cost_post,1)} \\\\"
        )
    tex_rows.append(r"\bottomrule")
    tex_rows.append(r"\end{tabular}")
    tex = "\n".join(tex_rows) + "\n"
    out_tex = OUT_DIR / "per_tech_sigma.tex"
    out_tex.write_text(tex)
    print(f"\nwrote {out_tex}")


if __name__ == "__main__":
    main()
