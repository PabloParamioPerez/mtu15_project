# STATUS: ALIVE
# LAST-AUDIT: 2026-05-19
# CLAIM: Per-(tech, hour-class) sigma_within of actual generation. Same as
#        within_hour_variance_by_tech.py but disaggregates the per-tech aggregate
#        to Critical / Flat / Midday hour-classes. Reveals that ISP15 exposure
#        differs strongly across hour-classes within a tech (Solar PV midday
#        has much higher within-hour variance than its 24h-mean would suggest).
#
# OUT: results/regressions/balancing/within_hour_variance/per_tech_hc_sigma.csv
#      results/regressions/balancing/within_hour_variance/per_tech_hc_sigma.tex

from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
A75 = REPO / "data/processed/entsoe/generation/gen_actual_per_type_all.parquet"
OUT_DIR = REPO / "results/regressions/balancing/within_hour_variance"
OUT_DIR.mkdir(parents=True, exist_ok=True)

PSR_LABEL = {
    "B01": "Biomass",
    "B14": "Solar Thermal",
    "B16": "Solar PV",
    "B17": "Other RES",
    "B19": "Wind onshore",
    "B11": "Hydro RES",
    "B12": "Hydro pump",
    "B15": "Marine",
    "B25": "Other RES bio",
    "B04": "CCGT",
    "B05": "Coal",
    "B09": "Geothermal",
    "B10": "Hydro reservoir",
    "B13": "Hydro pump load",
    "B18": "Hydro river",
    "B20": "Other",
}

# Hour-class definition same as main doc
HOUR_CLASS = {
    "Critical": [5, 6, 7, 8, 16, 17, 18, 19, 20, 21, 22],
    "Flat":     [1, 2, 3],
    "Midday":   [11, 12, 13, 14],
}

START = "2024-01-01"


def hour_class_of(h):
    for hc, hs in HOUR_CLASS.items():
        if int(h) in hs:
            return hc
    return None


def main():
    con = duckdb.connect()
    con.execute("SET memory_limit='8GB'")
    sql = f"""
    SELECT psr_type,
           CAST(isp_start_utc AS DATE) AS d,
           EXTRACT(hour FROM isp_start_utc) AS hour,
           quantity_mw AS value_mw
    FROM read_parquet('{A75}')
    WHERE isp_start_utc >= '{START}'
    """
    df = con.execute(sql).fetchdf()
    df["hour_class"] = df["hour"].apply(hour_class_of)
    df = df[df["hour_class"].notna()].copy()
    df["tech"] = df["psr_type"].map(PSR_LABEL).fillna(df["psr_type"])
    print(f"Loaded {len(df):,} rows; {df['tech'].nunique()} techs")

    # Per (tech, date, hour, hour_class) compute within-hour mean and std across the 15-min cells
    perhour = (df.groupby(["tech", "d", "hour", "hour_class"])
                 .agg(mean_mw=("value_mw", "mean"),
                      std_mw=("value_mw", "std"),
                      n=("value_mw", "count"))
                 .reset_index())
    # Need at least 4 within-hour observations for a well-defined std
    perhour = perhour[perhour["n"] == 4].copy()

    # Aggregate to per (tech, hour_class): mean of per-hour means/stds
    agg = (perhour.groupby(["tech", "hour_class"])
                  .agg(mean_mw=("mean_mw", "mean"),
                       sigma_w=("std_mw", "mean"),
                       n_hours=("d", "count"))
                  .reset_index())
    agg["cv"] = agg["sigma_w"] / agg["mean_mw"].clip(lower=1)
    agg.to_csv(OUT_DIR / "per_tech_hc_sigma.csv", index=False)
    print(f"  wrote per_tech_hc_sigma.csv")

    # Build tex table: rows = tech, columns = (critical mean/sigma, flat mean/sigma, midday mean/sigma)
    techs_keep = ["CCGT", "Coal", "Hydro reservoir", "Hydro river", "Hydro pump", "Hydro RES",
                  "Nuclear" if "Nuclear" in agg["tech"].values else "Other", "Solar PV", "Wind onshore",
                  "Solar Thermal", "Biomass", "Other RES"]
    # Pivot
    piv = agg.pivot_table(index="tech", columns="hour_class",
                          values=["mean_mw", "sigma_w"]).round(0)
    rows = [r"\begin{tabular}{l r r r r r r}", r"\toprule",
            r" & \multicolumn{2}{c}{Critical} & \multicolumn{2}{c}{Flat} & \multicolumn{2}{c}{Midday} \\",
            r"\cmidrule(lr){2-3}\cmidrule(lr){4-5}\cmidrule(lr){6-7}",
            r"Tech & $\bar Q$ MW & $\sigma_w$ MW & $\bar Q$ & $\sigma_w$ & $\bar Q$ & $\sigma_w$ \\",
            r"\midrule"]
    for tech in techs_keep:
        if tech not in piv.index:
            continue
        r = piv.loc[tech]
        cells = [tech]
        for hc in ["Critical", "Flat", "Midday"]:
            m = r.get(("mean_mw", hc), np.nan)
            s = r.get(("sigma_w", hc), np.nan)
            cells.append(f"{m:,.0f}" if not pd.isna(m) else "---")
            cells.append(f"{s:,.0f}" if not pd.isna(s) else "---")
        rows.append(" & ".join(cells) + r" \\")
    rows.extend([r"\bottomrule", r"\end{tabular}"])
    (OUT_DIR / "per_tech_hc_sigma.tex").write_text(
        "% Per-(tech, hour-class) within-hour mean MW and sigma_within (mean of std across 4 within-hour ISP15 cells).\n"
        + "\n".join(rows))
    print(f"  wrote per_tech_hc_sigma.tex")


if __name__ == "__main__":
    main()
