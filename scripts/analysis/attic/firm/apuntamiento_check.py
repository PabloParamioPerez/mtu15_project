# STATUS: ALIVE
# LAST-AUDIT: 2026-05-18
# FEEDS: descriptive_facts.tex Table 33 (apuntamiento / capture rate, annual + Jan-May).
# CLAIM: Compute the apuntamiento (capture rate) — generation-weighted divided by
#        arithmetic-mean DA price — per technology per year, on a FULL hour grid
#        with proper handling of sparse A75 (left-join, zero-fill so night hours
#        of Solar PV are correctly counted with q=0 in the denominator).
#
# Source: OMIE marginalpdbc_all.parquet (full DA spot coverage 2023-2026) +
#         ENTSO-E A75 gen_actual_per_type_all.parquet.
#
# Previous version of this table used ESIOS indicator 600 (which has 2025-2026
# month gaps) and an inner join that dropped night hours -- both biased the
# denominator. This version corrects both.
#
# Output:
#   results/regressions/firm/apuntamiento/tab_apuntamiento.tex

from __future__ import annotations
from pathlib import Path
import duckdb
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
GEN  = REPO / "data/processed/entsoe/generation/gen_actual_per_type_all.parquet"
DA   = REPO / "data/processed/omie/mercado_diario/precios/marginalpdbc_all.parquet"
OUT  = REPO / "results/regressions/firm/apuntamiento"
OUT.mkdir(parents=True, exist_ok=True)


def main():
    con = duckdb.connect()

    # OMIE DA price: collapse period to hour-of-day (post-MTU15-DA the period is
    # quarter-hourly), aggregate to (date, hour_local) means, convert to UTC.
    da_raw = con.execute(f"""
    SELECT date::DATE AS d, period, price_es_eur_mwh AS p, mtu_minutes
    FROM '{DA}'
    WHERE date::DATE >= '2023-01-01' AND date::DATE <= '2026-05-31'
      AND price_es_eur_mwh IS NOT NULL
    """).df()
    da_raw["hr_local"] = (da_raw["period"].astype(int) - 1) // (60 // da_raw["mtu_minutes"].clip(lower=1))
    da_h = da_raw.groupby(["d", "hr_local"], as_index=False).agg(p_da=("p", "mean"))
    da_h["ts_local"] = pd.to_datetime(da_h["d"]) + pd.to_timedelta(da_h["hr_local"], unit="h")
    da_h["ts_local"] = da_h["ts_local"].dt.tz_localize(
        "Europe/Madrid", ambiguous="NaT", nonexistent="shift_forward"
    )
    da_h["hr_utc"] = da_h["ts_local"].dt.tz_convert("UTC").dt.tz_localize(None)
    da = da_h[["hr_utc", "p_da"]].dropna()
    print(f"OMIE DA hourly rows (UTC): {len(da):,}, mean {da['p_da'].mean():.1f}")

    # ENTSO-E A75 actual generation, hourly MWh per tech.
    gen = con.execute(f"""
    SELECT date_trunc('hour', isp_start_utc) AS hr_utc,
           psr_type,
           SUM(quantity_mw * mtu_minutes / 60.0) AS energy_mwh
    FROM '{GEN}'
    WHERE psr_type IN ('B14', 'B16', 'B19')
      AND isp_start_utc >= TIMESTAMP '2023-01-01'
      AND isp_start_utc <  TIMESTAMP '2026-06-01'
    GROUP BY 1, 2
    """).df()
    gen["hr_utc"] = pd.to_datetime(gen["hr_utc"])
    gen_wide = gen.pivot_table(index="hr_utc", columns="psr_type", values="energy_mwh",
                                fill_value=0).reset_index()

    # LEFT JOIN gen onto the full DA hour grid; fill missing gen with 0.
    df = da.merge(gen_wide, on="hr_utc", how="left")
    for tech in ("B14", "B16", "B19"):
        if tech not in df.columns:
            df[tech] = 0.0
        df[tech] = df[tech].fillna(0.0)
    df["year"] = df["hr_utc"].dt.year
    df["month"] = df["hr_utc"].dt.month
    print(f"Merged rows (full DA hour grid 2023-2026): {len(df):,}")

    def app(grp, tech):
        if grp[tech].sum() == 0:
            return None
        weighted = (grp[tech] * grp["p_da"]).sum() / grp[tech].sum()
        arith = grp["p_da"].mean()
        return weighted / arith if arith > 0 else None

    rows = []
    rows.append(r"% auto-built by scripts/analysis/firm/apuntamiento_check.py.")
    rows.append(r"% Source: OMIE marginalpdbc_all.parquet (DA spot, full coverage) +")
    rows.append(r"%         ENTSO-E A75 gen_actual_per_type, left-joined onto full hour grid.")
    rows.append(r"\begin{tabular}{l r r r r}")
    rows.append(r"\toprule")
    rows.append(r"Window & Solar PV (B16) & Wind onshore (B19) & Solar Thermal (B14) & arithm.\ mean DA \\")
    rows.append(r"\midrule")
    rows.append(r"\multicolumn{5}{l}{\textit{Full calendar year}} \\")
    for y in sorted(df["year"].unique()):
        g = df[df["year"] == y]
        if g["month"].nunique() < 12:
            continue
        rows.append(
            f"{y} & {app(g,'B16'):.3f} & {app(g,'B19'):.3f} & {app(g,'B14'):.3f}"
            f" & {g['p_da'].mean():.1f} \\\\"
        )
    rows.append(r"\midrule")
    rows.append(r"\multicolumn{5}{l}{\textit{Jan--May (same-window comparison)}} \\")
    for y in sorted(df["year"].unique()):
        g = df[(df["year"] == y) & (df["month"] <= 5)]
        if len(g) == 0:
            continue
        if g["month"].nunique() < 5 and y != df["year"].max():
            continue
        rows.append(
            f"{y} & {app(g,'B16'):.3f} & {app(g,'B19'):.3f} & {app(g,'B14'):.3f}"
            f" & {g['p_da'].mean():.1f} \\\\"
        )
    rows.append(r"\bottomrule")
    rows.append(r"\end{tabular}")

    out_tex = OUT / "tab_apuntamiento.tex"
    out_tex.write_text("\n".join(rows) + "\n")
    print(f"wrote {out_tex}")


if __name__ == "__main__":
    main()
