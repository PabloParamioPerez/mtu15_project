# STATUS: ALIVE
# LAST-AUDIT: 2026-05-29
# FEEDS: scripts/analysis/bid/bsts_wedge_volatility.R -- daily series of
#        within-day wedge volatility (SD across the 24 clock-hours of the
#        DA - IDA price wedge) for a BSTS on whether MTU15 increased
#        within-day wedge dispersion (the granularity story would predict
#        yes; the level BSTS in §4(ii) is a null, so volatility is the
#        natural follow-up).
#
# Also keeps |wedge| daily mean and the daily wedge p90 - p10 spread as
# secondary volatility measures, plus the per-hour-class within-class SD
# (critical / midday / flat) -- did the wedge SD increase more in critical
# hours than in flat or midday ones?
#
# OUT: data/derived/panels/wedge_volatility_panel.parquet

from pathlib import Path
import duckdb
import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
DA  = REPO / "data/processed/omie/mercado_diario/precios/marginalpdbc_all.parquet"
IDA = REPO / "data/processed/omie/mercado_intradiario_subastas/precios/marginalpibc_all.parquet"
BASE = REPO / "data/derived/panels/bsts_daily_panel.parquet"
OUT  = REPO / "data/derived/panels/wedge_volatility_panel.parquet"


def main():
    con = duckdb.connect()
    print("Computing per-(date, clock_hour) DA and IDA prices...")
    df = con.execute(f"""
        WITH da AS (
            SELECT CAST(date AS DATE) d,
                   CASE WHEN COALESCE(mtu_minutes, 60) = 60 THEN period - 1
                        ELSE CAST(FLOOR((period - 1) / 4.0) AS INT) END
                        AS clock_hour,
                   AVG(price_es_eur_mwh) AS da_p
            FROM '{DA}' WHERE price_es_eur_mwh IS NOT NULL
            GROUP BY 1, 2
        ),
        ida AS (
            SELECT CAST(date AS DATE) d,
                   CASE WHEN COALESCE(mtu_minutes, 60) = 60 THEN period - 1
                        ELSE CAST(FLOOR((period - 1) / 4.0) AS INT) END
                        AS clock_hour,
                   AVG(price_es_eur_mwh) AS ida_p
            FROM '{IDA}' WHERE price_es_eur_mwh IS NOT NULL
            GROUP BY 1, 2
        )
        SELECT da.d, da.clock_hour,
               da.da_p - ida.ida_p AS wedge
        FROM da JOIN ida USING (d, clock_hour)
    """).fetchdf()
    print(f"  {len(df):,} (date, clock_hour) cells")

    # Per-day within-day SD across the 24 clock-hours; require >= 20 hours
    # of data so an incomplete day doesn't bias the SD downward.
    print("Computing daily within-day SD of the wedge...")
    g = df.groupby("d")["wedge"]
    daily = pd.DataFrame({
        "n_hours": g.count(),
        "wedge_sd":      g.std(),           # within-day SD across clock-hours
        "wedge_abs":     g.apply(lambda s: s.abs().mean()),
        "wedge_p10":     g.quantile(0.10),
        "wedge_p90":     g.quantile(0.90),
        "wedge_mean":    g.mean(),
    }).reset_index()
    daily["wedge_iqr"] = daily["wedge_p90"] - daily["wedge_p10"]
    daily.loc[daily["n_hours"] < 20, ["wedge_sd", "wedge_abs", "wedge_iqr"]] = np.nan
    daily["d"] = pd.to_datetime(daily["d"])

    # Within-hour-class SD of the wedge (SD across the clock-hours that
    # belong to a given hour-class, on each day). Critical = 11 hours,
    # midday = 4 hours, flat = 3 hours; min-hours guard requires the day
    # to have at least 75% of the class's hours present.
    CRITICAL = {5, 6, 7, 8, 16, 17, 18, 19, 20, 21, 22}
    MIDDAY   = {11, 12, 13, 14}
    FLAT     = {1, 2, 3}
    def hc(h):
        if h in CRITICAL: return "critical"
        if h in MIDDAY:   return "midday"
        if h in FLAT:     return "flat"
        return None
    print("Computing within-hour-class wedge SD...")
    df["hour_class"] = df["clock_hour"].map(hc)
    sub = df[df["hour_class"].notna()]
    hc_sd = (sub.groupby(["d", "hour_class"])["wedge"]
              .agg(["std", "count"])
              .rename(columns={"std": "sd", "count": "n"})
              .reset_index())
    min_required = {"critical": 9, "midday": 3, "flat": 3}
    hc_sd.loc[hc_sd.apply(
        lambda r: r["n"] < min_required[r["hour_class"]], axis=1), "sd"] = np.nan
    hc_sd_wide = (hc_sd.pivot(index="d", columns="hour_class", values="sd")
                       .rename(columns={"critical": "wedge_sd_critical",
                                         "midday":   "wedge_sd_midday",
                                         "flat":     "wedge_sd_flat"})
                       .reset_index())
    hc_sd_wide["d"] = pd.to_datetime(hc_sd_wide["d"])
    daily = daily.merge(hc_sd_wide, on="d", how="left")

    # Merge covariates from bsts_daily_panel (wind/solar/gas standard; plus
    # fase1_gwh as REE-redispatch intensity, useful to absorb the reforzada-
    # intensity drift that the reforzada-constant windows do not).
    base = pd.read_parquet(BASE)[
        ["d", "wind_gwh", "solar_gwh", "gas_eur", "fase1_gwh"]]
    base["d"] = pd.to_datetime(base["d"])
    out = daily.merge(base, on="d", how="left").sort_values("d").reset_index(drop=True)
    print(f"  {len(out):,} days; wedge_sd range "
          f"{out['wedge_sd'].min():.2f}-{out['wedge_sd'].max():.2f}, "
          f"wedge_iqr range "
          f"{out['wedge_iqr'].min():.2f}-{out['wedge_iqr'].max():.2f}")

    OUT.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(OUT, index=False)
    print(f"Wrote {OUT}")


if __name__ == "__main__":
    main()
