# STATUS: ALIVE
# LAST-AUDIT: 2026-05-11
# FEEDS: thesis paper.tex §A.7 (appendix: hour-classification metrics)
# CLAIM: Builds an appendix LaTeX table with the 24-hour classification metrics
# used to define critical vs flat hours. Self-contained: regenerates the per-hour
# CSV from ENTSO-E load (A65) and wind+solar (A75) parquets.

from __future__ import annotations
from pathlib import Path
import duckdb
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
LOAD = REPO / "data" / "processed" / "entsoe" / "load" / "load_actual_all.parquet"
WIND_SOLAR = REPO / "data" / "processed" / "entsoe" / "generation" / "wind_solar_actual_all.parquet"
CSV = REPO / "results" / "regressions" / "firm" / "critical_hours_thesis" / "hour_classification_metrics.csv"
OUT = REPO / "thesis" / "paper" / "tables" / "tab_hour_classification.tex"
CSV.parent.mkdir(parents=True, exist_ok=True)

CRITICAL = {5, 6, 7, 8, 16, 17, 18, 19, 20, 21, 22}
FLAT = {1, 2, 3}


def cls(h):
    if h in CRITICAL: return "critical"
    if h in FLAT: return "flat"
    return "dropped"


DATE_FROM = "2023-01-01"
DATE_TO   = "2026-05-15"


def build_csv() -> pd.DataFrame:
    """Recompute hour-level metrics from ENTSO-E parquets at 15-min ISP.

    Window is the largest 15-min ISP window available for both Spanish A65
    (actual load) and A75 (actual wind+solar): from 2023-01-01 onward.
    Pre-2023 the same ENTSO-E series are only 60-min, so within-hour variance
    of residual demand is undefined (one observation per hour).
    """
    con = duckdb.connect()
    df = con.execute(f"""
        WITH load_isp AS (
            SELECT (isp_start_utc AT TIME ZONE 'Europe/Madrid')::DATE AS d,
                   EXTRACT(HOUR FROM (isp_start_utc AT TIME ZONE 'Europe/Madrid')) AS hour,
                   EXTRACT(MINUTE FROM (isp_start_utc AT TIME ZONE 'Europe/Madrid')) AS minute,
                   load_mw
            FROM '{LOAD}'
            WHERE isp_start_utc >= TIMESTAMP '{DATE_FROM}' AND isp_start_utc < TIMESTAMP '{DATE_TO}'
              AND mtu_minutes = 15
        ),
        vre_per_isp AS (
            SELECT isp_start_utc,
                   (isp_start_utc AT TIME ZONE 'Europe/Madrid')::DATE AS d,
                   EXTRACT(HOUR FROM (isp_start_utc AT TIME ZONE 'Europe/Madrid')) AS hour,
                   EXTRACT(MINUTE FROM (isp_start_utc AT TIME ZONE 'Europe/Madrid')) AS minute,
                   SUM(CASE WHEN psr_type='B16' THEN quantity_mw ELSE 0 END) AS solar_mw,
                   SUM(CASE WHEN psr_type IN ('B18','B19') THEN quantity_mw ELSE 0 END) AS wind_mw
            FROM '{WIND_SOLAR}'
            WHERE isp_start_utc >= TIMESTAMP '{DATE_FROM}' AND isp_start_utc < TIMESTAMP '{DATE_TO}'
              AND mtu_minutes = 15
            GROUP BY 1,2,3,4
        ),
        per_quarter AS (
            SELECT l.d, l.hour, l.minute,
                   l.load_mw,
                   COALESCE(v.solar_mw, 0) AS solar_mw,
                   COALESCE(v.wind_mw, 0)  AS wind_mw,
                   l.load_mw - COALESCE(v.solar_mw, 0) - COALESCE(v.wind_mw, 0) AS netload_mw
            FROM load_isp l
            LEFT JOIN vre_per_isp v
              ON l.d = v.d AND l.hour = v.hour AND l.minute = v.minute
        ),
        with_dev AS (
            SELECT *, netload_mw - AVG(netload_mw) OVER (PARTITION BY d, hour) AS dev
            FROM per_quarter
        ),
        per_day_hour AS (
            SELECT d, hour,
                   AVG(load_mw)    AS load_mw,
                   AVG(solar_mw)   AS solar_mw,
                   AVG(wind_mw)    AS wind_mw,
                   AVG(netload_mw) AS netload_mw,
                   sqrt(AVG(POWER(dev, 2))) AS sigma_within_mw,
                   MAX(CASE WHEN minute = 45 THEN load_mw END)
                       - MAX(CASE WHEN minute =  0 THEN load_mw END) AS delta_load_mw
            FROM with_dev
            GROUP BY 1,2
        )
        SELECT hour,
               AVG(load_mw)        / 1000.0 AS load_gw,
               AVG(solar_mw)       / 1000.0 AS solar_gw,
               AVG(wind_mw)        / 1000.0 AS wind_gw,
               AVG(netload_mw)     / 1000.0 AS netload_gw,
               AVG(sigma_within_mw)         AS sigma_netload_mw,
               AVG(delta_load_mw)           AS delta_load_mw
        FROM per_day_hour GROUP BY 1 ORDER BY 1
    """).df()
    df.to_csv(CSV, index=False)
    print(f"wrote: {CSV}")
    return df


def main():
    # Recompute from ENTSO-E parquets when available; otherwise fall back to
    # the cached CSV (e.g. when the external SSD holding data/processed is
    # not mounted). The metrics are a structural feature of the within-day
    # pattern, not regime-specific, so the cached values are stable.
    if LOAD.exists() and WIND_SOLAR.exists():
        df = build_csv()
    else:
        print(f"ENTSO-E parquets not mounted; using cached {CSV}")
        df = pd.read_csv(CSV)
    df["class"] = df["hour"].apply(cls)
    df = df.sort_values("hour")
    # Coefficient of variation: within-hour SD relative to the residual-demand
    # level. sigma in MW, residual demand in GW -> CV in %. Both the absolute
    # within-hour swing (sigma) and the level (residual demand) matter; CV is
    # the level-normalised version of sigma.
    df["cv_pct"] = df["sigma_netload_mw"] / (df["netload_gw"] * 1000.0) * 100.0
    tex = []
    tex.append(r"\begin{tabular}{l r r r r r r r l}")
    tex.append(r"\toprule")
    # Two-line column headers so the table fits on the page without resizebox.
    tex.append(
        r"Clock-hour & \makecell{Demand\\(GW)} & \makecell{Solar\\(GW)} & \makecell{Wind\\(GW)} & "
        r"\makecell{Residual\\demand (GW)} & \makecell{$\sigma_{\text{within}}$\\(MW)} & "
        r"\makecell{CV\\(\%)} & \makecell{$\Delta$ demand\\(MW)} & Class \\"
    )
    tex.append(r"\midrule")
    for _, r in df.iterrows():
        h = int(r["hour"])
        emph = r"\textbf{" if r["class"] == "critical" else (r"\textit{" if r["class"] == "flat" else "{")
        end = "}"
        # Row-color prefix: matches the red/blue bands in the seasonal figures.
        prefix = r"\critrow " if r["class"] == "critical" else (r"\flatrow " if r["class"] == "flat" else "")
        # Clock-hour label: hour as 24h format "HH:00--(HH+1):00"
        hh = f"{h:02d}:00--{(h+1)%24:02d}:00"
        # Render Δ with minus only (positive shown without sign)
        d = r["delta_load_mw"]
        delta_str = f"$-${abs(d):.0f}" if d < 0 else f"{d:.0f}"
        line = (f"{prefix}{emph}{hh}{end} & {r['load_gw']:.1f} & {r['solar_gw']:.1f} & "
                f"{r['wind_gw']:.1f} & "
                f"{r['netload_gw']:.1f} & {r['sigma_netload_mw']:.0f} & "
                f"{r['cv_pct']:.1f} & "
                f"{delta_str} & \\textit{{{r['class']}}} \\\\")
        tex.append(line)
    tex.append(r"\bottomrule")
    tex.append(r"\end{tabular}")
    OUT.write_text("\n".join(tex))
    print(f"wrote: {OUT}")


if __name__ == "__main__":
    main()
