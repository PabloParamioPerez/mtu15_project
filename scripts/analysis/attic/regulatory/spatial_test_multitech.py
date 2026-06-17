# STATUS: ALIVE
# LAST-AUDIT: 2026-05-31
# CLAIM: Extends the spatial test (volatility -> restrictions) beyond
#        CCGT to all dispatchable techs (CCGT, Hydro, Hydro_pump) at the
#        UNIT level. For each unit, compute (a) MTU15-IDA pre-blackout
#        within-hour MW SD from PHF, and (b) post-blackout vs pre-
#        blackout daily PHF-PDBF jump. Regress jump on volatility with
#        tech fixed effects. Tests whether the within-tech volatility
#        rank correlates with the post-blackout restriction burden.
#
# IN:  data/processed/omie/.../phf_all.parquet, pdbf_all.parquet
#      data/derived/panels/bid_shape_critical_flat/_unit_map.parquet
# OUT: results/regressions/regulatory/spatial_blackout/per_unit_multitech.csv

from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
PHF = REPO / "data/processed/omie/mercado_intradiario_subastas/programas/phf_all.parquet"
PDBF = REPO / "data/processed/omie/mercado_diario/programas/pdbf_all.parquet"
UMAP = REPO / "data/derived/panels/bid_shape_critical_flat/_unit_map.parquet"
OUT = REPO / "results/regressions/regulatory/spatial_blackout"
OUT.mkdir(parents=True, exist_ok=True)


def main():
    con = duckdb.connect()
    con.execute("SET memory_limit='6GB'"); con.execute("SET threads=4")

    TECHS = ('CCGT', 'Hydro', 'Hydro_pump')

    # 1. Per-unit MTU15-IDA pre-blackout within-hour MW SD (volatility)
    q_vol = f"""
    WITH disp AS (
      SELECT unit_code, tech_group, firm_class
      FROM '{UMAP}' WHERE tech_group IN {TECHS}
    ),
    phf AS (
      SELECT date::DATE AS d, period, mtu_minutes, unit_code,
             MAX_BY(assigned_power_mw, session_number) AS mw
      FROM '{PHF}'
      WHERE date BETWEEN '2025-03-19' AND '2025-04-27'
        AND period BETWEEN 1 AND 96
      GROUP BY date, period, unit_code, mtu_minutes
    ),
    phf_clock AS (
      SELECT p.d, p.unit_code,
             CAST(CEIL(p.period * (p.mtu_minutes / 60.0)) AS INT) AS clock_hour,
             stddev_pop(p.mw) AS sd_mw, AVG(p.mw) AS mean_mw, COUNT(*) AS n_q
      FROM phf p JOIN disp d USING (unit_code)
      WHERE p.mw IS NOT NULL AND p.mw > 0
      GROUP BY p.d, p.unit_code, clock_hour
    )
    SELECT p.unit_code, d.tech_group, d.firm_class,
           COUNT(*) AS n_obs,
           AVG(p.sd_mw)                            AS vol_sd_mw,
           AVG(p.mean_mw)                          AS mean_mw,
           AVG(p.sd_mw / NULLIF(p.mean_mw, 0)) * 100 AS vol_cv_pct
    FROM phf_clock p JOIN disp d USING (unit_code)
    WHERE p.n_q = 4
    GROUP BY p.unit_code, d.tech_group, d.firm_class
    HAVING COUNT(*) >= 30
    """
    vol = con.execute(q_vol).fetchdf()
    print(f"Volatility computed for {len(vol)} units")
    print(vol.groupby('tech_group').size())

    # 2. Per-unit pre/post-blackout PHF-PDBF daily MWh with same-calendar
    #    matching to absorb seasonality. Post = 2025-05-01 to 2025-12-31
    #    (May-Dec 2025); pre same-cal = 2024-05-01 to 2024-12-31 (May-Dec
    #    2024). The diff cancels seasonal mix by construction.
    q_gap = f"""
    WITH disp AS (
      SELECT unit_code, tech_group FROM '{UMAP}' WHERE tech_group IN {TECHS}
    ),
    phf_daily AS (
      SELECT date::DATE AS d, unit_code,
             SUM(GREATEST(assigned_power_mw, 0) * mtu_minutes / 60.0) AS mwh_phf
      FROM (
        SELECT date, period, unit_code, mtu_minutes,
               MAX_BY(assigned_power_mw, session_number) AS assigned_power_mw
        FROM '{PHF}'
        WHERE (date BETWEEN '2024-05-01' AND '2024-12-31'
               OR date BETWEEN '2025-05-01' AND '2025-12-31')
          AND period BETWEEN 1 AND 96
        GROUP BY date, period, unit_code, mtu_minutes
      )
      WHERE assigned_power_mw IS NOT NULL
      GROUP BY 1, 2
    ),
    pdbf_daily AS (
      SELECT date::DATE AS d, unit_code,
             SUM(GREATEST(assigned_power_mw, 0) * mtu_minutes / 60.0) AS mwh_pdbf
      FROM '{PDBF}'
      WHERE (date BETWEEN '2024-05-01' AND '2024-12-31'
             OR date BETWEEN '2025-05-01' AND '2025-12-31')
        AND period BETWEEN 1 AND 96 AND assigned_power_mw IS NOT NULL
      GROUP BY 1, 2
    ),
    gap AS (
      SELECT p.d, p.unit_code,
             p.mwh_phf - COALESCE(b.mwh_pdbf, 0) AS gap_mwh
      FROM phf_daily p LEFT JOIN pdbf_daily b USING (d, unit_code)
      JOIN disp USING (unit_code)
    )
    SELECT unit_code,
           AVG(CASE WHEN EXTRACT(YEAR FROM d) = 2024 THEN gap_mwh END) AS pre_avg_mwh,
           AVG(CASE WHEN EXTRACT(YEAR FROM d) = 2025 THEN gap_mwh END) AS post_avg_mwh,
           COUNT(CASE WHEN EXTRACT(YEAR FROM d) = 2024 THEN 1 END) AS n_pre,
           COUNT(CASE WHEN EXTRACT(YEAR FROM d) = 2025 THEN 1 END) AS n_post
    FROM gap GROUP BY 1
    """
    gap = con.execute(q_gap).fetchdf()
    gap['jump_gwh'] = (gap['post_avg_mwh'] - gap['pre_avg_mwh']) / 1000.0
    print(f"\n--- Same-calendar matching: May-Dec 2024 (pre) vs May-Dec 2025 (post) ---")

    df = vol.merge(gap, on='unit_code', how='inner').dropna(subset=['vol_sd_mw','jump_gwh'])
    df.to_csv(OUT / "per_unit_multitech.csv", index=False)
    print(f"Merged panel: {len(df)} units")

    # 3. Cross-sectional regressions
    import statsmodels.formula.api as smf

    print("\n=== Pooled: jump_gwh ~ vol_sd_mw ===")
    m1 = smf.ols("jump_gwh ~ vol_sd_mw", data=df).fit()
    print(m1.summary().tables[1])

    print("\n=== With tech FE: jump_gwh ~ vol_sd_mw + C(tech_group) ===")
    m2 = smf.ols("jump_gwh ~ vol_sd_mw + C(tech_group)", data=df).fit()
    print(m2.summary().tables[1])

    print("\n=== Within-tech: vol slope per tech (no FE, vol_sd_mw interacted) ===")
    for t in df['tech_group'].unique():
        sub = df[df['tech_group'] == t]
        if len(sub) < 5: continue
        m = smf.ols("jump_gwh ~ vol_sd_mw", data=sub).fit()
        coef = m.params['vol_sd_mw']
        pval = m.pvalues['vol_sd_mw']
        n = len(sub)
        rho_s = sub[['vol_sd_mw','jump_gwh']].corr(method='spearman').iloc[0,1]
        print(f"  {t:12s} n={n:3d}  beta={coef:+7.3f} (p={pval:.3f})  spearman={rho_s:+.2f}")


if __name__ == "__main__":
    main()
