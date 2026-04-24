"""Overnight robustness block — tables + data files, NO auto-generated figures.

Outputs:
  data/derived/bootstrap_lerner.parquet
  data/derived/slope_sensitivity_lerner.parquet
  data/derived/placebo_lerner.parquet
  data/derived/placebo_lerner_summary.parquet
  data/derived/hour_of_day_lerner.parquet
  logs/robustness_<ts>.log

Runtime: ~5-15 min. No new downloads; uses existing parquet.
"""
from __future__ import annotations

import logging
import pathlib
import sys
import time
import warnings
from datetime import datetime

import duckdb
import numpy as np
import pandas as pd

warnings.filterwarnings('ignore')

REPO = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / 'src'))

from mtu.notebook_utils import (  # noqa: E402
    DAY_AHEAD_REFORM,
    IDA_REFORM,
    INTRADAY_REFORM,
    ISP15_REFORM,
    PROJECT_ROOT,
)

LOG_DIR = REPO / 'logs'
LOG_DIR.mkdir(exist_ok=True)
TS = datetime.now().strftime('%Y%m%d_%H%M%S')
LOG_PATH = LOG_DIR / f'robustness_{TS}.log'
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[logging.FileHandler(LOG_PATH), logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger('robust')

REGIME_ORDER = ['pre-IDA', '3-sess', 'ISP15 window', 'DA60/ID15', 'DA15/ID15']


def assign_regime(d):
    d = pd.Timestamp(d)
    if d < IDA_REFORM:
        return 'pre-IDA'
    if d < ISP15_REFORM:
        return '3-sess'
    if d < INTRADAY_REFORM:
        return 'ISP15 window'
    if d < DAY_AHEAD_REFORM:
        return 'DA60/ID15'
    return 'DA15/ID15'


LER = PROJECT_ROOT / 'data/derived/firm_lerner_hourly.parquet'

con = duckdb.connect()
con.execute("SET memory_limit='10GB'")
con.execute("SET threads=6")


def bootstrap_lerner():
    log.info('BOOTSTRAP: 500 iterations × 4 firms × 5 regimes')
    t0 = time.time()
    df = con.sql(f"""
        SELECT date, firm, lerner_index
        FROM '{LER}'
        WHERE lerner_index BETWEEN 0 AND 1
    """).df()
    df['date'] = pd.to_datetime(df['date'])
    df['regime'] = df['date'].apply(assign_regime)

    rng = np.random.default_rng(2026)
    N_BOOT = 500
    results = []
    for firm in ['GE', 'IB', 'GN', 'HC']:
        for regime in REGIME_ORDER:
            vals = df.loc[(df['firm'] == firm) & (df['regime'] == regime),
                          'lerner_index'].values
            if len(vals) < 10:
                continue
            meds = np.empty(N_BOOT)
            for b in range(N_BOOT):
                meds[b] = np.median(rng.choice(vals, size=len(vals), replace=True))
            lo, hi = np.percentile(meds, [2.5, 97.5])
            results.append({
                'firm': firm, 'regime': regime, 'n': int(len(vals)),
                'median': float(np.median(vals)),
                'ci_lo': float(lo), 'ci_hi': float(hi),
                'bootstrap_sd': float(meds.std(ddof=1)),
            })
    out = pd.DataFrame(results)
    out_path = REPO / 'data/derived/bootstrap_lerner.parquet'
    out.to_parquet(out_path, index=False)
    log.info(f'  wrote {out_path.name} in {time.time()-t0:.1f}s')
    log.info(f'  sample row:\n{out.head(5).to_string()}')


def slope_sensitivity():
    log.info('SENSITIVITY: recompute Lerner with ±5/10/15/25 slope windows')
    t0 = time.time()
    results = []
    for delta in [5.0, 10.0, 15.0, 25.0]:
        con.execute("""
            CREATE OR REPLACE TEMP TABLE sell_agg AS
            SELECT date,
                   CASE WHEN period_raw LIKE 'H%Q%'
                        THEN CAST(regexp_extract(period_raw, 'H([0-9]+)Q', 1) AS INTEGER)
                        ELSE CAST(period_raw AS INTEGER) END AS hour,
                   price_eur_mwh, SUM(power_mw) AS mw
            FROM 'data/processed/omie/mercado_diario/curvas/curva_pbc_all.parquet'
            WHERE curve_type='O' AND country IN ('MI','ES')
              AND (offer_typology IS NULL OR offer_typology='S')
              AND power_mw IS NOT NULL AND price_eur_mwh IS NOT NULL
            GROUP BY 1,2,3
        """)
        con.execute("""
            CREATE OR REPLACE TEMP TABLE cum_supply AS
            SELECT date, hour, price_eur_mwh,
                   SUM(mw) OVER (PARTITION BY date,hour ORDER BY price_eur_mwh
                                  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_mw
            FROM sell_agg
        """)
        con.execute("""
            CREATE OR REPLACE TEMP TABLE clearing AS
            SELECT date,
                   CASE WHEN mtu_minutes=15 THEN CEIL(period/4.0)::INTEGER ELSE period END AS hour,
                   AVG(price_es_eur_mwh) AS p_star
            FROM 'data/processed/omie/mercado_diario/precios/marginalpdbc_all.parquet'
            WHERE price_es_eur_mwh IS NOT NULL
            GROUP BY 1,2
        """)
        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE slope AS
            SELECT c.date, c.hour, c.p_star,
                   MAX(CASE WHEN cs.price_eur_mwh <= c.p_star + {delta} THEN cs.cum_mw END)
                   - MAX(CASE WHEN cs.price_eur_mwh <= c.p_star - {delta} THEN cs.cum_mw END)
                     AS num
            FROM clearing c
            LEFT JOIN cum_supply cs ON cs.date=c.date AND cs.hour=c.hour
            GROUP BY c.date, c.hour, c.p_star
        """)
        lerner = con.execute(f"""
            WITH firm_q AS (
                SELECT date,
                       CASE WHEN mtu_minutes=15 THEN CEIL(period/4.0)::INTEGER ELSE period END AS hour,
                       grupo_empresarial AS firm,
                       SUM(assigned_power_mw)/CASE WHEN mtu_minutes=15 THEN 4.0 ELSE 1.0 END AS q_mwh
                FROM 'data/processed/omie/mercado_diario/programas/pdbce_all.parquet'
                WHERE offer_type=1 AND assigned_power_mw IS NOT NULL
                  AND grupo_empresarial IN ('GE','IB','GN','HC')
                GROUP BY 1,2,3, mtu_minutes
            ),
            total_q AS (
                SELECT date,
                       CASE WHEN mtu_minutes=15 THEN CEIL(period/4.0)::INTEGER ELSE period END AS hour,
                       SUM(assigned_power_mw)/CASE WHEN mtu_minutes=15 THEN 4.0 ELSE 1.0 END AS q_total
                FROM 'data/processed/omie/mercado_diario/programas/pdbce_all.parquet'
                WHERE offer_type=1 AND assigned_power_mw>0
                GROUP BY 1,2, mtu_minutes
            )
            SELECT f.date, f.firm, f.q_mwh, t.q_total, s.p_star,
                   s.num / (2.0*{delta}) AS slope
            FROM firm_q f
            JOIN total_q t USING (date, hour)
            JOIN slope s USING (date, hour)
            WHERE f.q_mwh > 0 AND s.num > 0 AND s.p_star > 0 AND t.q_total > f.q_mwh
        """).df()
        lerner['s'] = lerner['q_mwh'] / lerner['q_total']
        lerner['L'] = lerner['q_mwh'] / (lerner['p_star'] * (1 - lerner['s']) * lerner['slope'])
        lerner = lerner[(lerner['L'] >= 0) & (lerner['L'] <= 1)]
        lerner['date'] = pd.to_datetime(lerner['date'])
        lerner['regime'] = lerner['date'].apply(assign_regime)
        tbl = lerner.groupby(['firm', 'regime'])['L'].median().reset_index()
        tbl['delta_eur'] = delta
        results.append(tbl)
        log.info(f'  ±{delta:.0f} EUR done (rows={len(lerner)})')
    out = pd.concat(results, ignore_index=True)
    out_path = REPO / 'data/derived/slope_sensitivity_lerner.parquet'
    out.to_parquet(out_path, index=False)
    log.info(f'  wrote {out_path.name} in {time.time()-t0:.1f}s')
    # Show GE pivoted
    ge = out[out['firm'] == 'GE'].pivot(index='delta_eur', columns='regime', values='L')
    ge = ge.reindex(columns=REGIME_ORDER)
    log.info(f'\nGE median L by (delta, regime):\n{ge.round(3).to_string()}')


def placebo_dates():
    log.info('PLACEBO: 200 fake reform dates around MTU15-IDA boundary')
    t0 = time.time()
    df = con.sql(f"""
        SELECT date, firm, lerner_index
        FROM '{LER}'
        WHERE lerner_index BETWEEN 0 AND 1
    """).df()
    df['date'] = pd.to_datetime(df['date'])

    real_date = pd.Timestamp(INTRADAY_REFORM)
    real_deltas = {}
    for firm in ['GE', 'IB', 'GN', 'HC']:
        sub = df[df['firm'] == firm]
        win = sub[(sub['date'] >= real_date - pd.Timedelta(days=120)) &
                  (sub['date'] <= real_date + pd.Timedelta(days=120))]
        pre = win[win['date'] < real_date]['lerner_index'].median()
        post = win[win['date'] >= real_date]['lerner_index'].median()
        real_deltas[firm] = float(post - pre)

    rng = np.random.default_rng(2026)
    N = 200
    all_dates = pd.date_range('2024-03-01', '2025-07-01', freq='D')
    forbid = [IDA_REFORM, ISP15_REFORM, INTRADAY_REFORM, DAY_AHEAD_REFORM]
    ok = [d for d in all_dates
          if all(abs((d - pd.Timestamp(f)).days) > 30 for f in forbid)]
    fake_dates = rng.choice(ok, size=N, replace=False)

    placebo_rows = []
    for T in fake_dates:
        for firm in ['GE', 'IB', 'GN', 'HC']:
            sub = df[df['firm'] == firm]
            win = sub[(sub['date'] >= T - pd.Timedelta(days=120)) &
                      (sub['date'] <= T + pd.Timedelta(days=120))]
            pre = win[win['date'] < T]['lerner_index'].median()
            post = win[win['date'] >= T]['lerner_index'].median()
            placebo_rows.append({
                'firm': firm, 'fake_date': pd.Timestamp(T),
                'delta': float(post - pre) if np.isfinite(post - pre) else np.nan,
            })
    placebo = pd.DataFrame(placebo_rows)
    placebo.to_parquet(REPO / 'data/derived/placebo_lerner.parquet', index=False)

    summary = []
    for firm in ['GE', 'IB', 'GN', 'HC']:
        p = placebo[placebo['firm'] == firm]['delta'].dropna().abs()
        real = abs(real_deltas[firm])
        emp_p = float((p >= real).mean()) if len(p) else np.nan
        summary.append({
            'firm': firm,
            'real_delta': real_deltas[firm],
            'placebo_n': int(len(p)),
            'placebo_median_abs': float(p.median()),
            'placebo_p95_abs': float(p.quantile(0.95)),
            'empirical_p': emp_p,
        })
    summary_df = pd.DataFrame(summary)
    summary_df.to_parquet(REPO / 'data/derived/placebo_lerner_summary.parquet', index=False)
    log.info(f'  wrote placebo files in {time.time()-t0:.1f}s')
    log.info(f'  placebo summary:\n{summary_df.to_string()}')


def hour_of_day_profile():
    log.info('HOUR-OF-DAY Lerner profile')
    t0 = time.time()
    df = con.sql(f"""
        SELECT date, hour, firm, lerner_index
        FROM '{LER}'
        WHERE lerner_index BETWEEN 0 AND 1
    """).df()
    df['date'] = pd.to_datetime(df['date'])
    df['regime'] = df['date'].apply(assign_regime)
    out = df.groupby(['firm', 'hour', 'regime'])['lerner_index'].median().reset_index()
    out.to_parquet(REPO / 'data/derived/hour_of_day_lerner.parquet', index=False)
    log.info(f'  wrote hour_of_day_lerner.parquet in {time.time()-t0:.1f}s')
    ge = out[out['firm'] == 'GE'].pivot(index='regime', columns='hour',
                                          values='lerner_index').reindex(REGIME_ORDER)
    log.info(f'\nGE median Lerner heatmap (rows=regime, cols=hour):\n{ge.round(2).to_string()}')


def main():
    log.info('=' * 70)
    log.info(f'Robustness pipeline starting at {TS}')
    log.info('=' * 70)
    t0 = time.time()
    for fn in [bootstrap_lerner, slope_sensitivity, placebo_dates, hour_of_day_profile]:
        try:
            fn()
        except Exception as e:
            log.exception(f'{fn.__name__} FAILED: {e}')
    log.info('=' * 70)
    log.info(f'ALL DONE in {time.time()-t0:.1f}s')
    log.info(f'Log: {LOG_PATH}')


if __name__ == '__main__':
    main()
