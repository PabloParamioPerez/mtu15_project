"""Shared utilities for the `explore/` notebooks.

Consolidates the boilerplate that currently repeats verbatim across
notebooks 02, 03, and 04: the project-root discovery, the four reform-date
constants, the five-regime window list, and the `add_regime_shading` helper.

Importable from any notebook as:

    from mtu.notebook_utils import (
        PROJECT_ROOT,
        IDA_REFORM, ISP15_REFORM, INTRADAY_REFORM, DAY_AHEAD_REFORM,
        REGIME_WINDOWS, REGIME_COLORS, add_regime_shading,
    )
"""
from pathlib import Path
import pandas as pd

# Project root = three parents up from this file (src/mtu/notebook_utils.py).
PROJECT_ROOT = Path(__file__).resolve().parents[2]

# Reform dates (see CLAUDE.md §"Reform dates").
IDA_REFORM       = pd.Timestamp('2024-06-14')  # 6 MIBEL sessions → 3 European IDA sessions
ISP15_REFORM     = pd.Timestamp('2024-12-01')  # REE imbalance settlement period → MTU15
INTRADAY_REFORM  = pd.Timestamp('2025-03-19')  # OMIE intraday auctions + continuous → MTU15
DAY_AHEAD_REFORM = pd.Timestamp('2025-10-01')  # OMIE day-ahead → MTU15

# Five regime windows, chronological. (label, lo, hi) inclusive bounds.
REGIME_WINDOWS = [
    ('DA60/ID60 (6-sess)', pd.Timestamp('2023-12-01'), pd.Timestamp('2024-06-13')),
    ('DA60/ID60 (3-sess)', pd.Timestamp('2024-06-14'), pd.Timestamp('2024-11-30')),
    ('ISP15 window',       pd.Timestamp('2024-12-01'), pd.Timestamp('2025-03-18')),
    ('DA60/ID15',          pd.Timestamp('2025-03-19'), pd.Timestamp('2025-09-30')),
    ('DA15/ID15',          pd.Timestamp('2025-10-01'), pd.Timestamp('2030-01-01')),
]

# Five regime colours, chronological (pre-IDA | 3-sess | ISP15 | DA60/ID15 | DA15/ID15).
REGIME_COLORS = ['#e8f4f8', '#fff8e1', '#ffe0b2', '#fce4ec', '#e8f5e9']


def add_regime_shading(ax, start='2023-01-01', end='2026-06-01'):
    """Shade the five reform regimes on a time-axis matplotlib axis.

    Draws axvspan bands for each regime and dashed axvlines at the four
    reform dates. Matches the shading used in nb03 and nb04.
    """
    bounds = [IDA_REFORM, ISP15_REFORM, INTRADAY_REFORM, DAY_AHEAD_REFORM]
    edges = [pd.Timestamp(start)] + bounds + [pd.Timestamp(end)]
    for i, (lo, hi) in enumerate(zip(edges[:-1], edges[1:])):
        ax.axvspan(lo, hi, color=REGIME_COLORS[i], alpha=0.35, zorder=0)
    for d in bounds:
        ax.axvline(d, color='black', lw=0.8, ls='--', zorder=1)
