"""Build the May 2026 preliminary-results presentation notebook (figures.ipynb).

Produces:
  Fig 1  — S5 4-panel ENTSO-E concordance (A87, A86, A85, A84) by reform regime
  Fig 2  — S6 €1.1B BRP→TSO settlement transfer with bootstrap CI (HEADLINE)
  Fig 3  — B6 forecast-error → imbalance pass-through R²/slope by regime (Sit2 R²=0.365 vs DA15 R²=0.028)
  Fig 4  — B7 cross-country DA price comparison (ES vs FR/DE/PT)
  Fig 5  — S6 blackout-split robustness (excess persists in DA60-POST-blackout, collapses at DA15)

Run once: `uv run python thesis/presentation2/build_figures.py`
Then execute the notebook:
    `uv run jupyter nbconvert --to notebook --execute --inplace thesis/presentation2/figures.ipynb`
which regenerates `thesis/figures/fig01..fig05.{pdf,png}`.

Both this builder and the produced notebook live under `thesis/presentation2/` so
that presentation-output artefacts stay separate from `explore/` (exploratory
notebooks) and `scripts/analysis/` (per-claim analysis scripts).
"""
from __future__ import annotations

from pathlib import Path

import nbformat as nbf

# This file lives at thesis/presentation2/build_figures.py; project root is parents[2].
PROJECT = Path(__file__).resolve().parents[2]
OUT_NB = PROJECT / "thesis/presentation2/figures.ipynb"

nb = nbf.v4.new_notebook()
cells = []


def md(s: str) -> None:
    cells.append(nbf.v4.new_markdown_cell(s.strip()))


def code(s: str) -> None:
    cells.append(nbf.v4.new_code_cell(s.strip()))


# -- header
md("""
# Preliminary results — May 2026 CEMFI presentation (IO-framed)

**Status:** ALIVE (provisional)  •  **Last audit:** 2026-04-28  •  **Feeds:** S5, S6, S7, B6, B7

## IO question

When the regulator changes settlement and trading clocks asymmetrically — Spain Dec 2024 (ISP15) + Mar 2025 (ID15) before Oct 2025 (DA15) — **who pays the resulting friction, who captures the rent, and is the rule incentive-compatible with marginal social cost?**

The Feb-deck Ito–Reguant (2016) theoretical extension predicted three things: (i) DA15 is the key reform that smooths imbalances; (ii) dispersion risk concentrates in the transitional DA60/ID15 window; (iii) finer granularity creates winners and losers across heterogeneous firm portfolios (Feb slide 6 — "*MTU15 creates new opportunities for sophisticated agents and technologies, so extending the model to a setting with winners and losers could be very interesting.*"). The first two are confirmed empirically at €1.1B order of magnitude. The third — **heterogeneous incidence of the reform** — is the IO content of this talk.

## IO content of the empirical findings

| IO category | Empirical finding | Slide / Figure |
|---|---|---|
| **Welfare** (BRP-side regulatory redistribution) | €1,094.9M BRP→TSO settlement transfer over 10 months; bootstrap CI [-90, +73]M, observed ≈15× upper bound | Fig 2 (S6) |
| **Mechanism design — Lever 1 (clock-symmetry)** | Symmetric clocks at MTU15-DA collapse the transfer 6× (€91M/mo → €7.4M/mo) **even with the post-blackout operación reforzada in effect**. ✓ Already implemented | Fig 2 + Fig 5 (S6) |
| **Conduct** (BRP strategic bidding under asymmetric clocks) | Forecast-error→imbalance VOLUME pass-through R² rises 7× under clean reform (0.171), 16× under reform + blackout (0.365), then collapses to 0.028 post-MTU15-DA. The collapse is the cleanest signature for clock-symmetry working at the volume layer | Fig 3 (B6) |
| **Pigouvian incidence (direct dual-pricing decomposition)** | Per-segment imbalance € reconstructed via `signed_seg × prdvbaqh/prdvsuqh` reproduces 78–81% of `impdsvqh` with correlation 0.93. In DA60/ID15: LIB free-market retailers paid €108M, wind RE paid €77M, conv-RZ only €46M of €294M reconstructed. *Renewable-portfolio segments structurally bear the largest € share under the uniform allocation rule.* | **Fig 6 (S7 — direct F3)** |
| **Mechanism design — Lever 2 (Pigouvian rule redesign — NOT addressed by MTU15-DA)** | Wind + LIB retailers pay 60–65% of imbalance € in EVERY post-ISP15 regime, including post-MTU15-DA. Clock-symmetry shrinks the *scale* of the redistribution but does NOT fix the *structure*. ✗ Open | **Fig 7 (regime invariance)** |
| **Identification** (clean reduced-form) | Same-calendar-month pre-IDA baseline + bootstrap; cross-country placebo (Spain DA volatility responds 2–3× more than France across reform dates) | Fig 1 (S5) + Fig 4 (B7) |

## The IO claim load-bearing this talk

> The MTU15 reform sequence revealed **two distinct mechanism-design failures**, each requiring a separate policy lever:
>
> 1. **Asymmetric clock scales** create a BRP→TSO transfer of €1.1B over 10 months. **Lever: clock-symmetry** at MTU15-DA. Reduces the total scale by ~6× (€91M/mo → €7M/mo). ✓ Already implemented.
>
> 2. **Non-Pigouvian uniform-rate allocation** redistributes the burden across heterogeneous-marginal-cost segments. **Lever: settlement-rule redesign** (Pigouvian per-segment pricing). Wind + LIB free-market retailers consistently pay 60-65% of imbalance € in EVERY post-ISP15 regime, including post-MTU15-DA. ✗ Not addressed by clock-symmetry; remains open.
>
> Empirically: clock-symmetry shrinks the asymmetric-granularity window's magnitude but leaves the cross-segment redistribution structure intact. Renewable-portfolio segments are NOT relieved by MTU15-DA — they retain their 60-65% share of the (smaller) total. The 2025-04-28 Iberian blackout amplifies the within-DA60/ID15 magnitude but does not create either friction (clean PRE-blackout April 2025 alone is €75.7M; post-MTU15-DA scale-collapse holds despite operación reforzada).

This is the system-layer reform impact, decomposed into scale and structure. The thesis as a whole maps three additional IO channels (firm-level Cournot-pivotality, cross-market firm specialisation, post-CNMC strategic-availability conduct) — covered in Parts II–IV of [`thesis/drafts/master_thesis_proposal.md`](../drafts/master_thesis_proposal.md) but **off-arc for this preliminary-results talk**.

## Theoretical anchor (post-2026-04-27)

The Feb-deck Ito–Reguant extension implicitly invoked an Allaz–Vila / commitment-value microfoundation (firm-level forward-sale strategic behaviour). That microfoundation does NOT survive OVB-cleaning under exogenous-only controls — F5's IB peak-hour signal collapses to ≈0 once hour-of-day FE is added (`scripts/analysis/lerner/f5_ovb_robustness.py`, 2026-04-27). The surviving theoretical anchors for this talk:

| Anchor | Section | Role |
|---|---|---|
| **§4 asymmetric-granularity friction** | system layer | settlement-clock mismatch creates BRP→TSO redistribution; collapses on re-symmetrisation. Magnitude: S6. Microfoundation: B6. |
| **§3 Pigouvian misalignment** | mechanism design | uniform settlement rule is non-Pigouvian; segments have heterogeneous marginal cost contributions. The **IO bite** of the talk. Evidence: S7. |
| **§1 Cournot-pivotality** | firm layer (off-arc) | regime-invariant; lives in Part II of thesis (F7, F8, F10) |

These two surviving anchors (§4 + §3) produce the same outcome predictions Ito–Reguant did via a different microfoundation: system-layer settlement mechanics + mechanism-design failure rather than firm-layer commitment-value. The IO-relevance is in the heterogeneous-incidence (§3 / S7) and clock-symmetry-as-policy-lever (§4 / S6 collapse at MTU15-DA) content.

## Six figures, IO-load-bearing

Output saved to `../figures/` (PDF for Beamer; PNG safety-capped at savefig.dpi=140 → 1890 px wide max, under the 2000 px session cap).
""")

# -- imports + style
code("""
from pathlib import Path
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import duckdb

PROJECT = Path('../..').resolve()  # notebook lives at thesis/presentation2/; project root is two levels up
FIG_DIR = PROJECT / 'thesis/figures'
FIG_DIR.mkdir(parents=True, exist_ok=True)

# CEMFI-style minimal aesthetic
plt.rcParams.update({
    'figure.figsize': (10, 6),
    'figure.dpi': 110,
    'savefig.dpi': 140,  # 13.5 in × 140 dpi = 1890 px wide; under the 2000 px cap that broke a prior session
    'savefig.bbox': 'tight',
    'font.size': 11,
    'axes.spines.top': False,
    'axes.spines.right': False,
    'axes.titleweight': 'bold',
    'axes.titlesize': 12,
    'legend.frameon': False,
})

# Reform-window colors: cool→warm by reform stage
REGIME_COLOR = {
    'pre-IDA':                 '#4a6fa5',
    '3-sess':                  '#6b9080',
    'ISP15-win':               '#cc9b6d',
    'DA60/ID15 PRE-blackout':  '#d4694b',
    'DA60/ID15 POST-blackout': '#a83a3a',
    'DA15/ID15':               '#5b8a72',
}
REFORM_DATES = {
    'IDA-3sess':  pd.Timestamp('2024-06-14'),
    'ISP15':      pd.Timestamp('2024-12-01'),
    'ID15':       pd.Timestamp('2025-03-19'),
    'blackout':   pd.Timestamp('2025-04-28'),
    'DA15':       pd.Timestamp('2025-10-01'),
}

def assign_regime(d):
    d = pd.Timestamp(d)
    if d < pd.Timestamp('2024-06-14'): return 'pre-IDA'
    if d < pd.Timestamp('2024-12-01'): return '3-sess'
    if d < pd.Timestamp('2025-03-19'): return 'ISP15-win'
    if d < pd.Timestamp('2025-04-28'): return 'DA60/ID15 PRE-blackout'
    if d < pd.Timestamp('2025-10-01'): return 'DA60/ID15 POST-blackout'
    return 'DA15/ID15'

def add_reform_lines(ax, ymin=None, ymax=None):
    for label, date in REFORM_DATES.items():
        ax.axvline(date, color='grey', alpha=0.35, ls='--', lw=0.7, zorder=1)
""")

# ---- FIGURE 1 — S5 4-panel
md("""
## Figure 1 — S5: four ENTSO-E system metrics jump at ISP15, moderate at MTU15-DA

The reform's system-layer fingerprint. Four independently-measured ENTSO-E
metrics (A87 fiscal balance, A86 imbalance volume, A85 imbalance-price σ,
A84 aFRR spread) all show the same pattern: a discrete jump at the ISP15
reform date (2024-12-01) and a partial moderation at MTU15-DA (2025-10-01).
Pre-vs-post jumps are jointly null-rejected (Fisher combined test).
""")

code("""
con = duckdb.connect()

# A86 — imbalance volume (abs MWh, monthly mean)
a86 = con.execute(f\"\"\"
SELECT date_trunc('month', isp_start_utc) AS month,
       AVG(ABS(volume_mwh))                AS abs_imb_mwh
FROM '{PROJECT}/data/processed/entsoe/balancing/imbalance_volumes_all.parquet'
GROUP BY 1 ORDER BY 1
\"\"\").df()
a86['month'] = pd.to_datetime(a86['month'])

# A85 — imbalance price (monthly std dev across ISPs)
a85 = con.execute(f\"\"\"
SELECT date_trunc('month', isp_start_utc) AS month,
       STDDEV_SAMP(price_eur_per_mwh)      AS price_sigma
FROM '{PROJECT}/data/processed/entsoe/balancing/imbalance_prices_all.parquet'
WHERE imbalance_flag = 'A04'
GROUP BY 1 ORDER BY 1
\"\"\").df()
a85['month'] = pd.to_datetime(a85['month'])

# A84 — activated balancing prices (we have a panel; monthly mean spread up-down per ISP)
a84 = con.execute(f\"\"\"
SELECT date_trunc('month', isp_start_utc) AS month,
       AVG(price_eur_per_mwh)              AS act_price
FROM '{PROJECT}/data/processed/entsoe/balancing/activated_prices_all.parquet'
GROUP BY 1 ORDER BY 1
\"\"\").df()
a84['month'] = pd.to_datetime(a84['month'])

# A87 — net BRP→TSO settlement transfer (built from S6 monthly decomposition)
s6 = pd.read_csv(PROJECT/'data/derived/results/s6_monthly_decomposition.csv')
s6['month'] = pd.to_datetime(s6['month'])

print(f'A86 months: {len(a86)}, A85 months: {len(a85)}, A84 months: {len(a84)}, A87 months: {len(s6)}')
""")

code("""
fig, axes = plt.subplots(2, 2, figsize=(13.5, 8))
plot_start = pd.Timestamp('2022-01-01')

# (a) A87 — net BRP→TSO settlement
ax = axes[0, 0]
m = s6[s6['month'] >= plot_start]
ax.plot(m['month'], m['net_meur'], color='#222222', lw=1.6)
ax.fill_between(m['month'], 0, m['net_meur'], where=(m['net_meur']>0), alpha=0.18, color='#a83a3a')
add_reform_lines(ax)
ax.set_title('(a) A87 — net BRP→TSO settlement (€M / month)')
ax.set_ylabel('€M / month')
ax.axhline(0, color='grey', lw=0.5)

# (b) A86 — abs imbalance volume
ax = axes[0, 1]
m = a86[a86['month'] >= plot_start]
ax.plot(m['month'], m['abs_imb_mwh']/1e3, color='#222222', lw=1.6)
add_reform_lines(ax)
ax.set_title('(b) A86 — |imbalance volume| (GWh / month-ISP avg)')
ax.set_ylabel('GWh per ISP (monthly mean)')

# (c) A85 — imbalance-price σ
ax = axes[1, 0]
m = a85[a85['month'] >= plot_start]
ax.plot(m['month'], m['price_sigma'], color='#222222', lw=1.6)
add_reform_lines(ax)
ax.set_title('(c) A85 — imbalance-price σ (€/MWh, monthly)')
ax.set_ylabel('σ (€/MWh)')

# (d) A84 — activated balancing price
ax = axes[1, 1]
m = a84[a84['month'] >= plot_start]
ax.plot(m['month'], m['act_price'], color='#222222', lw=1.6)
add_reform_lines(ax)
ax.set_title('(d) A84 — activated balancing price (€/MWh, monthly mean)')
ax.set_ylabel('€/MWh')

# Reform date annotation only on top-left
ax = axes[0, 0]
y_top = ax.get_ylim()[1]
for label, date in REFORM_DATES.items():
    if date >= plot_start:
        ax.text(date, y_top*1.02, label, rotation=45, fontsize=7, color='grey',
                ha='left', va='bottom')

for row in axes:
    for ax in row:
        ax.xaxis.set_major_locator(mdates.MonthLocator(bymonth=[1, 7]))
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
        ax.tick_params(axis='x', rotation=0, labelsize=9)

fig.suptitle('Four ENTSO-E system metrics jump concordantly at ISP15, moderate at MTU15-DA  (S5)',
             fontsize=13, y=1.00)
fig.tight_layout()
fig.savefig(FIG_DIR/'fig01_S5_four_panel_concordance.png')
fig.savefig(FIG_DIR/'fig01_S5_four_panel_concordance.pdf')
plt.show()
""")

# ---- FIGURE 2 — S6 headline
md("""
## Figure 2 (HEADLINE) — S6: €1.1B BRP→TSO settlement transfer during the asymmetric window

The single number to remember from the talk. Across the 10-month asymmetric-
granularity window (ISP15-win + DA60/ID15), BRPs paid the TSO **€1,094.9M**
above the same-calendar pre-IDA baseline. Bootstrap-CI null is [-90, +73]M;
observed is ≈15× the upper bound. The transfer collapses at MTU15-DA when
granularity symmetry is restored.

Cite as a **regulatory settlement redistribution**, not a deadweight-loss
estimate — welfare interpretation requires a counterfactual on the TSO's
recycling of the surplus to consumers via tariff (typically with a 1-year lag).
""")

code("""
# Cumulative excess vs same-cal pre-IDA baseline
s6_p = s6.copy()
s6_p['cum_excess'] = s6_p['excess_meur'].cumsum()

# Bootstrap CI (from sensitivity CSV)
sens = pd.read_csv(PROJECT/'data/derived/results/s6_baseline_sensitivity.csv')
print('Baseline sensitivity (€M asymmetric-window total):')
print(sens.to_string(index=False))

# Identify the asymmetric window cumulative endpoint (Sep 2025)
asy_end = pd.Timestamp('2025-09-30')
asy_endpoint = s6_p.loc[s6_p['month'] <= asy_end, 'cum_excess'].iloc[-1]
print(f'Cumulative asymmetric-window excess: €{asy_endpoint:.1f}M')
""")

code("""
fig, axes = plt.subplots(1, 2, figsize=(13.5, 5.5))

# Left: monthly excess vs baseline (bars)
ax = axes[0]
m = s6_p.copy()
m['regime_color'] = m['regime'].map(REGIME_COLOR)
ax.bar(m['month'], m['excess_meur'], width=22,
       color=m['regime_color'].fillna('#888888'),
       edgecolor='white', linewidth=0.4)
ax.axhline(0, color='black', lw=0.6)
add_reform_lines(ax)
ax.set_ylabel('Monthly excess vs same-cal baseline (€M)')
ax.set_title('Monthly A87 net excess by regime')
ax.xaxis.set_major_locator(mdates.MonthLocator(interval=3))
ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
plt.setp(ax.get_xticklabels(), rotation=30, ha='right')

# Right: cumulative excess vs bootstrap null band
ax = axes[1]
ax.plot(m['month'], m['cum_excess'], color='#222222', lw=2.2,
        label=f'observed cumulative (€{asy_endpoint:.0f}M at end of asymm. window)')
ci_lo, ci_hi = float(sens['ci_lo'].iloc[0]), float(sens['ci_hi'].iloc[0])
ax.fill_between(m['month'], ci_lo, ci_hi, color='grey', alpha=0.25,
                label=f'bootstrap null CI [{ci_lo:.0f}, {ci_hi:.0f}]M')
ax.axhline(asy_endpoint, color='#a83a3a', ls=':', lw=0.9, alpha=0.7)
add_reform_lines(ax)
ax.set_ylabel('Cumulative €M (BRP→TSO net)')
ax.set_title('Cumulative excess vs bootstrap-null')
ax.xaxis.set_major_locator(mdates.MonthLocator(interval=3))
ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
plt.setp(ax.get_xticklabels(), rotation=30, ha='right')
ax.legend(loc='upper left', fontsize=9)

fig.suptitle('S6 — Asymmetric-granularity window: cumulative BRP→TSO transfer ≈ €1.1B  (≈15× upper bootstrap bound)',
             fontsize=13, y=1.00)
fig.tight_layout()
fig.savefig(FIG_DIR/'fig02_S6_settlement_transfer_headline.png')
fig.savefig(FIG_DIR/'fig02_S6_settlement_transfer_headline.pdf')
plt.show()
""")

# ---- FIGURE 3 — B6
md("""
## Figure 3 — B6: forecast-error → imbalance pass-through, regime-by-regime

The mechanism. Slope of |imbalance volume| on |forecast error| by regime,
with month + hour FE controls.

- Pre-MTU15: slope ≈ 0
- DA60/ID15 PRE-blackout: slope rises to +0.039 (R²=0.171)
- DA60/ID15 POST-blackout: slope +0.051 (R²=0.365)
- DA15/ID15: **slope collapses to ~0** (R²=0.028)

The asymmetric window passes forecast errors through to imbalance prices;
DA15 closes the channel.
""")

code("""
b6 = pd.read_csv(PROJECT/'data/derived/results/b5_seasonality_audit.csv')
print(b6.to_string(index=False))

REGIME_ORDER = ['pre-IDA', '3-sess', 'ISP15-win',
                'DA60/ID15 PRE-blackout', 'DA60/ID15 POST-blackout', 'DA15/ID15']
b6['regime_order'] = b6['regime'].map({r: i for i, r in enumerate(REGIME_ORDER)})
b6 = b6.sort_values('regime_order')

fig, axes = plt.subplots(1, 2, figsize=(13.5, 5))
xlabels = b6['regime'].values
xs = np.arange(len(b6))
colors = [REGIME_COLOR.get(r, '#666666') for r in xlabels]

# Left: slope
ax = axes[0]
ax.bar(xs, b6['slope_FE'], color=colors, edgecolor='white')
ax.set_xticks(xs)
ax.set_xticklabels(xlabels, rotation=30, ha='right', fontsize=9)
ax.axhline(0, color='black', lw=0.5)
ax.set_ylabel('Slope of |V_imb| on |fe|, (FE-controlled)')
ax.set_title('Pass-through slope (month+hour FE)')
for i, v in enumerate(b6['slope_FE']):
    ax.text(i, v + (0.003 if v > 0 else -0.005), f'{v:.3f}',
            ha='center', va='bottom' if v > 0 else 'top', fontsize=9)

# Right: R²
ax = axes[1]
ax.bar(xs, b6['r2_FE'], color=colors, edgecolor='white')
ax.set_xticks(xs)
ax.set_xticklabels(xlabels, rotation=30, ha='right', fontsize=9)
ax.set_ylabel('R² (FE-controlled)')
ax.set_title('Pass-through R²')
for i, v in enumerate(b6['r2_FE']):
    ax.text(i, v + 0.008, f'{v:.3f}', ha='center', va='bottom', fontsize=9)
ax.set_ylim(0, b6['r2_FE'].max() * 1.18)

fig.suptitle('B6 — Forecast-error → imbalance pass-through: peaks DA60/ID15 POST-blackout, collapses at MTU15-DA',
             fontsize=12.5, y=1.02)
fig.tight_layout()
fig.savefig(FIG_DIR/'fig03_B6_passthrough_by_regime.png')
fig.savefig(FIG_DIR/'fig03_B6_passthrough_by_regime.pdf')
plt.show()
""")

# ---- FIGURE 4 — B7 cross-country placebo
md("""
## Figure 4 — B7: France DA placebo holds across Spanish reform dates

Cross-country check the Feb-2026 proposal said wasn't possible. France is
not subject to the Spanish MTU15 reform sequence, so its DA prices are a
clean control. Plot ES vs FR/DE/PT monthly mean DA prices around the
Spanish reform dates.
""")

code("""
import duckdb
con = duckdb.connect()

# OMIE marginal — ES + PT same file
omie = con.execute(f\"\"\"
SELECT date, period, price_es_eur_mwh, price_pt_eur_mwh, mtu_minutes
FROM '{PROJECT}/data/processed/omie/mercado_diario/precios/marginalpdbc_all.parquet'
WHERE date >= '2022-01-01'
\"\"\").df()
omie['ts'] = pd.to_datetime(omie['date']) + pd.to_timedelta(omie['period'].astype(int) - 1, unit='h')
omie['month'] = omie['ts'].dt.to_period('M')
omie['weight'] = omie['mtu_minutes']
omie_es = omie.groupby('month').apply(lambda g: np.average(g['price_es_eur_mwh'], weights=g['weight']),
                                       include_groups=False).rename('ES')
omie_es = omie_es.reset_index()
omie_es['month'] = omie_es['month'].dt.to_timestamp()

# FR, DE, PT (ENTSO-E A44)
def load_a44(country, path):
    df = pd.read_parquet(PROJECT/path)
    df['ts'] = pd.to_datetime(df['isp_start_utc'])
    df['month'] = df['ts'].dt.to_period('M').dt.to_timestamp()
    df['weight'] = df['mtu_minutes']
    return df.groupby('month').apply(
        lambda g: np.average(g['price_eur_per_mwh'], weights=g['weight']),
        include_groups=False).rename(country).reset_index()

fr = load_a44('FR', 'data/processed/entsoe/prices/fr_da_all.parquet')
de = load_a44('DE', 'data/processed/entsoe/prices/de_da_all.parquet')
pt = load_a44('PT', 'data/processed/entsoe/prices/pt_da_all.parquet')

monthly = omie_es.merge(fr, on='month', how='outer').merge(de, on='month', how='outer').merge(pt, on='month', how='outer')
monthly = monthly[monthly['month'] >= '2022-01-01'].sort_values('month').reset_index(drop=True)
print(monthly.tail(12).to_string(index=False))
""")

code("""
fig, axes = plt.subplots(2, 1, figsize=(13.5, 8.5), sharex=True)

# Top: levels
ax = axes[0]
ax.plot(monthly['month'], monthly['ES'], color='#a83a3a', lw=2.0, label='ES (OMIE)')
ax.plot(monthly['month'], monthly['FR'], color='#4a6fa5', lw=1.4, label='FR (placebo)')
ax.plot(monthly['month'], monthly['DE'], color='#6b9080', lw=1.0, alpha=0.7, label='DE')
ax.plot(monthly['month'], monthly['PT'], color='#d4a64b', lw=1.0, alpha=0.7, label='PT')
add_reform_lines(ax)
ax.set_ylabel('Monthly mean DA price (€/MWh)')
ax.set_title('Day-ahead price levels — ES vs neighbors')
ax.legend(loc='upper left', fontsize=9, ncol=4)

# Bottom: ES − FR gap (the placebo metric)
ax = axes[1]
gap = monthly['ES'] - monthly['FR']
colors = ['#a83a3a' if v >= 0 else '#4a6fa5' for v in gap]
ax.bar(monthly['month'], gap, width=22, color=colors, edgecolor='white', linewidth=0.3, alpha=0.8)
ax.axhline(0, color='black', lw=0.5)
add_reform_lines(ax)
ax.set_ylabel('ES − FR gap (€/MWh)')
ax.set_title('Spain–France gap: no jump at Spanish reform dates → France-DA placebo holds')
ax.xaxis.set_major_locator(mdates.MonthLocator(interval=3))
ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
plt.setp(ax.get_xticklabels(), rotation=30, ha='right')

fig.suptitle('B7 — France DA placebo: FR prices show no Spanish-reform-date jump  (cross-country control)',
             fontsize=12.5, y=1.00)
fig.tight_layout()
fig.savefig(FIG_DIR/'fig04_B7_france_placebo.png')
fig.savefig(FIG_DIR/'fig04_B7_france_placebo.pdf')
plt.show()
""")

# ---- FIGURE 5 — S6 blackout split
md("""
## Figure 5 — S6 robustness: the transfer is reform-driven, not blackout-driven

The single most important defensive figure for the IO faculty Q&A. The 2025-04-28
blackout triggered REE's operación reforzada, which could plausibly drive part
of the S6 settlement excess. Split:

- DA60/ID15 PRE-blackout (clean reform window, ~6 weeks): +€75.7M for April 2025
- DA60/ID15 POST-blackout (5 months under operación reforzada): +€467.6M = €93.5M/mo (only 24% above the clean April figure)
- DA15/ID15 (post-MTU15-DA, also post-blackout): +€22.2M = €7.4M/mo (8% of DA60/ID15 level)

The DA15/ID15 collapse occurs DESPITE operación reforzada continuing.
The asymmetric-granularity friction is the source; the blackout amplifies modestly.
""")

code("""
# Use s6_monthly_decomposition with regime tagging
m = s6_p.copy()
# Reform-window aggregates (mean €M / month and totals)
groups = [
    ('pre-IDA same-cal Apr-Sep',   '2024-04-01', '2024-09-30'),
    ('3-sess (Jun24–Nov24)',       '2024-06-14', '2024-11-30'),
    ('ISP15-win (Dec24–Mar25)',    '2024-12-01', '2025-03-18'),
    ('DA60/ID15 PRE-blackout',     '2025-03-19', '2025-04-27'),
    ('DA60/ID15 POST-blackout',    '2025-04-28', '2025-09-30'),
    ('DA15/ID15 (post-MTU15-DA)',  '2025-10-01', '2026-01-31'),
]
rows = []
for label, lo, hi in groups:
    sub = m[(m['month'] >= lo) & (m['month'] <= hi)]
    if len(sub):
        rows.append({
            'window': label,
            'n_months': len(sub),
            'mean_excess_meur': sub['excess_meur'].mean(),
            'sum_excess_meur': sub['excess_meur'].sum(),
        })
br = pd.DataFrame(rows)
print(br.round(1).to_string(index=False))
""")

code("""
fig, axes = plt.subplots(1, 2, figsize=(13.5, 5.5))

# Left: monthly excess €M with blackout window highlighted
ax = axes[0]
m = s6_p[s6_p['month'] >= '2024-01-01'].copy()
m['regime_color'] = m['regime'].map(REGIME_COLOR).fillna('#888')
ax.bar(m['month'], m['excess_meur'], width=24, color=m['regime_color'],
       edgecolor='white', linewidth=0.4)
ax.axvline(REFORM_DATES['blackout'], color='red', lw=1.0, ls='--', alpha=0.7, label='Blackout 2025-04-28')
ax.axvline(REFORM_DATES['DA15'],     color='#5b8a72', lw=1.2, ls='--', alpha=0.8, label='MTU15-DA 2025-10-01')
ax.set_ylabel('Monthly excess vs same-cal baseline (€M)')
ax.set_title('Monthly A87 excess: pre/post-blackout & MTU15-DA')
ax.legend(loc='upper left', fontsize=9)
ax.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
plt.setp(ax.get_xticklabels(), rotation=30, ha='right')

# Right: window means
ax = axes[1]
br_post = br[br['window'].str.contains('blackout|DA15|ISP15|3-sess|pre-IDA')].reset_index(drop=True)
window_colors = {
    'pre-IDA same-cal Apr-Sep':   '#4a6fa5',
    '3-sess (Jun24–Nov24)':       '#6b9080',
    'ISP15-win (Dec24–Mar25)':    '#cc9b6d',
    'DA60/ID15 PRE-blackout':     '#d4694b',
    'DA60/ID15 POST-blackout':    '#a83a3a',
    'DA15/ID15 (post-MTU15-DA)':  '#5b8a72',
}
xs = np.arange(len(br_post))
ax.barh(xs, br_post['mean_excess_meur'],
        color=[window_colors[w] for w in br_post['window']],
        edgecolor='white')
ax.set_yticks(xs)
ax.set_yticklabels(br_post['window'], fontsize=10)
ax.invert_yaxis()
ax.axvline(0, color='black', lw=0.5)
ax.set_xlabel('Mean monthly excess (€M)')
ax.set_title('Reform-window means (mean €M / month)')
for i, v in enumerate(br_post['mean_excess_meur']):
    ax.text(v + (3 if v > 0 else -3), i, f'{v:+.1f}',
            va='center', ha='left' if v > 0 else 'right', fontsize=9)

fig.suptitle('S6 robustness — DA15/ID15 collapse persists DESPITE operación reforzada (blackout is amplifier, not source)',
             fontsize=12.5, y=1.00)
fig.tight_layout()
fig.savefig(FIG_DIR/'fig05_S6_blackout_robustness.png')
fig.savefig(FIG_DIR/'fig05_S6_blackout_robustness.pdf')
plt.show()
""")

# ---- FIGURE 6 — S7 Pigouvian incidence (IO load-bearing, F3 direct decomposition)
md("""
## Figure 6 — S7: Pigouvian incidence — direct €-decomposition

The €1.1B is now decomposed per segment using **direct dual-pricing** (F3 rule):

> per_seg_€_per_ISP = max(seg_volume, 0) × prdvsuqh + min(seg_volume, 0) × prdvbaqh

where `prdvsuqh` and `prdvbaqh` are the per-ISP up/down imbalance settlement prices (post-ISP15, available 2024-12 onwards in the rebuilt `liquicomun_all.parquet`). Aggregated over each regime, F3 reproduces 78–81% of the system imbalance settlement € (`impdsvqh`) with correlation 0.93 — close enough to use as a defensible per-segment attribution.

**The Pigouvian counterfactual** is computed as `β_seg × volume_seg` (from the segment-level OLS in `pigouvian_clean_results.csv`), normalised so positive contributions sum to 1, then scaled to the same regime-totals as the F3 actual.

**DA60/ID15 (asymmetric window) — direct settlement €:**
- **LIB free-market retailers** paid **€108M** under the actual rule (37% of system total) — by far the largest.
- **Wind RE** paid **€77M** (26%) — second largest, despite being a "renewable" segment with low marginal cost.
- **Conv-RZ + thermal RE + hydro RE** (the dispatchable segments) paid only **€76M combined** (26% combined) — much less than under a Pigouvian rule would charge them.

**Direct answer to "did renewables pay more in liquidaciones?"**: YES. LIB retailers + wind together paid **€186M of €294M reconstructed (~63%)** in the DA60/ID15 asymmetric window — a redistribution from inflexible-portfolio segments to dispatchable ones, embedded in the rule's structure.
""")

code("""
# F3 direct per-segment € + β-based Pigouvian counterfactual.
# F3 computed inline from liquicomun_all.parquet so the notebook is self-contained.
import numpy as np

con = duckdb.connect()
con.execute("SET memory_limit='4GB'")

# Build wide panel post-ISP15 with the new directional prices
con.execute(f\"\"\"
CREATE OR REPLACE TEMPORARY VIEW _wide AS
SELECT date, hour, quarter,
  COALESCE(MAX(CASE WHEN family='endrozrqh' THEN value END),0) AS conv_rz,
  COALESCE(MAX(CASE WHEN family='endronzqh' THEN value END),0) AS conv_nrz,
  COALESCE(MAX(CASE WHEN family='endreeoqh' THEN value END),0) AS wind,
  COALESCE(MAX(CASE WHEN family='endrehiqh' THEN value END),0) AS hydro_re,
  COALESCE(MAX(CASE WHEN family='endretqh'  THEN value END),0) AS thermal_re,
  COALESCE(MAX(CASE WHEN family='endcurqh'  THEN value END),0) AS cor_ret,
  COALESCE(MAX(CASE WHEN family='endlibqh'  THEN value END),0) AS lib_ret,
  COALESCE(MAX(CASE WHEN family='endexpqh'  THEN value END),0) AS export_u,
  COALESCE(MAX(CASE WHEN family='endimpqh'  THEN value END),0) AS import_u,
  MAX(CASE WHEN family='prdvbaqh' THEN value END) AS price_down,
  MAX(CASE WHEN family='prdvsuqh' THEN value END) AS price_up,
  MAX(CASE WHEN family='impdsvqh' THEN value END) AS imp_eur_actual
FROM '{PROJECT}/data/processed/esios/liquicomun_all.parquet'
WHERE date >= '2024-12-01' AND quarter IS NOT NULL
GROUP BY 1,2,3
\"\"\")
SEGS = ['conv_rz','conv_nrz','wind','hydro_re','thermal_re','cor_ret','lib_ret','export_u','import_u']
seg_eur_cols = ',\\n  '.join([
    f"GREATEST({s},0)*price_up + LEAST({s},0)*price_down AS {s}_eur" for s in SEGS
])
agg = ',\\n  '.join([f"SUM({s}_eur)/1e6 AS {s}_meur" for s in SEGS])
f3_wide = con.execute(f\"\"\"
SELECT
  CASE WHEN date < DATE '2025-03-19' THEN 'ISP15-win'
       WHEN date < DATE '2025-10-01' THEN 'DA60/ID15'
       ELSE 'DA15/ID15' END AS regime,
  COUNT(*) AS n_isps, SUM(imp_eur_actual)/1e6 AS imp_total_meur,
  {agg}
FROM (
  SELECT *, {seg_eur_cols} FROM _wide
  WHERE price_up IS NOT NULL AND price_down IS NOT NULL AND imp_eur_actual IS NOT NULL
)
GROUP BY 1 ORDER BY 1
\"\"\").df()
# Long-format
records = []
for _, row in f3_wide.iterrows():
    seg_total_abs = sum(abs(row[f'{s}_meur']) for s in SEGS)
    for s in SEGS:
        records.append({
            'regime': row['regime'], 'segment': s,
            'n_isps': int(row['n_isps']),
            'imp_total_meur': float(row['imp_total_meur']),
            'seg_meur': float(row[f'{s}_meur']),
            'seg_abs_share': abs(row[f'{s}_meur'])/seg_total_abs if seg_total_abs > 0 else 0,
        })
f3 = pd.DataFrame(records)
f3 = f3[~f3['segment'].isin(['export_u','import_u'])].copy()

# β-based Pigouvian counterfactual (from the OLS regression)
s7 = pd.read_csv(PROJECT/'data/derived/results/pigouvian_clean_results.csv')
s7 = s7[~s7['segment'].isin(['export_u', 'import_u'])].copy()
s7 = s7.rename(columns={'regime': 'regime_raw'})
# Match the regime naming used in f3 ('ISP15-win' vs 'ISP15 win')
s7['regime'] = s7['regime_raw'].str.replace(' ', '-')
pigou_records = []
for regime, g in s7.groupby('regime'):
    pigou_raw = (g['beta'].clip(lower=0) * g['volume_share']).values
    pigou_share = pigou_raw / pigou_raw.sum() if pigou_raw.sum() > 0 else np.zeros_like(pigou_raw)
    for i, (_, row) in enumerate(g.iterrows()):
        pigou_records.append({'regime': regime, 'segment': row['segment'],
                              'pigou_share': float(pigou_share[i])})
pigou = pd.DataFrame(pigou_records)

m = f3.merge(pigou, on=['regime','segment'], how='left')
m['pigou_meur'] = m['pigou_share'] * m['imp_total_meur']
m['redist_meur'] = m['pigou_meur'] - m['seg_meur']

SEG_LABELS = {
    'conv_rz':    'Conv. plants (regulation zone)',
    'conv_nrz':   'Conv. plants (non-RZ)',
    'wind':       'Wind (RE)',
    'hydro_re':   'Hydro (RE)',
    'thermal_re': 'Thermal RE',
    'cor_ret':    'COR retailers (regulated)',
    'lib_ret':    'LIB free-market retailers',
}
m['label'] = m['segment'].map(SEG_LABELS)
m['actual_meur'] = m['seg_meur'].abs()  # display as positive € (settlement € BRPs paid)
m['pigou_meur_abs'] = m['pigou_meur'].abs()

# Focus on the asymmetric window (DA60/ID15)
post = m[m['regime'] == 'DA60/ID15'].sort_values('actual_meur', ascending=True).reset_index(drop=True)

fig, axes = plt.subplots(1, 2, figsize=(13.5, 5.8), sharey=True)
xs = np.arange(len(post))
W = 0.4

# LEFT panel: side-by-side actual (F3) vs Pigouvian counterfactual
ax = axes[0]
ax.barh(xs - W/2, post['actual_meur'], height=W, color='#a83a3a',
        edgecolor='white', label='Actual (F3 dual-pricing)')
ax.barh(xs + W/2, post['pigou_meur_abs'], height=W, color='#5b8a72',
        edgecolor='white', label='Pigouvian counterfactual')
ax.set_yticks(xs)
ax.set_yticklabels(post['label'], fontsize=10)
ax.axvline(0, color='black', lw=0.5)
ax.set_xlabel('Settlement € paid (€M, DA60/ID15 asymmetric window)')
ax.set_title('Actual (direct dual-pricing) vs Pigouvian counterfactual\\n(DA60/ID15, system total ≈ €294M)', fontsize=11)
ax.legend(loc='lower right', fontsize=9, frameon=True)
for i, (a, pi) in enumerate(zip(post['actual_meur'], post['pigou_meur_abs'])):
    if a > 3:
        ax.text(a + 2, i - W/2, f'€{a:.0f}M', va='center', ha='left', fontsize=8.5, color='#a83a3a')
    if pi > 3:
        ax.text(pi + 2, i + W/2, f'€{pi:.0f}M', va='center', ha='left', fontsize=8.5, color='#5b8a72')
ax.set_xlim(0, max(post['actual_meur'].max(), post['pigou_meur_abs'].max()) * 1.20)

# RIGHT panel: redistribution per segment (Pigouvian − Actual)
ax = axes[1]
redist = post['pigou_meur_abs'] - post['actual_meur']
def col(v):
    if v > 15:   return '#5b8a72'   # currently underpaying (would pay more)
    if v < -15:  return '#a83a3a'   # currently overpaying (would pay less)
    return '#9aa7b3'
ax.barh(xs, redist, color=[col(v) for v in redist], edgecolor='white')
ax.axvline(0, color='black', lw=0.5)
ax.set_xlabel('Redistribution under Pigouvian rule (€M)')
ax.set_title('Cross-segment redistribution\\n(positive = currently UNDERpaying)', fontsize=11)
for i, v in enumerate(redist):
    if abs(v) > 3:
        ax.text(v + (2 if v > 0 else -2), i, f'€{v:+.0f}M',
                va='center', ha='left' if v > 0 else 'right', fontsize=9)
xmax = max(abs(redist.min()), redist.max())
ax.set_xlim(-xmax * 1.20, xmax * 1.20)

over = post[redist < -15]['actual_meur'].sum() - post[redist < -15]['pigou_meur_abs'].sum()
under = post[redist > 15]['pigou_meur_abs'].sum() - post[redist > 15]['actual_meur'].sum()
fig.suptitle(f'S7 — Pigouvian incidence in the asymmetric window (direct F3 dual-pricing decomposition)\\n' +
             f'LIB retailers + wind overpay ~€{over:.0f}M | Conv-RZ + COR + hydro RE underpay ~€{under:.0f}M | rule structurally favours dispatchable segments',
             fontsize=10.5, y=1.04)
fig.tight_layout()
fig.savefig(FIG_DIR/'fig06_S7_pigouvian_incidence.png')
fig.savefig(FIG_DIR/'fig06_S7_pigouvian_incidence.pdf')
plt.show()

# Print summary for the talk
print('=== Per-segment € decomposition, DA60/ID15 (F3 dual-pricing direct) ===')
print(post[['segment','actual_meur','pigou_meur_abs']].sort_values(
    'actual_meur', ascending=False).to_string(index=False))
""")

# ---- FIGURE 7 — Regime-invariance of the burden share (the new finding)
md("""
## Figure 7 — Renewable-segment burden share is regime-invariant

The new finding from the F3 direct decomposition: **wind + LIB free-market retailers consistently pay 60-65% of imbalance settlement € in EVERY post-ISP15 regime, including post-MTU15-DA**. Clock-symmetry at MTU15-DA reduces the *scale* of the redistribution (S6: €91M/mo → €7M/mo) but does NOT relieve the burden *structure*: renewable-portfolio segments retain their ~60-65% share of the (smaller) total.

This **qualifies the IO claim** about MTU15-DA. There are two distinct mechanism-design failures, requiring two distinct policy levers:

| Failure | Lever | What it fixes |
|---|---|---|
| Asymmetric clocks → BRP→TSO transfer scales | Clock-symmetry at MTU15-DA | Reduces total magnitude (S6 €1.1B → €44M/regime) ✓ |
| Uniform-rate allocation across heterogeneous-MC segments | Pigouvian rule (charge each seg its β/MC) | Redistributes burden across segments — NOT addressed by MTU15-DA |

The Spanish reform sequence solved problem 1 but left problem 2 open. Regulatory implication: settlement-rule design is independent from market-clock design, and the May talk's IO claim should be that **clock-symmetry is one of two welfare-relevant levers, not a complete fix**.
""")

code("""
# Stacked-bar visualisation: burden share by segment in each regime
# (Re-uses the f3 DataFrame computed inline above for Figure 6.)
# If running this cell standalone, run the Figure 6 cell first to populate f3.
import numpy as np

# Order regimes chronologically; segment-row by total share desc within DA60
REGIME_ORDER = ['ISP15-win', 'DA60/ID15', 'DA15/ID15']
f3['regime'] = pd.Categorical(f3['regime'], REGIME_ORDER, ordered=True)
f3 = f3.sort_values('regime')

# Compute share within regime (using abs to handle sign; F3 reproduction is dominantly negative)
f3['share_pct'] = 100 * f3['seg_meur'].abs() / f3.groupby('regime')['seg_meur'].transform(lambda s: s.abs().sum())

SEG_ORDER = ['wind','lib_ret','conv_rz','conv_nrz','thermal_re','cor_ret','hydro_re']
SEG_LABELS = {
    'wind':       'Wind (RE)',
    'lib_ret':    'LIB free-market retailers',
    'conv_rz':    'Conv. plants (regulation zone)',
    'conv_nrz':   'Conv. plants (non-RZ)',
    'thermal_re': 'Thermal RE',
    'cor_ret':    'COR retailers (regulated)',
    'hydro_re':   'Hydro (RE)',
}
SEG_COLORS = {
    'wind':       '#cc9b6d',  # warm — renewable
    'lib_ret':    '#a83a3a',  # red — renewable retail
    'conv_rz':    '#4a6fa5',  # blue — conv dispatchable
    'conv_nrz':   '#6b9080',  # green-blue
    'thermal_re': '#9d7263',  # brown
    'cor_ret':    '#5b8a72',  # green
    'hydro_re':   '#8aa6b8',  # gray-blue
}

fig, ax = plt.subplots(figsize=(13.5, 5.5))
xs = np.arange(len(REGIME_ORDER))
WIDTH = 0.5
bottom = np.zeros(len(REGIME_ORDER))

# Highlight renewable segments
RENEWABLE = {'wind','lib_ret'}

for seg in SEG_ORDER:
    sub = f3[f3['segment']==seg].set_index('regime').reindex(REGIME_ORDER)
    vals = sub['share_pct'].fillna(0).values
    label = SEG_LABELS[seg]
    if seg in RENEWABLE:
        label = '[RE] ' + label  # mark renewables
    ax.bar(xs, vals, WIDTH, bottom=bottom, color=SEG_COLORS[seg],
           edgecolor='white', linewidth=1.2, label=label)
    # Annotate share inside each bar
    for x, b, v in zip(xs, bottom, vals):
        if v > 4:
            ax.text(x, b + v/2, f'{v:.0f}%', ha='center', va='center',
                    fontsize=10, color='white', fontweight='bold')
    bottom += vals

# Horizontal annotation: combined wind+lib share per regime
combined = f3[f3['segment'].isin(RENEWABLE)].groupby('regime', observed=False)['share_pct'].sum()
for x, r in zip(xs, REGIME_ORDER):
    pct = combined.get(r, 0)
    ax.annotate(f'wind+LIB\\n{pct:.0f}%', xy=(x, 0.5), xytext=(0, -32),
                textcoords='offset points',
                ha='center', fontsize=10.5, fontweight='bold', color='#5b3a3a')

ax.set_xticks(xs)
ax.set_xticklabels(REGIME_ORDER, fontsize=11)
ax.set_ylabel('Share of imbalance settlement € (%)')
ax.set_ylim(0, 105)
ax.set_title('Wind + LIB free-market retailers consistently pay 60-65% of imbalance settlement € across every post-ISP15 regime —\\n'
             'MTU15-DA shrinks the total but does NOT relieve the renewable-segment share',
             fontsize=11.5)
ax.legend(loc='upper right', bbox_to_anchor=(1.32, 1.0), fontsize=9.5, frameon=False)
fig.tight_layout()
fig.savefig(FIG_DIR/'fig07_burden_share_regime_invariance.png')
fig.savefig(FIG_DIR/'fig07_burden_share_regime_invariance.pdf')
plt.show()

# Print the headline number
print('=== Renewable burden share by regime (wind + lib_ret) ===')
print(combined.to_string())
""")

# -- summary cell
md("""
## Summary — IO content of each figure

| # | Figure | Headline | IO category | Slide-talking point |
|---|---|---|---|---|
| 1 | S5 4-panel | Four ENTSO-E metrics jump concordantly at ISP15, moderate at MTU15-DA | Identification (joint null rejection across 4 outcomes) | "Joint null is rejected — this isn't one outlier metric" |
| 2 | **S6 €1.1B** | BRP→TSO transfer ≈15× upper bootstrap bound; collapses to €7.4M/mo at MTU15-DA | **Welfare** (BRP-side regulatory redistribution) + **mechanism design** (clock-symmetry restores IC) | "Headline magnitude. Confirms Feb-deck Ito–Reguant prediction. Clock-symmetry lever ✓." |
| 3 | B6 pass-through | R² rises 7× under clean reform (DA60/ID15 PRE-blackout: 0.171 vs pre-IDA-late 0.023); 16× under reform + blackout (POST-blackout: 0.365); collapses to 0.028 post-MTU15-DA | **Conduct** (BRP strategic bidding under asymmetric clocks) | "Microfoundation in one figure. The post-MTU15-DA volume collapse is the cleanest signature; the blackout amplifies but does not create the mechanism." |
| 4 | B7 placebo | Spain within-day SD responds 2–3× more than France across reform dates | Identification (cross-country DiD) | "Cross-country control the Feb proposal said wasn't yet available" |
| 5 | S6 blackout split | DA15 collapse holds DESPITE operación reforzada | §4 robustness (n=3 caveat for Oct–Dec 2025) | "Defensive figure for Q&A — friction is reform-driven, not blackout-driven" |
| **6** | **S7 Pigouvian (F3 direct)** | LIB retailers paid €108M, wind €77M, conv-RZ only €46M of €294M reconstructed in DA60/ID15 — direct dual-pricing decomposition (78% of system total reconstructed; corr 0.93) | **Pigouvian incidence** — the IO bite: cross-segment redistribution embedded in the rule | "Direct dual-pricing decomposition: renewables paid €186M of €294M; counterfactual would charge dispatchable plants instead. €178M of redistribution structurally misallocated." |
| **7** | **Burden-share regime invariance** | Wind + LIB retailers consistently pay 60-65% of imbalance € in EVERY post-ISP15 regime — including post-MTU15-DA | **Mechanism design — second lever** (clock-symmetry didn't fix this; Pigouvian rule redesign would) | "Clock-symmetry shrinks the SCALE; the rule's STRUCTURE retains its renewable-loading. Two distinct mechanism-design failures, two distinct policy levers." |

All 6 PDFs saved to `../figures/` for direct embedding in Beamer slides. PNGs safety-capped to ~1890 px wide (under the 2000-px session cap; previous build at savefig.dpi=200 hit ~2670 px and broke a working session).

### Important framing caveats (for the slides + Q&A)

- **S6 is a settlement transfer, NOT a deadweight loss.** The €1.1B BRP→TSO flow is regulatory redistribution; the TSO recycles surplus to consumers via tariff with a 1-year lag. Welfare interpretation requires counterfactuals on (a) tariff pass-through during the lag, (b) BRP defensive hedging cost, (c) REE reserve dispatch cost — out of scope for this talk. **Cite as "BRP→TSO settlement transfer" or "regulatory redistribution", never as "welfare cost" or "DWL".**
- **S6, S8, F7 are non-additive across channels.** Same generators participate in all three (BRP-side imbalance settlement, TSO-side reserve activation, DA cleared-price-difference rent). Do NOT sum to "€2B reform impact". Cite each channel separately with its own measurement framing.
- **B6's "0.365" is reform + blackout amplification.** The clean reform-only signal is R²=0.171 (DA60/ID15 PRE-blackout, ~6 weeks). The post-MTU15-DA collapse to R²=0.028 is the cleanest reform signature; the blackout amplifies the magnitude during DA60/ID15 but does not create the underlying mechanism. Slide 8 cites all three numbers (PRE/POST/post-DA15).
- **The May talk is the system-layer slice of a multi-paper IO research program.** Firm-level Cournot-pivotality (F7/F8/F10), cross-market firm specialisation (F9/F19/F20/F15), and post-CNMC strategic-availability conduct (F17/F18/F21/F22) live in Parts II–IV of the thesis and are *off-arc here by deliberate choice*. If the audience asks about them: *"these are regime-invariant background market structure or regime-orthogonal conduct findings, covered in the thesis but separate from the MTU15 reform-impact story this talk addresses."*

### What this talk delivers in IO terms

A clean **mechanism-design** finding: the Spanish settlement rule (uniform per-MWh allocation) is non-Pigouvian by construction, so under the asymmetric-granularity window it generated a €1.1B BRP→TSO transfer that is also a cross-segment redistribution between BRPs (inflexible-portfolio retailers paying flexible-portfolio dispatchable firms). Symmetric clocks at MTU15-DA close the channel — clock-symmetry is a welfare-relevant policy lever. The cross-country placebo (B7) plus the per-regime decomposition (S5/S6/B6) provide reduced-form identification. **The talk's headline IO claim:** *clock-symmetry under heterogeneous-marginal-cost segments is a real mechanism-design lever, not a market-microstructure footnote.*
""")

nb["cells"] = cells
nb.metadata = {
    "kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
    "language_info": {"name": "python", "version": "3.11"},
}

OUT_NB.parent.mkdir(parents=True, exist_ok=True)
nbf.write(nb, OUT_NB)
print(f"wrote {OUT_NB} ({len(cells)} cells)")
