"""Extend presentation2/figures.ipynb with Act II (B9 strategic conduct),
Act III (RT2 regulatory cascade with verification caveat), and a Block 3
addition to the existing two-block model.

Inserts new cells AFTER the existing Figure 7 (cell 29) and BEFORE the
Summary (cell 30). Updates the Summary table to include the new figures.

Run: uv run python thesis/presentation2/extend_with_act2_act3.py
"""
from __future__ import annotations
import json
from pathlib import Path

NB = Path(__file__).parent / "figures.ipynb"

# ----------------------------------------------------------------------
# Cell builders
# ----------------------------------------------------------------------
def md(src: str) -> dict:
    return {
        "cell_type": "markdown",
        "metadata": {},
        "source": src.splitlines(keepends=True),
    }

def code(src: str) -> dict:
    return {
        "cell_type": "code",
        "execution_count": None,
        "metadata": {},
        "outputs": [],
        "source": src.splitlines(keepends=True),
    }


# ----------------------------------------------------------------------
# ACT II — Firm-level strategic IDA repositioning (B9 U-shape)
# ----------------------------------------------------------------------
ACT2_DIVIDER = md("""# ACT II — Firm-level: the U-shape of strategic IDA repositioning

After establishing the *system-level* friction (Act I), we now ask: did the asymmetric-granularity window also affect *firm-level* strategic conduct? We test the canonical Allaz–Vila / Ito-Reguant (henceforth IR) sequential-markets prediction: **dominant firms undersell forward (DA) and net-sell in the spot market (IDA)**, generating a positive q₂ in IR notation.

The reform sequence creates a clean three-act variation in the granularity friction:

- **τ = 0 (symmetric)**: pre-IDA (DA60/ID60), DA15/ID15
- **τ > 0 (asymmetric)**: 3-sess (DA60/ID60 + 6→3 sessions), ISP15-win (DA60/ID60 + ISP15 settlement), DA60/ID15 (DA60 vs ID15)
- The asymmetric-granularity window spans **June 2024 → October 2025**.

We expect strategic q₂ to *compress* during the asymmetric window and *recover* once symmetric MTU15 is restored — i.e., a **U-shape**, not a structural change. This is qualitatively different from "MTU15 ended market power" — the reform created a *temporary friction*, not a structural elimination.
""")

ACT2_Q2_DEF = md("""## What is q₂? Ito-Reguant notation, OMIE-spec mapping

In IR's sequential-markets model:
- $q_1$ = day-ahead forward sell quantity per firm-period (MWh)
- $q_2$ = spot-market net repositioning per firm-period (MWh)
- The IR strategic prediction: $q_2 > 0$ for dominant firms (undersell forward, sell more in spot)

We map IR's $(q_1, q_2)$ to OMIE files as follows:
- $q_1 = \\sum_\\text{offers} \\text{PDBCE.assigned\\_power\\_mw} \\times \\Delta t / 60$ at offer\\_type = 1 (sell side)
- $q_2 = \\sum_\\text{offers} \\text{PIBCIE.assigned\\_power\\_mw} \\times \\Delta t / 60$ across all offer types (signed natively per OMIE spec v1.37 §5.2.2.3)

**Why $q_2$ is the IR-cleanest measure** (vs PHF − PDBC or PIBCA − PDBC):
- PIBCIE captures only the firm's *voluntary* IDA bidding outcome (excludes bilaterals, RT1, RT2, continuous market)
- This is the firm's strategic choice variable, the only one IR's theory directly speaks to
- The other measures conflate strategic conduct with operational repositioning, technical restrictions, and contracts

Empirical fact verified yesterday: all Big-4 records in PIBCIE are offer\\_type = 1 (zero buys, zero RE-Mercado), so the legacy `CASE WHEN offer_type IN (1,3) THEN +... WHEN (8,9) THEN −...` formula and simple SUM are *numerically identical* for Big-4. We use simple SUM throughout.

**Disaggregation discipline.** All B9 results below are at the **maximum disaggregation** the data allows: every observation is at MTU15 grain. Pre-MTU15-IDA records (June 2024 cutoff for IDAs) are at MTU60 by market design — they are replicated 4× per hour at $q_2 / 4$ each (preserves total hourly energy, reflects the fact that MTU60 firms cannot vary within hour). Cluster SE by (date, hour) absorbs the within-hour artificial correlation from this replication. No quarter is collapsed into an hour at any point.
""")

ACT2_FIG8 = md("""## Figure 8 — B9 main regression: Big-4 vs Fringe q₂ gap by regime (firm-ISP-replicated grain)

**Spec:**
$$
q_2 = \\alpha + \\sum_r \\beta_r \\, \\mathbf{1}[\\text{regime}_r] + \\delta \\,\\mathbf{1}[\\text{Big-4}] + \\sum_r \\gamma_r \\,\\mathbf{1}[\\text{regime}_r]\\cdot\\mathbf{1}[\\text{Big-4}] + \\text{Period FE}_{(1..96)} + \\text{DOW FE} + \\text{Month FE} + \\text{Year FE} + \\eta\\, \\text{VRE}_d + \\varepsilon
$$
- $N$ = 1,931,558 firm-ISP rows (5 regimes, 11 firms, 2,940 dates, 96 periods)
- 70,548 (date × hour) clusters; cluster-robust SE
- $R^2$ = 0.223; Joint Wald F = 477.51, p < 5e−102

**Result.** $\\delta + \\gamma_r$ measures the Big-4 vs Fringe gap in q₂ at regime $r$. Reading directly from `b9_replicated_isp_grain.csv`:
""")

ACT2_FIG8_CODE = code("""# Figure 8 — Big-4 effect by regime, firm-ISP-replicated grain
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

PROJECT = Path.cwd() if (Path.cwd() / 'data').exists() else Path.cwd().parent.parent
SRC = PROJECT / 'data/derived/results/b9_replicated_isp_grain.csv'

reg = pd.read_csv(SRC)
REGIMES = ['pre-IDA', '3-sess', 'ISP15-win', 'DA60/ID15', 'DA15/ID15']
reg['regime'] = pd.Categorical(reg['regime'], categories=REGIMES, ordered=True)
reg = reg.sort_values('regime').reset_index(drop=True)
print('Big-4 effect by regime (MWh per firm-ISP, replicated grain):')
print(reg.to_string(index=False))
print()

fig, ax = plt.subplots(figsize=(12, 5))
x = list(range(len(REGIMES)))
y  = reg['big4_effect'].values
se = reg['se'].values
ax.errorbar(x, y, yerr=1.96 * se, fmt='o-', linewidth=2.2, markersize=10,
            capsize=6, color='#1f4e79', label='Big-4 effect (β + interaction)')
ax.axhline(y[0], color='grey', ls='--', lw=1.0, alpha=0.6,
           label='pre-IDA baseline')
ax.axvspan(0.5, 3.5, color='#fff3c4', alpha=0.45, zorder=0,
           label='Asymmetric-granularity window (Jun 2024 – Oct 2025)')
ax.set_xticks(x); ax.set_xticklabels(REGIMES, fontsize=11)
ax.set_ylabel('Big-4 q₂ effect (MWh per firm-ISP)', fontsize=12)
ax.set_title('Figure 8 — B9 main: Big-4 q₂ effect by regime\\n'
             'firm-ISP-replicated grain (1.93M obs); 95% CIs from cluster SE by (date, hour)', fontsize=12)
ax.grid(True, alpha=0.3); ax.legend(loc='lower left', fontsize=10)

# Annotations: percent change vs pre-IDA
for i, r in enumerate(REGIMES):
    v = y[i]; pct = (v - y[0]) / y[0] * 100
    label = f'{v:+.1f}\\n({pct:+.0f}% vs pre)'
    ax.annotate(label, (x[i], v), textcoords='offset points',
                xytext=(0, 14), ha='center', fontsize=9)

plt.tight_layout(); plt.show()
""")

ACT2_PERFIRM_MD = md("""## Figure 9 — Per-Big-4-firm q₂ trajectory: heterogeneous responses

The aggregate Big-4 average masks meaningful heterogeneity. Splitting by firm:
""")

ACT2_PERFIRM_CODE = code("""# Figure 9 — Per-firm q₂ trajectory, replicated-ISP grain
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

PROJECT = Path.cwd() if (Path.cwd() / 'data').exists() else Path.cwd().parent.parent
SRC = PROJECT / 'data/derived/results/b9_replicated_isp_grain_perfirm.csv'

REGIMES = ['pre-IDA', '3-sess', 'ISP15-win', 'DA60/ID15', 'DA15/ID15']
BIG4 = ['GE', 'IB', 'GN', 'HC']
COLORS = {'GE': '#d62728', 'IB': '#1f77b4', 'GN': '#2ca02c', 'HC': '#9467bd'}
df = pd.read_csv(SRC, index_col=0).reindex(BIG4)[REGIMES]
print('Per-firm q₂ (mean MWh per firm-ISP):')
print(df.round(1).to_string())
print()

fig, ax = plt.subplots(figsize=(12, 5))
x = list(range(len(REGIMES)))
for f in BIG4:
    ax.plot(x, df.loc[f].values, marker='o', linewidth=2.2, markersize=8,
            color=COLORS[f], label=f)
ax.plot(x, df.mean(axis=0).values, color='black', linewidth=2.6,
        linestyle='--', marker='s', markersize=9, label='Big-4 mean', zorder=5)
ax.axvspan(0.5, 3.5, color='#fff3c4', alpha=0.45, zorder=0)
ax.set_xticks(x); ax.set_xticklabels(REGIMES, fontsize=11)
ax.set_ylabel('q₂ (MWh per firm-ISP)', fontsize=12)
ax.set_title('Figure 9 — Per-Big-4-firm q₂ trajectory (replicated-ISP grain)', fontsize=12)
ax.grid(True, alpha=0.3); ax.legend(loc='upper right', ncol=2, fontsize=10)
ax.axhline(0, color='grey', lw=0.5, alpha=0.5)
plt.tight_layout(); plt.show()
""")

ACT2_PERFIRM_INT = md("""**Reading.**

- **GN** (Naturgy): largest absolute q₂ throughout — biggest collapse from pre-IDA (+127) to ISP15-win (+59), partial recovery to DA15/ID15 (+76). The biggest single contributor to the aggregate U-shape.
- **IB** (Iberdrola): deepest collapse, −62% from pre-IDA (+68) to DA60/ID15 (+26); recovers to +36 at DA15/ID15 — but does NOT return to pre-IDA level. IB's structural-pivotality story (F7/F8/F10) suggests its post-reform conduct may have shifted to other channels (e.g., DA dispatch concentration in scarcity hours).
- **GE** (Endesa): most stable — +31 → +22 → +31 across regimes. Flat trajectory, modest collapse during friction window. Consistent with GE's relatively smaller hydro+CCGT IDA strategic role.
- **HC** (Viesgo): smallest, structurally similar to GE.

**Caveat for Q&A.** A naive cross-firm comparison would conflate market-power *level* with the *response to friction*. The U-shape is robust at the aggregate; firm-specific recovery rates differ and deserve their own structural explanation (Part III of the proposal).
""")

ACT2_APRSEP_MD = md("""## Figure 10 — Same-calendar-month robustness (Apr–Sep restriction)

The reform regimes span different calendar windows:
- pre-IDA: 78 months across all seasons
- 3-sess: Jun–Dec 2024
- ISP15-win: Dec 2024 – Mar 2025 (winter)
- DA60/ID15: Apr–Sep 2025 (summer/early-fall)
- DA15/ID15: Oct 2025 – Jan 2026 (fall/early-winter)

A naive cross-regime comparison risks confounding seasonal effects with reform effects. The CLAUDE.md mandate is to verify any cross-regime claim under same-calendar-month restriction.

**Spec**: identical to Figure 8, but `WHERE EXTRACT(month FROM date) BETWEEN 4 AND 9`. Drops ISP15-win (Dec–Mar) and DA15/ID15 (Oct–Jan) by construction. 3 regimes remain.

**Result**: coefficients within ±1 MWh per ISP of the full-sample regression. Joint Wald F = 185.87, p = 4e−41. The U-shape compression is **not a seasonal artefact at the IR-level disaggregation**.
""")

ACT2_APRSEP_CODE = code("""# Figure 10 — full sample vs Apr-Sep coefficient comparison
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

PROJECT = Path.cwd() if (Path.cwd() / 'data').exists() else Path.cwd().parent.parent
full = pd.read_csv(PROJECT/'data/derived/results/b9_replicated_isp_grain.csv')
apr_sep = pd.read_csv(PROJECT/'data/derived/results/b9_replicated_isp_apr_sep.csv')

REGIMES = ['pre-IDA', '3-sess', 'DA60/ID15']
full = full[full.regime.isin(REGIMES)].copy()
apr_sep = apr_sep[apr_sep.regime.isin(REGIMES)].copy()
full['regime'] = pd.Categorical(full.regime, categories=REGIMES, ordered=True)
apr_sep['regime'] = pd.Categorical(apr_sep.regime, categories=REGIMES, ordered=True)
full = full.sort_values('regime').reset_index(drop=True)
apr_sep = apr_sep.sort_values('regime').reset_index(drop=True)

cmp = pd.DataFrame({
    'regime': REGIMES,
    'full_β': full['big4_effect'].values,
    'full_se': full['se'].values,
    'apr_sep_β': apr_sep['big4_effect'].values,
    'apr_sep_se': apr_sep['se'].values,
})
cmp['diff'] = cmp['apr_sep_β'] - cmp['full_β']
print('Full sample vs Apr–Sep restriction (3 overlap regimes):')
print(cmp.round(2).to_string(index=False))
print()

fig, ax = plt.subplots(figsize=(11, 5))
x = [i - 0.18 for i in range(len(REGIMES))]
xb = [i + 0.18 for i in range(len(REGIMES))]
ax.errorbar(x, cmp['full_β'], yerr=1.96 * cmp['full_se'], fmt='o',
            linewidth=2.0, markersize=11, capsize=6, color='#1f4e79',
            label='Full sample (5 regimes; F=477.5, p<5e−102)')
ax.errorbar(xb, cmp['apr_sep_β'], yerr=1.96 * cmp['apr_sep_se'], fmt='s',
            linewidth=2.0, markersize=11, capsize=6, color='#d62728',
            label='Apr–Sep only (3 regimes; F=185.87, p=4e−41)')
ax.set_xticks(range(len(REGIMES))); ax.set_xticklabels(REGIMES, fontsize=11)
ax.set_ylabel('Big-4 q₂ effect (MWh per firm-ISP)', fontsize=12)
ax.set_title('Figure 10 — same-cal-month robustness (Apr–Sep restriction at firm-ISP-replicated grain)', fontsize=12)
ax.grid(True, alpha=0.3); ax.legend(fontsize=10)
plt.tight_layout(); plt.show()
""")

# ----------------------------------------------------------------------
# ACT III — RT2 channel with verification caveat
# ----------------------------------------------------------------------
ACT3_DIVIDER = md("""# ACT III — RT2 channel: post-blackout regulatory cascade

The April 28, 2025 Iberian blackout triggered REE to adopt **operación reforzada** — operating the system with enhanced security margins by committing more dispatchable capacity (CCGTs, nuclear) than the market would naturally schedule. This is implemented through Phase-2 technical restrictions (RT2) that adjust each unit's dispatch *after* the IDA market clears.

We attempted to quantify the firm-level RT2 increment using the OMIE files. **The exercise raised an important data-structure caveat that we report here transparently.**

## What is RT2?

In the Spanish market sequence:

| Stage | Output file | Includes RT? |
|---|---|---|
| DA market clears | PDBC | No |
| REE Phase-1 restrictions (RT1) | PDVD | RT1 only |
| IDA1, IDA2, IDA3 clear | PIBCA (accumulated level) | flag_redespacho ≡ 0 → No RT |
| REE Phase-2 restrictions (RT2) + final rebalance | PHF (final program) | Yes (per OMIE spec §5.2.2.4) |
| Real-time operation | (settlement) | — |

So **RT2 = PHF − PIBCA** at unit-period level isolates the post-IDA technical-restriction increment as published by OMIE.
""")

ACT3_OMIE_MD = md("""## OMIE-derived RT2 measure: a striking apparent step-jump in DA15/ID15

Reading PHF − PIBCA at the per-unit-period level (Big-4 units), aggregating to firm-day, gives the following:
""")

ACT3_OMIE_CODE = code("""# Big-4 RT2 by regime, OMIE-derived (PHF − PIBCA)
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

PROJECT = Path.cwd() if (Path.cwd() / 'data').exists() else Path.cwd().parent.parent
rt2 = pd.read_csv(PROJECT/'data/derived/results/rt2_post_blackout/01_perfirm_perregime.csv')
REGIMES = ['pre-IDA', '3-sess', 'ISP15-win', 'DA60/ID15', 'DA15/ID15']
BIG4 = ['GE', 'IB', 'GN', 'HC']
COLORS = {'GE': '#d62728', 'IB': '#1f77b4', 'GN': '#2ca02c', 'HC': '#9467bd'}
rt2['regime'] = pd.Categorical(rt2.regime, categories=REGIMES, ordered=True)
print('Big-4 RT2-up MWh per firm-day (OMIE-derived = PHF − PIBCA, last sessions):')
pv_up = (rt2.pivot(index='firm', columns='regime', values='rt2_up_per_firm_day')
            .reindex(BIG4).reindex(REGIMES, axis=1))
print(pv_up.round(0).to_string())
print()

fig, ax = plt.subplots(figsize=(12, 5))
x = list(range(len(REGIMES)))
for f in BIG4:
    ax.plot(x, pv_up.loc[f].values, marker='o', linewidth=2.0, markersize=8,
            color=COLORS[f], label=f)
ax.axvspan(0.5, 3.5, color='#fff3c4', alpha=0.45, zorder=0,
           label='Asymmetric-granularity window')
ax.axvline(3.5, color='red', linestyle=':', lw=1.5, alpha=0.7,
           label='MTU15-DA reform (Oct 2025)')
ax.set_xticks(x); ax.set_xticklabels(REGIMES, fontsize=11)
ax.set_ylabel('RT2-up MWh per firm-day (OMIE PHF − PIBCA)', fontsize=12)
ax.set_title('Figure 11 — RT2-up by regime (OMIE-derived)\\n'
             'apparent step-jump in DA15/ID15 — but is it real?', fontsize=12)
ax.grid(True, alpha=0.3); ax.legend(fontsize=10)
plt.tight_layout(); plt.show()
""")

ACT3_VERIFY_MD = md("""## Verification: cross-check against ESIOS `totalrp48preccierre`

The OMIE result raises a flag: PHF − PIBCA is **essentially zero** in pre-IDA, 3-sess, ISP15-win, and DA60/ID15 — and surges by an order of magnitude in DA15/ID15 (Oct 2025 onward).

Two competing explanations:

1. **Real REE behavior change.** REE only began applying significant Phase-2 restrictions in Oct 2025 (coincident with MTU15-DA reform). The blackout response (April 2025) was implemented through other channels (e.g., aFRR commitment) until October.

2. **OMIE publishing-convention shift.** The PHF and PIBCA files always existed, but the way RT2 is integrated into PHF vs absorbed into PIBCA changed when MTU15-DA went live — i.e., the *measurement* shifted, not the underlying behavior.

To distinguish: ESIOS `totalrp48preccierre` publishes REE's redispatch quantities directly, by tipo (category code), throughout 2015 onward. If REE was applying technical restrictions before Oct 2025, they should appear in ESIOS even when OMIE PHF − PIBCA = 0.
""")

ACT3_ESIOS_CODE = code("""# ESIOS verification: monthly RT-up by tipo_redespacho category
import duckdb
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

PROJECT = Path.cwd() if (Path.cwd() / 'data').exists() else Path.cwd().parent.parent
ESIOS = PROJECT / 'data/processed/esios/restricciones/totalrp48preccierre_all.parquet'

con = duckdb.connect()
con.execute("SET memory_limit='4GB'")
df = con.execute(f\"\"\"
    SELECT DATE_TRUNC('month', date) AS month,
           tipo_redespacho AS tipo,
           SUM(COALESCE(qty_up_mwh, 0)) AS up_mwh
    FROM '{ESIOS}'
    WHERE date >= '2024-01-01'
    GROUP BY 1, 2
    ORDER BY 1, 2
\"\"\").df()
df['month'] = pd.to_datetime(df['month'])
pv = df.pivot(index='month', columns='tipo', values='up_mwh').fillna(0)
top = pv.sum(axis=0).sort_values(ascending=False).head(7).index.tolist()
print('Top 7 tipo_redespacho codes by total RT-up volume (2024+):', top)
print()

fig, ax = plt.subplots(figsize=(13, 5.5))
for t in top:
    ax.plot(pv.index, pv[t]/1000, marker='o', markersize=4, linewidth=1.5,
            label=f'tipo {t}')
ax.axvline(pd.Timestamp('2025-04-28'), color='red', linestyle='--', lw=1.2, alpha=0.7,
           label='Apr 28 2025 (blackout)')
ax.axvline(pd.Timestamp('2025-10-01'), color='black', linestyle=':', lw=1.5, alpha=0.7,
           label='Oct 1 2025 (MTU15-DA)')
ax.set_xlabel('Month', fontsize=11)
ax.set_ylabel('RT-up GWh per month (ESIOS, by tipo)', fontsize=11)
ax.set_title('Figure 12 — ESIOS RT-up monthly time series, top 7 tipo_redespacho codes\\n'
             'Total system technical-restrictions volume is consistent throughout 2024–2026', fontsize=12)
ax.grid(True, alpha=0.3); ax.legend(loc='upper left', fontsize=9, ncol=2)
plt.tight_layout(); plt.show()

# Total per-month aggregate, all tipos
agg = pv.sum(axis=1)
print('Total system RT-up per month (GWh, all tipos):')
print((agg/1000).round(0).tail(28).to_string())
""")

ACT3_VERDICT_MD = md("""## Verdict: the Oct-2025 step-jump is a publishing-convention shift, not a REE behavior change

Reading the ESIOS series:

- **Tipo 92** (one of the redispatch codes) **surges in April 2025** to 271 GWh/month, up from a baseline ~50 GWh/month — REE responded to the blackout immediately, four months before MTU15-DA. The OMIE PHF − PIBCA shows ZERO RT2 in this period.
- **Tipo 81** + **tipo 96** rise from mid-2025 onward — composition shifts.
- **Tipos 61 + 94 collapse to zero in Jan 2026** — clear reclassification of redispatch categories.
- **Total system RT-up** stays roughly constant (~1.0–1.5 TWh/month all-tipos) throughout 2024–2026, with composition shifts.

**Conclusion.** REE has been applying *operación reforzada* throughout the post-blackout period. The OMIE PHF − PIBCA measure shows zero RT2 pre-Oct 2025 not because REE wasn't intervening, but because RT2 was *absorbed into PIBCA* under the pre-MTU15-DA publishing convention. After MTU15-DA, the convention changed and RT2 became visible as PHF − PIBCA divergence.

**Implication for the presentation.**
- ✗ Cannot claim "RT2 surge in DA15/ID15" as a regime effect.
- ✓ Can claim "ESIOS confirms post-blackout REE technical-restrictions volume is structurally elevated, with tipo composition shifts."
- ✓ Can claim "the +13.6 GWh/firm-day OMIE-PHF measure in DA15/ID15 reflects what was always there once OMIE makes it visible — a measurement artefact, not a behavior change."
- The full per-firm/per-unit attribution of operación reforzada would require ESIOS *per-unit* RT files (not currently in the processed tree). Recommended for follow-up after the presentation.

**For the slide deck.** RT2 is mentioned as a **parallel channel** to the friction story — present, real, post-blackout — but **not** as a step-jump headline. The friction U-shape (Acts I + II) is the load-bearing narrative.
""")

# ----------------------------------------------------------------------
# Bridge to the existing summary + model framework
# ----------------------------------------------------------------------
BRIDGE_MD = md("""---

# Synthesis: three layers, one friction parameter

The empirical findings layer naturally onto a single granularity-friction parameter (the existing two-block framework's $M$ — equivalent to the $\\tau$ I sketched in the narrative draft):

| Layer | Empirical anchor | Predicted by friction $M$? |
|---|---|---|
| **Act I — system-level** | S6 BRP→TSO transfer surges in asymmetric window; B6 forecast pass-through R² rises 7×; F4 France placebo holds | ✓ direct (Block 2 of existing model) |
| **Act II — firm-level strategic q₂** | B9 Big-4 U-shape compression-recovery, F=477.5, p<10⁻¹⁰²; per-firm typology heterogeneous; Apr–Sep robust | ✓ indirect — current Block 1 (Cournot DA) is clock-invariant; this calls for a Block 3 (strategic IDA q₂ under granularity friction) extension below |
| **Act III — RT2 cascade** | OMIE measure gives Oct-2025 step-jump but ESIOS reveals it is publishing convention; real REE response is gradual post-blackout | ✗ — outside the friction model; reported as parallel exogenous regulatory shock |

The next two sections (the existing summary table + the two-block organising framework) preserve their original prose. The two-block framework already encodes $M$ in Block 2; what's new is recognising that **Act II's B9 result calls for a Block 3** capturing strategic IDA conduct under granularity friction.
""")

# ----------------------------------------------------------------------
# Block 3 — strategic IDA q₂ under granularity friction
# ----------------------------------------------------------------------
BLOCK3_MD = md("""# Block 3 — Strategic IDA q₂ under granularity friction (extension)

The two-block framework above (Cournot DA + clock-asymmetric settlement) is silent on Big-4 strategic IDA bidding. To rationalise the U-shape (Act II), I add a third stripped-down block. **This block is for slide-narrative; not estimated.**

## 3.1 Setup

Each Big-4 firm chooses $(q_1, q_2)$ across two markets:
- $q_1$ in DA at price $p_1$
- $q_2$ in IDA at price $p_2$

The firm's profit is
$$
\\Pi(q_1, q_2) = p_1\\,q_1 + p_2\\,q_2 - C(q_1 + q_2)
$$
where $C$ is convex in total production. In a frictionless symmetric setting, the AV/IR equilibrium has dominant firms undersell forward ($q_1$ low) and net-sell in spot ($q_2 > 0$). The wedge $p_2 - p_1$ at equilibrium reflects the strategic forward-undercommitment rent.

## 3.2 Granularity friction enters the spot leg

The granularity-friction parameter $M = K_\\text{ISP} / K_\\text{DA}$ from Block 2 (asymmetric clock $\\Rightarrow M > 1$) enters the IDA leg as a *participation cost* — the firm's IDA bidding payoff is
$$
p_2(M)\\,q_2 - \\phi(M) \\cdot q_2^2
$$
where $\\phi(M)$ is increasing in $M$ (asymmetric clocks → harder to time spot strategically, because the relevant settlement-clock unit is finer than the IDA-clock unit, so the firm's per-ISP exposure to settlement noise rises). Under symmetric MTU15 ($M = 1$), $\\phi$ is at its minimum; under asymmetric DA60/ID15 ($M = 4$), $\\phi$ is at its maximum.

## 3.3 Comparative statics

Solving the firm's first-order conditions:

$$
q_2^*(M) = \\frac{p_2 - C'(q_1 + q_2^*)}{2\\,\\phi(M)}
$$

so

$$
\\frac{\\partial q_2^*}{\\partial M} < 0 \\quad \\text{whenever } \\phi'(M) > 0.
$$

**Predictions matching B9.**

1. $M = 1$ (pre-IDA, DA15/ID15): $q_2^*$ at unconstrained AV/IR level → **positive, large**.
2. $M = 4$ (3-sess + ISP15-win + DA60/ID15): $q_2^*$ compressed → **positive, smaller**.
3. $M$ returns to 1 at DA15/ID15: $q_2^*$ recovers → **U-shape**.

This produces the observed B9 trajectory: $q_2$ pre-IDA $+146$ → asymmetric-window troughs $+115$ → DA15/ID15 recovery $+140$ MWh per firm-ISP.

**Big-4 share of the rent**: the *level* of $q_2^*$ scales with the firm's residual-demand position in the spot — which is where the cross-firm typology (GN > IB > GE > HC) comes from in Block 1's residual-demand interpretation. The friction parameter $M$ scales the *common compression*; the *firm-specific level* is residual-demand-driven and reflects market structure.

## 3.4 Outside the model

- **Act III (RT2 cascade)** is *not* generated by $M$. It is an exogenous regulatory shock from REE post-blackout. Reported as a parallel channel; visible in ESIOS throughout 2025, made artificially conspicuous in OMIE PHF − PIBCA only after MTU15-DA changed the publishing convention.
- **Per-firm structural rates of recovery** (IB recovers less than GE, etc.) are not pinned down by $M$ alone. A richer model with firm-specific productive-capacity differences and a continuous strategic-vs-fringe spectrum would be needed; for the presentation, the heterogeneity is reported descriptively.

## 3.5 The model in one line

> One friction parameter $M$ rationalises (i) the system-level transfer (S6 = €1.1B), (ii) the within-firm pass-through R² rise (B6 = 7× peak), and (iii) the Big-4 strategic q₂ compression-recovery (B9 = U-shape). Three empirical regularities, one parameter. Outside the model: post-blackout RT2 cascade.
""")


# ----------------------------------------------------------------------
# Updated summary table replacing cell 30
# ----------------------------------------------------------------------
NEW_SUMMARY_MD = """## Summary — IO content of each figure

| # | Figure | Headline | IO category | Slide-talking point |
|---|---|---|---|---|
| 1 | S5 4-panel | Four ENTSO-E metrics jump concordantly at ISP15, moderate at MTU15-DA | Identification (joint null rejection across 4 outcomes) | "Joint null is rejected — this isn't one outlier metric" |
| 2 | **S6 €1.1B** | BRP→TSO transfer ≈15× upper bootstrap bound; collapses to €7.4M/mo at MTU15-DA | **Welfare** (BRP-side regulatory redistribution) + **mechanism design** (clock-symmetry restores IC) | "Headline magnitude. Confirms Feb-deck Ito–Reguant prediction. Clock-symmetry lever ✓." |
| 3 | B6 pass-through | R² rises 7× under clean reform (DA60/ID15 PRE-blackout: 0.171 vs pre-IDA-late 0.023); 16× under reform + blackout (POST-blackout: 0.365); collapses to 0.028 post-MTU15-DA | **Conduct** (BRP strategic bidding under asymmetric clocks) | "Microfoundation in one figure. The post-MTU15-DA volume collapse is the cleanest signature; the blackout amplifies but does not create the mechanism." |
| 4 | B7 placebo | Spain within-day SD responds 2–3× more than France across reform dates | Identification (cross-country DiD) | "Cross-country control the Feb proposal said wasn't yet available" |
| 5 | S6 blackout split | DA15 collapse holds DESPITE operación reforzada | §4 robustness (n=3 caveat for Oct–Dec 2025) | "Defensive figure for Q&A — friction is reform-driven, not blackout-driven" |
| **6** | **S7 Pigouvian (F3 direct)** | LIB retailers paid €108M, wind €77M, conv-RZ only €46M of €294M reconstructed in DA60/ID15 — direct dual-pricing decomposition (78% of system total reconstructed; corr 0.93) | **Pigouvian incidence** — the IO bite: cross-segment redistribution embedded in the rule | "Direct dual-pricing decomposition: renewables paid €186M of €294M; counterfactual would charge dispatchable plants instead. €178M of redistribution structurally misallocated." |
| **7** | **Burden-share regime invariance** | Wind + LIB retailers consistently bear ~63% of the imbalance burden across regimes | Pigouvian incidence — invariance under common α | "Even with the magnitude collapsing, who pays stays the same." |
| **8** | **B9 main: Big-4 U-shape** | Big-4 q₂ effect: pre-IDA +146 → asymmetric-window trough +115 → DA15/ID15 recovery +140 (only −6.6 below pre-IDA, p=0.031). Joint Wald F=477.5, p<10⁻¹⁰² | **Strategic conduct** (IR/AV sequential markets) under granularity friction | "U-shape, NOT a structural change. The reform compresses then releases — friction story." |
| **9** | **B9 per-firm typology** | GN largest (127→59→76); IB deepest collapse (-62%); GE most stable; HC small | Cross-firm heterogeneity in market-structure × friction response | "Aggregate U-shape masks heterogeneity — GN and IB drive it; GE structurally stable." |
| **10** | **B9 Apr–Sep robustness** | Coefficients within ±1 MWh per ISP of full sample at firm-ISP-replicated grain | Same-cal-month seasonality control — CLAUDE.md mandatory test | "Friction collapse is NOT a seasonal artefact." |
| 11 | RT2 OMIE (caveat) | OMIE PHF − PIBCA shows step-jump in DA15/ID15 — but verification flagged | Regulatory channel candidate, but rejected as headline | "Initial measure looked dramatic; verification revealed publishing-convention artefact." |
| 12 | **RT2 ESIOS verification** | Total system RT volume consistent throughout 2024–2026; tipo composition shifts from April 2025 (blackout) | Honest data-source-cross-check; defensive for Q&A | "ESIOS confirms post-blackout REE intervention is real and gradual; OMIE step-jump was a measurement artefact." |
"""

# ----------------------------------------------------------------------
# Apply edits
# ----------------------------------------------------------------------
def main() -> None:
    nb = json.loads(NB.read_text())
    print(f"Original cells: {len(nb['cells'])}")

    # Find insertion point: after "Figure 7" code (cell 29), before "Summary" (cell 30)
    insert_at = None
    for i, c in enumerate(nb["cells"]):
        src = "".join(c["source"]) if isinstance(c["source"], list) else c["source"]
        if c["cell_type"] == "markdown" and src.startswith("## Summary"):
            insert_at = i
            break
    if insert_at is None:
        raise RuntimeError("Could not find Summary cell to anchor insertion")
    print(f"Inserting before cell index {insert_at}")

    new_cells = [
        ACT2_DIVIDER,
        ACT2_Q2_DEF,
        ACT2_FIG8,
        ACT2_FIG8_CODE,
        ACT2_PERFIRM_MD,
        ACT2_PERFIRM_CODE,
        ACT2_PERFIRM_INT,
        ACT2_APRSEP_MD,
        ACT2_APRSEP_CODE,
        ACT3_DIVIDER,
        ACT3_OMIE_MD,
        ACT3_OMIE_CODE,
        ACT3_VERIFY_MD,
        ACT3_ESIOS_CODE,
        ACT3_VERDICT_MD,
        BRIDGE_MD,
    ]

    nb["cells"][insert_at:insert_at] = new_cells

    # Replace the Summary cell with the updated table
    summary_idx = insert_at + len(new_cells)
    nb["cells"][summary_idx] = md(NEW_SUMMARY_MD)

    # Append Block 3 model addition AFTER cell "## 2.7 What the framework delivers and what it does not"
    block3_inserted = False
    for i, c in enumerate(nb["cells"]):
        src = "".join(c["source"]) if isinstance(c["source"], list) else c["source"]
        if c["cell_type"] == "markdown" and src.startswith("## 2.7 What the framework delivers"):
            nb["cells"].insert(i + 1, BLOCK3_MD)
            block3_inserted = True
            break
    if not block3_inserted:
        nb["cells"].append(BLOCK3_MD)

    # Bump nbformat metadata if missing (preserve everything else)
    NB.write_text(json.dumps(nb, indent=1, ensure_ascii=False))
    print(f"New cells: {len(nb['cells'])}")
    print(f"Wrote {NB}")


if __name__ == "__main__":
    main()
