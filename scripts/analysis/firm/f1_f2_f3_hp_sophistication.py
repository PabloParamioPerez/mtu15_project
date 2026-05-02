# STATUS: ALIVE
# LAST-AUDIT: 2026-05-02
# FEEDS: F1, F2, F3 — Hortaçsu-Puller sophistication test
# CLAIM: F1/F2/F3 Lerner indices are computed from the Cournot first-order
#        condition treated as if firms behave optimally:
#          L_i_implied = q_i / (p × (1 − s_i) × |∂S/∂p|)
#        This script tests whether the firms' ACTUAL marginal bids match
#        the FOC-implied markup. If realized markup ≪ implied, F1/F2/F3
#        overstate conduct (the firms are not in fact extracting Cournot
#        rent); if realized ≈ implied, the firms are sophisticated and
#        F1/F2/F3 numbers reflect real strategic bidding (HP 2008 finding).
"""Hortaçsu-Puller sophistication test for F1/F2/F3.

Compares two markup measures per (firm, hour) on the post-MTU15-IDA panel
(2025-03-19 onward, where det bid prices are clean):

  Implied (Cournot FOC, from `firm_lerner_hourly.parquet`):
      lerner_implied = q_i / (p × (1 − s_i) × |∂S/∂p|)

  Realized (volume-weighted-marginal bid, from `det_all.parquet`):
      For each unit, the marginal bid = price of the last cleared tranche
      (the tranche whose cumulative-tranche-quantity first reaches the
      unit's cleared quantity in pdbc.assigned_power_mw). Firm-level marginal
      bid = volume-weighted across firm's units. Realized lerner =
      (clearing_price − firm_marginal_bid) / clearing_price.

If realized ≈ implied → firms bid consistent with Cournot FOC (sophisticated).
If realized ≪ implied → firms not extracting the FOC-implied rent (F1/F2/F3
inflated by formula not matched by conduct).

Output:
  results/regressions/f1_f2_f3_hp_sophistication.csv
"""
from __future__ import annotations

from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

PROJECT = Path(__file__).resolve().parents[3]
DET     = PROJECT / "data" / "processed" / "omie" / "mercado_diario" / "ofertas" / "det_all.parquet"
CAB     = PROJECT / "data" / "processed" / "omie" / "mercado_diario" / "ofertas" / "cab_all.parquet"
PDBC    = PROJECT / "data" / "processed" / "omie" / "mercado_diario" / "programas" / "pdbc_all.parquet"
PDBCE   = PROJECT / "data" / "processed" / "omie" / "mercado_diario" / "programas" / "pdbce_all.parquet"
LERNER  = PROJECT / "data" / "derived" / "panels" / "firm_lerner_hourly.parquet"
OUT     = PROJECT / "results" / "regressions" / "f1_f2_f3_hp_sophistication.csv"

CUTOFF  = "2025-03-19"           # MTU15-IDA reform — det bid prices clean from here
BIG4    = ["GE", "IB", "GN", "HC"]


def assign_regime(d) -> str:
    d = pd.Timestamp(d)
    if d < pd.Timestamp("2025-03-19"): return "pre-MTU15-IDA"      # excluded by cutoff
    if d < pd.Timestamp("2025-10-01"): return "DA60/ID15"
    return "DA15/ID15"


def main() -> None:
    con = duckdb.connect()
    con.execute("SET memory_limit='8GB'")
    con.execute("SET threads=4")
    con.execute("SET preserve_insertion_order=false")

    # ============================================================
    # 1. Unit → firm mapping (from pdbce; takes most-recent firm per unit)
    # ============================================================
    print("[1/5] Unit → firm mapping from pdbce…", flush=True)
    unit_firm = con.execute(f"""
        SELECT unit_code, grupo_empresarial AS firm
        FROM (
            SELECT unit_code, grupo_empresarial,
                   ROW_NUMBER() OVER (PARTITION BY unit_code ORDER BY date DESC) AS rn
            FROM '{PDBCE}'
            WHERE grupo_empresarial IS NOT NULL
              AND date >= '{CUTOFF}'
        )
        WHERE rn = 1
    """).df()
    print(f"   {len(unit_firm):,} unit→firm rows; Big-4 share: "
          f"{(unit_firm.firm.isin(BIG4)).mean():.1%}", flush=True)

    # ============================================================
    # 2. Cleared quantity per unit per (date, period) — sell side, post-cutoff
    # ============================================================
    print("[2/5] Cleared quantity per unit (pdbc, sell-side, post-2025-03-19)…", flush=True)
    cleared = con.execute(f"""
        SELECT date, period, unit_code, mtu_minutes,
               assigned_power_mw AS cleared_mw
        FROM '{PDBC}'
        WHERE date >= '{CUTOFF}'
          AND assigned_power_mw IS NOT NULL
          AND assigned_power_mw > 0
          AND offer_type = 1   -- sell side
    """).df()
    print(f"   cleared rows: {len(cleared):,}", flush=True)

    # ============================================================
    # 3. For each cleared unit-period, find the marginal cleared tranche price.
    #    det is keyed by offer_code; cab joins offer_code → unit_code + buy_sell.
    #    Sell side: buy_sell='V'. Tranche ordering = segment_number ascending.
    # ============================================================
    print("[3/5] Marginal cleared tranche price per unit-period (det × cab × cleared)…", flush=True)
    print("       Filter: only tranches with price ≤ clearing (drop cap blocking tranches)", flush=True)
    con.register("cleared", cleared)
    # Build hourly clearing price (avg across MTU15 ISPs in the hour for post-MTU15-DA)
    # firm_lerner_hourly already has clearing_price_eur_mwh per (date, hour); we use that.
    marginal = con.execute(f"""
        WITH offer_unit AS (
            SELECT date, offer_code, unit_code
            FROM '{CAB}'
            WHERE buy_sell = 'V' AND date >= '{CUTOFF}'
        ),
        clearing_per_period AS (
            -- Use firm_lerner_hourly's clearing_price (hourly), broadcast to each period
            -- For MTU60: period IS hour. For MTU15: hour = ceil(period/4).
            SELECT DISTINCT date, hour, clearing_price_eur_mwh
            FROM '{LERNER}'
        ),
        tranches AS (
            SELECT d.date, d.period, d.offer_code,
                   d.segment_number,
                   d.price_eur_mwh, d.quantity_mw, d.mtu_minutes,
                   CASE WHEN d.mtu_minutes = 60 THEN d.period
                        ELSE ((d.period - 1) / 4) + 1 END AS hour
            FROM '{DET}' d
            WHERE d.date >= '{CUTOFF}'
              AND d.price_eur_mwh > 0
              AND d.quantity_mw > 0
        ),
        tranches_filtered AS (
            -- only sell-side tranches that bid AT OR BELOW clearing price
            SELECT t.date, t.period, t.offer_code, ou.unit_code,
                   t.segment_number, t.price_eur_mwh, t.quantity_mw,
                   c.clearing_price_eur_mwh
            FROM tranches t
            JOIN offer_unit ou ON t.date = ou.date AND t.offer_code = ou.offer_code
            JOIN clearing_per_period c ON t.date = c.date AND t.hour = c.hour
            WHERE t.price_eur_mwh <= c.clearing_price_eur_mwh
        ),
        tranches_cum AS (
            SELECT date, period, offer_code, unit_code, segment_number,
                   price_eur_mwh, quantity_mw, clearing_price_eur_mwh,
                   SUM(quantity_mw) OVER (
                       PARTITION BY date, period, offer_code
                       ORDER BY price_eur_mwh, segment_number
                       ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                   ) AS cum_q
            FROM tranches_filtered
        )
        SELECT t.date, t.period, t.unit_code, t.price_eur_mwh AS marginal_bid_eur,
               c.cleared_mw, c.mtu_minutes
        FROM tranches_cum t
        JOIN cleared c
          ON t.date = c.date AND t.period = c.period AND t.unit_code = c.unit_code
        WHERE t.cum_q >= c.cleared_mw
          AND (t.cum_q - t.quantity_mw) < c.cleared_mw
    """).df()
    print(f"   marginal-bid rows: {len(marginal):,}", flush=True)
    print(f"   marginal_bid range: €{marginal.marginal_bid_eur.min():.1f} – €{marginal.marginal_bid_eur.max():.1f}", flush=True)
    print(f"   marginal_bid mean / median: €{marginal.marginal_bid_eur.mean():.1f} / €{marginal.marginal_bid_eur.median():.1f}", flush=True)

    # Map unit → firm
    marginal = marginal.merge(unit_firm, on="unit_code", how="left")
    marginal["firm"] = marginal["firm"].fillna("OTHER")

    # ============================================================
    # 4. Aggregate to (firm, date, hour) — volume-weighted marginal bid
    # ============================================================
    print("[4/5] Aggregate marginal bid to (firm, date, hour)…", flush=True)
    marginal["hour"] = np.where(
        marginal["mtu_minutes"] == 60,
        marginal["period"].astype(int),
        ((marginal["period"].astype(int) - 1) // 4) + 1,
    )
    # Weight by cleared MWh (= cleared_mw × mtu_minutes/60). For an MTU60 record this is
    # cleared_mw × 1; for MTU15 cleared_mw × 0.25 per ISP, then summed within the hour.
    marginal["cleared_mwh"] = marginal["cleared_mw"] * marginal["mtu_minutes"] / 60.0
    marginal["wbid_x_q"] = marginal["marginal_bid_eur"] * marginal["cleared_mwh"]

    fh = marginal.groupby(["date", "hour", "firm"], as_index=False).agg(
        cleared_mwh=("cleared_mwh", "sum"),
        wbid_x_q  =("wbid_x_q",   "sum"),
    )
    fh["firm_marginal_bid_eur"] = fh["wbid_x_q"] / fh["cleared_mwh"]
    fh = fh.drop(columns=["wbid_x_q"])
    fh["date"] = pd.to_datetime(fh["date"])
    print(f"   firm-hour rows: {len(fh):,}", flush=True)

    # ============================================================
    # 5. Join with firm_lerner_hourly + clearing price; compute realized & implied Lerner
    # ============================================================
    print("[5/5] Join with firm_lerner_hourly + marginalpdbc; compare implied vs realized…", flush=True)
    lerner = con.execute(f"""
        SELECT date, hour, firm,
               q_mwh, s_share, clearing_price_eur_mwh, supply_slope_mw_per_eur,
               lerner_index AS lerner_implied
        FROM '{LERNER}'
        WHERE date >= '{CUTOFF}'
          AND firm IN ('GE','IB','GN','HC')
    """).df()
    lerner["date"] = pd.to_datetime(lerner["date"])

    panel = fh.merge(lerner, on=["date", "hour", "firm"], how="inner")
    panel["lerner_realized"] = (panel["clearing_price_eur_mwh"] - panel["firm_marginal_bid_eur"]) / panel["clearing_price_eur_mwh"]
    panel["regime"] = panel["date"].apply(assign_regime)
    # Drop rows with non-finite values
    panel = panel.replace([np.inf, -np.inf], np.nan).dropna(
        subset=["lerner_implied", "lerner_realized", "clearing_price_eur_mwh"]
    )
    panel = panel[panel["clearing_price_eur_mwh"] > 0]

    print(f"   joined firm-hour panel: {len(panel):,} rows", flush=True)

    # ============================================================
    # 6. Aggregate by firm × regime
    # ============================================================
    rows = []
    for firm in BIG4:
        for regime in ["DA60/ID15", "DA15/ID15"]:
            sub = panel[(panel.firm == firm) & (panel.regime == regime)]
            if len(sub) < 30:
                continue
            rows.append({
                "firm":              firm,
                "regime":            regime,
                "n_hours":           int(len(sub)),
                "implied_lerner_mean":   float(sub.lerner_implied.mean()),
                "implied_lerner_med":    float(sub.lerner_implied.median()),
                "realized_lerner_mean":  float(sub.lerner_realized.mean()),
                "realized_lerner_med":   float(sub.lerner_realized.median()),
                "ratio_real_to_impl_mean": float(sub.lerner_realized.mean() / sub.lerner_implied.mean()) if sub.lerner_implied.mean() > 0 else float("nan"),
                "corr_real_impl":        float(sub[["lerner_realized","lerner_implied"]].corr().iloc[0,1]),
                "mean_clearing_price":   float(sub.clearing_price_eur_mwh.mean()),
                "mean_marginal_bid":     float(sub.firm_marginal_bid_eur.mean()),
                "mean_realized_markup_eur": float((sub.clearing_price_eur_mwh - sub.firm_marginal_bid_eur).mean()),
                "implied_markup_eur_mean":  float((sub.lerner_implied * sub.clearing_price_eur_mwh).mean()),
            })
    out = pd.DataFrame(rows)

    print()
    print("=" * 110)
    print("F1/F2/F3 HORTAÇSU-PULLER SOPHISTICATION TEST")
    print("=" * 110)
    print(f"  Sample: {len(panel):,} firm-hours, post-2025-03-19 (clean bid prices)")
    print(f"  Implied Lerner = q_i / (p × (1 − s_i) × |∂S/∂p|)  — Cournot FOC, from firm_lerner_hourly")
    print(f"  Realized Lerner = (p − firm_marginal_bid) / p     — actual marginal-bid cushion vs clearing")
    print(f"  Ratio realized / implied: 1.0 = sophisticated; <1 = firm leaves Cournot rent on the table")
    print()

    fmt = "{:<5}  {:<11}  {:>8}  {:>11}  {:>11}  {:>9}  {:>9}  {:>10}  {:>10}"
    print(fmt.format("firm", "regime", "n_hours",
                     "impl_L mean", "real_L mean", "ratio", "corr",
                     "impl_€mu", "real_€mu"))
    print("-" * 115)
    for _, r in out.iterrows():
        print(fmt.format(
            r["firm"], r["regime"],
            f"{r['n_hours']:,}",
            f"{r['implied_lerner_mean']:.3f}",
            f"{r['realized_lerner_mean']:.3f}",
            f"{r['ratio_real_to_impl_mean']:.2f}",
            f"{r['corr_real_impl']:.2f}",
            f"{r['implied_markup_eur_mean']:.1f}",
            f"{r['mean_realized_markup_eur']:.1f}",
        ))
    print()

    OUT.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUT, index=False)
    print(f"wrote {OUT}")


if __name__ == "__main__":
    main()
