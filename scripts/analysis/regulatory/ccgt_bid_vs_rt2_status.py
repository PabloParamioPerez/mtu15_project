# STATUS: ALIVE
# LAST-AUDIT: 2026-05-16
# FEEDS: provisional.tex §6/§geo (bid-price conditional on RT2-up state)
# CLAIM: For each CCGT unit-day in 2024-2025, compute the unit's
#        quantity-weighted DA bid price and label the day by RT2-up
#        magnitude (= PHF_max − PDBF). Compare bid-price distributions
#        across days the unit was RT2-up vs not. Hypothesis (user):
#        firms bid HIGHER when they expect REE to call them up under
#        reforzada (since RT2 is pay-as-bid at the unit's own offer).

from __future__ import annotations

import sys
from pathlib import Path

import duckdb
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO / "src"))
from mtu.classification.units import firm_unit_panel  # noqa: E402

DET  = REPO / "data" / "processed" / "omie" / "mercado_diario" / "ofertas" / "det_all.parquet"
CAB  = REPO / "data" / "processed" / "omie" / "mercado_diario" / "ofertas" / "cab_all.parquet"
PDBF = REPO / "data" / "processed" / "omie" / "mercado_diario" / "programas" / "pdbf_all.parquet"
PHF  = REPO / "data" / "processed" / "omie" / "mercado_intradiario_subastas" / "programas" / "phf_all.parquet"
UNITS_CSV = REPO / "data" / "external" / "omie_reference" / "lista_unidades.csv"
ZONE_MAP  = REPO / "data" / "external" / "ccgt_zonal_map.csv"
OUTDIR = REPO / "results" / "regressions" / "regulatory" / "ccgt_bid_vs_rt2"
OUTDIR.mkdir(parents=True, exist_ok=True)
FIGDIR = REPO / "figures" / "working"

PIVOTAL = ("IB", "GE", "GN", "HC")
PRE_W  = ("2024-01-01", "2025-04-01")
POST_W = ("2025-05-01", "2026-02-01")


def _month_iter(start, end):
    s = pd.Timestamp(start); e = pd.Timestamp(end)
    cur = pd.Timestamp(s.year, s.month, 1)
    while cur < e:
        nxt = cur + pd.offsets.MonthBegin(1)
        yield cur.date(), nxt.date()
        cur = nxt


def build_panel(window) -> pd.DataFrame:
    units = firm_unit_panel(csv_path=str(UNITS_CSV), scheme="short", mode="primary_owner")
    ccgt = units[(units["tech_group"] == "CCGT") & (units["parent"].isin(PIVOTAL))][
        ["unit_code", "parent"]].rename(columns={"parent": "firm"})
    con = duckdb.connect(); con.execute("PRAGMA threads=4"); con.execute("SET memory_limit='6GB'")
    con.register("ccgt_units", ccgt)
    rows = []
    for m0, m1 in _month_iter(*window):
        # Unit-day quantity-weighted mean bid price (DA sell-side, latest version)
        q_bid = f"""
        WITH cab_l AS (
            SELECT date::DATE AS d, offer_code, version, unit_code,
                   ROW_NUMBER() OVER (PARTITION BY date::DATE, offer_code, unit_code
                                      ORDER BY version DESC) AS rn
            FROM '{CAB}'
            WHERE buy_sell = 'V' AND date::DATE >= DATE '{m0}' AND date::DATE < DATE '{m1}'
        ),
        det AS (
            SELECT date::DATE AS d, offer_code, version, period,
                   price_eur_mwh AS p, quantity_mw AS q, mtu_minutes
            FROM '{DET}'
            WHERE date::DATE >= DATE '{m0}' AND date::DATE < DATE '{m1}'
              AND price_eur_mwh IS NOT NULL AND quantity_mw > 0
        )
        SELECT d.d AS day, c.unit_code, u.firm,
               SUM(d.p * d.q * (d.mtu_minutes / 60.0)) / NULLIF(SUM(d.q * (d.mtu_minutes / 60.0)), 0) AS qw_bid_eur_mwh,
               SUM(d.q * (d.mtu_minutes / 60.0)) AS total_bid_mwh
        FROM det d
          JOIN cab_l c ON c.rn = 1 AND c.d = d.d AND c.offer_code = d.offer_code AND c.version = d.version
          JOIN ccgt_units u ON c.unit_code = u.unit_code
        GROUP BY 1, 2, 3
        """
        b = con.execute(q_bid).df()
        q_pdbf = f"""
        SELECT date::DATE AS day, p.unit_code,
               SUM(p.assigned_power_mw * (p.mtu_minutes / 60.0)) AS pdbf_mwh
        FROM '{PDBF}' p JOIN ccgt_units u ON p.unit_code = u.unit_code
        WHERE date::DATE >= DATE '{m0}' AND date::DATE < DATE '{m1}'
          AND p.assigned_power_mw IS NOT NULL
        GROUP BY 1, 2
        """
        p = con.execute(q_pdbf).df()
        q_phf = f"""
        WITH lat AS (
            SELECT date::DATE AS d, period, unit_code, assigned_power_mw, mtu_minutes,
                   ROW_NUMBER() OVER (PARTITION BY date::DATE, period, unit_code
                                      ORDER BY session_number DESC) AS rn
            FROM '{PHF}'
            WHERE date::DATE >= DATE '{m0}' AND date::DATE < DATE '{m1}'
              AND assigned_power_mw IS NOT NULL
        )
        SELECT lat.d AS day, lat.unit_code,
               SUM(lat.assigned_power_mw * (lat.mtu_minutes / 60.0)) AS phf_mwh
        FROM lat JOIN ccgt_units u ON lat.unit_code = u.unit_code
        WHERE lat.rn = 1
        GROUP BY 1, 2
        """
        h = con.execute(q_phf).df()
        # Merge
        m = b.merge(p, on=["day", "unit_code"], how="outer").merge(
                  h, on=["day", "unit_code"], how="outer").fillna(0.0)
        # Re-fill firm
        m = m.merge(ccgt, on="unit_code", how="left")
        m["firm"] = m["firm_y"].combine_first(m["firm_x"]).fillna(m["firm_y"])
        m = m[["day", "unit_code", "firm", "qw_bid_eur_mwh", "total_bid_mwh", "pdbf_mwh", "phf_mwh"]]
        m["day"] = pd.to_datetime(m["day"])
        m["rt2_mwh"] = m["phf_mwh"] - m["pdbf_mwh"]
        rows.append(m)
        print(f"  {m0}: {len(m)} (day, unit) cells", flush=True)
    return pd.concat(rows, ignore_index=True)


def main():
    print("=== Pre-blackout panel ===")
    pre = build_panel(PRE_W); pre["regime"] = "pre"
    print("=== Post-blackout panel ===")
    post = build_panel(POST_W); post["regime"] = "post"
    panel = pd.concat([pre, post], ignore_index=True)
    panel.to_csv(OUTDIR / "unit_day_panel.csv", index=False)

    # Classify each unit-day by RT2 status (binary: rt2_up_day if rt2_mwh > 100 MWh ≈ 1 GW for 0.1h)
    panel["rt2_state"] = pd.cut(panel["rt2_mwh"],
                                 bins=[-np.inf, 100, 1000, np.inf],
                                 labels=["~0", "small_up", "large_up"])

    print("\n=== qw bid price (EUR/MWh) by firm × regime × RT2 state ===")
    summary = panel.dropna(subset=["qw_bid_eur_mwh"]).groupby(
        ["firm", "regime", "rt2_state"], observed=True).agg(
        n=("qw_bid_eur_mwh", "size"),
        median_bid=("qw_bid_eur_mwh", "median"),
        p25_bid=("qw_bid_eur_mwh", lambda v: np.quantile(v, 0.25) if len(v) else np.nan),
        p75_bid=("qw_bid_eur_mwh", lambda v: np.quantile(v, 0.75) if len(v) else np.nan),
    ).round(1).reset_index()
    print(summary.to_string(index=False))
    summary.to_csv(OUTDIR / "bid_by_rt2_state.csv", index=False)


if __name__ == "__main__":
    main()
