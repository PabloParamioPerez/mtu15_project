# STATUS: ALIVE
# LAST-AUDIT: 2026-05-04
# FEEDS: Bid-shape differentiation across quarters of an hour (post-MTU15-DA)
# CLAIM: After MTU15-DA (2025-10-01), Big-4 strategically differentiate their
#        bid ladders across the 4 quarters of an hour, more so in critical hours
#        than flat hours. Tested across CCGT, wind, solar, hydro, nuclear.
"""Bid-shape measures across the 4 quarters of an hour, by tech × firm.

For each unit-day-hour post-MTU15-DA (Oct 1, 2025+), compute:

  (1) ladder_identical: 1 if all 4 quarter-bid-ladders are byte-identical
                        (same set of (price, quantity) tranches)
  (2) n_tranches_q:     number of distinct price tranches per quarter
  (3) ladder_slope_q:   (price_top − price_bottom) / total_qty per quarter
  (4) avg_price_q:      qty-weighted mean price per quarter (existing v3 measure)

Then compare critical vs flat hours by tech × firm.

The headline test is (1): pre-MTU15-DA, ladder identity is forced (one bid per
hour). Post-MTU15-DA, identity rate < 100% means strategic across-quarter
differentiation. If identity rate is lower in critical hours than flat hours,
the granularity-amplifies-strategic-bidding mechanism is supported.

Data:
  cab_all.parquet:  offer header (date, offer_code, version, unit_code, buy_sell)
  det_all.parquet:  bid details (date, offer_code, version, period, price, qty)
  lista_unidades.csv: unit_code → technology

Output:
  results/regressions/bid_shape_by_tech.csv
  figures/working/bid_shape_by_tech.png
"""
from __future__ import annotations

from pathlib import Path

import duckdb
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

PROJECT  = Path(__file__).resolve().parents[3]
CAB      = PROJECT / "data" / "processed" / "omie" / "mercado_diario" / "ofertas" / "cab_all.parquet"
DET      = PROJECT / "data" / "processed" / "omie" / "mercado_diario" / "ofertas" / "det_all.parquet"
PDBCE    = PROJECT / "data" / "processed" / "omie" / "mercado_diario" / "programas" / "pdbce_all.parquet"
UNITS    = PROJECT / "data" / "external" / "omie_reference" / "lista_unidades.csv"

OUT_DIR_R = PROJECT / "results" / "regressions"
OUT_DIR_F = PROJECT / "figures" / "working"

MTU15_DA_DATE = pd.Timestamp("2025-10-01")
CRITICAL_HOURS = [7, 8, 16, 17, 18]
BIG4 = ["IB", "GE", "GN", "HC"]


def tech_bucket(t: str | None) -> str:
    if t is None or pd.isna(t): return "unknown"
    t = str(t)
    if "Solar Fotov" in t or "Solar Térm" in t: return "solar"
    if "Eólica" in t: return "wind"
    if "Hidráulica" in t or "Hidraulic" in t: return "hydro"
    if "Ciclo Combinado" in t: return "CCGT"
    if "Nuclear" in t: return "nuclear"
    if "Térmica no Renovab" in t: return "thermal_nonRE"
    if "Térmica Renovable" in t: return "thermal_RE"
    return "other"


def main() -> None:
    con = duckdb.connect()
    con.execute("SET memory_limit='8GB'")
    con.execute("SET threads=4")

    print("[setup] unit → firm + tech…", flush=True)
    # Firm-1: from PDBCE grupo_empresarial (thermal/hydro/nuclear)
    firms_thermal = con.execute(f"""
        SELECT unit_code, grupo_empresarial AS firm FROM (
          SELECT unit_code, grupo_empresarial,
                 ROW_NUMBER() OVER (PARTITION BY unit_code ORDER BY date DESC) AS rn
          FROM '{PDBCE}' WHERE grupo_empresarial IS NOT NULL) WHERE rn = 1
    """).df()
    # Firm-2: from owner_agent in lista_unidades (renewables go here)
    units = pd.read_csv(UNITS)[["unit_code", "technology", "owner_agent"]]
    units["tech"] = units["technology"].apply(tech_bucket)

    def owner_to_firm(s: str | None) -> str | None:
        if s is None or pd.isna(s): return None
        s = str(s).upper()
        if "IBERDROLA" in s: return "IB"
        if "ENDESA" in s or "ENEL GREEN POWER" in s: return "GE"
        if "NATURGY" in s or "GAS NATURAL" in s: return "GN"
        if "EDP" in s or "HIDROELÉCTRICA DEL CANTÁBRICO" in s or "HC ENERG" in s: return "HC"
        return None
    units["firm_owner"] = units["owner_agent"].apply(owner_to_firm)
    units = units.merge(firms_thermal, on="unit_code", how="left")
    # Use grupo_empresarial when present (thermal/hydro/nuclear), else owner-based mapping
    units["firm_combined"] = units["firm"].fillna(units["firm_owner"])
    uft = units[units["firm_combined"].notna()][["unit_code", "tech"]].copy()
    uft["firm"] = units.loc[uft.index, "firm_combined"].values
    uft = uft[uft.firm.isin(BIG4)]
    con.register("uft", uft[["unit_code","firm","tech"]])
    print(f"   {len(uft):,} Big-4 unit-codes mapped (thermal via PDBCE + RE via owner_agent)", flush=True)
    print(uft.groupby(["firm","tech"]).size().unstack(fill_value=0).to_string())

    # ---------------------------------------------------------------
    # Pull DET joined to CAB and uft, post-MTU15-DA only
    # We need 4 quarter-bid-ladders per (date, hour=period//4, unit, version)
    # ---------------------------------------------------------------
    print("\n[1] DET+CAB join, post-MTU15-DA Big-4 sell offers…", flush=True)
    bids = con.execute(f"""
        WITH cab AS (
          SELECT CAST(date AS DATE) AS d, offer_code, version, unit_code, buy_sell,
                 ROW_NUMBER() OVER (PARTITION BY CAST(date AS DATE), offer_code, unit_code
                                    ORDER BY version DESC) AS rn
          FROM '{CAB}'
          WHERE buy_sell = 'V'
            AND CAST(date AS DATE) >= DATE '2025-10-01'
        ),
        cab_latest AS (SELECT * FROM cab WHERE rn = 1),
        det AS (
          SELECT CAST(d.date AS DATE) AS d, d.offer_code, d.version, d.period,
                 d.price_eur_mwh AS price, d.quantity_mw AS qty
          FROM '{DET}' d
          WHERE CAST(d.date AS DATE) >= DATE '2025-10-01'
            AND d.period BETWEEN 1 AND 96
            AND d.quantity_mw IS NOT NULL AND d.quantity_mw > 0
        )
        SELECT det.d AS date, det.period,
               CAST((det.period - 1) / 4 AS INTEGER) AS hour,
               ((det.period - 1) % 4) AS quarter,
               cab_latest.unit_code, uft.firm, uft.tech,
               det.price, det.qty
        FROM det
          JOIN cab_latest USING (d, offer_code, version)
          JOIN uft ON cab_latest.unit_code = uft.unit_code
        WHERE det.period BETWEEN 1 AND 96
    """).df()
    print(f"   {len(bids):,} (date, period, unit, tranche) rows", flush=True)
    if len(bids) == 0:
        print("ERROR: no Big-4 sell offers found post-MTU15-DA"); return

    # ---------------------------------------------------------------
    # Per quarter (date, hour, unit, quarter): tranche count, ladder slope, avg price
    # ---------------------------------------------------------------
    print("\n[2] per-quarter measures…", flush=True)
    q_meas = (bids.groupby(["date","hour","unit_code","firm","tech","quarter"])
                  .agg(n_tranches=("price","nunique"),
                       p_top=("price","max"),
                       p_bot=("price","min"),
                       q_total=("qty","sum"),
                       p_avg=("price", lambda s: float(np.average(s, weights=bids.loc[s.index, "qty"])))
                       )
                  .reset_index())
    q_meas["slope"] = (q_meas["p_top"] - q_meas["p_bot"]) / q_meas["q_total"].clip(lower=1.0)

    # ---------------------------------------------------------------
    # Per unit-hour: ladder identity test
    # We hash each quarter's (sorted (price, qty)) ladder; identical iff all 4 hash same
    # ---------------------------------------------------------------
    print("[3] ladder-identity hash per unit-hour…", flush=True)
    bids_sorted = bids.sort_values(["date","hour","unit_code","quarter","price","qty"])
    # ladder string per quarter
    bids_sorted["pq"] = (bids_sorted["price"].round(3).astype(str)
                         + "|" + bids_sorted["qty"].round(3).astype(str))
    ladder_per_q = (bids_sorted.groupby(["date","hour","unit_code","firm","tech","quarter"])["pq"]
                               .apply(lambda s: ",".join(s)).reset_index())
    # one row per (unit_hour, quarter); pivot to one row per unit_hour with 4 cols
    pv = ladder_per_q.pivot_table(index=["date","hour","unit_code","firm","tech"],
                                  columns="quarter", values="pq", aggfunc="first").reset_index()
    pv["n_quarters_with_data"] = pv[[0,1,2,3]].notna().sum(axis=1)
    # identity if all 4 quarters present and all equal
    def all_identical(row):
        vals = [row[0], row[1], row[2], row[3]]
        if any(pd.isna(v) for v in vals): return np.nan  # incomplete hour
        return int(len(set(vals)) == 1)
    pv["ladder_identical"] = pv.apply(all_identical, axis=1)

    pv["regime"] = "DA15/ID15"
    pv["critical"] = pv["hour"].isin(CRITICAL_HOURS).astype(int)
    pv_complete = pv[pv["n_quarters_with_data"] == 4].copy()
    print(f"   {len(pv_complete):,} unit-hours with all 4 quarters of data", flush=True)

    # ---------------------------------------------------------------
    # Aggregate by tech × firm × critical/flat
    # ---------------------------------------------------------------
    print("\n[4] aggregating bid-shape measures by tech × firm × critical/flat…", flush=True)
    out_rows = []
    print()
    print("=" * 110)
    print("Bid-ladder identity rate (1 = all 4 quarters byte-identical), Big-4 post-MTU15-DA")
    print("=" * 110)
    for tech in ["CCGT","wind","solar","hydro","nuclear","thermal_nonRE","thermal_RE"]:
        tdf = pv_complete[pv_complete.tech == tech]
        if len(tdf) < 100: continue
        for firm in BIG4 + ["ALL"]:
            fdf = tdf if firm == "ALL" else tdf[tdf.firm == firm]
            if len(fdf) < 100: continue
            crit_rate = fdf[fdf.critical==1]["ladder_identical"].mean()
            flat_rate = fdf[fdf.critical==0]["ladder_identical"].mean()
            n_crit = (fdf.critical==1).sum()
            n_flat = (fdf.critical==0).sum()
            out_rows.append({"measure":"ladder_identity_rate","tech":tech,"firm":firm,
                             "critical_rate": crit_rate, "flat_rate": flat_rate,
                             "diff_crit_minus_flat": crit_rate - flat_rate,
                             "n_crit": n_crit, "n_flat": n_flat})
            if firm == "ALL":
                print(f"  {tech:14s}  ALL Big-4 :  flat {flat_rate*100:5.1f}%  |  critical {crit_rate*100:5.1f}%  "
                      f"|  Δ {(crit_rate-flat_rate)*100:+5.1f} pp  (n_crit={n_crit:,}, n_flat={n_flat:,})")

    # CV of p_avg across the 4 quarters of a unit-hour (v3 measure for comparison)
    print()
    print("=" * 110)
    print("CV(p_avg) across 4 quarters, per unit-hour, distribution by tech × critical/flat")
    print("=" * 110)
    cv_panel = (q_meas.groupby(["date","hour","unit_code","firm","tech"])
                      .agg(p_mean=("p_avg","mean"), p_std=("p_avg","std"),
                           n_q=("p_avg","count"))
                      .reset_index())
    cv_panel = cv_panel[cv_panel["n_q"] == 4].copy()
    cv_panel["cv_p"] = (cv_panel["p_std"] / cv_panel["p_mean"].abs().clip(lower=1e-3)).clip(upper=10)
    cv_panel["differentiated_1pct"] = (cv_panel["cv_p"] > 0.01).astype(int)
    cv_panel["critical"] = cv_panel["hour"].isin(CRITICAL_HOURS).astype(int)

    for tech in ["CCGT","wind","solar","hydro","nuclear"]:
        tdf = cv_panel[cv_panel.tech == tech]
        if len(tdf) < 100: continue
        crit = tdf[tdf.critical == 1]
        flat = tdf[tdf.critical == 0]
        if len(crit) < 50 or len(flat) < 50: continue
        rate_c = crit.differentiated_1pct.mean() * 100
        rate_f = flat.differentiated_1pct.mean() * 100
        cv_med_c = crit.cv_p.median() * 100
        cv_med_f = flat.cv_p.median() * 100
        cv_p90_c = crit.cv_p.quantile(0.90) * 100
        cv_p90_f = flat.cv_p.quantile(0.90) * 100
        print(f"  {tech:12s}: rate(CV>1%) flat {rate_f:5.1f}%  crit {rate_c:5.1f}%  "
              f"|  median CV flat {cv_med_f:5.2f}%  crit {cv_med_c:5.2f}%  "
              f"|  P90 CV flat {cv_p90_f:5.2f}%  crit {cv_p90_c:5.2f}%")
        out_rows.append({"measure":"cv_pavg","tech":tech,"firm":"ALL",
                         "rate_diff1pct_crit": rate_c, "rate_diff1pct_flat": rate_f,
                         "cv_med_crit": cv_med_c, "cv_med_flat": cv_med_f,
                         "cv_p90_crit": cv_p90_c, "cv_p90_flat": cv_p90_f})

    # n_tranches and slope by tech × critical/flat
    print()
    print("=" * 110)
    print("Per-quarter measures (tranches per quarter, ladder slope EUR/MW), Big-4 post-MTU15-DA")
    print("=" * 110)
    for tech in ["CCGT","wind","solar","hydro","nuclear"]:
        tdf = q_meas[q_meas.tech == tech]
        if len(tdf) < 100: continue
        for firm in BIG4 + ["ALL"]:
            fdf = tdf if firm == "ALL" else tdf[tdf.firm == firm]
            if len(fdf) < 100: continue
            fdf = fdf.assign(critical=fdf["hour"].isin(CRITICAL_HOURS).astype(int))
            crit_t = fdf[fdf.critical==1]["n_tranches"].mean()
            flat_t = fdf[fdf.critical==0]["n_tranches"].mean()
            crit_s = fdf[fdf.critical==1]["slope"].mean()
            flat_s = fdf[fdf.critical==0]["slope"].mean()
            crit_p = fdf[fdf.critical==1]["p_avg"].mean()
            flat_p = fdf[fdf.critical==0]["p_avg"].mean()
            out_rows.append({"measure":"per_quarter","tech":tech,"firm":firm,
                             "n_tranches_crit": crit_t, "n_tranches_flat": flat_t,
                             "slope_crit": crit_s, "slope_flat": flat_s,
                             "pavg_crit": crit_p, "pavg_flat": flat_p})
            if firm == "ALL":
                print(f"  {tech:14s}  ALL Big-4 :  tranches flat {flat_t:.2f} crit {crit_t:.2f}  "
                      f"|  slope flat {flat_s:6.3f} crit {crit_s:6.3f}  "
                      f"|  p_avg flat {flat_p:6.1f} crit {crit_p:6.1f}")

    pd.DataFrame(out_rows).to_csv(OUT_DIR_R / "bid_shape_by_tech.csv", index=False)

    # ---------------------------------------------------------------
    # Figure: identity rate critical vs flat by tech × firm
    # ---------------------------------------------------------------
    OUT_DIR_F.mkdir(parents=True, exist_ok=True)
    df_id = pd.DataFrame([r for r in out_rows if r["measure"] == "ladder_identity_rate" and r["firm"] == "ALL"])
    if len(df_id) > 0:
        fig, axes = plt.subplots(1, 2, figsize=(13, 5))
        techs = df_id.tech.tolist()
        x = np.arange(len(techs))
        w = 0.35
        axes[0].bar(x - w/2, df_id.flat_rate*100, w, label="flat hours", color="tab:blue")
        axes[0].bar(x + w/2, df_id.critical_rate*100, w, label="critical hours", color="tab:red")
        axes[0].set_xticks(x); axes[0].set_xticklabels(techs, rotation=15)
        axes[0].set_ylabel("Bid-ladder identity rate (%)")
        axes[0].set_title("% of unit-hours where all 4 quarter-ladders are byte-identical\n"
                          "(lower = more strategic across-quarter differentiation)")
        axes[0].legend(); axes[0].set_ylim(0, 105); axes[0].grid(alpha=0.3)
        axes[1].bar(x, df_id.diff_crit_minus_flat*100, color="tab:purple")
        axes[1].axhline(0, color="black", lw=0.6)
        axes[1].set_xticks(x); axes[1].set_xticklabels(techs, rotation=15)
        axes[1].set_ylabel("Δ identity rate (critical − flat, pp)")
        axes[1].set_title("Critical-hour identity gap (negative = more differentiation in critical)")
        axes[1].grid(alpha=0.3)
        plt.tight_layout()
        plt.savefig(OUT_DIR_F / "bid_shape_by_tech.png", dpi=110, bbox_inches="tight")
        plt.close()

    print(f"\nwrote {OUT_DIR_R / 'bid_shape_by_tech.csv'}")
    print(f"wrote {OUT_DIR_F / 'bid_shape_by_tech.png'}")
    print("Done.")


if __name__ == "__main__":
    main()
