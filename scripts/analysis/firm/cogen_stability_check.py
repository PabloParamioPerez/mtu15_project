# STATUS: ALIVE
# LAST-AUDIT: 2026-05-31
# CLAIM: Cogen's response to MTU15-DA in *preliminary*'s Spec C
#        framework (in-band sigma_p and N_eff, h=50 EUR/MWh on DA,
#        critical-vs-flat DiD) is essentially zero. Cleared MWh per
#        day is also flat across the full reform sequence (~35-44
#        GWh/day vs CCGT 39->129 GWh/day, 3.3x).
#
# Specifically, on the DA15 Spec C windows used by *preliminary*:
#   pre  2025-04-28 .. 2025-09-30 (reforzada-era pre)
#   post 2025-10-01 .. 2025-12-31 (post-DA15)
# h=50 EUR/MWh (preliminary's window-and-market-specific DA15 DA p90),
# critical={5..8} U {16..22}, flat={1,2,3}:
#
#   Cogen sigma_p DiD = -0.08, N_eff DiD = -0.004     (essentially zero)
#   CCGT  sigma_p DiD = +0.79, N_eff DiD = +1.99      (substantial)
#
# Preliminary results §4.C reports CCGT DA15 DA sigma_p DiD = +0.68***,
# N_eff DiD = +1.89*** -- our recomputation reproduces this within
# rounding (mean-based vs FE-regression coefficient explains the gap).
#
# IN:  data/processed/omie/mercado_diario/{ofertas/{det_all,cab_all},precios/marginalpdbc_all}.parquet
#      data/processed/omie/mercado_intradiario_subastas/{ofertas/{idet_all,icab_all},precios/marginalpibc_all}.parquet
#      data/processed/omie/mercado_intradiario_subastas/programas/phf_all.parquet
#      data/derived/panels/bid_shape_critical_flat/_unit_map.parquet
# OUT: results/regressions/firm/cogen_stability/cogen_phf_by_regime.csv
#      results/regressions/firm/cogen_stability/cogen_specc_did_h50.csv

from pathlib import Path

import duckdb
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
DET = REPO / "data/processed/omie/mercado_diario/ofertas/det_all.parquet"
CAB = REPO / "data/processed/omie/mercado_diario/ofertas/cab_all.parquet"
MPDBC = REPO / "data/processed/omie/mercado_diario/precios/marginalpdbc_all.parquet"
IDET = REPO / "data/processed/omie/mercado_intradiario_subastas/ofertas/idet_all.parquet"
ICAB = REPO / "data/processed/omie/mercado_intradiario_subastas/ofertas/icab_all.parquet"
MPIBC = REPO / "data/processed/omie/mercado_intradiario_subastas/precios/marginalpibc_all.parquet"
PHF = REPO / "data/processed/omie/mercado_intradiario_subastas/programas/phf_all.parquet"
UMAP = REPO / "data/derived/panels/bid_shape_critical_flat/_unit_map.parquet"
OUT = REPO / "results/regressions/firm/cogen_stability"
OUT.mkdir(parents=True, exist_ok=True)

# Preliminary windows + bandwidths (cf. preliminary results §1 + §4.C):
PRE = ("2025-04-28", "2025-09-30")
POST = ("2025-10-01", "2025-12-31")
H_DA = 50.0   # DA15 DA, p90-p50 of MCP distribution in DA15 window
H_IDA = 58.0  # DA15 IDA, same


def cleared_per_regime(con):
    q = f"""
    WITH final_program AS (
      SELECT date, period, unit_code, mtu_minutes,
             MAX_BY(assigned_power_mw, session_number) AS mw_final
      FROM '{PHF}' WHERE date >= '2024-01-01' AND period BETWEEN 1 AND 96
      GROUP BY date, period, unit_code, mtu_minutes
    ),
    classified AS (
      SELECT
        CASE WHEN fp.date < '2024-06-14' THEN '1.pre-IDA'
             WHEN fp.date < '2024-12-01' THEN '2.3-sess'
             WHEN fp.date < '2025-03-19' THEN '3.ISP15-win'
             WHEN fp.date < '2025-04-28' THEN '4.MTU15-IDA pre-blk'
             WHEN fp.date < '2025-10-01' THEN '5.MTU15-IDA post-blk'
             ELSE                            '6.DA15/ID15' END AS regime,
        COALESCE(um.tech_group, 'Unknown') AS tech_group,
        fp.date, fp.mw_final * fp.mtu_minutes / 60.0 AS mwh
      FROM final_program fp LEFT JOIN '{UMAP}' um USING (unit_code)
      WHERE fp.mw_final > 0
    )
    SELECT tech_group, regime, COUNT(DISTINCT date) AS n_days,
           ROUND(SUM(mwh) / 1000.0 / COUNT(DISTINCT date), 2) AS gwh_per_day
    FROM classified
    WHERE tech_group IN ('CCGT','Cogen','Biomass','Nuclear')
    GROUP BY tech_group, regime ORDER BY tech_group, regime
    """
    return con.execute(q).fetchdf()


def per_curve_da(con, tech, d_lo, d_hi, h):
    q = f"""
    WITH cab_t AS (
      SELECT date, offer_code, version, unit_code, mtu_minutes FROM '{CAB}' cab
      JOIN (SELECT unit_code FROM '{UMAP}' WHERE tech_group = '{tech}')
        USING (unit_code)
      WHERE cab.buy_sell = 'V' AND cab.date BETWEEN '{d_lo}' AND '{d_hi}'
    ),
    bids AS (
      SELECT cab.date, d.period, cab.unit_code, cab.mtu_minutes,
             d.price_eur_mwh AS p, CAST(d.quantity_mw AS DOUBLE) AS q
      FROM '{DET}' d JOIN cab_t cab USING (date, offer_code, version)
      WHERE d.quantity_mw > 0 AND d.price_eur_mwh IS NOT NULL
    ),
    mcp AS (SELECT date, period, price_es_eur_mwh AS mcp FROM '{MPDBC}'
            WHERE date BETWEEN '{d_lo}' AND '{d_hi}'),
    joined AS (
      SELECT b.*, m.mcp,
             CAST(CEIL(b.period * (b.mtu_minutes / 60.0)) AS INT) AS clock_hour
      FROM bids b LEFT JOIN mcp m USING (date, period)
      WHERE m.mcp IS NOT NULL AND ABS(b.p - m.mcp) <= {h}
    ),
    pc AS (
      SELECT date, period, unit_code, clock_hour,
             SUM(q) AS sw, SUM(q*p) AS swp, SUM(q*p*p) AS swp2,
             SUM(q*q) AS sw2, COUNT(*) AS n_tr
      FROM joined GROUP BY date, period, unit_code, clock_hour
    )
    SELECT
      CASE WHEN clock_hour IN (5,6,7,8,16,17,18,19,20,21,22) THEN 'Critical'
           WHEN clock_hour IN (1,2,3) THEN 'Flat' END AS hc,
      COUNT(*) AS n_curves, COUNT(DISTINCT unit_code) AS n_units,
      ROUND(AVG(n_tr), 3) AS mean_n_tr,
      ROUND(AVG(sqrt(GREATEST(swp2/sw - (swp/sw)*(swp/sw), 0))), 3) AS sigma_p,
      ROUND(AVG((sw*sw)/NULLIF(sw2,0)), 3) AS n_eff
    FROM pc WHERE clock_hour IN (1,2,3,5,6,7,8,16,17,18,19,20,21,22)
    GROUP BY hc ORDER BY hc
    """
    df = con.execute(q).fetchdf()
    df["tech"] = tech
    return df


def did_for(rows, tech):
    rows = rows[rows.tech == tech]
    rec = {"tech": tech}
    for w in ["pre", "post"]:
        for hc in ["Critical", "Flat"]:
            r = rows[(rows.win == w) & (rows.hc == hc)]
            if len(r):
                rec[f"sigma_p_{w}_{hc[0].lower()}"] = float(r.sigma_p.iloc[0])
                rec[f"n_eff_{w}_{hc[0].lower()}"] = float(r.n_eff.iloc[0])
                rec[f"n_curves_{w}_{hc[0].lower()}"] = int(r.n_curves.iloc[0])
    rec["sigma_p_DiD"] = round(
        (rec.get("sigma_p_post_c", 0) - rec.get("sigma_p_post_f", 0))
        - (rec.get("sigma_p_pre_c", 0) - rec.get("sigma_p_pre_f", 0)), 3)
    rec["n_eff_DiD"] = round(
        (rec.get("n_eff_post_c", 0) - rec.get("n_eff_post_f", 0))
        - (rec.get("n_eff_pre_c", 0) - rec.get("n_eff_pre_f", 0)), 3)
    return rec


def main():
    con = duckdb.connect()
    con.execute("SET memory_limit='8GB'"); con.execute("SET threads=4")

    print("=== A. PHF cleared GWh/day by tech × regime ===")
    cleared = cleared_per_regime(con)
    cleared.to_csv(OUT / "cogen_phf_by_regime.csv", index=False)
    print(cleared.pivot(index="tech_group", columns="regime",
                       values="gwh_per_day").to_string())

    print("\n=== B. Spec C cells at preliminary's h=50 (DA15 DA), pre/post × crit/flat ===")
    rows = []
    for tech in ("Cogen", "CCGT", "Biomass", "Nuclear"):
        for w, (lo, hi) in [("pre", PRE), ("post", POST)]:
            df = per_curve_da(con, tech, lo, hi, H_DA)
            df["win"] = w
            rows.append(df)
    rows = pd.concat(rows, ignore_index=True)
    print(rows.to_string(index=False))

    print("\n=== C. DiD on sigma_p and N_eff at preliminary's h=50, DA15 ===")
    did_rows = [did_for(rows, t) for t in ("Cogen", "CCGT", "Biomass", "Nuclear")]
    did = pd.DataFrame(did_rows)
    cols = ["tech", "sigma_p_pre_c", "sigma_p_pre_f", "sigma_p_post_c",
            "sigma_p_post_f", "sigma_p_DiD",
            "n_eff_pre_c", "n_eff_pre_f", "n_eff_post_c", "n_eff_post_f",
            "n_eff_DiD"]
    did = did[cols]
    did.to_csv(OUT / "cogen_specc_did_h50.csv", index=False)
    print(did.to_string(index=False))
    print(f"\nwrote:\n  {OUT}/cogen_phf_by_regime.csv\n  {OUT}/cogen_specc_did_h50.csv")


if __name__ == "__main__":
    main()
