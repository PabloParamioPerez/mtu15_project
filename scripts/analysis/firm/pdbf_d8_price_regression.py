# STATUS: ALIVE
# LAST-AUDIT: 2026-05-02
# FEEDS: D8 mechanism — bilateral channel ~ DA price level (gas-cap-crash hypothesis)
# CLAIM: D8's Q1 2024 break is well-explained by Spanish DA price level changes;
#        bilat_share rises with DA price level, falls when DA price collapses.
"""D8 price-crash hypothesis test at unit-day grain.

LHS: unit-day bilat_share (Big-4 nuclear or hydro)
RHS: contemporaneous monthly mean DA price (€/MWh, monthly mean to avoid
     daily noise) + unit FE + cal-month FE + DOW FE + year FE.
Cluster SE by date.

If β(DA_price) is significantly positive and economically meaningful, the
gas-cap-expiry / price-crash hypothesis is empirically supported as the
mechanism for D8's Q1 2024 bilateral-channel collapse.

Run separately for nuclear and hydro panels.

Output:
  results/regressions/pdbf_d8_price_regression.csv
"""
from __future__ import annotations

from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
import statsmodels.api as sm

PROJECT = Path(__file__).resolve().parents[3]
PDBF    = PROJECT / "data" / "processed" / "omie" / "mercado_diario" / "programas" / "pdbf_all.parquet"
PDBCE   = PROJECT / "data" / "processed" / "omie" / "mercado_diario" / "programas" / "pdbce_all.parquet"
LISTA   = PROJECT / "data" / "external" / "omie_reference" / "lista_unidades.csv"
PRICES  = PROJECT / "data" / "processed" / "omie" / "mercado_diario" / "precios" / "marginalpdbc_all.parquet"
OUT     = PROJECT / "results" / "regressions" / "pdbf_d8_price_regression.csv"


def fit_ols_cluster(y, X, cluster):
    return sm.OLS(y, X).fit(cov_type="cluster", cov_kwds={"groups": cluster})


def main() -> None:
    con = duckdb.connect()
    con.execute("SET memory_limit='8GB'")
    con.execute("SET threads=4")

    # firm + tech mapping
    firms = con.execute(f"""
        SELECT unit_code, grupo_empresarial AS firm FROM (
          SELECT unit_code, grupo_empresarial,
                 ROW_NUMBER() OVER (PARTITION BY unit_code ORDER BY date DESC) AS rn
          FROM '{PDBCE}' WHERE grupo_empresarial IS NOT NULL) WHERE rn = 1
    """).df()
    lista = pd.read_csv(LISTA)[["unit_code", "technology"]]
    map_uf = firms.merge(lista, on="unit_code", how="left")

    def tech_group(t):
        if not isinstance(t, str): return "Other"
        tl = t.lower()
        if "nuclear" in tl: return "Nuclear"
        if "ombeo" in tl or "idráulica" in tl: return "Hydro"
        return "Other"

    map_uf["tech_group"] = map_uf["technology"].apply(tech_group)
    con.register("uf", map_uf[["unit_code", "firm", "tech_group"]])

    # Monthly mean DA price
    print("[setup] monthly Spanish DA price…", flush=True)
    monthly_price = con.execute(f"""
        SELECT date_trunc('month', CAST(date AS DATE)) AS month,
               AVG(price_es_eur_mwh) AS da_price_mean,
               MEDIAN(price_es_eur_mwh) AS da_price_median
        FROM '{PRICES}'
        GROUP BY 1 ORDER BY 1
    """).df()
    monthly_price["month"] = pd.to_datetime(monthly_price["month"])

    # Unit-day panel of bilat_share, Big-4 nuclear + hydro
    print("[panel] unit-day bilat_share, Big-4 nuclear + hydro…", flush=True)
    panel = con.execute(f"""
        SELECT CAST(p.date AS DATE) AS date, p.unit_code, uf.firm, uf.tech_group,
               SUM(CASE WHEN p.offer_type = 4 AND p.assigned_power_mw > 0
                        THEN p.assigned_power_mw * p.mtu_minutes / 60.0
                        ELSE 0 END) AS bilateral_mwh,
               SUM(CASE WHEN p.offer_type = 1 AND p.assigned_power_mw > 0
                        THEN p.assigned_power_mw * p.mtu_minutes / 60.0
                        ELSE 0 END) AS auction_mwh
        FROM '{PDBF}' p JOIN uf USING (unit_code)
        WHERE uf.firm IN ('IB','GE','GN','HC')
          AND uf.tech_group IN ('Nuclear','Hydro')
        GROUP BY 1, 2, 3, 4
    """).df()
    panel["date"] = pd.to_datetime(panel["date"])
    panel["month"] = panel["date"].dt.to_period("M").dt.to_timestamp()
    panel["total_mwh"] = panel["bilateral_mwh"] + panel["auction_mwh"]
    panel = panel[panel["total_mwh"] > 0].copy()
    panel["bilat_share"] = panel["bilateral_mwh"] / panel["total_mwh"]
    panel["dow"]   = panel["date"].dt.dayofweek
    panel["cal_month"] = panel["date"].dt.month
    panel["year"]  = panel["date"].dt.year
    panel = panel.merge(monthly_price[["month","da_price_mean"]], on="month", how="left")

    print(f"   merged unit-day panel: {len(panel):,} rows", flush=True)

    rows_out = []
    for tech in ["Nuclear", "Hydro"]:
        sub = panel[panel.tech_group == tech].copy()
        if len(sub) < 100: continue
        # Within-unit demeaning
        sub["bilat_share_dm"] = sub["bilat_share"] - sub.groupby("unit_code")["bilat_share"].transform("mean")
        cols = {"const": np.ones(len(sub))}
        cols["da_price_eur_mwh"] = sub["da_price_mean"].values
        for d_ in range(1, 7):
            cols[f"DOW{d_}"] = (sub["dow"] == d_).astype(float).values
        for m_ in range(2, 13):
            cols[f"M{m_}"] = (sub["cal_month"] == m_).astype(float).values
        for yr in sorted(sub.year.unique())[1:]:
            cols[f"Y{yr}"] = (sub["year"] == yr).astype(float).values

        X = pd.DataFrame(cols, index=sub.index)
        y = sub["bilat_share_dm"].values
        cluster = sub["date"].dt.strftime("%Y%m%d").astype(np.int64).values
        m = fit_ols_cluster(y, X.values, cluster)
        b   = m.params[X.columns.get_loc("da_price_eur_mwh")]
        se  = m.bse[X.columns.get_loc("da_price_eur_mwh")]
        p   = m.pvalues[X.columns.get_loc("da_price_eur_mwh")]
        n_clusters = len(np.unique(cluster))
        print(f"\n=== {tech} (N={len(sub):,}, G={n_clusters:,}) ===")
        print(f"  β(da_price) = {b:+.6f} bilat_share per €/MWh  (SE {se:.6f}, p={p:.2e})")
        print(f"  R² = {m.rsquared:.3f}")
        # Effect-size translation: a €60 price drop (Dec 2023 €72 → Apr 2024 €13)
        delta_60 = b * 60
        print(f"  → A €60 drop in DA price (the Q1 2024 magnitude) implies bilat_share change of {delta_60*100:+.2f}pp")
        rows_out.append({"tech": tech, "n": len(sub), "n_clusters": n_clusters,
                        "beta_da_price": b, "se": se, "p": p,
                        "implied_pp_per_60eur_drop": delta_60 * 100,
                        "rsq": m.rsquared})

    out = pd.DataFrame(rows_out)
    OUT.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUT, index=False)
    print(f"\nwrote {OUT}")


if __name__ == "__main__":
    main()
