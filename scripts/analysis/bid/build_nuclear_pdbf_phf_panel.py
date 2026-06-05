# STATUS: ALIVE
# LAST-AUDIT: 2026-06-05
# FEEDS: bsts_nuclear_pdbf.R --- daily nuclear PDBF (auction + bilateral) and
#        PHF (final post-IDA total) panels for the §A.8 Nuclear PDBF shortfall
#        analysis. PDBF source: data/processed/omie/mercado_diario/programas
#        /pdbf_all.parquet (sum across nuclear units; assigned_power_mw is
#        instantaneous power, period length depends on mtu_minutes). PHF
#        source: data/processed/omie/mercado_intradiario_subastas/programas
#        /phf_all.parquet (final hourly programme post-IDA).
#
# OUT: data/derived/panels/nuclear_pdbf_phf_daily.parquet
#      columns: d, nuclear_pdbf_gwh, nuclear_phf_gwh,
#               wind_gwh, solar_gwh, gas_eur

from pathlib import Path
import duckdb
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
PDBF  = REPO / "data/processed/omie/mercado_diario/programas/pdbf_all.parquet"
PHF   = REPO / "data/processed/omie/mercado_intradiario_subastas/programas/phf_all.parquet"
UNITS = REPO / "data/external/omie_reference/lista_unidades.csv"
COVPANEL = REPO / "data/derived/panels/bsts_quantities_panel.parquet"
OUT   = REPO / "data/derived/panels/nuclear_pdbf_phf_daily.parquet"


def main() -> None:
    units = pd.read_csv(UNITS)
    nuclear = set(
        units.loc[units["technology"].str.contains("nuclear", case=False, na=False),
                  "unit_code"].tolist()
    )
    print(f"nuclear units: {len(nuclear)}")

    con = duckdb.connect()
    con.execute("SET memory_limit='10GB'; SET threads=4")
    con.register("nu", pd.DataFrame({"unit_code": list(nuclear)}))

    # PDBF: sum nuclear assigned MW × period length in hours = MWh
    df_pdbf = con.execute(f"""
    SELECT CAST(date AS DATE) AS d,
           SUM(assigned_power_mw * COALESCE(mtu_minutes, 60) / 60.0) / 1000.0
             AS nuclear_pdbf_gwh
    FROM '{PDBF}'
    JOIN nu USING (unit_code)
    WHERE assigned_power_mw > 0
      AND date BETWEEN '2022-01-01' AND '2026-02-01'
    GROUP BY 1 ORDER BY 1
    """).fetchdf()

    # PHF: same on the IDA final-programme side
    df_phf = con.execute(f"""
    DESCRIBE SELECT * FROM '{PHF}' LIMIT 1
    """).df()
    print("phf columns:")
    print(df_phf.to_string())

    # PHF: take ONLY the latest session per (date, period, unit) to get the
    # final post-IDA programme (avoids triple-counting across the 3 sessions).
    df_phf = con.execute(f"""
    WITH ranked AS (
      SELECT CAST(date AS DATE) AS d, period, unit_code, session_number,
             assigned_power_mw, COALESCE(mtu_minutes, 60) AS mtu,
             ROW_NUMBER() OVER (PARTITION BY date::DATE, period, unit_code
                                ORDER BY session_number DESC) AS rn
      FROM '{PHF}'
      JOIN nu USING (unit_code)
      WHERE assigned_power_mw > 0
        AND date BETWEEN '2022-01-01' AND '2026-02-01'
    )
    SELECT d, SUM(assigned_power_mw * mtu / 60.0) / 1000.0 AS nuclear_phf_gwh
    FROM ranked WHERE rn = 1
    GROUP BY 1 ORDER BY 1
    """).fetchdf()

    panel = df_pdbf.merge(df_phf, on="d", how="outer")
    panel["d"] = pd.to_datetime(panel["d"])

    # Bring in covariates from the Spec A quantity panel
    cov = pd.read_parquet(COVPANEL)[["d", "wind_gwh", "solar_gwh", "gas_eur"]]
    cov["d"] = pd.to_datetime(cov["d"])
    panel = panel.merge(cov, on="d", how="left").sort_values("d")
    OUT.parent.mkdir(parents=True, exist_ok=True)
    panel.to_parquet(OUT, index=False)

    print(f"Wrote {OUT}: {len(panel):,} rows")
    print(f"Date range: {panel['d'].min()} -> {panel['d'].max()}")
    print(panel.describe().round(2).to_string())


if __name__ == "__main__":
    main()
