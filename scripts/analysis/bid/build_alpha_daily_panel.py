# STATUS: ALIVE
# LAST-AUDIT: 2026-06-05
# FEEDS: BSTS on the bid-curve intercept alpha as a level outcome.
#        Aggregates per-curve alpha to daily mean per (tech, market) and
#        merges with the existing Spec-A covariates (wind, solar, gas).
#
# OUT: data/derived/panels/alpha_daily_panel.parquet
#      columns: d, tech, market, alpha_mean, n_curves,
#               wind_gwh, solar_gwh, gas_eur

from pathlib import Path
import duckdb
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
DET   = REPO / "data/processed/omie/mercado_diario/ofertas/det_all.parquet"
CAB   = REPO / "data/processed/omie/mercado_diario/ofertas/cab_all.parquet"
MPDBC = REPO / "data/processed/omie/mercado_diario/precios/marginalpdbc_all.parquet"
IDET  = REPO / "data/processed/omie/mercado_intradiario_subastas/ofertas/idet_all.parquet"
ICAB  = REPO / "data/processed/omie/mercado_intradiario_subastas/ofertas/icab_all.parquet"
MPIBC = REPO / "data/processed/omie/mercado_intradiario_subastas/precios/marginalpibc_all.parquet"
UNITS = REPO / "data/external/omie_reference/lista_unidades.csv"
COVPANEL = REPO / "data/derived/panels/bsts_quantities_panel.parquet"
OUT  = REPO / "data/derived/panels/alpha_daily_panel.parquet"

# Window-and-market specific bandwidth (window-pooled p90-p50 of MCP)
BANDS = {
    ("ID15", "da"):  50, ("ID15", "ida"): 62,
    ("DA15", "da"):  50, ("DA15", "ida"): 58,
}
# Tech buckets we care about
TECH_KEEP = {"CCGT", "Hydro", "Hydro_pump"}


def tech_bucket(t):
    if t is None: return None
    s = str(t).lower()
    if "ciclo combinado" in s:        return "CCGT"
    if "hidráulica generación" in s: return "Hydro"
    if "bombeo" in s:                 return "Hydro_pump"
    return None


def _fetch_alpha_per_curve_da(con, lo, hi, h):
    """Recover alpha from in-band tranches using closed-form OLS at the SQL
    level. We need: for each (date, period, unit), the OLS intercept of
    p_k = alpha + beta*Q_k, fit on the K in-band tranches sorted ASC by
    price with Q_k = cumulative sum.

    Computing cumulative sums per curve at SQL is straightforward with a
    window function; OLS alpha is then mean(p) - beta*mean(Q), with
    beta = SUM((p-pbar)*(Q-Qbar)) / SUM((Q-Qbar)^2). We materialise the
    per-curve sufficient statistics and compute alpha in pandas."""
    q = f"""
    WITH cab_l AS (
      SELECT d, offer_code, unit_code FROM (
        SELECT CAST(date AS DATE) d, offer_code, unit_code, version,
               ROW_NUMBER() OVER (PARTITION BY CAST(date AS DATE), offer_code, unit_code
                                  ORDER BY version DESC) rn
        FROM '{CAB}' WHERE date BETWEEN '{lo}' AND '{hi}' AND buy_sell='V'
      ) WHERE rn=1
    ),
    det AS (
      SELECT CAST(date AS DATE) d, offer_code, period,
             price_eur_mwh p, quantity_mw q
      FROM '{DET}' WHERE date BETWEEN '{lo}' AND '{hi}' AND quantity_mw>0
    ),
    mp AS (
      SELECT CAST(date AS DATE) d, period, price_es_eur_mwh p_clear
      FROM '{MPDBC}' WHERE date BETWEEN '{lo}' AND '{hi}' AND price_es_eur_mwh IS NOT NULL
    ),
    inband AS (
      SELECT mp.d, mp.period, c.unit_code, dv.p, dv.q,
             ROW_NUMBER() OVER (PARTITION BY mp.d, mp.period, c.unit_code
                                ORDER BY dv.p) rnk
      FROM det dv
        JOIN cab_l c ON dv.d=c.d AND dv.offer_code=c.offer_code
        JOIN mp ON mp.d=dv.d AND mp.period=dv.period
      WHERE dv.p BETWEEN mp.p_clear - {h} AND mp.p_clear + {h}
    ),
    cum AS (
      SELECT d, period, unit_code, p, q,
             SUM(q) OVER (PARTITION BY d, period, unit_code ORDER BY rnk
                          ROWS UNBOUNDED PRECEDING) AS Q
      FROM inband
    )
    SELECT d, period, unit_code,
           COUNT(*) AS K,
           AVG(p) AS pbar, AVG(Q) AS Qbar,
           SUM(p*Q) - COUNT(*)*AVG(p)*AVG(Q) AS num,
           SUM(Q*Q) - COUNT(*)*AVG(Q)*AVG(Q) AS den
    FROM cum
    GROUP BY 1,2,3 HAVING COUNT(*) >= 2
    """
    return con.execute(q).fetchdf()


def _fetch_alpha_per_curve_ida(con, lo, hi, h):
    q = f"""
    WITH icab_l AS (
      SELECT d, session_number, offer_code, version, unit_code FROM (
        SELECT CAST(date AS DATE) d, session_number, offer_code, version, unit_code,
               ROW_NUMBER() OVER (PARTITION BY CAST(date AS DATE), session_number,
                                                offer_code, unit_code
                                  ORDER BY version DESC) rn
        FROM '{ICAB}' WHERE date BETWEEN '{lo}' AND '{hi}' AND buy_sell='V'
      ) WHERE rn=1
    ),
    idet AS (
      SELECT CAST(date AS DATE) d, session_number, offer_code, version, unit_code,
             period, price_eur_mwh p, quantity_mw q
      FROM '{IDET}' WHERE date BETWEEN '{lo}' AND '{hi}' AND quantity_mw>0
    ),
    mp AS (
      SELECT CAST(date AS DATE) d, session_number, period, price_es_eur_mwh p_clear
      FROM '{MPIBC}' WHERE date BETWEEN '{lo}' AND '{hi}' AND price_es_eur_mwh IS NOT NULL
    ),
    inband AS (
      SELECT mp.d, mp.session_number, mp.period, c.unit_code, dv.p, dv.q,
             ROW_NUMBER() OVER (PARTITION BY mp.d, mp.session_number, mp.period, c.unit_code
                                ORDER BY dv.p) rnk
      FROM idet dv
        JOIN icab_l c
          ON dv.d=c.d AND dv.session_number=c.session_number
         AND dv.offer_code=c.offer_code AND dv.version=c.version
         AND dv.unit_code=c.unit_code
        JOIN mp ON mp.d=dv.d AND mp.session_number=dv.session_number
                AND mp.period=dv.period
    ),
    cum AS (
      SELECT d, session_number, period, unit_code, p, q,
             SUM(q) OVER (PARTITION BY d, session_number, period, unit_code
                          ORDER BY rnk ROWS UNBOUNDED PRECEDING) AS Q
      FROM inband
    )
    SELECT d, session_number, period, unit_code,
           COUNT(*) AS K,
           AVG(p) AS pbar, AVG(Q) AS Qbar,
           SUM(p*Q) - COUNT(*)*AVG(p)*AVG(Q) AS num,
           SUM(Q*Q) - COUNT(*)*AVG(Q)*AVG(Q) AS den
    FROM cum
    GROUP BY 1,2,3,4 HAVING COUNT(*) >= 2
    """
    return con.execute(q).fetchdf()


def _month_chunks(lo, hi):
    lo_d = pd.to_datetime(lo); hi_d = pd.to_datetime(hi)
    chunks = []
    cur = pd.Timestamp(lo_d.year, lo_d.month, 1)
    while cur <= hi_d:
        nxt = (cur + pd.offsets.MonthBegin(1)) - pd.Timedelta(days=1)
        a = max(cur, lo_d); b = min(nxt, hi_d)
        chunks.append((a.strftime("%Y-%m-%d"), b.strftime("%Y-%m-%d")))
        cur = cur + pd.offsets.MonthBegin(1)
    return chunks


def collect_one(window_key, lo, hi, market, h, units_df):
    fetch = _fetch_alpha_per_curve_da if market == "da" else _fetch_alpha_per_curve_ida
    parts = []
    for a, b in _month_chunks(lo, hi):
        print(f"  collecting {window_key} {market} h={h} ({a} -> {b}) ...", flush=True)
        con = duckdb.connect()
        con.execute("SET memory_limit='6GB'"); con.execute("SET threads=2")
        con.execute("SET preserve_insertion_order=false")
        try:
            df = fetch(con, a, b, h)
        except Exception as e:
            print(f"    ERROR {e}; skipping chunk", flush=True)
            con.close(); continue
        con.close()
        df["beta"] = df["num"] / df["den"].where(df["den"] > 0, other=pd.NA)
        df["alpha"] = df["pbar"] - df["beta"] * df["Qbar"]
        df["d"] = pd.to_datetime(df["d"])
        df = df.merge(units_df[["unit_code", "tech"]], on="unit_code", how="inner")
        df = df[df["tech"].isin(TECH_KEEP)]
        g = (df.dropna(subset=["alpha"])
                .groupby(["d", "tech"])
                .agg(alpha_mean=("alpha", "mean"),
                     n_curves=("alpha", "size"))
                .reset_index())
        parts.append(g)
        del df
    out = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()
    out["market"] = market
    return out


def main():
    import sys
    units = pd.read_csv(UNITS)
    units["tech"] = units["technology"].apply(tech_bucket)
    units = units.dropna(subset=["tech"])[["unit_code", "tech"]].drop_duplicates("unit_code")
    print(f"units kept: {len(units):,}")

    H_BSTS = 60
    markets = sys.argv[1:] if len(sys.argv) > 1 else ["da", "ida"]
    OUT.parent.mkdir(parents=True, exist_ok=True)
    cov = pd.read_parquet(COVPANEL)[["d", "wind_gwh", "solar_gwh", "gas_eur"]]
    cov["d"] = pd.to_datetime(cov["d"])
    parts = []
    for m in markets:
        df = collect_one(f"FULL_{m.upper()}", "2022-01-01", "2026-04-30",
                          m, H_BSTS, units)
        side_out = OUT.parent / f"alpha_daily_panel_{m}.parquet"
        df_m = df.merge(cov, on="d", how="left").sort_values(["tech", "d"])
        df_m.to_parquet(side_out, index=False)
        print(f"  wrote {side_out}: {len(df_m):,} rows", flush=True)
        parts.append(df_m)
    full = pd.concat(parts, ignore_index=True).sort_values(["tech", "market", "d"])
    full.to_parquet(OUT, index=False)
    print(f"\nWrote {OUT}: {len(full):,} rows")
    print(full.groupby(["tech", "market"]).agg(
        n=("alpha_mean", "size"),
        alpha_min=("alpha_mean", "min"),
        alpha_max=("alpha_mean", "max"),
    ).round(2))


if __name__ == "__main__":
    main()
