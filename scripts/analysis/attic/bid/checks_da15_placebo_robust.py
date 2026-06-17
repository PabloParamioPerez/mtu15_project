# STATUS: ALIVE
# LAST-AUDIT: 2026-06-06
# FEEDS: thesis/paper/thesis.tex / presentation slides --- DA15 same-calendar
#        placebo on the ROBUST outcomes: sigma_p (lead) and per-curve
#        in-band MW-weighted mean price (alpha proxy), per-session for
#        the IDA side.
#
# RATIONALE (user 2026-06-06): "REMEMBER THAT SIGMA_P IS MORE ROBUST THAT
#        THE SLOPE METRICS, SINCE THE SLOPE METRICS SOMETIMES COME FROM
#        REGRESSIONS ON NOT TOO MANY TRANCHES!" -- so we lead with sigma_p
#        same-cal and add alpha (level), skip beta/gamma headline same-cal.
#
# WINDOWS: same-calendar DA15 = Oct-Dec 2024 vs Oct-Dec 2025.
# OUT: results/regressions/bid/mtu15_critical_flat/checks_da15_placebo.csv
#      Console summary.

from pathlib import Path
import duckdb
import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
CAB    = REPO / "data/processed/omie/mercado_diario/ofertas/cab_all.parquet"
DET    = REPO / "data/processed/omie/mercado_diario/ofertas/det_all.parquet"
ICAB   = REPO / "data/processed/omie/mercado_intradiario_subastas/ofertas/icab_all.parquet"
IDET   = REPO / "data/processed/omie/mercado_intradiario_subastas/ofertas/idet_all.parquet"
MCPDA  = REPO / "data/processed/omie/mercado_diario/precios/marginalpdbc_all.parquet"
MCPIDA = REPO / "data/processed/omie/mercado_intradiario_subastas/precios/marginalpibc_all.parquet"
UMAP   = REPO / "data/derived/panels/bid_shape_critical_flat/_unit_map.parquet"
OUT    = REPO / "results/regressions/bid/mtu15_critical_flat/checks_da15_placebo.csv"

H_BAND = 150.0
CRIT = set(range(5, 9)) | set(range(16, 23))
FLAT = {1, 2, 3}

PRE_LO,  PRE_HI  = "2024-10-01", "2024-12-31"
POST_LO, POST_HI = "2025-10-01", "2025-12-31"


def fetch_sums_da(con):
    q = f"""
    WITH u AS (SELECT unit_code FROM '{UMAP}' WHERE tech_group='CCGT'),
         mcp AS (
           SELECT CAST(date AS DATE) AS d, period, price_es_eur_mwh AS mcp
           FROM '{MCPDA}'
           WHERE date BETWEEN '{PRE_LO}' AND '{POST_HI}'
             AND price_es_eur_mwh IS NOT NULL
         ),
         banded AS (
           SELECT CAST(c.date AS DATE) AS d, c.offer_code, c.unit_code,
                  dt.period, dt.price_eur_mwh AS p, dt.quantity_mw AS q,
                  CASE WHEN dt.period<=24 THEN dt.period
                       ELSE CAST(CEIL(dt.period/4.0) AS INT) END AS hour
           FROM '{CAB}' c JOIN '{DET}' dt
             ON c.date=dt.date AND c.offer_code=dt.offer_code AND c.version=dt.version
           JOIN u ON c.unit_code=u.unit_code
           JOIN mcp m ON CAST(c.date AS DATE)=m.d AND dt.period=m.period
           WHERE c.buy_sell='V' AND dt.price_eur_mwh IS NOT NULL AND dt.quantity_mw>0
             AND ABS(dt.price_eur_mwh-m.mcp)<={H_BAND}
             AND c.date BETWEEN '{PRE_LO}' AND '{POST_HI}'
         )
    SELECT d, unit_code, period, hour, COUNT(*) n,
           SUM(q) sw, SUM(q*q) sw2,
           SUM(q*p) swp, SUM(q*p*p) swpp
    FROM banded GROUP BY 1,2,3,4
    """
    return con.execute(q).df()


def fetch_sums_ida(con, sess):
    q = f"""
    WITH u AS (SELECT unit_code FROM '{UMAP}' WHERE tech_group='CCGT'),
         mcp_raw AS (
           SELECT CAST(date AS DATE) AS d, session_number, period,
                  price_es_eur_mwh AS mcp,
                  ROW_NUMBER() OVER (PARTITION BY date::DATE, session_number, period
                                      ORDER BY mtu_minutes ASC) AS rn
           FROM '{MCPIDA}'
           WHERE session_number={sess} AND price_es_eur_mwh IS NOT NULL
             AND date BETWEEN '{PRE_LO}' AND '{POST_HI}'
         ),
         mcp AS (SELECT d, session_number, period, mcp FROM mcp_raw WHERE rn=1),
         banded AS (
           SELECT CAST(c.date AS DATE) AS d, c.offer_code, c.unit_code,
                  dt.period, dt.price_eur_mwh AS p, dt.quantity_mw AS q,
                  CASE WHEN dt.period<=24 THEN dt.period
                       ELSE CAST(CEIL(dt.period/4.0) AS INT) END AS hour
           FROM '{ICAB}' c JOIN '{IDET}' dt
             ON c.date=dt.date AND c.session_number=dt.session_number
              AND c.offer_code=dt.offer_code AND c.version=dt.version
           JOIN u ON c.unit_code=u.unit_code
           JOIN mcp m ON CAST(c.date AS DATE)=m.d AND c.session_number=m.session_number
                     AND dt.period=m.period
           WHERE c.buy_sell='V' AND c.session_number={sess}
             AND c.block_order_avg_price_eur IS NULL
             AND dt.price_eur_mwh IS NOT NULL AND dt.quantity_mw>0
             AND ABS(dt.price_eur_mwh-m.mcp)<={H_BAND}
             AND c.date BETWEEN '{PRE_LO}' AND '{POST_HI}'
         )
    SELECT d, unit_code, period, hour, COUNT(*) n,
           SUM(q) sw, SUM(q*q) sw2,
           SUM(q*p) swp, SUM(q*p*p) swpp
    FROM banded GROUP BY 1,2,3,4
    """
    return con.execute(q).df()


def did_critical_flat(df, outcome):
    df = df.copy()
    df["d"] = pd.to_datetime(df["d"])
    df["hour_class"] = df["hour"].apply(
        lambda h: "critical" if h in CRIT else ("flat" if h in FLAT else None))
    df = df.dropna(subset=["hour_class"])
    df["regime"] = np.where(df["d"] < pd.to_datetime(POST_LO), "pre", "post")
    if len(df) == 0:
        return {"crit_pre": np.nan, "crit_post": np.nan,
                "flat_pre": np.nan, "flat_post": np.nan,
                "did": np.nan, "n": 0}
    g = (df.groupby(["regime", "hour_class"], observed=True)[outcome]
            .median().reset_index())
    piv = g.set_index(["regime", "hour_class"])[outcome]
    try:
        cp_pre, cp_post = piv.loc[("pre", "critical")], piv.loc[("post", "critical")]
        fl_pre, fl_post = piv.loc[("pre", "flat")],     piv.loc[("post", "flat")]
        did = (cp_post - cp_pre) - (fl_post - fl_pre)
    except KeyError:
        cp_pre = cp_post = fl_pre = fl_post = did = np.nan
    return {"crit_pre": cp_pre, "crit_post": cp_post,
            "flat_pre": fl_pre, "flat_post": fl_post,
            "did": did, "n": int(len(df))}


def add_outcomes(df):
    df = df.copy()
    df["alpha"]   = df["swp"] / df["sw"]                                # MW-weighted mean
    var_p = (df["swpp"] - df["swp"]**2/df["sw"]) / df["sw"]
    df["sigma_p"] = np.sqrt(np.clip(var_p, 0, None))
    df["n_eff"]   = df["sw"]**2 / df["sw2"]
    return df


def main():
    con = duckdb.connect(); con.execute("SET threads=4; SET memory_limit='6GB'")
    rows = []

    print("[DA CCGT pooled]", flush=True)
    sums = fetch_sums_da(con)
    print(f"  curves: {len(sums):,}", flush=True)
    out = add_outcomes(sums)
    for outcome in ("sigma_p", "alpha", "n_eff"):
        r = did_critical_flat(out, outcome)
        rows.append({"cell": "DA15 DA CCGT pooled", "outcome": outcome, **r})
        print(f"  {outcome:>8s}: crit {r['crit_pre']:7.3f} -> {r['crit_post']:7.3f}  "
              f"flat {r['flat_pre']:7.3f} -> {r['flat_post']:7.3f}  "
              f"DiD = {r['did']:+7.3f}  n={r['n']:,}")

    for sess in (1, 2, 3):
        print(f"\n[IDA{sess} CCGT]", flush=True)
        sums = fetch_sums_ida(con, sess)
        print(f"  curves: {len(sums):,}", flush=True)
        out = add_outcomes(sums)
        for outcome in ("sigma_p", "alpha", "n_eff"):
            r = did_critical_flat(out, outcome)
            rows.append({"cell": f"DA15 IDA{sess} CCGT", "outcome": outcome, **r})
            print(f"  {outcome:>8s}: crit {r['crit_pre']:7.3f} -> {r['crit_post']:7.3f}  "
                  f"flat {r['flat_pre']:7.3f} -> {r['flat_post']:7.3f}  "
                  f"DiD = {r['did']:+7.3f}  n={r['n']:,}")
    con.close()

    out_df = pd.DataFrame(rows)
    OUT.parent.mkdir(parents=True, exist_ok=True)
    out_df.to_csv(OUT, index=False)
    print(f"\nWrote {OUT}")

    print("\n=== DA15 same-cal placebo summary (Oct-Dec 24 vs Oct-Dec 25) ===")
    pivot = out_df.pivot(index="cell", columns="outcome", values="did")
    print(pivot.round(3).to_string())


if __name__ == "__main__":
    main()
