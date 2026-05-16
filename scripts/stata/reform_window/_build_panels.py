# STATUS: ALIVE
# LAST-AUDIT: 2026-05-16
# FEEDS: scripts/stata/reform_window/*.do
# CLAIM: Build DDD panels for ALL (reform window, market) combinations.
#        Windows: DA15 (Oct 2025 reform), IDA15 (Mar 2025 reform).
#        Markets: DA (marginalpdbc), IDA-S1 (marginalpibc Session 1).
#        For each (window, market) combination, write two .dta files:
#          - {window}_{market}_period.dta : one row per (date, period) for
#               OLS-FE DDD on prices in levels.
#          - {window}_{market}_spread.dta : one row per date with the
#               within-day mean price by hour-class (crit, flat). For
#               DR-DiD on prices in levels (drdid called once per hour-
#               class, DDD = ATT_crit - ATT_flat).
# OUTPUT: 4 windows*markets * 2 panel-types = 8 .dta files under
#         results/regressions/firm/reform_window/panels/

from __future__ import annotations

from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
PANELS = REPO / "results" / "regressions" / "firm" / "reform_window" / "panels"
PANELS.mkdir(parents=True, exist_ok=True)

MARGINALPDBC = REPO / "data" / "processed" / "omie" / "mercado_diario" / "precios" / "marginalpdbc_all.parquet"
MARGINALPIBC = REPO / "data" / "processed" / "omie" / "mercado_intradiario_subastas" / "precios" / "marginalpibc_all.parquet"

CRIT_HOURS = (5, 6, 7, 8, 16, 17, 18, 19, 20, 21, 22)
FLAT_HOURS = (1, 2, 3)

WINDOWS: dict[str, dict[str, pd.Timestamp]] = {
    "da15": {
        "y25_pre_s":  pd.Timestamp("2025-04-28"),
        "y25_pre_e":  pd.Timestamp("2025-09-30"),
        "y25_post_s": pd.Timestamp("2025-10-01"),
        "y25_post_e": pd.Timestamp("2026-02-13"),
        "y24_pre_s":  pd.Timestamp("2024-04-28"),
        "y24_pre_e":  pd.Timestamp("2024-09-30"),
        "y24_post_s": pd.Timestamp("2024-10-01"),
        "y24_post_e": pd.Timestamp("2025-02-13"),
    },
    "ida15": {
        "y25_pre_s":  pd.Timestamp("2024-12-09"),
        "y25_pre_e":  pd.Timestamp("2025-03-18"),
        "y25_post_s": pd.Timestamp("2025-03-19"),
        "y25_post_e": pd.Timestamp("2025-04-27"),
        "y24_pre_s":  pd.Timestamp("2023-12-09"),
        "y24_pre_e":  pd.Timestamp("2024-03-18"),
        "y24_post_s": pd.Timestamp("2024-03-19"),
        "y24_post_e": pd.Timestamp("2024-04-27"),
    },
}


def _load_prices(market: str, date_lo: pd.Timestamp, date_hi: pd.Timestamp) -> pd.DataFrame:
    """Load DA or IDA-S1 clearing prices within [date_lo, date_hi]."""
    con = duckdb.connect()
    if market == "da":
        df = con.execute(f"""
            SELECT
                CAST(date AS DATE)            AS date,
                CAST(period AS INTEGER)       AS period,
                CAST(mtu_minutes AS INTEGER)  AS mtu_minutes,
                price_es_eur_mwh              AS price
            FROM '{MARGINALPDBC}'
            WHERE price_es_eur_mwh IS NOT NULL
              AND CAST(date AS DATE) BETWEEN DATE '{date_lo.date()}' AND DATE '{date_hi.date()}'
        """).df()
    elif market == "ida":
        df = con.execute(f"""
            SELECT
                CAST(date AS DATE)            AS date,
                CAST(session_number AS INTEGER) AS session_number,
                CAST(period AS INTEGER)       AS period,
                CAST(mtu_minutes AS INTEGER)  AS mtu_minutes,
                price_es_eur_mwh              AS price
            FROM '{MARGINALPIBC}'
            WHERE price_es_eur_mwh IS NOT NULL
              AND session_number = 1
              AND CAST(date AS DATE) BETWEEN DATE '{date_lo.date()}' AND DATE '{date_hi.date()}'
        """).df()
    else:
        raise ValueError(market)
    return df


def _add_clockhour(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])
    df["clockhour"] = np.where(
        df["mtu_minutes"] == 60,
        df["period"].astype(int),
        ((df["period"].astype(int) - 1) // 4) + 1,
    )
    return df


def _filter_critflat(df: pd.DataFrame) -> pd.DataFrame:
    keep_crit = df["clockhour"].isin(CRIT_HOURS)
    keep_flat = df["clockhour"].isin(FLAT_HOURS)
    df = df.loc[keep_crit | keep_flat].copy()
    df["crit"] = keep_crit.loc[df.index].astype(int)
    return df


def _add_treatment_indicators(df: pd.DataFrame, w: dict) -> pd.DataFrame:
    df = df.copy()
    y25_pre  = (df["date"] >= w["y25_pre_s"])  & (df["date"] <= w["y25_pre_e"])
    y25_post = (df["date"] >= w["y25_post_s"]) & (df["date"] <= w["y25_post_e"])
    y24_pre  = (df["date"] >= w["y24_pre_s"])  & (df["date"] <= w["y24_pre_e"])
    y24_post = (df["date"] >= w["y24_post_s"]) & (df["date"] <= w["y24_post_e"])
    in_sample = y25_pre | y25_post | y24_pre | y24_post
    df = df.loc[in_sample].copy()
    df["y25"]  = (y25_pre | y25_post).loc[df.index].astype(int)
    df["post"] = (y25_post | y24_post).loc[df.index].astype(int)
    df["crit_post"]     = df["crit"] * df["post"]
    df["crit_y25"]      = df["crit"] * df["y25"]
    df["post_y25"]      = df["post"] * df["y25"]
    df["crit_post_y25"] = df["crit"] * df["post"] * df["y25"]
    df["dow"]   = df["date"].dt.dayofweek.astype(int)
    df["month"] = df["date"].dt.month.astype(int)
    df["year"]  = df["date"].dt.year.astype(int)
    df["date_stata"] = (df["date"] - pd.Timestamp("1960-01-01")).dt.days.astype(int)
    return df


def _build_period_panel(prices: pd.DataFrame, w: dict, label: str) -> pd.DataFrame:
    df = _add_clockhour(prices)
    df = _filter_critflat(df)
    df = _add_treatment_indicators(df, w)
    cols = ["date", "date_stata", "period", "mtu_minutes", "clockhour", "dow", "month", "year",
            "price", "crit", "post", "y25",
            "crit_post", "crit_y25", "post_y25", "crit_post_y25"]
    df = df[cols].reset_index(drop=True)
    print(f"[{label}-period] N={len(df):,d} (dates {df['date'].min().date()}..{df['date'].max().date()})")
    return df


def _build_spread_panel(period_panel: pd.DataFrame, label: str) -> pd.DataFrame:
    grp = period_panel.groupby(
        ["date", "date_stata", "y25", "post", "dow", "month", "year"], as_index=False)
    crit_mean = grp.apply(lambda g: pd.Series({"price_crit": g.loc[g["crit"] == 1, "price"].mean()}))
    flat_mean = grp.apply(lambda g: pd.Series({"price_flat": g.loc[g["crit"] == 0, "price"].mean()}))
    out = crit_mean.merge(flat_mean, on=["date", "date_stata", "y25", "post", "dow", "month", "year"])
    out["spread"] = out["price_crit"] - out["price_flat"]
    out = out.dropna(subset=["price_crit", "price_flat"]).reset_index(drop=True)
    out["post_y25"] = out["post"] * out["y25"]
    print(f"[{label}-spread] N={len(out):,d} dates")
    return out


def main():
    # Determine date range needed per window so the DuckDB queries are cheap.
    for window_name, w in WINDOWS.items():
        date_lo = min(w["y24_pre_s"], w["y25_pre_s"])
        date_hi = max(w["y24_post_e"], w["y25_post_e"])
        for market in ("da", "ida"):
            print(f"\n=== window={window_name}  market={market} ===")
            prices = _load_prices(market, date_lo, date_hi)
            print(f"  loaded {len(prices):,d} raw price rows ({market})")
            label = f"{window_name}_{market}"
            period_panel = _build_period_panel(prices, w, label)
            spread_panel = _build_spread_panel(period_panel, label)
            period_panel.to_stata(PANELS / f"{label}_period.dta", write_index=False, version=118)
            spread_panel.to_stata(PANELS / f"{label}_spread.dta", write_index=False, version=118)

    print(f"\nAll 8 panels written to {PANELS}")


if __name__ == "__main__":
    main()
