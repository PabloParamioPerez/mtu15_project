# STATUS: ALIVE
# LAST-AUDIT: 2026-05-18
# FEEDS: bidding_internal.tex §3.3 (band-width sensitivity)
# CLAIM: Re-compute per-cell mw_in_band and the share metric at three half-widths
#        (h = 25, 50, 100 EUR/MWh) and tabulate side-by-side for the DA market.
#        Tests whether the reform-window pattern is robust to band-width choice.

from __future__ import annotations
from pathlib import Path
import duckdb
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
OUT  = REPO / "results" / "regressions" / "bid" / "bid_shape"
TEX  = OUT / "tex"

DET = REPO / "data/processed/omie/mercado_diario/ofertas/det_all.parquet"
CAB = REPO / "data/processed/omie/mercado_diario/ofertas/cab_all.parquet"
MP  = REPO / "data/processed/omie/mercado_diario/precios/marginalpdbc_all.parquet"
UNITS = REPO / "data/external/omie_reference/lista_unidades.csv"

START = "2024-06-14"
END   = "2026-01-31"

REGIMES = {
    "3-sess":              ("2024-06-14", "2024-11-30"),
    "ISP15-win":           ("2024-12-01", "2025-03-18"),
    "DA60/ID15 pre-blk":   ("2025-03-19", "2025-04-27"),
    "DA60/ID15 post-blk":  ("2025-04-28", "2025-09-30"),
    "DA15/ID15":           ("2025-10-01", "2025-12-31"),
}
REGIME_ORDER = list(REGIMES.keys())


def map_tech(s):
    if not isinstance(s, str): return "Other"
    t = s.lower()
    if "ciclo combinado" in t: return "CCGT"
    if "nuclear" in t: return "Nuclear"
    if "bombeo" in t and "puro" in t: return "Hydro_pump"
    if "bombeo" in t and "turb" in t: return "Hydro_pump"
    if "hidráulica generación" in t: return "Hydro"
    if "re mercado eólica" in t: return "Wind"
    if "re mercado solar fotovolt" in t: return "Solar PV"
    return "Other"


def tag_regime(date_series):
    out = pd.Series(index=date_series.index, dtype="object")
    for r, (a, b) in REGIMES.items():
        out.loc[(date_series >= a) & (date_series <= b)] = r
    return out


def hour_class(h):
    if h in (5, 6, 7, 8, 16, 17, 18, 19, 20, 21, 22): return "Critical"
    if h in (1, 2, 3): return "Flat"
    if h in (11, 12, 13, 14): return "Midday"
    return "Dropped"


def run(h):
    print(f"\n=== Computing for h = {h} EUR/MWh ===")
    units = pd.read_csv(UNITS)
    units["tech_group"] = units["technology"].apply(map_tech)
    con = duckdb.connect()
    con.execute("PRAGMA threads=4")
    con.execute("SET memory_limit='6GB'")
    con.register("uft", units[["unit_code", "tech_group"]].drop_duplicates("unit_code"))

    sql = f"""
    WITH cab AS (
        SELECT date::DATE AS d, offer_code, version, unit_code,
               ROW_NUMBER() OVER (PARTITION BY date::DATE, offer_code, unit_code ORDER BY version DESC) AS rn
        FROM '{CAB}' WHERE buy_sell='V' AND date::DATE BETWEEN '{START}' AND '{END}'
    ),
    cab_l AS (SELECT * FROM cab WHERE rn=1),
    det AS (
        SELECT date::DATE AS d, offer_code, version, period, price_eur_mwh AS p, quantity_mw AS q,
               mtu_minutes
        FROM '{DET}'
        WHERE date::DATE BETWEEN '{START}' AND '{END}' AND quantity_mw > 0
    ),
    prices AS (
        SELECT date::DATE AS d, period, price_es_eur_mwh AS p_clear
        FROM '{MP}' WHERE date::DATE BETWEEN '{START}' AND '{END}' AND price_es_eur_mwh IS NOT NULL
    ),
    joined AS (
        SELECT pr.d, pr.period, c.unit_code, dv.q,
               (dv.p BETWEEN pr.p_clear - {h} AND pr.p_clear + {h}) AS in_band,
               dv.mtu_minutes
        FROM det dv JOIN cab_l c USING (d, offer_code, version) JOIN prices pr USING (d, period)
    )
    SELECT d AS date, period, unit_code,
           CAST(mtu_minutes AS INT) AS mtu_minutes,
           SUM(CASE WHEN in_band THEN q ELSE 0 END) AS mw_in_band,
           SUM(q) AS mw_total
    FROM joined GROUP BY 1, 2, 3, 4
    """
    df = con.execute(sql).df()
    df = df.merge(units[["unit_code", "tech_group"]], on="unit_code", how="left")
    df["tech_group"] = df["tech_group"].fillna("Other")
    df["date"] = pd.to_datetime(df["date"])
    df["regime"] = tag_regime(df["date"])
    df = df.dropna(subset=["regime"])
    df["hour"] = df.apply(lambda r: int((r["period"] - 1) / 4) if r["mtu_minutes"] == 15
                                       else int(r["period"] - 1), axis=1)
    df["hour_class"] = df["hour"].apply(hour_class)
    df["share"] = df["mw_in_band"] / df["mw_total"].clip(lower=1e-6)
    df["year_month"] = df["date"].dt.strftime("%Y-%m")

    # Per-tech, per-(regime, hour-class) means (avg across months within regime)
    agg_m = df.groupby(["tech_group", "regime", "hour_class", "year_month"]).agg(
        share=("share", "mean"),
        mw_in_band=("mw_in_band", "mean"),
    ).reset_index()
    agg = agg_m.groupby(["tech_group", "regime", "hour_class"]).agg(
        share=("share", "mean"),
        mw_in_band=("mw_in_band", "mean"),
    ).reset_index()
    agg["share_pct"] = (agg["share"] * 100).round(1)
    agg["mw_in_band"] = agg["mw_in_band"].round(0)
    agg["h"] = h
    return agg


def main():
    out_path = OUT / "h_sensitivity.csv"
    all_agg = []
    for h in [25, 100]:
        all_agg.append(run(h))
    combined = pd.concat(all_agg, ignore_index=True)
    combined.to_csv(out_path, index=False)
    print(f"\nWrote {out_path}")

    # Compact tex table: CCGT only, share_pct, all 3 hour-classes and 5 regimes × h=25,50,100
    # Pull h=50 from existing normalized csv
    df50 = pd.read_csv(OUT / "DA_descriptive_normalized.csv")
    df50["h"] = 50
    df50 = df50[["tech_group", "regime", "hour_class", "share_in_band"]].copy()
    df50["share_pct"] = (df50["share_in_band"] * 100).round(1)
    df50["h"] = 50
    combined_all = pd.concat([combined[["tech_group", "regime", "hour_class", "share_pct", "h"]],
                              df50[["tech_group", "regime", "hour_class", "share_pct", "h"]]],
                              ignore_index=True)
    ccgt = combined_all[(combined_all["tech_group"] == "CCGT") & (combined_all["hour_class"].isin(["Critical","Flat"]))]

    # Build a wide table
    rows = []
    rows.append(r"\begin{tabular}{l l r r r r r}")
    rows.append(r"\toprule")
    rows.append(r"Regime & Hour-class & $h = 25$ & $h = 50$ & $h = 100$ & 25$\to$50 & 50$\to$100 \\")
    rows.append(r"\midrule")
    for r in REGIME_ORDER:
        for hc in ["Critical", "Flat"]:
            row = [r if hc == "Critical" else "", hc]
            piv = ccgt[(ccgt["regime"] == r) & (ccgt["hour_class"] == hc)]
            for h_val in [25, 50, 100]:
                v = piv[piv["h"] == h_val]["share_pct"]
                row.append(f"{v.iloc[0]:.1f}" if len(v) else "---")
            # ratios
            v25 = piv[piv["h"] == 25]["share_pct"].iloc[0] if len(piv[piv["h"] == 25]) else None
            v50 = piv[piv["h"] == 50]["share_pct"].iloc[0] if len(piv[piv["h"] == 50]) else None
            v100 = piv[piv["h"] == 100]["share_pct"].iloc[0] if len(piv[piv["h"] == 100]) else None
            r2550 = f"{v50/v25:.2f}" if v25 and v50 else "---"
            r50100 = f"{v100/v50:.2f}" if v50 and v100 else "---"
            row.append(r2550)
            row.append(r50100)
            rows.append(" & ".join(row) + r" \\")
    rows.append(r"\bottomrule")
    rows.append(r"\end{tabular}")
    out_tex = TEX / "tab_bidshape_DA_h_sensitivity_ccgt.tex"
    out_tex.write_text("% auto-built — h sensitivity, CCGT only, DA, share metric\n" + "\n".join(rows))
    print(f"Wrote {out_tex}")


if __name__ == "__main__":
    main()
