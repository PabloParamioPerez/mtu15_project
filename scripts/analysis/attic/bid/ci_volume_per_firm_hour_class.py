# STATUS: ALIVE
# LAST-AUDIT: 2026-05-19
# CLAIM: Per-(firm, tech, hour-class, regime) continuous-intraday SELL volume
#        (GWh/day) and per-(firm, tech, hour-class) within-hour quarter std
#        (MWh), Fourier+DOW deseasonalised where data supports it. Sibling of
#        ci_volume_per_quarter_deseasonalized.py with FIRM x TECH x HOUR-CLASS
#        grouping added.
#
# OUT: results/regressions/bid/ci_volume_per_firm_hour_class/
#        per_firm_tech_hc_volume.csv  (per (firm, tech, hour_class, regime))
#        tab_ci_volume_per_firm_tech_hc.tex

from __future__ import annotations
from pathlib import Path
import sys

import duckdb
import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
if str(REPO / "src") not in sys.path:
    sys.path.insert(0, str(REPO / "src"))
from mtu.analysis.sa_fwl import fit_sa, attach_design_columns  # noqa: E402

TRADES = REPO / "data/processed/omie/mercado_intradiario_continuo/transacciones/trades_all.parquet"
UNITS = REPO / "data/external/omie_reference/lista_unidades.csv"
OUT_DIR = REPO / "results/regressions/bid/ci_volume_per_firm_hour_class"

START = "2022-01-01"
END = "2026-05-15"
K_HARMONICS = 4

REGIME_DATES = [
    ("3sess",         pd.Timestamp("2024-06-14"), pd.Timestamp("2024-11-30")),
    ("ISP15win",      pd.Timestamp("2024-12-01"), pd.Timestamp("2025-03-18")),
    ("MTU15IDA_pre",  pd.Timestamp("2025-03-19"), pd.Timestamp("2025-04-27")),
    ("MTU15IDA_post", pd.Timestamp("2025-04-28"), pd.Timestamp("2025-09-30")),
    ("DA15_ID15",     pd.Timestamp("2025-10-01"), pd.Timestamp("2026-05-15")),
]
HOUR_CLASS = {
    "Critical": [5, 6, 7, 8, 16, 17, 18, 19, 20, 21, 22],
    "Flat":     [1, 2, 3],
    "Midday":   [11, 12, 13, 14],
}
TECHS_PRICE_SETTING = ["CCGT", "Hydro", "Hydro_pump"]
FIRMS_ORDER = ["IB", "GE", "GN", "HC", "REP"]


def tech_bucket(t):
    if t is None: return "Other"
    t = str(t).lower()
    if "ciclo combinado" in t: return "CCGT"
    if "nuclear" in t: return "Nuclear"
    if "hidráulica generación" in t: return "Hydro"
    if "bombeo" in t: return "Hydro_pump"
    if "re mercado eólica" in t: return "Wind"
    if "re mercado solar fotovolt" in t: return "Solar_PV"
    return "Other"


def firm_bucket(o):
    if not isinstance(o, str): return "OTH"
    o = o.lower()
    if "iberdrola" in o: return "IB"
    if "endesa" in o: return "GE"
    if "naturgy" in o or "gas natural" in o: return "GN"
    if "edp" in o or "hidroel" in o or "hidrocan" in o: return "HC"
    if "repsol" in o: return "REP"
    return "OTH"


def hour_class_of(h):
    for hc, hs in HOUR_CLASS.items():
        if int(h) in hs:
            return hc
    return None


def build_panel():
    con = duckdb.connect()
    con.execute("SET memory_limit='12GB'")
    units = pd.read_csv(UNITS)
    units["tech"] = units["technology"].apply(tech_bucket)
    units["firm"] = units["owner_agent"].apply(firm_bucket)
    con.register("u", units[["unit_code", "tech", "firm"]])

    sql = f"""
    WITH t AS (
      SELECT CAST(delivery_date AS DATE) AS d,
             CASE WHEN mtu_minutes = 60 THEN period - 1
                  ELSE CAST(FLOOR((period - 1) / 4.0) AS INT) END AS clock_hour,
             period, mtu_minutes,
             seller_unit, quantity_mw * mtu_minutes / 60.0 AS mwh
      FROM read_parquet('{TRADES}')
      WHERE delivery_date >= '{START}' AND delivery_date <= '{END}'
    )
    SELECT t.d, t.clock_hour, u.firm, u.tech, SUM(t.mwh) AS mwh
    FROM t JOIN u ON t.seller_unit = u.unit_code
    GROUP BY 1, 2, 3, 4
    """
    df = con.execute(sql).fetchdf()
    df["d"] = pd.to_datetime(df["d"])
    df["hour_class"] = df["clock_hour"].apply(hour_class_of)
    df = df[df["hour_class"].notna()].copy()
    daily = (df.groupby(["d", "firm", "tech", "hour_class"])["mwh"].sum().reset_index())
    daily["gwh"] = daily["mwh"] / 1000
    return daily


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    print("Building per-(firm, tech, hour-class) daily CI panel...")
    daily = build_panel()
    print(f"  {len(daily):,} (firm, tech, hour_class, day) rows")

    # Raw means per regime
    daily["regime"] = "preIDA"
    for label, lo, hi in REGIME_DATES:
        m = (daily["d"] >= lo) & (daily["d"] <= hi)
        daily.loc[m, "regime"] = label

    raw_means = (daily.groupby(["firm", "tech", "hour_class", "regime"])["gwh"].mean()
                       .reset_index())

    # SA per (firm, tech, hour_class)
    print("Running SA per (firm, tech, hour_class)...")
    sa_rows = []
    for firm in FIRMS_ORDER:
        for tech in TECHS_PRICE_SETTING:
            for hc in HOUR_CLASS:
                sub = daily[(daily["firm"] == firm)
                            & (daily["tech"] == tech)
                            & (daily["hour_class"] == hc)].copy()
                if len(sub) < 200:
                    continue
                sub_full = attach_design_columns(sub, [r[:3] for r in REGIME_DATES], K=K_HARMONICS)
                res = fit_sa(sub_full, "gwh", [r[:3] for r in REGIME_DATES],
                             transform="log", K=K_HARMONICS, min_obs=200)
                if res is None:
                    continue
                for label, _, _ in REGIME_DATES:
                    sa_rows.append({"firm": firm, "tech": tech, "hour_class": hc,
                                    "regime": label, "gwh_sa": res[f"{label}_sa"]})
    sa = pd.DataFrame(sa_rows)
    merged = raw_means.merge(sa, on=["firm", "tech", "hour_class", "regime"], how="left")
    merged.to_csv(OUT_DIR / "per_firm_tech_hc_volume.csv", index=False)
    print(f"  wrote {OUT_DIR}/per_firm_tech_hc_volume.csv")

    # === Table: rows = (tech, firm, hour_class), columns = regimes ===
    pivot_raw = raw_means.pivot_table(index=["tech", "firm", "hour_class"],
                                      columns="regime", values="gwh")
    pivot_sa = sa.pivot_table(index=["tech", "firm", "hour_class"],
                               columns="regime", values="gwh_sa")

    regime_labels = [
        ("preIDA",       "pre-IDA"),
        ("3sess",        "3-sess"),
        ("ISP15win",     "ISP15-win"),
        ("MTU15IDA_pre", "DA60/ID15 pre-blk"),
        ("MTU15IDA_post","DA60/ID15 post-blk"),
        ("DA15_ID15",    "DA15/ID15"),
    ]
    rows = [r"\begin{tabular}{l l l " + "r " * len(regime_labels) + r"}",
            r"\toprule",
            "Tech & Firm & Hour-cl & " + " & ".join(lbl for _, lbl in regime_labels) + r" \\",
            r"\midrule"]
    last_tech, last_firm = None, None
    for tech in TECHS_PRICE_SETTING:
        for firm in FIRMS_ORDER:
            for hc in ["Critical", "Flat", "Midday"]:
                key = (tech, firm, hc)
                if key not in pivot_raw.index:
                    continue
                tech_lbl = tech.replace("_", " ") if tech != last_tech else ""
                firm_lbl = firm if (tech != last_tech or firm != last_firm) else ""
                if tech != last_tech and last_tech is not None:
                    rows.append(r"\addlinespace")
                last_tech, last_firm = tech, firm
                cells = [tech_lbl, firm_lbl, hc]
                for r_lab, _ in regime_labels:
                    raw_v = pivot_raw.loc[key].get(r_lab, np.nan) if key in pivot_raw.index else np.nan
                    sa_v = pivot_sa.loc[key].get(r_lab, np.nan) if key in pivot_sa.index else np.nan
                    if pd.isna(raw_v) and pd.isna(sa_v):
                        cells.append("--")
                    elif pd.isna(sa_v):
                        cells.append(f"{raw_v:.2f}")
                    else:
                        cells.append(f"{raw_v:.2f}~{{\\color{{seasoncol}}\\scriptsize[{sa_v:.2f}]}}")
                rows.append(" & ".join(cells) + r" \\")
    rows.extend([r"\bottomrule", r"\end{tabular}"])
    (OUT_DIR / "tab_ci_volume_per_firm_tech_hc.tex").write_text(
        "% Per-(firm, tech, hour-class) CI sell volume GWh/day; raw + SA per regime.\n"
        + "\n".join(rows))
    print(f"  wrote {OUT_DIR}/tab_ci_volume_per_firm_tech_hc.tex")


if __name__ == "__main__":
    main()
