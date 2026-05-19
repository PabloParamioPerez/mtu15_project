# STATUS: ALIVE
# LAST-AUDIT: 2026-05-19
# CLAIM: Per-(firm, tech, hour-class, regime) post-DA gap (PHF_last - PDBC) in
#        GWh/day for the price-setting techs. Sibling of
#        ree_pdbf_to_phf_alltech.py with FIRM x HOUR-CLASS grouping.
#        Decomposes the per-firm aggregate post-DA gap into critical/flat/midday
#        components, revealing when each firm's CCGT/Hydro inclusion happens.
#
# OUT: results/regressions/regulatory/pdbf_to_phf_per_firm_hour_class/
#        per_firm_tech_hc_gap.csv
#        tab_post_da_gap_per_firm_tech_hc.tex

from __future__ import annotations
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[3]

PDBC = REPO / "data/processed/omie/mercado_diario/programas/pdbc_all.parquet"
PHF  = REPO / "data/processed/omie/mercado_intradiario_subastas/programas/phf_all.parquet"
UNITS_CSV = REPO / "data/external/omie_reference/lista_unidades.csv"
OUT_DIR = REPO / "results/regressions/regulatory/pdbf_to_phf_per_firm_hour_class"
OUT_DIR.mkdir(parents=True, exist_ok=True)

START = "2024-06-14"
END = "2026-02-28"

REGIME_DATES = [
    ("3sess",         pd.Timestamp("2024-06-14"), pd.Timestamp("2024-11-30")),
    ("ISP15win",      pd.Timestamp("2024-12-01"), pd.Timestamp("2025-03-18")),
    ("MTU15IDA_pre",  pd.Timestamp("2025-03-19"), pd.Timestamp("2025-04-27")),
    ("MTU15IDA_post", pd.Timestamp("2025-04-28"), pd.Timestamp("2025-09-30")),
    ("DA15_ID15",     pd.Timestamp("2025-10-01"), pd.Timestamp("2026-02-28")),
]
HOUR_CLASS = {
    "Critical": [5, 6, 7, 8, 16, 17, 18, 19, 20, 21, 22],
    "Flat":     [1, 2, 3],
    "Midday":   [11, 12, 13, 14],
}
TECHS = ["CCGT", "Hydro", "Hydro_pump"]
FIRMS = ["IB", "GE", "GN", "HC", "REP"]


def tech_bucket(t):
    if t is None: return "Other"
    t = str(t).lower()
    if "ciclo combinado" in t: return "CCGT"
    if "nuclear" in t: return "Nuclear"
    if "hidráulica generación" in t: return "Hydro"
    if "bombeo" in t: return "Hydro_pump"
    return "Other"


def firm_bucket(o):
    if not isinstance(o, str): return "OTH"
    o = o.lower()
    if "iberdrola" in o: return "IB"
    if "endesa" in o: return "GE"
    if "naturgy" in o or "gas natural" in o: return "GN"
    if "edp" in o or "hidroel" in o: return "HC"
    if "repsol" in o: return "REP"
    return "OTH"


def hour_class_of(h):
    for hc, hs in HOUR_CLASS.items():
        if int(h) in hs:
            return hc
    return None


def main():
    print("Loading units...")
    units = pd.read_csv(UNITS_CSV)
    units["tech"] = units["technology"].apply(tech_bucket)
    units["firm"] = units["owner_agent"].apply(firm_bucket)
    units = units[(units["tech"].isin(TECHS)) & (units["firm"].isin(FIRMS))][
        ["unit_code", "firm", "tech"]
    ].drop_duplicates("unit_code")

    con = duckdb.connect()
    con.execute("SET memory_limit='12GB'")
    con.execute("SET threads=4")
    con.register("u", units)

    print("Building per (firm, tech, day, hour) PHF-last vs PDBC...")
    sql = f"""
    WITH pdbc AS (
      SELECT CAST(date AS DATE) AS d, period, unit_code,
             assigned_power_mw AS pdbc_mw,
             COALESCE(mtu_minutes, 60) AS mtu,
             CASE WHEN COALESCE(mtu_minutes, 60) = 60 THEN period - 1
                  ELSE CAST(FLOOR((period - 1) / 4.0) AS INT) END AS clock_hour
      FROM read_parquet('{PDBC}')
      WHERE date >= '{START}' AND date <= '{END}'
    ),
    phf_last AS (
      SELECT CAST(date AS DATE) AS d, period, unit_code,
             MAX(session_number) AS last_session
      FROM read_parquet('{PHF}')
      WHERE date >= '{START}' AND date <= '{END}'
      GROUP BY 1, 2, 3
    ),
    phf AS (
      SELECT CAST(p.date AS DATE) AS d, p.period, p.unit_code,
             p.assigned_power_mw AS phf_mw,
             COALESCE(p.mtu_minutes, 60) AS mtu,
             CASE WHEN COALESCE(p.mtu_minutes, 60) = 60 THEN p.period - 1
                  ELSE CAST(FLOOR((p.period - 1) / 4.0) AS INT) END AS clock_hour
      FROM read_parquet('{PHF}') p
        JOIN phf_last pl ON CAST(p.date AS DATE)=pl.d AND p.period=pl.period
                         AND p.unit_code=pl.unit_code AND p.session_number=pl.last_session
      WHERE p.date >= '{START}' AND p.date <= '{END}'
    ),
    j AS (
      SELECT COALESCE(pdbc.d, phf.d) AS d,
             COALESCE(pdbc.period, phf.period) AS period,
             COALESCE(pdbc.unit_code, phf.unit_code) AS unit_code,
             COALESCE(pdbc.clock_hour, phf.clock_hour) AS clock_hour,
             COALESCE(pdbc.pdbc_mw, 0) AS pdbc_mw,
             COALESCE(phf.phf_mw, 0) AS phf_mw,
             COALESCE(pdbc.mtu, phf.mtu, 60) AS mtu
      FROM pdbc FULL OUTER JOIN phf
        ON pdbc.d=phf.d AND pdbc.period=phf.period AND pdbc.unit_code=phf.unit_code
    )
    SELECT j.d, j.clock_hour, u.firm, u.tech,
           SUM((j.phf_mw - j.pdbc_mw) * j.mtu/60.0) / 1000.0 AS gap_gwh
    FROM j JOIN u ON j.unit_code = u.unit_code
    GROUP BY 1, 2, 3, 4
    """
    df = con.execute(sql).fetchdf()
    df["d"] = pd.to_datetime(df["d"])
    df["hour_class"] = df["clock_hour"].apply(hour_class_of)
    df = df[df["hour_class"].notna()].copy()
    print(f"  {len(df):,} (firm, tech, day, hour) rows")

    # Daily aggregate per (firm, tech, hour_class, day)
    daily = (df.groupby(["d", "firm", "tech", "hour_class"])["gap_gwh"].sum().reset_index())

    # Regime label
    daily["regime"] = "other"
    for label, lo, hi in REGIME_DATES:
        m = (daily["d"] >= lo) & (daily["d"] <= hi)
        daily.loc[m, "regime"] = label

    # Per (firm, tech, hour_class, regime) mean gap GWh/day
    out = (daily.groupby(["firm", "tech", "hour_class", "regime"])["gap_gwh"].mean()
                 .reset_index())
    out.to_csv(OUT_DIR / "per_firm_tech_hc_gap.csv", index=False)
    print(f"  wrote {OUT_DIR}/per_firm_tech_hc_gap.csv")

    # Build tex table: rows = (tech, firm, hour_class), columns = regimes
    pivot = out.pivot_table(index=["tech", "firm", "hour_class"], columns="regime", values="gap_gwh")
    regime_labels = [
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
    for tech in TECHS:
        for firm in FIRMS:
            for hc in ["Critical", "Flat", "Midday"]:
                key = (tech, firm, hc)
                if key not in pivot.index:
                    continue
                tech_lbl = tech.replace("_", " ") if tech != last_tech else ""
                firm_lbl = firm if (tech != last_tech or firm != last_firm) else ""
                if tech != last_tech and last_tech is not None:
                    rows.append(r"\addlinespace")
                last_tech, last_firm = tech, firm
                cells = [tech_lbl, firm_lbl, hc]
                for r_lab, _ in regime_labels:
                    v = pivot.loc[key].get(r_lab, np.nan) if key in pivot.index else np.nan
                    cells.append(f"{v:+.2f}" if not pd.isna(v) else "---")
                rows.append(" & ".join(cells) + r" \\")
    rows.extend([r"\bottomrule", r"\end{tabular}"])
    (OUT_DIR / "tab_post_da_gap_per_firm_tech_hc.tex").write_text(
        "% Per-(firm, tech, hour-class) post-DA gap PHF_last - PDBC, GWh/day, by regime.\n"
        + "\n".join(rows))
    print(f"  wrote {OUT_DIR}/tab_post_da_gap_per_firm_tech_hc.tex")


if __name__ == "__main__":
    main()
