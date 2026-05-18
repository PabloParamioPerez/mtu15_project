# STATUS: ALIVE
# LAST-AUDIT: 2026-05-18
# FEEDS: bidding_internal.tex §3.4 (parallel-trends descriptive check, DA-side)
# CLAIM: For each reform, run the same pairwise PC1 regression on the real
#        post-period and on calendar-shifted placebo years (one and two years
#        earlier). The descriptive question is whether the real-year
#        coefficient differs from the placebo-year coefficients. We do NOT
#        label the difference an ATT.
#
# Inputs:
#   results/regressions/bid/fpca/
#     pc_scores_<tech>.parquet              real 2024-06-14 onward
#     pc_scores_<tech>_placebo.parquet      placebo 2022-01-01 to 2024-06-13
#
# Output:
#   results/regressions/bid/fpca/
#     coeffs_placebo_pairwise.csv           long format
#     tex/tab_fpca_placebo_<reform>.tex     per-reform placebo-vs-real PC1 table

from __future__ import annotations
from pathlib import Path
import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
IN   = REPO / "results" / "regressions" / "bid" / "fpca"
TEX  = IN / "tex"
UNITS = REPO / "data/external/omie_reference/lista_unidades.csv"

N_PCS = 5
HOUR_CLASSES = ["Critical", "Flat", "Midday"]
TECHS_FOCUS = ["CCGT", "Hydro", "Nuclear"]  # extend if placebo data exists
FIRMS_FOCUS = ["GN", "IB", "GE", "HC", "REP"]

# Real and placebo window definitions per reform.
# Keys: "year_tag" — "real" / "p23" (2023 placebo) / "p22" (2022 placebo).
WINDOWS = {
    "ISP15": {
        "real": ("2024-06-14", "2024-11-30", "2024-12-01", "2025-03-18"),
        "p23":  ("2023-06-14", "2023-11-30", "2023-12-01", "2024-03-18"),
        "p22":  ("2022-06-14", "2022-11-30", "2022-12-01", "2023-03-18"),
    },
    "MTU15-IDA": {
        "real": ("2024-12-01", "2025-03-18", "2025-03-19", "2025-04-27"),
        "p23":  ("2023-12-01", "2024-03-18", "2024-03-19", "2024-04-27"),
        "p22":  ("2022-12-01", "2023-03-18", "2023-03-19", "2023-04-27"),
    },
    "MTU15-DA": {
        "real": ("2025-04-28", "2025-09-30", "2025-10-01", "2026-01-31"),
        "p23":  ("2023-04-28", "2023-09-30", "2023-10-01", "2024-01-31"),
        "p22":  ("2022-04-28", "2022-09-30", "2022-10-01", "2023-01-31"),
    },
}


def hour_class(h):
    if h in (5, 6, 7, 8, 16, 17, 18, 19, 20, 21, 22): return "Critical"
    if h in (1, 2, 3): return "Flat"
    if h in (11, 12, 13, 14): return "Midday"
    return "Dropped"


def map_firm(s):
    if not isinstance(s, str): return "OTH"
    o = s.lower()
    if "iberdrola" in o: return "IB"
    if "endesa" in o: return "GE"
    if "naturgy" in o or "gas natural" in o: return "GN"
    if "edp" in o or "hidroel" in o: return "HC"
    if "repsol" in o: return "REP"
    if "acciona" in o: return "ACC"
    if "axpo" in o: return "AXPO"
    if "gesternova" in o: return "GST"
    return "OTH"


def load_units_firm_map():
    raw = pd.read_csv(UNITS)
    raw["firm"] = raw["owner_agent"].apply(map_firm)
    return raw[["unit_code", "firm"]].drop_duplicates("unit_code").set_index("unit_code")["firm"]


def fourier_basis(doy, K=3):
    arr = np.zeros((len(doy), 2 * K), dtype=np.float32)
    angle = 2 * np.pi * doy.values.astype(np.float32) / 365.25
    for k in range(1, K + 1):
        arr[:, 2*(k-1)]   = np.sin(k * angle)
        arr[:, 2*(k-1)+1] = np.cos(k * angle)
    return arr


def load_panel(tech: str) -> pd.DataFrame | None:
    fp_real = IN / f"pc_scores_{tech.replace(' ', '_')}.parquet"
    fp_plac = IN / f"pc_scores_{tech.replace(' ', '_')}_placebo.parquet"
    parts = []
    if fp_real.exists():
        parts.append(pd.read_parquet(fp_real))
    if fp_plac.exists():
        parts.append(pd.read_parquet(fp_plac))
    if not parts:
        return None
    df = pd.concat(parts, ignore_index=True)
    df["date"] = pd.to_datetime(df["date"])
    df["mtu_minutes"] = np.where(df["period"] > 24, 15, 60)
    df["hour"] = np.where(df["mtu_minutes"] == 15,
                          ((df["period"] - 1) // 4).astype(int),
                          (df["period"] - 1).astype(int))
    df["quarter"] = np.where(df["mtu_minutes"] == 15,
                             ((df["period"] - 1) % 4 + 1).astype(int), 1)
    df["hour_class"] = df["hour"].apply(hour_class)
    df = df[df["hour_class"].isin(HOUR_CLASSES)].copy()
    df["dow"] = df["date"].dt.dayofweek
    df["doy"] = df["date"].dt.dayofyear
    df["ym"] = df["date"].dt.strftime("%Y-%m")
    if "firm" not in df.columns:
        unit_to_firm = load_units_firm_map()
        df["firm"] = df["entity"].map(unit_to_firm).fillna("OTH")
    return df


def run_pairwise_one_pc(df_sub: pd.DataFrame, y_col: str, post_start: pd.Timestamp):
    df_sub = df_sub.dropna(subset=["hour_class", y_col]).copy()
    if len(df_sub) < 50:
        return (np.nan, np.nan, np.nan)
    df_sub["post"] = (df_sub["date"] >= post_start).astype(np.float32)

    X_post = df_sub[["post"]].astype(np.float32).to_numpy()
    X_postxFlat = (df_sub["post"] * (df_sub["hour_class"] == "Flat")).astype(np.float32).to_numpy().reshape(-1, 1)
    X_postxMid  = (df_sub["post"] * (df_sub["hour_class"] == "Midday")).astype(np.float32).to_numpy().reshape(-1, 1)
    X_hcFlat = (df_sub["hour_class"] == "Flat").astype(np.float32).to_numpy().reshape(-1, 1)
    X_hcMid  = (df_sub["hour_class"] == "Midday").astype(np.float32).to_numpy().reshape(-1, 1)

    X_ym  = pd.get_dummies(df_sub["ym"], drop_first=True).astype(np.float32).to_numpy()
    X_dow = pd.get_dummies(df_sub["dow"], drop_first=True).astype(np.float32).to_numpy()
    X_hr  = pd.get_dummies(df_sub["hour"], drop_first=True).astype(np.float32).to_numpy()
    X_q   = pd.get_dummies(df_sub["quarter"], drop_first=True).astype(np.float32).to_numpy()
    X_fou = fourier_basis(df_sub["doy"], K=3)

    X = np.hstack([
        np.ones((len(df_sub), 1), dtype=np.float32),
        X_post, X_postxFlat, X_postxMid,
        X_hcFlat, X_hcMid,
        X_fou, X_ym, X_dow, X_hr, X_q,
    ])
    y = df_sub[y_col].astype(np.float32).to_numpy()
    try:
        coef, *_ = np.linalg.lstsq(X, y, rcond=None)
    except np.linalg.LinAlgError:
        return (np.nan, np.nan, np.nan)
    return (float(coef[1]), float(coef[2]), float(coef[3]))


def fit_placebo(tech: str):
    df = load_panel(tech)
    if df is None or len(df) == 0:
        return None
    print(f"\n=== Placebo pairwise for {tech}: {len(df):,} rows ({df['date'].min()} → {df['date'].max()}) ===")

    rows = []
    for reform, year_windows in WINDOWS.items():
        for year_tag, (pre_a, pre_b, post_a, post_b) in year_windows.items():
            pre_a_ts, pre_b_ts = pd.Timestamp(pre_a), pd.Timestamp(pre_b)
            post_a_ts, post_b_ts = pd.Timestamp(post_a), pd.Timestamp(post_b)
            mask = ((df["date"] >= pre_a_ts) & (df["date"] <= pre_b_ts)) | \
                   ((df["date"] >= post_a_ts) & (df["date"] <= post_b_ts))
            df_pair = df[mask]
            if len(df_pair) == 0:
                continue
            firms_in_pair = sorted(df_pair["firm"].unique())
            for firm in firms_in_pair:
                if firm not in FIRMS_FOCUS:
                    continue
                df_sub = df_pair[df_pair["firm"] == firm]
                if len(df_sub) < 100:
                    continue
                for k in range(1, N_PCS + 1):
                    c_post, c_flat, c_mid = run_pairwise_one_pc(df_sub, f"PC{k}", post_a_ts)
                    rows.append({"tech": tech, "firm": firm, "reform": reform,
                                 "year": year_tag, "PC": f"PC{k}", "hour_class": "Critical",
                                 "coef": c_post, "n_rows": len(df_sub)})
                    rows.append({"tech": tech, "firm": firm, "reform": reform,
                                 "year": year_tag, "PC": f"PC{k}", "hour_class": "Flat",
                                 "coef": (c_post + c_flat) if not np.isnan(c_post) else np.nan,
                                 "n_rows": len(df_sub)})
                    rows.append({"tech": tech, "firm": firm, "reform": reform,
                                 "year": year_tag, "PC": f"PC{k}", "hour_class": "Midday",
                                 "coef": (c_post + c_mid) if not np.isnan(c_post) else np.nan,
                                 "n_rows": len(df_sub)})
                print(f"  {reform} | {year_tag} | {firm}: done")
    return pd.DataFrame(rows)


def write_tex_comparison(all_coef: pd.DataFrame):
    """For each reform, emit one tex table with rows = (tech, firm),
       columns = (p22, p23, real) × Critical hour-class only (PC1).
       Critical hour-class is the most economically relevant for thermal techs.
    """
    YEAR_ORDER = ["p22", "p23", "real"]
    YEAR_LABEL = {"p22": "2022 placebo", "p23": "2023 placebo", "real": "Real"}

    for reform in WINDOWS.keys():
        sub = all_coef[(all_coef["reform"] == reform)
                       & (all_coef["PC"] == "PC1")
                       & (all_coef["hour_class"] == "Critical")
                       & (all_coef["tech"].isin(TECHS_FOCUS))
                       & (all_coef["firm"].isin(FIRMS_FOCUS))]
        piv = sub.pivot_table(index=["tech", "firm"], columns="year",
                              values="coef")
        for y in YEAR_ORDER:
            if y not in piv.columns:
                piv[y] = np.nan
        piv = piv[YEAR_ORDER].round(0)

        rows = []
        rows.append(r"\begin{tabular}{l l r r r}")
        rows.append(r"\toprule")
        rows.append(" & & " + " & ".join(YEAR_LABEL[y] for y in YEAR_ORDER) + r" \\")
        rows.append(r"Tech & Firm & (2022 calendar) & (2023 calendar) & (2024-25 calendar) \\")
        rows.append(r"\midrule")
        for (tech, firm), r in piv.iterrows():
            tech_label = tech.replace("_", " ")
            row = [tech_label, firm]
            for y in YEAR_ORDER:
                v = r.get(y, np.nan) if isinstance(r, pd.Series) else np.nan
                row.append(f"{v:+.0f}" if not pd.isna(v) else "---")
            rows.append(" & ".join(row) + r" \\")
        rows.append(r"\bottomrule")
        rows.append(r"\end{tabular}")
        out = TEX / f"tab_fpca_placebo_{reform.replace('/', '_').replace('-', '_')}.tex"
        out.write_text(f"% auto-built — placebo-vs-real {reform} on PC1 Critical-hours\n" + "\n".join(rows))
        print(f"  wrote {out}")


def main():
    all_rows = []
    for tech in TECHS_FOCUS:
        out = fit_placebo(tech)
        if out is not None and len(out):
            all_rows.append(out)
    if not all_rows:
        print("nothing to write")
        return
    all_coef = pd.concat(all_rows, ignore_index=True)
    all_coef.to_csv(IN / "coeffs_placebo_pairwise.csv", index=False)
    print(f"\nTotal: {len(all_coef):,} coefficient rows")
    write_tex_comparison(all_coef)
    print("Done.")


if __name__ == "__main__":
    main()
