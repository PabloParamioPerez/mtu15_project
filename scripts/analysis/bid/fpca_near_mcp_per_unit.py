# STATUS: ALIVE
# LAST-AUDIT: 2026-05-19
# FEEDS: descriptive_facts.tex §3.4 (per-unit near-MCP fPCA, max granularity).
# CLAIM: Re-fit the near-MCP fPCA SEPARATELY per unit (CCGT / Hydro / Hydro_pump)
#        on the already functionally-deseasonalised curves. Each unit gets its
#        OWN eigen-basis; the reform shift inside the unit is summarised by
#          - level shift     = mean over q of sum_k beta_k * phi_k(q)
#          - tilt magnitude  = std over q of the fitted shift
#        Both in MCP-centred EUR/MWh on the curve domain. The same regression is
#        run per (unit, reform, hour-class), so each unit yields up to
#        3 reforms x 3 hour-classes = 9 (level, tilt) pairs.
#
# Scope: price-setting techs only. Min 500 cells per (unit) to fit a basis.
# Pairwise regression requires >= 200 rows in the pre+post window.
#
# Output:
#   results/regressions/bid/fpca/per_unit/
#     coeffs_pairwise_nearmcp_H50_sa_per_unit.csv
#     evr_per_unit.csv
#   results/regressions/bid/fpca/tex/
#     tab_fpca_nearmcp_per_unit_evr.tex
#     tab_fpca_nearmcp_per_unit_<reform>.tex   (Critical + Flat + Midday columns)

from __future__ import annotations
from pathlib import Path
import gc

import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
IN = REPO / "results/regressions/bid/fpca"
OUT = IN / "per_unit"
TEX = IN / "tex"
OUT.mkdir(parents=True, exist_ok=True)
TEX.mkdir(parents=True, exist_ok=True)

UNITS = REPO / "data/external/omie_reference/lista_unidades.csv"

H = 50
N_PCS = 5
N_QUANTILES = 99
SAMPLE_PER_STRATUM = 500
MIN_CELLS_PER_UNIT = 500
TECHS = ["CCGT", "Hydro", "Hydro_pump"]
HOUR_CLASSES = ["Critical", "Flat", "Midday"]

REGIME_DATES = [
    ("3sess",         pd.Timestamp("2024-06-14"), pd.Timestamp("2024-11-30")),
    ("ISP15win",      pd.Timestamp("2024-12-01"), pd.Timestamp("2025-03-18")),
    ("MTU15IDA_pre",  pd.Timestamp("2025-03-19"), pd.Timestamp("2025-04-27")),
    ("MTU15IDA_post", pd.Timestamp("2025-04-28"), pd.Timestamp("2025-09-30")),
    ("DA15_ID15",     pd.Timestamp("2025-10-01"), pd.Timestamp("2026-05-15")),
]
PAIRS = [
    ("ISP15",      "3sess",          "ISP15win"),
    ("MTU15-IDA",  "ISP15win",       "MTU15IDA_pre"),
    ("MTU15-DA",   "MTU15IDA_post",  "DA15_ID15"),
]


def hour_class_of(h):
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
    return "OTH"


def load_unit_meta():
    raw = pd.read_csv(UNITS)
    raw["firm"] = raw["owner_agent"].apply(map_firm)
    return raw[["unit_code", "firm", "technology"]].drop_duplicates("unit_code").set_index("unit_code")


def pca_svd(X, n_components):
    mean = X.mean(axis=0)
    Xc = X - mean
    _, s, Vt = np.linalg.svd(Xc, full_matrices=False)
    components = Vt[:n_components]
    total = (s**2).sum() / max(X.shape[0]-1, 1)
    evr = (s[:n_components]**2 / max(X.shape[0]-1, 1)) / total
    return mean.astype(np.float32), components.astype(np.float32), evr.astype(np.float32)


def run_pairwise(df_cell, y_col, post_regime):
    if len(df_cell) < 50:
        return np.nan
    d = df_cell.dropna(subset=[y_col]).copy()
    d["post"] = (d["regime"] == post_regime).astype(np.float32)
    Xpost = d[["post"]].to_numpy(np.float32)
    Xym = pd.get_dummies(d["ym"], drop_first=True).to_numpy(np.float32)
    Xhr = pd.get_dummies(d["hour"], drop_first=True).to_numpy(np.float32)
    Xq = pd.get_dummies(d["quarter"], drop_first=True).to_numpy(np.float32)
    X = np.hstack([np.ones((len(d), 1), dtype=np.float32), Xpost, Xym, Xhr, Xq])
    y = d[y_col].to_numpy(np.float32)
    try:
        coef, *_ = np.linalg.lstsq(X.astype(np.float64), y.astype(np.float64), rcond=None)
    except np.linalg.LinAlgError:
        return np.nan
    return float(coef[1])


def load_sa_panel(tech: str) -> pd.DataFrame | None:
    qpath = IN / f"quantile_curves_{tech.replace(' ', '_')}_nearmcp_H{H}_sa.parquet"
    if not qpath.exists():
        return None
    qcols = [f"q{i:02d}" for i in range(1, N_QUANTILES + 1)]
    df = pd.read_parquet(qpath, columns=["date", "period", "entity", "regime", "ym"] + qcols)
    df["date"] = pd.to_datetime(df["date"])
    df = df.dropna(subset=qcols).reset_index(drop=True)
    df["mtu_minutes"] = np.where(df["period"] > 24, 15, 60)
    df["hour"] = np.where(df["mtu_minutes"] == 15,
                          ((df["period"] - 1) // 4).astype(int),
                          (df["period"] - 1).astype(int))
    df["quarter"] = np.where(df["mtu_minutes"] == 15,
                             ((df["period"] - 1) % 4 + 1).astype(int), 1)
    df["hour_class"] = df["hour"].apply(hour_class_of)
    df = df[df["hour_class"].isin(HOUR_CLASSES)].copy()
    df["regime"] = df["regime"].astype(str)
    return df


def main():
    meta = load_unit_meta()
    all_evr, all_coef = [], []
    for tech in TECHS:
        print(f"\n=== {tech} ===")
        df = load_sa_panel(tech)
        if df is None:
            continue
        qcols = [f"q{i:02d}" for i in range(1, N_QUANTILES + 1)]
        for unit in sorted(df["entity"].astype(str).unique()):
            sub = df[df["entity"].astype(str) == unit]
            if len(sub) < MIN_CELLS_PER_UNIT:
                continue
            firm = meta.loc[unit, "firm"] if unit in meta.index else "?"
            # Fit basis on stratified sample
            sample = (sub.groupby(["regime", "ym"], group_keys=False)
                          .apply(lambda g: g.sample(min(SAMPLE_PER_STRATUM, len(g)), random_state=42)))
            Xs = sample[qcols].to_numpy(np.float32)
            mean, comp, evr = pca_svd(Xs, N_PCS)
            all_evr.append({"tech": tech, "unit": unit, "firm": firm, "n": int(len(sub)),
                            "PC1": float(evr[0]), "PC2": float(evr[1]), "PC3": float(evr[2]),
                            "PC4": float(evr[3]), "PC5": float(evr[4])})
            # Project all rows
            Xc = sub[qcols].to_numpy(np.float32) - mean
            scores = Xc @ comp.T
            scored = sub[["date", "period", "regime", "ym", "hour", "quarter", "hour_class"]].copy()
            for k in range(N_PCS):
                scored[f"PC{k+1}"] = scores[:, k].astype(np.float32)
            # Pairwise per (reform, hour_class)
            for reform, pre, post in PAIRS:
                pair = scored[scored["regime"].isin([pre, post])]
                for hc in HOUR_CLASSES:
                    cell = pair[pair["hour_class"] == hc]
                    if len(cell) < 200:
                        continue
                    betas = np.zeros(N_PCS, dtype=np.float64)
                    for k in range(1, N_PCS + 1):
                        b = run_pairwise(cell, f"PC{k}", post)
                        if not np.isnan(b):
                            betas[k-1] = b
                    fitted = betas @ comp
                    all_coef.append({
                        "tech": tech, "unit": unit, "firm": firm,
                        "reform": reform, "hour_class": hc,
                        "n_cell": int(len(cell)),
                        "level_shift": float(np.mean(fitted)),
                        "tilt_std":    float(np.std(fitted)),
                        "beta_PC1": float(betas[0]),
                        "beta_PC2": float(betas[1]),
                        "beta_PC3": float(betas[2]),
                    })
            print(f"  {unit:12s} ({firm:3s})  n={len(sub):6d}  EVR=[{evr[0]:.2f}, {evr[1]:.2f}, {evr[2]:.2f}]")
        gc.collect()

    if not all_coef:
        print("No rows produced.")
        return
    evr_df = pd.DataFrame(all_evr)
    evr_df.to_csv(OUT / "evr_per_unit.csv", index=False)
    coef_df = pd.DataFrame(all_coef)
    coef_df.to_csv(OUT / "coeffs_pairwise_nearmcp_H50_sa_per_unit.csv", index=False)
    print(f"\nWrote evr_per_unit.csv ({len(evr_df):,} units) and coeffs_pairwise_nearmcp_H50_sa_per_unit.csv ({len(coef_df):,} rows)")

    # ==================== Tables ====================

    # EVR table — one row per unit, grouped by tech and firm
    evr_df = evr_df.sort_values(["tech", "firm", "unit"]).reset_index(drop=True)
    rows = [r"\begin{tabular}{l l l r r r r}", r"\toprule",
            r"Tech & Firm & Unit & $n$ & PC1 & PC2 & PC3 \\", r"\midrule"]
    last_tech, last_firm = None, None
    for _, r in evr_df.iterrows():
        tech_lbl = r["tech"].replace("_", " ") if r["tech"] != last_tech else ""
        firm_lbl = r["firm"] if (r["tech"] != last_tech or r["firm"] != last_firm) else ""
        if r["tech"] != last_tech and last_tech is not None:
            rows.append(r"\addlinespace")
        last_tech, last_firm = r["tech"], r["firm"]
        rows.append(" & ".join([
            tech_lbl, firm_lbl, str(r["unit"]),
            f"{int(r['n']):,}",
            f"{r['PC1']:.2f}", f"{r['PC2']:.2f}", f"{r['PC3']:.2f}",
        ]) + r" \\")
    rows.extend([r"\bottomrule", r"\end{tabular}"])
    (TEX / "tab_fpca_nearmcp_per_unit_evr.tex").write_text(
        "% Per-unit near-MCP SA fPCA: PC1-PC3 EVR per unit (own basis per unit)\n" + "\n".join(rows))
    print("  wrote tab_fpca_nearmcp_per_unit_evr.tex")

    # Reform tables — rows = (tech, firm, unit), columns = Critical/Flat/Midday level + tilt
    coef_df = coef_df.sort_values(["tech", "firm", "unit"]).reset_index(drop=True)
    for reform, _, _ in PAIRS:
        sub = coef_df[coef_df["reform"] == reform]
        if sub.empty:
            continue
        # pivot
        piv = sub.pivot_table(index=["tech", "firm", "unit"], columns="hour_class",
                              values=["level_shift", "tilt_std"])
        rows = [r"\begin{tabular}{l l l r r r r r r}", r"\toprule",
                r" & & & \multicolumn{2}{c}{Critical} & \multicolumn{2}{c}{Flat} & \multicolumn{2}{c}{Midday} \\",
                r"\cmidrule(lr){4-5}\cmidrule(lr){6-7}\cmidrule(lr){8-9}",
                r"Tech & Firm & Unit & Level & Tilt & Level & Tilt & Level & Tilt \\", r"\midrule"]
        last_tech, last_firm = None, None
        for idx, r in piv.iterrows():
            tech, firm, unit = idx
            tech_lbl = tech.replace("_", " ") if tech != last_tech else ""
            firm_lbl = firm if (tech != last_tech or firm != last_firm) else ""
            if tech != last_tech and last_tech is not None:
                rows.append(r"\addlinespace")
            last_tech, last_firm = tech, firm
            cells = [tech_lbl, firm_lbl, unit]
            for hc in HOUR_CLASSES:
                lv = r.get(("level_shift", hc), np.nan)
                tl = r.get(("tilt_std", hc), np.nan)
                cells.append(f"{lv:+.2f}" if not pd.isna(lv) else "---")
                cells.append(f"{tl:.2f}" if not pd.isna(tl) else "---")
            rows.append(" & ".join(cells) + r" \\")
        rows.extend([r"\bottomrule", r"\end{tabular}"])
        out = TEX / f"tab_fpca_nearmcp_per_unit_{reform.replace('/', '_').replace(' ', '_')}.tex"
        out.write_text(f"% Per-unit near-MCP SA fPCA: {reform} level + tilt per (unit, hour-class)\n"
                       + "\n".join(rows))
        print(f"  wrote {out.name}")

    print("\nAll per-unit near-MCP fPCA done.")


if __name__ == "__main__":
    main()
