# STATUS: ALIVE
# LAST-AUDIT: 2026-05-19
# FEEDS: descriptive_facts.tex §3.4 (maximum-granularity per-(firm, hour-class)
#        near-MCP fPCA AFTER functional SA).
# CLAIM: Re-fit the near-MCP fPCA SEPARATELY per (tech, firm, hour-class) on
#        the already-functionally-deseasonalised curves. Each cell has its own
#        eigen-basis. The reform shift within a cell is summarised by:
#          - level shift     = mean over q of sum_k beta_k * phi_k(q)
#          - tilt magnitude  = std over q of the same fitted shift
#        Both in MCP-centred EUR/MWh on the curve domain.
#
#        Hour-class is now part of the partitioning (Critical / Flat / Midday).
#        The score regression within a cell is just post + month/hour FE (no
#        post x hour-class interaction since hour-class is fixed; "hour" still
#        varies within a class, so hour FE still meaningful).
#
# Scope: price-setting techs only (CCGT, Hydro, Hydro_pump). Min 1000 cells per
# (tech, firm, hour-class) to fit a basis.
#
# Input: results/regressions/bid/fpca/quantile_curves_<tech>_nearmcp_H50_sa.parquet
#
# Output:
#   results/regressions/bid/fpca/per_firm_hour/
#     pc_basis_<tech>_<firm>_<hc>_nearmcp_H50_sa.npz
#     coeffs_pairwise_nearmcp_H50_sa_per_firm_hour.csv
#   results/regressions/bid/fpca/tex/
#     tab_fpca_nearmcp_per_firm_hour_evr.tex
#     tab_fpca_nearmcp_per_firm_hour_<reform>.tex

from __future__ import annotations
from pathlib import Path
import gc

import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
IN = REPO / "results/regressions/bid/fpca"
OUT = IN / "per_firm_hour"
TEX = IN / "tex"
OUT.mkdir(parents=True, exist_ok=True)
TEX.mkdir(parents=True, exist_ok=True)

UNITS = REPO / "data/external/omie_reference/lista_unidades.csv"

H = 50
N_PCS = 5
N_QUANTILES = 99
SAMPLE_PER_STRATUM = 500
MIN_CELLS_PER_BASIS = 1000

TECHS_PRICE_SETTING = ["CCGT", "Hydro", "Hydro_pump"]
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


def load_unit_to_firm():
    raw = pd.read_csv(UNITS)
    raw["firm"] = raw["owner_agent"].apply(map_firm)
    return raw[["unit_code", "firm"]].drop_duplicates("unit_code").set_index("unit_code")["firm"]


def pca_svd(X, n_components):
    mean = X.mean(axis=0)
    Xc = X - mean
    _, s, Vt = np.linalg.svd(Xc, full_matrices=False)
    components = Vt[:n_components]
    total = (s**2).sum() / max(X.shape[0]-1, 1)
    evr = (s[:n_components]**2 / max(X.shape[0]-1, 1)) / total
    return mean.astype(np.float32), components.astype(np.float32), evr.astype(np.float32)


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
    unit_to_firm = load_unit_to_firm()
    df["firm"] = df["entity"].astype(str).map(unit_to_firm).fillna("OTH")
    df["regime"] = df["regime"].astype(str)
    return df


def run_pairwise(df_cell, y_col, post_regime):
    if len(df_cell) < 50:
        return np.nan
    d = df_cell.dropna(subset=[y_col]).copy()
    d["post"] = (d["regime"] == post_regime).astype(np.float32)
    Xpost = d[["post"]].to_numpy(np.float32)
    # Hour-class is fixed within cell; "hour" still varies within class.
    Xym = pd.get_dummies(d["ym"], drop_first=True).to_numpy(np.float32)
    Xhr = pd.get_dummies(d["hour"], drop_first=True).to_numpy(np.float32)
    Xq = pd.get_dummies(d["quarter"], drop_first=True).to_numpy(np.float32)
    X = np.hstack([np.ones((len(d), 1), dtype=np.float32), Xpost, Xym, Xhr, Xq])
    y = d[y_col].to_numpy(np.float32)
    try:
        coef, *_ = np.linalg.lstsq(X.astype(np.float64), y.astype(np.float64), rcond=None)
    except np.linalg.LinAlgError:
        return np.nan
    return float(coef[1])  # post coefficient


def fit_one_cell(df: pd.DataFrame, tech: str, firm: str, hc: str):
    """Fit fPCA on a single (tech, firm, hour-class) sub-panel; return (basis, scores)."""
    cell = df[(df["firm"] == firm) & (df["hour_class"] == hc)]
    if len(cell) < MIN_CELLS_PER_BASIS:
        return None, None
    qcols = [f"q{i:02d}" for i in range(1, N_QUANTILES + 1)]
    sample = (cell.groupby(["regime", "ym"], group_keys=False)
                  .apply(lambda g: g.sample(min(SAMPLE_PER_STRATUM, len(g)), random_state=42)))
    Xs = sample[qcols].to_numpy(np.float32)
    mean, comp, evr = pca_svd(Xs, N_PCS)
    np.savez(OUT / f"pc_basis_{tech.replace(' ', '_')}_{firm}_{hc}_nearmcp_H{H}_sa.npz",
             mean=mean, components=comp, explained_variance_ratio=evr)
    # Project all cell rows
    Xc = cell[qcols].to_numpy(np.float32) - mean
    scores = Xc @ comp.T
    out = cell[["date", "period", "regime", "ym", "hour", "quarter", "hour_class", "firm"]].copy()
    for k in range(N_PCS):
        out[f"PC{k+1}"] = scores[:, k].astype(np.float32)
    return (mean, comp, evr), out


def main():
    all_rows = []
    evr_rows = []
    for tech in TECHS_PRICE_SETTING:
        print(f"\n=== {tech} ===")
        df = load_sa_panel(tech)
        if df is None:
            continue
        for firm in sorted(df["firm"].unique()):
            for hc in HOUR_CLASSES:
                basis, scores = fit_one_cell(df, tech, firm, hc)
                if basis is None:
                    continue
                mean, comp, evr = basis
                n_cell = len(scores)
                evr_rows.append({"tech": tech, "firm": firm, "hour_class": hc,
                                 "n": n_cell, "PC1": float(evr[0]), "PC2": float(evr[1]),
                                 "PC3": float(evr[2]), "PC4": float(evr[3]), "PC5": float(evr[4])})
                # Pairwise per reform
                for reform, pre, post in PAIRS:
                    sub = scores[scores["regime"].isin([pre, post])]
                    if len(sub) < 200:
                        continue
                    betas = np.zeros(N_PCS, dtype=np.float64)
                    for k in range(1, N_PCS + 1):
                        b = run_pairwise(sub, f"PC{k}", post)
                        if not np.isnan(b):
                            betas[k-1] = b
                    fitted = betas @ comp  # shape (99,)
                    all_rows.append({
                        "tech": tech, "firm": firm, "hour_class": hc, "reform": reform,
                        "n_pair": int(len(sub)),
                        "level_shift": float(np.mean(fitted)),
                        "tilt_std":    float(np.std(fitted)),
                        "fitted_norm": float(np.linalg.norm(fitted)),
                        "beta_PC1": float(betas[0]),
                        "beta_PC2": float(betas[1]),
                        "beta_PC3": float(betas[2]),
                    })
                print(f"  {firm:4s} {hc:9s} n={n_cell:6d}  EVR={[round(float(x),2) for x in evr[:3]]}")
        gc.collect()
    if not all_rows:
        print("No rows produced.")
        return
    coef = pd.DataFrame(all_rows)
    coef.to_csv(OUT / f"coeffs_pairwise_nearmcp_H{H}_sa_per_firm_hour.csv", index=False)
    pd.DataFrame(evr_rows).to_csv(OUT / f"evr_per_firm_hour.csv", index=False)
    print(f"\nWrote {len(coef):,} pairwise rows.")

    # === EVR table ===
    evr_df = pd.DataFrame(evr_rows)
    rows = [r"\begin{tabular}{l l l r r r r}", r"\toprule",
            r"Tech & Firm & Hour-class & $n$ & PC1 & PC2 & PC3 \\", r"\midrule"]
    last_tech = None
    for tech in TECHS_PRICE_SETTING:
        sub = evr_df[evr_df["tech"] == tech]
        last_firm = None
        for firm in sorted(sub["firm"].unique()):
            sub_f = sub[sub["firm"] == firm]
            for hc in HOUR_CLASSES:
                row = sub_f[sub_f["hour_class"] == hc]
                if row.empty:
                    continue
                r = row.iloc[0]
                tech_label = tech.replace("_", " ") if tech != last_tech else ""
                firm_label = firm if firm != last_firm else ""
                last_tech, last_firm = tech, firm
                rows.append(" & ".join([
                    tech_label, firm_label, hc,
                    f"{int(r['n']):,}",
                    f"{r['PC1']:.2f}", f"{r['PC2']:.2f}", f"{r['PC3']:.2f}",
                ]) + r" \\")
        if tech != TECHS_PRICE_SETTING[-1]:
            rows.append(r"\addlinespace")
    rows.extend([r"\bottomrule", r"\end{tabular}"])
    (TEX / "tab_fpca_nearmcp_per_firm_hour_evr.tex").write_text(
        "% Per-(tech, firm, hour-class) near-MCP SA fPCA: EVR (own basis per cell)\n"
        + "\n".join(rows))
    print(f"  wrote tab_fpca_nearmcp_per_firm_hour_evr.tex")

    # === Reform tables: rows = (tech, firm), columns = Critical/Flat/Midday level & tilt ===
    for reform, _, _ in PAIRS:
        sub = coef[(coef["reform"] == reform) & (coef["n_pair"] >= 200)]
        rows = [r"\begin{tabular}{l l r r r r r r}", r"\toprule",
                r" & & \multicolumn{2}{c}{Critical} & \multicolumn{2}{c}{Flat} & \multicolumn{2}{c}{Midday} \\",
                r"\cmidrule(lr){3-4}\cmidrule(lr){5-6}\cmidrule(lr){7-8}",
                r"Tech & Firm & Level & Tilt & Level & Tilt & Level & Tilt \\", r"\midrule"]
        last_tech = None
        for tech in TECHS_PRICE_SETTING:
            sub_t = sub[sub["tech"] == tech]
            for firm in sorted(sub_t["firm"].unique()):
                tech_label = tech.replace("_", " ") if tech != last_tech else ""
                last_tech = tech
                cells = [tech_label, firm]
                for hc in HOUR_CLASSES:
                    r = sub_t[(sub_t["firm"] == firm) & (sub_t["hour_class"] == hc)]
                    if r.empty:
                        cells.extend(["---", "---"])
                    else:
                        cells.append(f"{r.iloc[0]['level_shift']:+.2f}")
                        cells.append(f"{r.iloc[0]['tilt_std']:.2f}")
                rows.append(" & ".join(cells) + r" \\")
            if tech != TECHS_PRICE_SETTING[-1]:
                rows.append(r"\addlinespace")
        rows.extend([r"\bottomrule", r"\end{tabular}"])
        out = TEX / f"tab_fpca_nearmcp_per_firm_hour_{reform.replace('/', '_').replace(' ', '_')}.tex"
        out.write_text(f"% Per-(tech, firm, hour-class) near-MCP SA fPCA: {reform} level + tilt across hour-classes\n"
                       + "\n".join(rows))
        print(f"  wrote {out.name}")

    print("All per-(tech, firm, hour-class) near-MCP fPCA done.")


if __name__ == "__main__":
    main()
