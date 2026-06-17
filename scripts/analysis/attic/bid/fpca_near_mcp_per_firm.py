# STATUS: ALIVE
# LAST-AUDIT: 2026-05-19
# FEEDS: descriptive_facts.tex §3.4 (per-firm near-MCP fPCA, max granularity).
# CLAIM: Re-fit the near-MCP fPCA SEPARATELY per (tech, firm) so each firm's
#        bidding shape gets its own eigen-basis. The output: per-(tech, firm)
#        mean curve, eigenfunctions, EVR; per-(tech, firm, reform) pairwise
#        score regression on the firm's own basis, summarised as two scalars
#        per cell:
#          - level shift     = mean over q of sum_k beta_k * phi_k(q)
#          - tilt magnitude  = std over q of the same fitted shift
#        Both in MCP-centred EUR/MWh on the curve domain. These two scalars are
#        directly comparable across firms even though the eigenfunctions are
#        firm-specific.
#
# Scope: price-setting techs only (CCGT, Hydro, Hydro_pump) per the
# user-requested restriction. Nuclear and RES are inframarginal in DA and the
# near-MCP curve is too sparse for them.
#
# Input: results/regressions/bid/fpca/quantile_curves_<tech>_nearmcp_H50_sa.parquet
#        (produced by fpca_near_mcp.py)
#
# Output:
#   results/regressions/bid/fpca/per_firm/
#     pc_basis_<tech>_<firm>_nearmcp_H50_sa.npz
#     pc_scores_<tech>_<firm>_nearmcp_H50_sa.parquet
#     coeffs_pairwise_nearmcp_H50_sa_per_firm.csv
#   results/regressions/bid/fpca/tex/
#     tab_fpca_nearmcp_per_firm_evr.tex      (per-firm EVR table)
#     tab_fpca_nearmcp_per_firm_<reform>.tex (level + tilt scalars per reform)
#   figures/working/
#     fpca_eigenfuncs_<tech>_<firm>_nearmcp_H50_sa.pdf

from __future__ import annotations
from pathlib import Path
import gc

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

REPO = Path(__file__).resolve().parents[3]
IN = REPO / "results/regressions/bid/fpca"
PERFIRM = IN / "per_firm"
TEX = IN / "tex"
FIG = REPO / "figures/working"
PERFIRM.mkdir(parents=True, exist_ok=True)
TEX.mkdir(parents=True, exist_ok=True)
FIG.mkdir(parents=True, exist_ok=True)

UNITS = REPO / "data/external/omie_reference/lista_unidades.csv"

H = 50
N_PCS = 5
N_QUANTILES = 99
SAMPLE_PER_STRATUM = 1000
MIN_CELLS_PER_FIRM = 1000   # need enough cells to fit a basis

TECHS_PRICE_SETTING = ["CCGT", "Hydro", "Hydro_pump"]

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

HOUR_CLASSES = ["Critical", "Flat", "Midday"]
FIRMS_FOCUS = ["GN", "IB", "GE", "HC", "REP", "OTH"]


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


def fit_pca_per_firm(tech: str):
    """Load SA curves for this tech, partition by firm, fit per-firm PCA."""
    qpath = IN / f"quantile_curves_{tech.replace(' ', '_')}_nearmcp_H{H}_sa.parquet"
    if not qpath.exists():
        print(f"  {qpath.name} missing for {tech}, skip")
        return None

    qcols = [f"q{i:02d}" for i in range(1, N_QUANTILES + 1)]
    df = pd.read_parquet(qpath, columns=["date", "period", "entity", "regime", "ym"] + qcols)
    df["date"] = pd.to_datetime(df["date"])
    df = df.dropna(subset=qcols).reset_index(drop=True)

    # Map entity -> firm (entity is unit_code for strategic techs)
    unit_to_firm = load_unit_to_firm()
    df["firm"] = df["entity"].astype(str).map(unit_to_firm).fillna("OTH")

    print(f"\n=== {tech}: per-firm PCA on {len(df):,} SA cells ===")
    per_firm_scores = []
    per_firm_bases = {}
    for firm in sorted(df["firm"].unique()):
        sub = df[df["firm"] == firm]
        if len(sub) < MIN_CELLS_PER_FIRM:
            print(f"  {firm}: only {len(sub):,} cells, skip")
            continue
        # Stratified sample for basis fit
        sample = (sub.groupby(["regime", "ym"], group_keys=False)
                     .apply(lambda g: g.sample(min(SAMPLE_PER_STRATUM, len(g)), random_state=42)))
        Xs = sample[qcols].to_numpy(np.float32)
        mean, comp, evr = pca_svd(Xs, N_PCS)
        np.savez(PERFIRM / f"pc_basis_{tech.replace(' ', '_')}_{firm}_nearmcp_H{H}_sa.npz",
                 mean=mean, components=comp, explained_variance_ratio=evr)
        per_firm_bases[firm] = (mean, comp, evr)
        # Project all firm cells onto own basis
        Xc = sub[qcols].to_numpy(np.float32) - mean
        scores = Xc @ comp.T
        out = sub[["date", "period", "entity", "regime", "ym", "firm"]].copy()
        for k in range(N_PCS):
            out[f"PC{k+1}"] = scores[:, k].astype(np.float32)
        per_firm_scores.append(out)
        print(f"  {firm}: n={len(sub):,}, EVR={[round(float(x),3) for x in evr]}")
    if not per_firm_scores:
        return None
    all_scores = pd.concat(per_firm_scores, ignore_index=True)
    all_scores.to_parquet(PERFIRM / f"pc_scores_{tech.replace(' ', '_')}_nearmcp_H{H}_sa.parquet", index=False)
    return all_scores, per_firm_bases


def run_pairwise_one_pc(df_sub, y_col, post_regime):
    """Single PC pairwise regression: post + post x hour-class + hour-class + month/hour/quarter FE."""
    if len(df_sub) < 50:
        return (np.nan, np.nan, np.nan)
    d = df_sub.dropna(subset=["hour_class", y_col]).copy()
    d["post"] = (d["regime"] == post_regime).astype(np.float32)
    Xpost = d[["post"]].to_numpy(np.float32)
    XpostF = (d["post"] * (d["hour_class"] == "Flat")).to_numpy(np.float32).reshape(-1, 1)
    XpostM = (d["post"] * (d["hour_class"] == "Midday")).to_numpy(np.float32).reshape(-1, 1)
    XhcF = (d["hour_class"] == "Flat").to_numpy(np.float32).reshape(-1, 1)
    XhcM = (d["hour_class"] == "Midday").to_numpy(np.float32).reshape(-1, 1)
    Xym = pd.get_dummies(d["ym"], drop_first=True).to_numpy(np.float32)
    Xhr = pd.get_dummies(d["hour"], drop_first=True).to_numpy(np.float32)
    Xq = pd.get_dummies(d["quarter"], drop_first=True).to_numpy(np.float32)
    X = np.hstack([np.ones((len(d), 1), dtype=np.float32),
                   Xpost, XpostF, XpostM, XhcF, XhcM, Xym, Xhr, Xq])
    y = d[y_col].to_numpy(np.float32)
    try:
        coef, *_ = np.linalg.lstsq(X.astype(np.float64), y.astype(np.float64), rcond=None)
    except np.linalg.LinAlgError:
        return (np.nan, np.nan, np.nan)
    return (float(coef[1]), float(coef[2]), float(coef[3]))


def fit_pairwise_per_firm(tech: str, scores: pd.DataFrame, bases: dict):
    """Pairwise regression per (firm, reform, PC k), then build level/tilt scalars."""
    scores = scores.copy()
    scores["date"] = pd.to_datetime(scores["date"])
    scores["mtu_minutes"] = np.where(scores["period"] > 24, 15, 60)
    scores["hour"] = np.where(scores["mtu_minutes"] == 15,
                              ((scores["period"] - 1) // 4).astype(int),
                              (scores["period"] - 1).astype(int))
    scores["quarter"] = np.where(scores["mtu_minutes"] == 15,
                                 ((scores["period"] - 1) % 4 + 1).astype(int), 1)
    scores["hour_class"] = scores["hour"].apply(hour_class)
    scores = scores[scores["hour_class"].isin(HOUR_CLASSES)].copy()
    scores["ym"] = scores["date"].dt.strftime("%Y-%m")
    scores["regime"] = scores["regime"].astype(str)

    rows = []
    for firm in sorted(scores["firm"].unique()):
        if firm not in bases:
            continue
        _, comp_firm, _ = bases[firm]
        sub_firm = scores[scores["firm"] == firm]
        for reform, pre, post in PAIRS:
            sub_pair = sub_firm[sub_firm["regime"].isin([pre, post])]
            if len(sub_pair) < 50:
                continue
            # Run PC k = 1..5 in separate regressions for each hour-class
            betas_by_hc = {hc: np.zeros(N_PCS, dtype=np.float64) for hc in HOUR_CLASSES}
            for k in range(1, N_PCS + 1):
                c, cF, cM = run_pairwise_one_pc(sub_pair, f"PC{k}", post)
                if np.isnan(c):
                    continue
                betas_by_hc["Critical"][k-1] = c
                betas_by_hc["Flat"][k-1] = c + (cF if not np.isnan(cF) else 0.0)
                betas_by_hc["Midday"][k-1] = c + (cM if not np.isnan(cM) else 0.0)
            # Build fitted curve shift Delta f(q) = sum_k beta_k phi_k(q) per hour-class
            # comp_firm shape: (N_PCS, 99). Sum across PCs weighted by betas.
            for hc in HOUR_CLASSES:
                fitted = betas_by_hc[hc] @ comp_firm  # shape (99,)
                rows.append({
                    "tech": tech, "firm": firm, "reform": reform, "hour_class": hc,
                    "level_shift": float(np.mean(fitted)),
                    "tilt_std":    float(np.std(fitted)),
                    "fitted_norm": float(np.linalg.norm(fitted)),
                    "beta_PC1": float(betas_by_hc[hc][0]),
                    "beta_PC2": float(betas_by_hc[hc][1]),
                    "beta_PC3": float(betas_by_hc[hc][2]),
                    "n_pair_rows": int(len(sub_pair)),
                })
    return pd.DataFrame(rows)


def write_evr_table(all_bases):
    """One row per (tech, firm) listing PC1-PC3 EVR. Header groups by tech."""
    rows = [r"\begin{tabular}{l l r r r r r}", r"\toprule",
            r"Tech & Firm & PC1 & PC2 & PC3 & PC4 & PC5 \\", r"\midrule"]
    last_tech = None
    for (tech, firm, evr) in all_bases:
        tech_label = tech.replace("_", " ") if tech != last_tech else ""
        last_tech = tech
        cells = [tech_label, firm] + [f"{float(v):.2f}" for v in evr[:5]]
        rows.append(" & ".join(cells) + r" \\")
    rows.extend([r"\bottomrule", r"\end{tabular}"])
    out = TEX / f"tab_fpca_nearmcp_per_firm_evr.tex"
    out.write_text("% Per-(tech, firm) near-MCP SA fPCA: PC1-PC5 EVR per firm; each row uses that firm's OWN basis.\n"
                   + "\n".join(rows))
    print(f"  wrote {out.name}")


def write_reform_tables(all_coef: pd.DataFrame):
    """Per reform: one table with rows = (tech, firm), columns = level shift, tilt std,
    on Critical hours. Both scalars are in MCP-centred EUR/MWh, directly comparable
    across firms even though the underlying eigen-bases are firm-specific."""
    tech_order = TECHS_PRICE_SETTING
    for reform, _, _ in PAIRS:
        sub = all_coef[(all_coef["reform"] == reform)
                       & (all_coef["hour_class"] == "Critical")
                       & (all_coef["n_pair_rows"] >= 200)]
        rows = [r"\begin{tabular}{l l r r r r r}", r"\toprule",
                r"Tech & Firm & Level shift & Tilt (std) & PC1 $\beta$ & PC2 $\beta$ & PC3 $\beta$ \\",
                r"\midrule"]
        last_tech = None
        for tech in tech_order:
            sub_t = sub[sub["tech"] == tech]
            for firm in sorted(sub_t["firm"].unique()):
                r = sub_t[sub_t["firm"] == firm].iloc[0]
                tech_label = tech.replace("_", " ") if tech != last_tech else ""
                last_tech = tech
                rows.append(" & ".join([
                    tech_label, firm,
                    f"{r['level_shift']:+.2f}",
                    f"{r['tilt_std']:.2f}",
                    f"{r['beta_PC1']:+.1f}",
                    f"{r['beta_PC2']:+.1f}",
                    f"{r['beta_PC3']:+.1f}",
                ]) + r" \\")
            if tech != tech_order[-1]:
                rows.append(r"\addlinespace")
        rows.extend([r"\bottomrule", r"\end{tabular}"])
        out = TEX / f"tab_fpca_nearmcp_per_firm_{reform.replace('/', '_').replace(' ', '_')}.tex"
        out.write_text(f"% Per-(tech, firm) near-MCP SA fPCA: {reform} critical-hour level/tilt/beta scalars\n"
                       + "\n".join(rows))
        print(f"  wrote {out.name}")


def plot_eigenfunctions_per_firm(all_bases):
    """One figure per (tech, firm): mean + first 3 eigenfunctions."""
    q_grid = np.linspace(1, 99, 99)
    colors = ["#d62728", "#1f77b4", "#2ca02c", "#ff7f0e", "#9467bd"]
    for tech, firm, evr in all_bases:
        npz = PERFIRM / f"pc_basis_{tech.replace(' ', '_')}_{firm}_nearmcp_H{H}_sa.npz"
        if not npz.exists():
            continue
        b = np.load(npz)
        mean, comp = b["mean"], b["components"]
        fig, axes = plt.subplots(1, 2, figsize=(11, 4))
        axes[0].plot(q_grid, mean, color="black", lw=2)
        axes[0].axhline(0, color="grey", lw=0.5)
        axes[0].set_xlabel("Quantile of in-band MW (1-99)")
        axes[0].set_ylabel(r"$p - $MCP (EUR/MWh)")
        axes[0].set_title(f"{tech.replace('_',' ')} | {firm} | mean SA near-MCP curve")
        axes[0].grid(alpha=0.3)
        for k in range(min(3, comp.shape[0])):
            axes[1].plot(q_grid, comp[k], color=colors[k], label=f"PC{k+1} ({float(evr[k])*100:.0f}%)")
        axes[1].axhline(0, color="black", lw=0.5)
        axes[1].set_xlabel("Quantile of in-band MW (1-99)")
        axes[1].set_ylabel("Eigenfunction value")
        axes[1].set_title(f"{tech.replace('_',' ')} | {firm} | own eigenfunctions")
        axes[1].legend(loc="best", fontsize=9)
        axes[1].grid(alpha=0.3)
        fig.tight_layout()
        fig.savefig(FIG / f"fpca_eigenfuncs_{tech.replace(' ', '_')}_{firm}_nearmcp_H{H}_sa.pdf")
        plt.close(fig)


def main():
    all_bases_list = []
    all_coef_list = []
    for tech in TECHS_PRICE_SETTING:
        result = fit_pca_per_firm(tech)
        if result is None:
            continue
        scores, bases = result
        for firm, (_, _, evr) in bases.items():
            all_bases_list.append((tech, firm, evr))
        # Pairwise regression on per-firm bases
        coef = fit_pairwise_per_firm(tech, scores, bases)
        if len(coef):
            all_coef_list.append(coef)
        gc.collect()
    if not all_coef_list:
        print("No results produced.")
        return
    all_coef = pd.concat(all_coef_list, ignore_index=True)
    all_coef.to_csv(PERFIRM / f"coeffs_pairwise_nearmcp_H{H}_sa_per_firm.csv", index=False)
    print(f"\nTotal: {len(all_coef):,} per-firm coefficient rows")
    write_evr_table(all_bases_list)
    write_reform_tables(all_coef)
    plot_eigenfunctions_per_firm(all_bases_list)
    print("All per-firm near-MCP fPCA done.")


if __name__ == "__main__":
    main()
