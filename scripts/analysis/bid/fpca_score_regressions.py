# STATUS: ALIVE
# LAST-AUDIT: 2026-05-18
# FEEDS: bidding_internal.tex §3.4 (fPCA score regressions)
# CLAIM: Per tech, run OLS of each PC score on regime × hour-class with
#        seasonality + firm + hour FE. Output: coefficient table per tech,
#        plot of the fitted curve change in quantile space per (regime, hour-class)
#        as a sum_k beta_k * phi_k(q).
#
# Uses numpy SVD-style closed-form OLS (no sklearn / pyfixest needed).
#
# Output:
#   results/regressions/bid/fpca/
#     coeffs_<tech>.csv             coefficient table (long format)
#     tex/tab_fpca_<tech>.tex       headline table per tech
#   figures/working/
#     fpca_eigenfuncs_<tech>.pdf    mean + first 3 PCs vs quantile
#     fpca_curve_change_<tech>.pdf  fitted curve shift per (regime, hour-class)

from __future__ import annotations
from pathlib import Path
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

REPO = Path(__file__).resolve().parents[3]
IN   = REPO / "results" / "regressions" / "bid" / "fpca"
TEX  = IN / "tex"
TEX.mkdir(parents=True, exist_ok=True)
FIG  = REPO / "figures" / "working"
FIG.mkdir(parents=True, exist_ok=True)

UNITS = REPO / "data/external/omie_reference/lista_unidades.csv"

REGIMES_ORDERED = ["3-sess", "ISP15-win", "DA60/ID15 pre-blk", "DA60/ID15 post-blk", "DA15/ID15"]
HOUR_CLASSES = ["Critical", "Flat", "Midday"]
N_PCS = 5
TECHS = ["CCGT", "Hydro", "Hydro_pump", "Nuclear", "Wind", "Solar PV", "Cogen", "Solar Thermal"]


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


def prepare_panel(tech: str):
    fp = IN / f"pc_scores_{tech.replace(' ', '_')}.parquet"
    if not fp.exists():
        return None
    df = pd.read_parquet(fp)
    df["date"] = pd.to_datetime(df["date"])
    # period → hour, quarter
    # For DA: pre-MTU15-DA periods are 1-24 (hourly); post-MTU15-DA periods are 1-96 (15-min).
    # We need to detect which: rule = if period > 24 then 15-min else hourly.
    df["mtu_minutes"] = np.where(df["period"] > 24, 15, 60)
    df["hour"] = np.where(df["mtu_minutes"] == 15,
                          ((df["period"] - 1) // 4).astype(int),
                          (df["period"] - 1).astype(int))
    df["quarter"] = np.where(df["mtu_minutes"] == 15,
                             ((df["period"] - 1) % 4 + 1).astype(int),
                             1)
    df["hour_class"] = df["hour"].apply(hour_class)
    df = df[df["hour_class"].isin(HOUR_CLASSES)].copy()
    df["dow"] = df["date"].dt.dayofweek
    df["regime"] = df["regime"].astype("category")
    # firm column already present (entity = firm for non-strategic; entity = unit for strategic)
    if "firm" not in df.columns:
        unit_to_firm = load_units_firm_map()
        df["firm"] = df["entity"].map(unit_to_firm).fillna("OTH")
    return df


def run_regression(df, y_col):
    """OLS of y on (regime × hour_class) + firm + ym + dow + hour + quarter FE.
    Returns a dict of (regime, hour_class) → coefficient relative to 3-sess × Critical.
    Implementation: build sparse design matrix via pd.get_dummies, solve via lstsq.
    """
    REF_REGIME = "3-sess"
    REF_HOUR_CLASS = "Critical"

    # Reference baseline indicator
    df = df.dropna(subset=["regime", "hour_class", y_col])
    df["regime_hc"] = df["regime"].astype(str) + " :: " + df["hour_class"].astype(str)
    ref_key = f"{REF_REGIME} :: {REF_HOUR_CLASS}"

    # Design matrix: drop reference levels
    Xc_regime_hc = pd.get_dummies(df["regime_hc"], drop_first=False).drop(columns=[ref_key], errors="ignore")
    Xc_firm     = pd.get_dummies(df["firm"], prefix="firm", drop_first=True)
    Xc_ym       = pd.get_dummies(df["ym"], prefix="ym", drop_first=True)
    Xc_dow      = pd.get_dummies(df["dow"], prefix="dow", drop_first=True)
    Xc_hour     = pd.get_dummies(df["hour"], prefix="hr", drop_first=True)
    Xc_qq       = pd.get_dummies(df["quarter"], prefix="q", drop_first=True)

    X = pd.concat([Xc_regime_hc, Xc_firm, Xc_ym, Xc_dow, Xc_hour, Xc_qq], axis=1).astype(np.float32)
    X.insert(0, "intercept", 1.0)
    y = df[y_col].astype(np.float32).to_numpy()

    # OLS via lstsq
    coef, *_ = np.linalg.lstsq(X.to_numpy(), y, rcond=None)
    coef_series = pd.Series(coef, index=X.columns)
    # Extract regime × hour-class coefficients (which are the deviations from baseline)
    out = {}
    for r in REGIMES_ORDERED:
        for hc in HOUR_CLASSES:
            key = f"{r} :: {hc}"
            if r == REF_REGIME and hc == REF_HOUR_CLASS:
                out[(r, hc)] = 0.0
            elif key in coef_series.index:
                out[(r, hc)] = float(coef_series[key])
            else:
                out[(r, hc)] = np.nan
    return out


def fit_per_tech(tech: str):
    print(f"\n=== Regressions for {tech} ===")
    df = prepare_panel(tech)
    if df is None or len(df) == 0:
        print(f"  no data for {tech}")
        return None
    print(f"  panel: {len(df):,} rows")

    results = {}
    for k in range(1, N_PCS + 1):
        col = f"PC{k}"
        results[col] = run_regression(df, col)
        print(f"  done {col}")

    # Build long-format CSV
    rows = []
    for pc, d in results.items():
        for (r, hc), v in d.items():
            rows.append({"tech": tech, "PC": pc, "regime": r, "hour_class": hc, "coef": v})
    out_df = pd.DataFrame(rows)
    out_df.to_csv(IN / f"coeffs_{tech.replace(' ', '_')}.csv", index=False)
    print(f"  wrote coeffs_{tech.replace(' ', '_')}.csv ({len(out_df)} rows)")

    return results


def plot_eigenfunctions(tech: str):
    basis_path = IN / f"pc_basis_{tech.replace(' ', '_')}.npz"
    if not basis_path.exists():
        return
    b = np.load(basis_path)
    mean = b["mean"]
    components = b["components"]
    evr = b["explained_variance_ratio"]
    q_grid = np.linspace(1, 99, 99)

    fig, axes = plt.subplots(1, 2, figsize=(11, 4))
    axes[0].plot(q_grid, mean, color="black", lw=2)
    axes[0].set_xlabel("Quantile of cumulative MW (1-99)")
    axes[0].set_ylabel("Price (EUR/MWh)")
    axes[0].set_title(f"Mean bid curve ($S^{{-1}}(q)$), {tech}")
    axes[0].grid(alpha=0.3)
    colors = ["#d62728", "#1f77b4", "#2ca02c", "#ff7f0e", "#9467bd"]
    for k in range(min(3, components.shape[0])):
        axes[1].plot(q_grid, components[k], color=colors[k], label=f"PC{k+1} ({evr[k]*100:.0f}%)")
    axes[1].axhline(0, color="black", lw=0.5)
    axes[1].set_xlabel("Quantile of cumulative MW (1-99)")
    axes[1].set_ylabel("Eigenfunction value")
    axes[1].set_title(f"First 3 eigenfunctions, {tech}")
    axes[1].legend(loc="best", fontsize=9)
    axes[1].grid(alpha=0.3)
    fig.tight_layout()
    fig.savefig(FIG / f"fpca_eigenfuncs_{tech.replace(' ', '_')}.pdf")
    plt.close(fig)


def plot_curve_change(tech: str, results: dict):
    basis_path = IN / f"pc_basis_{tech.replace(' ', '_')}.npz"
    if not basis_path.exists() or results is None:
        return
    b = np.load(basis_path)
    components = b["components"]   # (N_PCS, 99)
    q_grid = np.linspace(1, 99, 99)

    fig, axes = plt.subplots(1, 3, figsize=(15, 4.2), sharey=True)
    colors = {"3-sess": "#7f7f7f", "ISP15-win": "#1f77b4",
              "DA60/ID15 pre-blk": "#ff7f0e", "DA60/ID15 post-blk": "#d62728",
              "DA15/ID15": "#9467bd"}
    for ax, hc in zip(axes, HOUR_CLASSES):
        for r in REGIMES_ORDERED:
            beta = np.array([results[f"PC{k+1}"].get((r, hc), np.nan) for k in range(N_PCS)])
            if np.any(np.isnan(beta)):
                continue
            change = beta @ components   # (99,)
            ax.plot(q_grid, change, color=colors[r], label=r, lw=1.6, alpha=0.9)
        ax.axhline(0, color="black", lw=0.5)
        ax.set_xlabel("Quantile of cumulative MW")
        ax.set_title(f"Hour-class: {hc}")
        ax.grid(alpha=0.3)
    axes[0].set_ylabel("Fitted bid-curve shift (EUR/MWh)")
    axes[0].legend(loc="best", fontsize=8)
    fig.suptitle(f"Fitted bid-curve shift across regimes — {tech}\n(reference: 3-sess × Critical = 0)")
    fig.tight_layout(rect=[0, 0, 1, 0.94])
    fig.savefig(FIG / f"fpca_curve_change_{tech.replace(' ', '_')}.pdf")
    plt.close(fig)


def write_tex_table(tech: str, results: dict):
    if results is None:
        return
    rows = []
    rows.append("% auto-built by fpca_score_regressions.py")
    rows.append("% Reference: 3-sess × Critical, coefficient = 0")
    rows.append(r"\begin{tabular}{l r r r r r r r r r r r r r r r}")
    rows.append(r"\toprule")
    header = "Regime & " + " & ".join([f"\\multicolumn{{5}}{{c}}{{{hc}}}" for hc in HOUR_CLASSES])
    rows.append(header + r" \\")
    rows.append(r"\cmidrule(lr){2-6}\cmidrule(lr){7-11}\cmidrule(lr){12-16}")
    sub = " & " + " & ".join([f"PC{k+1}" for k in range(N_PCS)] * 3) + r" \\"
    rows.append(sub)
    rows.append(r"\midrule")
    for r in REGIMES_ORDERED:
        row = [r]
        for hc in HOUR_CLASSES:
            for k in range(N_PCS):
                v = results[f"PC{k+1}"].get((r, hc), np.nan)
                row.append(f"{v:.1f}" if not np.isnan(v) else "---")
        rows.append(" & ".join(row) + r" \\")
    rows.append(r"\bottomrule")
    rows.append(r"\end{tabular}")
    out = TEX / f"tab_fpca_{tech.replace(' ', '_')}.tex"
    out.write_text("\n".join(rows))
    print(f"  wrote {out}")


def main():
    for tech in TECHS:
        results = fit_per_tech(tech)
        plot_eigenfunctions(tech)
        plot_curve_change(tech, results)
        write_tex_table(tech, results)
    print("\nAll fPCA regressions done.")


if __name__ == "__main__":
    main()
