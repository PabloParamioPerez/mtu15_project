# STATUS: ALIVE
# LAST-AUDIT: 2026-05-19
# FEEDS: descriptive_facts.tex §3.4 (fPCA descriptive of bid-shape changes).
# CLAIM: Functional pre-deseasonalisation of the per-(entity, date, period)
#        quantile bid-curves BEFORE PCA. For each tech and each quantile-grid
#        column q_j independently, fit
#            f_i(q_j) = alpha(q_j)
#                     + sum_r beta_r(q_j) * 1{regime(t_i) = r}
#                     + sum_{k=1..K} [a_k(q_j) c_k(t_i) + b_k(q_j) s_k(t_i)]
#                     + sum_{j=1..6} delta_j(q_j) * 1{dow(t_i) = j}
#                     + e_i(q_j),
#        and subtract the Fourier + DOW fitted part (NOT the regime part) from
#        every curve. The deseasonalised curves f_i^SA(q) keep regime variation
#        intact; PCA on f_i^SA delivers eigenfunctions orthogonal to the calendar
#        cycle by construction, so PC1 captures reform-driven shape variation
#        rather than mixing it with seasonal-cycle level shifts.
#
# Then refit PCA on a stratified sample of deseasonalised curves and project all
# curves onto the new basis.
#
# Output:
#   results/regressions/bid/fpca/
#     quantile_curves_<tech>_sa.parquet     deseasonalised curves
#     pc_basis_<tech>_sa.npz                mean (~0 by construction), eigenfunctions, EVR
#     pc_scores_<tech>_sa.parquet           projected scores on SA basis

from __future__ import annotations
from pathlib import Path
import gc

import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
IN   = REPO / "results" / "regressions" / "bid" / "fpca"

K_HARMONICS = 4   # match the scalar SA helper
N_PCS = 5
N_QUANTILES = 99
SAMPLE_PER_STRATUM = 1000

REGIME_DATES = [
    ("3sess",         pd.Timestamp("2024-06-14"), pd.Timestamp("2024-11-30")),
    ("ISP15win",      pd.Timestamp("2024-12-01"), pd.Timestamp("2025-03-18")),
    ("MTU15IDA_pre",  pd.Timestamp("2025-03-19"), pd.Timestamp("2025-04-27")),
    ("MTU15IDA_post", pd.Timestamp("2025-04-28"), pd.Timestamp("2025-09-30")),
    ("DA15_ID15",     pd.Timestamp("2025-10-01"), pd.Timestamp("2026-05-15")),
]

STRATEGIC = ["CCGT", "Hydro", "Hydro_pump", "Nuclear"]
NON_STRATEGIC = ["Wind", "Solar PV", "Cogen", "Solar Thermal"]


def regime_of(d):
    for label, lo, hi in REGIME_DATES:
        if lo <= d <= hi:
            return label
    return "preIDA"


def design_matrix(dates: pd.Series) -> tuple[np.ndarray, list[str]]:
    """Build [regime dummies | cos_k, sin_k | DOW dummies] design columns.

    Returns (X_full, seasonal_cols) where seasonal_cols indexes the Fourier+DOW
    columns we will strip after fitting (the regime part stays in the curve).
    """
    dates = pd.to_datetime(dates)
    doy = dates.dt.dayofyear.to_numpy()
    dow = dates.dt.dayofweek.to_numpy()
    n = len(dates)

    cols = []
    names = []
    # Regime dummies (pre-IDA is omitted)
    for label, lo, hi in REGIME_DATES:
        in_r = ((dates >= lo) & (dates <= hi)).to_numpy().astype(np.float32)
        cols.append(in_r)
        names.append(f"D_{label}")
    # Fourier basis on doy
    fourier_idx_start = len(cols)
    for k in range(1, K_HARMONICS + 1):
        cols.append(np.cos(2 * np.pi * k * doy / 365.0).astype(np.float32))
        names.append(f"cos_{k}")
        cols.append(np.sin(2 * np.pi * k * doy / 365.0).astype(np.float32))
        names.append(f"sin_{k}")
    # DOW dummies (Mon=0 omitted)
    dow_idx_start = len(cols)
    for j in range(1, 7):
        cols.append((dow == j).astype(np.float32))
        names.append(f"dow_{j}")

    X = np.column_stack(cols).astype(np.float32)
    # Prepend intercept
    X = np.column_stack([np.ones(n, dtype=np.float32), X])
    names = ["const"] + names

    seasonal_cols = [i + 1 for i in range(fourier_idx_start, len(names) - 1)]
    return X, names, seasonal_cols


def fit_per_grid_point(X: np.ndarray, Y: np.ndarray) -> np.ndarray:
    """Fit OLS y ~ X for each column of Y simultaneously. Returns (p x J)."""
    # Use lstsq (SVD-based, numerically stable) in float64. The normal-equations
    # path blows up with our 99-column Y and ~20-column near-collinear DOW/Fourier
    # design, producing coefficient magnitudes of order 1e9.
    X64 = X.astype(np.float64)
    Y64 = Y.astype(np.float64)
    beta, _, _, _ = np.linalg.lstsq(X64, Y64, rcond=None)
    return beta  # shape (p, J)


def deseasonalise_curves(df: pd.DataFrame, qcols: list[str]) -> pd.DataFrame:
    """For each grid column q_j, fit Fourier+DOW+regime regression on the FULL
    set of curves; strip the Fourier+DOW fitted part. Returns a new DataFrame
    with the SA quantile columns plus the original (date, period, entity) keys.
    """
    print(f"  designing per-grid-point regression on {len(df):,} rows...")
    X, names, seasonal_cols = design_matrix(df["date"])
    Y = df[qcols].to_numpy(dtype=np.float32)
    print(f"  X shape {X.shape}, Y shape {Y.shape}; solving {len(qcols)} columns simultaneously...")
    beta = fit_per_grid_point(X, Y)  # (p, J)
    # Sanity-check that the fitted seasonal coefficients are on the price scale
    seas_abs = np.abs(beta[seasonal_cols, :]).max()
    if seas_abs > 1e6:
        raise RuntimeError(
            f"Seasonal coefficient blew up (max |b| = {seas_abs:.3g}); "
            "design matrix is ill-conditioned, refusing to write SA curves."
        )
    # Seasonal-only fitted part: contribution of Fourier + DOW columns
    X_seas = X[:, seasonal_cols].astype(np.float64)
    beta_seas = beta[seasonal_cols, :]  # (n_seas, J)
    Y_seas_fit = (X_seas @ beta_seas).astype(np.float32)
    Y_sa = Y - Y_seas_fit
    print(f"  max |seasonal coef| = {seas_abs:.2f}; SA Y range "
          f"[{Y_sa.min():.1f}, {Y_sa.max():.1f}]")
    out = df[["date", "period", "entity"]].copy()
    out[qcols] = Y_sa.astype(np.float32)
    return out


def pca_svd(X: np.ndarray, n_components: int):
    mean = X.mean(axis=0)
    Xc = X - mean
    U, s, Vt = np.linalg.svd(Xc, full_matrices=False)
    components = Vt[:n_components]
    total_var = (s ** 2).sum() / max(X.shape[0] - 1, 1)
    explained_var = (s[:n_components] ** 2) / max(X.shape[0] - 1, 1)
    return mean.astype(np.float32), components.astype(np.float32), (explained_var / total_var).astype(np.float32)


def run_tech(tech: str):
    print(f"\n=== Functional-SA fPCA for {tech} ===")
    in_path = IN / f"quantile_curves_{tech.replace(' ', '_')}.parquet"
    if not in_path.exists():
        print(f"  {in_path} missing; run fpca_per_tech.py first")
        return

    qcols = [f"q{i:02d}" for i in range(1, N_QUANTILES + 1)]
    df = pd.read_parquet(in_path, columns=["date", "period", "entity"] + qcols)
    df["date"] = pd.to_datetime(df["date"])
    df = df[(df["date"] >= pd.Timestamp("2024-06-14"))
            & (df["date"] <= pd.Timestamp("2026-05-15"))].reset_index(drop=True)
    print(f"  loaded {len(df):,} curves")

    df_sa = deseasonalise_curves(df, qcols)
    sa_path = IN / f"quantile_curves_{tech.replace(' ', '_')}_sa.parquet"
    df_sa.to_parquet(sa_path, index=False)
    print(f"  wrote {sa_path}")

    # Stratified sample for PCA basis fitting
    df_sa["regime"] = df_sa["date"].apply(regime_of)
    df_sa["ym"] = df_sa["date"].dt.strftime("%Y-%m")
    sample = (df_sa.groupby(["regime", "ym"], group_keys=False)
                 .apply(lambda g: g.sample(min(SAMPLE_PER_STRATUM, len(g)), random_state=42)))
    print(f"  SA-sample for PCA: {len(sample):,} curves")

    X_sample = sample[qcols].to_numpy(dtype=np.float32)
    mean, components, evr = pca_svd(X_sample, N_PCS)
    print(f"  Explained variance ratios: {evr.round(3).tolist()}")
    print(f"  Cumulative: {evr.cumsum().round(3).tolist()}")

    np.savez(IN / f"pc_basis_{tech.replace(' ', '_')}_sa.npz",
             mean=mean, components=components, explained_variance_ratio=evr)

    print(f"  Projecting all {len(df_sa):,} SA curves...")
    Xc = df_sa[qcols].to_numpy(dtype=np.float32) - mean
    scores = Xc @ components.T
    out = df_sa[["date", "period", "entity", "regime", "ym"]].copy()
    for k in range(N_PCS):
        out[f"PC{k+1}"] = scores[:, k].astype(np.float32)
    out_path = IN / f"pc_scores_{tech.replace(' ', '_')}_sa.parquet"
    out.to_parquet(out_path, index=False)
    print(f"  wrote {out_path}")

    del df, df_sa, sample, X_sample, scores, out, Xc
    gc.collect()


def main():
    for tech in STRATEGIC + NON_STRATEGIC:
        run_tech(tech)
    print("\nAll techs done.")


if __name__ == "__main__":
    main()
