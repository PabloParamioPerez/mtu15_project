# Seasonality-adjustment via Frisch-Waugh-Lovell pooled regression.
#
# Spec used across all SA-producing scripts in scripts/analysis/:
#
#   g(y_t) = alpha + sum_r beta_r * 1{regime(t)=r}
#                  + sum_{k=1..K} [a_k cos(2 pi k doy/365) + b_k sin(2 pi k doy/365)]
#                  + sum_{j=1..6} delta_j * 1{dow(t)=j}
#                  + gamma' * X_t                          # optional exogenous controls
#                  + eps_t
#
# - Link g: 'log' for positive levels, 'logit' for bounded shares, 'identity' otherwise.
# - SA reference: alpha + beta_r (annual-mean Fourier=0; within-week DOW mean baked in
#   by averaging DOW contributions; controls X at panel mean).
# - SA value: Duan smearing on the residuals for 'log' and 'logit', so
#       y_hat_r^{SA} = mean_i g^{-1}(alpha + beta_r + within-week DOW mean
#                                    + X_bar' gamma + e_hat_i),
#   which corrects the Jensen bias non-parametrically and is robust to non-normal
#   residuals.
# - No HAC SE: the spec is used as a point-estimation device for the SA value,
#   not for inference on regime contrasts. We surface OLS p-values for diagnostic
#   tagging only.
#
# Pooled seasonality across regimes is a constraint, not a choice: the 40-day
# MTU15-IDA pre-blk window cannot identify regime-specific seasonal coefficients.

from __future__ import annotations
from typing import Iterable, Literal, Sequence

import numpy as np
import pandas as pd
import statsmodels.api as sm

DEFAULT_K = 4


def fourier_terms(doy: np.ndarray, K: int = DEFAULT_K) -> pd.DataFrame:
    """K-harmonic Fourier basis on day-of-year with annual denominator 365."""
    doy = np.asarray(doy, dtype=float)
    return pd.DataFrame({
        **{f"cos_{k}": np.cos(2 * np.pi * k * doy / 365.0) for k in range(1, K + 1)},
        **{f"sin_{k}": np.sin(2 * np.pi * k * doy / 365.0) for k in range(1, K + 1)},
    })


def dow_dummies(dates: pd.Series) -> pd.DataFrame:
    """Six day-of-week dummies (Monday=baseline, omitted)."""
    dates = pd.to_datetime(dates)
    dow = dates.dt.dayofweek.values
    return pd.DataFrame({
        f"dow_{i}": (dow == i).astype(float) for i in range(1, 7)
    })


def attach_design_columns(
    df: pd.DataFrame,
    regime_dates: Sequence[tuple],
    K: int = DEFAULT_K,
    date_col: str = "d",
) -> pd.DataFrame:
    """Attach Fourier, DOW dummies, and regime indicators to a daily dataframe.

    regime_dates: iterable of (label, start_ts, end_ts) tuples. Extra tuple
    elements (e.g., a display label) are ignored.
    """
    out = df.copy()
    out[date_col] = pd.to_datetime(out[date_col])
    doy = out[date_col].dt.dayofyear
    fk = fourier_terms(doy.values, K)
    dw = dow_dummies(out[date_col])
    out = pd.concat([out.reset_index(drop=True), fk.reset_index(drop=True),
                     dw.reset_index(drop=True)], axis=1)
    for tup in regime_dates:
        label, lo, hi = tup[0], tup[1], tup[2]
        out[f"D_{label}"] = ((out[date_col] >= lo) & (out[date_col] <= hi)).astype(float)
    return out


def _inverse_link(transform: Literal["log", "logit", "identity"]):
    if transform == "log":
        return np.exp
    if transform == "logit":
        return lambda z: 1.0 / (1.0 + np.exp(-z))
    if transform == "identity":
        return lambda z: z
    raise ValueError(f"unknown transform {transform!r}")


def _forward_link(value: pd.Series, transform: Literal["log", "logit", "identity"]) -> pd.Series:
    if transform == "log":
        return np.log(value.clip(lower=1e-6))
    if transform == "logit":
        p = value.clip(0.001, 0.999).astype(float)
        return np.log(p / (1.0 - p))
    if transform == "identity":
        return value.astype(float)
    raise ValueError(f"unknown transform {transform!r}")


def fit_sa(
    df: pd.DataFrame,
    value_col: str,
    regime_dates: Sequence[tuple],
    transform: Literal["log", "logit", "identity"] = "log",
    K: int = DEFAULT_K,
    extra_cols: Iterable[str] | None = None,
    date_col: str = "d",
    min_obs: int = 100,
) -> dict | None:
    """Fit the SA spec and return per-regime raw + SA values.

    Returns a dict with:
      'n', 'R2',
      for each regime label L:
        'L_raw'  : raw mean of value_col over regime L (in-sample)
        'L_sa'   : seasonally adjusted prediction (Duan-smeared for log/logit)
        'L_beta' : regression coefficient on the regime dummy
        'L_p'    : OLS p-value on the regime dummy (diagnostic only)
      and 'baseline_sa' (the alpha-only SA prediction at average DOW, annual-mean
      Fourier, panel-mean controls).

    Pooled seasonality across regimes is a constraint imposed by identification:
    with the shortest regime window only 40 days long, regime-specific Fourier
    coefficients are not estimable.
    """
    extra_cols = list(extra_cols) if extra_cols else []
    fourier_cols = [f"{p}_{k}" for k in range(1, K + 1) for p in ("cos", "sin")]
    dow_cols = [f"dow_{i}" for i in range(1, 7)]
    regime_cols = [f"D_{tup[0]}" for tup in regime_dates]

    needed = fourier_cols + dow_cols + regime_cols + extra_cols + [value_col]
    missing = [c for c in needed if c not in df.columns]
    if missing:
        raise KeyError(f"fit_sa missing columns {missing!r}; "
                       "call attach_design_columns first and ensure extra_cols are present")

    d = df.dropna(subset=needed).reset_index(drop=True)
    if len(d) < min_obs:
        return None

    y = _forward_link(d[value_col], transform).astype(float)
    cols = regime_cols + fourier_cols + dow_cols + extra_cols
    X = sm.add_constant(d[cols].astype(float))
    fit = sm.OLS(y, X).fit()

    back = _inverse_link(transform)

    # Reference linear predictor at:
    #   annual-mean Fourier (zeros), within-week DOW mean (1/7 weight on each dummy),
    #   panel-mean extra controls.
    eta_ref = float(fit.params["const"])
    for c in dow_cols:
        if c in fit.params.index:
            eta_ref += float(fit.params[c]) * (1.0 / 7.0)
    for c in extra_cols:
        if c in fit.params.index:
            eta_ref += float(fit.params[c]) * float(d[c].mean())

    resid = (y - fit.predict(X)).values

    if transform in ("log", "logit"):
        # Duan smearing: average inverse-link of (eta + residual) across in-sample residuals.
        baseline_sa = float(np.mean(back(eta_ref + resid)))
    else:
        baseline_sa = float(back(eta_ref))

    # raw per-regime mean of the untransformed outcome
    d["_regime_label"] = ""
    for tup in regime_dates:
        label, lo, hi = tup[0], tup[1], tup[2]
        mask = (d[date_col] >= lo) & (d[date_col] <= hi)
        d.loc[mask, "_regime_label"] = label
    raw_means = d.groupby("_regime_label", observed=True)[value_col].mean()

    out: dict = {"n": int(len(d)), "R2": float(fit.rsquared), "baseline_sa": baseline_sa}
    for tup in regime_dates:
        label = tup[0]
        beta = float(fit.params.get(f"D_{label}", 0.0))
        pval = float(fit.pvalues.get(f"D_{label}", np.nan))
        if transform in ("log", "logit"):
            sa_val = float(np.mean(back(eta_ref + beta + resid)))
        else:
            sa_val = float(back(eta_ref + beta))
        out[f"{label}_raw"] = float(raw_means.get(label, np.nan))
        out[f"{label}_sa"] = sa_val
        out[f"{label}_beta"] = beta
        out[f"{label}_p"] = pval
    return out
