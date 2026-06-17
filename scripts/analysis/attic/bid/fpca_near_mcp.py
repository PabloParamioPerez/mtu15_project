# STATUS: ALIVE
# LAST-AUDIT: 2026-05-19
# FEEDS: descriptive_facts.tex §3.4 (near-MCP fPCA companion to the full-curve
#        fPCA).
# CLAIM: Restrict each DA bid stack to tranches with |p - MCP| <= H (in EUR/MWh),
#        re-parameterise as a 99-point quantile curve in MCP-centred price
#        coordinates (delta_p = p - MCP), then apply the SAME functional-SA +
#        PCA + pairwise pipeline as the full-curve fPCA. Output curves live in
#        [-H, +H] EUR/MWh on the curve domain (instead of [0, 4000]); PCs
#        capture the bid SHAPE near the price-setting region rather than mixing
#        in the scarcity-tail level.
#
# Rationale (Sec.~ref{sec:prov:fpca}, paragraph "Near-MCP fPCA"): the full-curve
# fPCA has PC1 dominated by the 0-to-cap absolute-price dispersion, which is
# economically the strategic-withholding margin (parking unused MW at 4000).
# The near-MCP fPCA isolates the price-setting margin (how the unit's MW are
# distributed in the band where its bid actually affects the clearing).
#
# Pipeline (one script):
#   1. Build per-(unit/firm, date, period) near-MCP quantile curves per tech
#   2. Functional pre-deseasonalisation (per-grid-point Fourier + DOW + regime
#      regression; strip the seasonal-only fitted part)
#   3. fPCA via SVD on the SA curves; project all curves
#   4. Pairwise reform-window score regression (no Fourier/DOW since absorbed
#      at curve level)
#   5. Write tables (PC1 across hour-classes; PC1+PC2+PC3 across all techs;
#      EVR raw vs SA)
#
# Output: results/regressions/bid/fpca/
#   quantile_curves_<tech>_nearmcp_H<H>.parquet
#   quantile_curves_<tech>_nearmcp_H<H>_sa.parquet
#   pc_basis_<tech>_nearmcp_H<H>.npz / _sa.npz
#   pc_scores_<tech>_nearmcp_H<H>.parquet / _sa.parquet
#   coeffs_pairwise_nearmcp_H<H>_sa.csv
#   tex/tab_fpca_pairwise_<reform>_nearmcp_H<H>_sa.tex (+ _pc123)
#   tex/tab_fpca_evr_nearmcp_H<H>_raw_vs_sa.tex

from __future__ import annotations
from pathlib import Path
import gc

import duckdb
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

REPO = Path(__file__).resolve().parents[3]
DET = REPO / "data/processed/omie/mercado_diario/ofertas/det_all.parquet"
CAB = REPO / "data/processed/omie/mercado_diario/ofertas/cab_all.parquet"
MP  = REPO / "data/processed/omie/mercado_diario/precios/marginalpdbc_all.parquet"
UNITS = REPO / "data/external/omie_reference/lista_unidades.csv"
OUT  = REPO / "results/regressions/bid/fpca"
TEX  = OUT / "tex"
FIG  = REPO / "figures/working"

OUT.mkdir(parents=True, exist_ok=True)
TEX.mkdir(parents=True, exist_ok=True)
FIG.mkdir(parents=True, exist_ok=True)

# Configuration
H = 50.0                       # bandwidth in EUR/MWh around MCP (matches in-band kernel)
N_QUANTILES = 99
N_PCS = 5
SAMPLE_PER_STRATUM = 1000
K_HARMONICS = 4
MIN_IN_BAND_TRANCHES = 3

START = "2024-06-14"
END   = "2026-05-15"

STRATEGIC = ["CCGT", "Hydro", "Hydro_pump", "Nuclear"]
NON_STRATEGIC = ["Wind", "Solar PV", "Cogen", "Solar Thermal"]
TECHS = STRATEGIC + NON_STRATEGIC

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


# ============================================================================
# Tech / firm mapping
# ============================================================================

def map_tech(s):
    if not isinstance(s, str): return "Other"
    t = s.lower()
    if "ciclo combinado" in t: return "CCGT"
    if "nuclear" in t: return "Nuclear"
    if "bombeo mixto" in t or "consumo bombeo" in t: return "Pump_load"
    if "bombeo puro" in t or ("bombeo" in t and "turb" in t): return "Hydro_pump"
    if "hidráulica generación" in t: return "Hydro"
    if "re mercado hidráulica" in t: return "Hydro_RES"
    if "re mercado eólica" in t: return "Wind"
    if "re mercado solar fotovolt" in t: return "Solar PV"
    if "re mercado solar térmica" in t: return "Solar Thermal"
    if "re mercado térmica no renovab" in t: return "Cogen"
    if "comercializador" in t: return "Retailer"
    return "Other"


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


def hour_class(h):
    if h in (5, 6, 7, 8, 16, 17, 18, 19, 20, 21, 22): return "Critical"
    if h in (1, 2, 3): return "Flat"
    if h in (11, 12, 13, 14): return "Midday"
    return "Dropped"


def load_units():
    raw = pd.read_csv(UNITS)
    raw["tech_group"] = raw["technology"].apply(map_tech)
    raw["firm"] = raw["owner_agent"].apply(map_firm)
    return raw[["unit_code", "tech_group", "firm"]].drop_duplicates("unit_code")


# ============================================================================
# Step 1: Build near-MCP quantile curves per tech
# ============================================================================

def build_near_mcp_curves(tech: str, per_firm: bool):
    """Build per-(entity, date, period) 99-point quantile curves restricted to
    |p - MCP| <= H, in MCP-centred price coordinates (delta_p).
    """
    suffix = f"_nearmcp_H{int(H)}"
    out_path = OUT / f"quantile_curves_{tech.replace(' ', '_')}{suffix}.parquet"
    if out_path.exists():
        print(f"  {out_path.name} exists, skip")
        return out_path

    print(f"\n=== Build near-MCP curves for {tech} (per_firm={per_firm}, H={H}) ===")
    units = load_units()
    units_tech = units[units["tech_group"] == tech].copy()
    if len(units_tech) == 0:
        print(f"  no units for {tech}")
        return None
    print(f"  {len(units_tech)} units")

    con = duckdb.connect()
    con.execute("PRAGMA threads=4")
    con.execute("SET memory_limit='6GB'")
    con.register("uft", units_tech[["unit_code", "tech_group", "firm"]])

    entity_col = "firm" if per_firm else "unit_code"

    sql = f"""
    WITH cab AS (
      SELECT date::DATE AS d, offer_code, version, unit_code,
             ROW_NUMBER() OVER (PARTITION BY date::DATE, offer_code, unit_code
                                ORDER BY version DESC) AS rn
      FROM '{CAB}'
      WHERE buy_sell='V' AND date::DATE BETWEEN '{START}' AND '{END}'
    ),
    cab_l AS (SELECT * FROM cab WHERE rn=1),
    units_tech AS (SELECT * FROM uft),
    det AS (
      SELECT date::DATE AS d, offer_code, version, period,
             price_eur_mwh AS p, quantity_mw AS q, mtu_minutes
      FROM '{DET}'
      WHERE date::DATE BETWEEN '{START}' AND '{END}' AND quantity_mw > 0
    ),
    mp AS (
      SELECT date::DATE AS d, period, price_es_eur_mwh AS p_clear
      FROM '{MP}'
      WHERE date::DATE BETWEEN '{START}' AND '{END}'
        AND price_es_eur_mwh IS NOT NULL
    ),
    bids AS (
      SELECT c.d, c.unit_code, u.firm, dv.period, dv.p, dv.q, mp.p_clear
      FROM det dv
      JOIN cab_l c USING (d, offer_code, version)
      JOIN units_tech u USING (unit_code)
      JOIN mp ON mp.d = dv.d AND mp.period = dv.period
    ),
    bids_band AS (
      SELECT d AS date, period, {entity_col} AS entity,
             p - p_clear AS dp,
             q
      FROM bids
      WHERE p BETWEEN p_clear - {H} AND p_clear + {H}
    )
    SELECT date, period, entity, dp, q
    FROM bids_band
    ORDER BY date, period, entity, dp
    """
    print(f"  materialising in-band bid stack ({tech})...")
    df = con.execute(sql).df()
    print(f"  {len(df):,} in-band tranche rows")
    if len(df) == 0:
        return None
    df["date"] = pd.to_datetime(df["date"])
    df["dp"] = df["dp"].astype(np.float32)
    df["q"] = df["q"].astype(np.float32)
    df["period"] = df["period"].astype(np.int16)
    df["entity"] = df["entity"].astype("category")

    # Filter cells with >= MIN_IN_BAND_TRANCHES tranches
    counts = df.groupby(["date", "period", "entity"], observed=True, sort=False).size()
    keep = counts[counts >= MIN_IN_BAND_TRANCHES].index
    df = df.set_index(["date", "period", "entity"]).loc[keep].reset_index()
    print(f"  after >= {MIN_IN_BAND_TRANCHES}-tranche filter: {len(df):,} rows, "
          f"{df.groupby(['date','period','entity'], observed=True, sort=False).ngroups:,} cells")
    if len(df) == 0:
        return None

    # Build 99-point quantile curve on band MW
    q_grid = np.linspace(1/(N_QUANTILES+1), N_QUANTILES/(N_QUANTILES+1),
                         N_QUANTILES, dtype=np.float32)

    def quantile_curve(group):
        dps = group["dp"].to_numpy()
        qty = group["q"].to_numpy()
        cum = np.cumsum(qty)
        total = cum[-1]
        if total <= 0:
            return np.full(N_QUANTILES, np.nan, dtype=np.float32)
        targets = q_grid * total
        idx = np.searchsorted(cum, targets, side="left")
        idx = np.clip(idx, 0, len(dps) - 1)
        return dps[idx]

    grouped = df.groupby(["date", "period", "entity"], observed=True, sort=False)
    print(f"  {grouped.ngroups:,} curves to build")
    keys, curves = [], []
    for (d, p, e), group in grouped:
        keys.append((d, p, e))
        curves.append(quantile_curve(group))
    keys_arr = np.array(keys, dtype=object)
    curves_arr = np.stack(curves)
    cols = [f"q{int(round(q*100)):02d}" for q in q_grid]
    out_df = pd.DataFrame(curves_arr, columns=cols)
    out_df["date"] = pd.to_datetime(keys_arr[:, 0])
    out_df["period"] = keys_arr[:, 1].astype(np.int16)
    out_df["entity"] = keys_arr[:, 2].astype(str)
    out_df = out_df[["date", "period", "entity"] + cols]
    out_df.to_parquet(out_path, index=False)
    print(f"  wrote {out_path.name} ({len(out_df):,} rows, {out_path.stat().st_size/1e6:.1f} MB)")
    del df, out_df, keys, curves, curves_arr
    gc.collect()
    return out_path


# ============================================================================
# Step 2 + 3: Functional SA + PCA on near-MCP curves
# ============================================================================

def regime_of(d):
    for label, lo, hi in REGIME_DATES:
        if lo <= d <= hi:
            return label
    return "preIDA"


def design_matrix(dates: pd.Series):
    dates = pd.to_datetime(dates)
    doy = dates.dt.dayofyear.to_numpy()
    dow = dates.dt.dayofweek.to_numpy()
    cols, names = [], []
    for label, lo, hi in REGIME_DATES:
        in_r = ((dates >= lo) & (dates <= hi)).to_numpy().astype(np.float32)
        cols.append(in_r); names.append(f"D_{label}")
    fourier_idx_start = len(cols)
    for k in range(1, K_HARMONICS + 1):
        cols.append(np.cos(2 * np.pi * k * doy / 365.0).astype(np.float32)); names.append(f"cos_{k}")
        cols.append(np.sin(2 * np.pi * k * doy / 365.0).astype(np.float32)); names.append(f"sin_{k}")
    for j in range(1, 7):
        cols.append((dow == j).astype(np.float32)); names.append(f"dow_{j}")
    X = np.column_stack(cols).astype(np.float32)
    X = np.column_stack([np.ones(len(dates), dtype=np.float32), X])
    names = ["const"] + names
    seasonal_cols = [i + 1 for i in range(fourier_idx_start, len(names) - 1)]
    return X, names, seasonal_cols


def fit_and_strip_seasonal(X, Y):
    X64 = X.astype(np.float64); Y64 = Y.astype(np.float64)
    beta, *_ = np.linalg.lstsq(X64, Y64, rcond=None)
    return beta


def pca_svd(X, n_components):
    mean = X.mean(axis=0)
    Xc = X - mean
    _, s, Vt = np.linalg.svd(Xc, full_matrices=False)
    components = Vt[:n_components]
    total = (s**2).sum() / max(X.shape[0]-1, 1)
    evr = (s[:n_components]**2 / max(X.shape[0]-1, 1)) / total
    return mean.astype(np.float32), components.astype(np.float32), evr.astype(np.float32)


def sa_and_pca(tech: str):
    """Functional SA + PCA on near-MCP curves; writes both raw and SA basis + scores."""
    suffix = f"_nearmcp_H{int(H)}"
    qpath = OUT / f"quantile_curves_{tech.replace(' ', '_')}{suffix}.parquet"
    if not qpath.exists():
        return
    qcols = [f"q{i:02d}" for i in range(1, N_QUANTILES + 1)]
    df = pd.read_parquet(qpath, columns=["date", "period", "entity"] + qcols)
    df["date"] = pd.to_datetime(df["date"])
    df = df[(df["date"] >= pd.Timestamp(START)) & (df["date"] <= pd.Timestamp(END))]
    df = df.dropna(subset=qcols).reset_index(drop=True)
    if len(df) == 0:
        return
    print(f"\n=== SA + PCA for {tech} (near-MCP, H={H}) === n={len(df):,}")

    # Raw PCA (no SA) for the EVR comparison
    df["regime"] = df["date"].apply(regime_of)
    df["ym"] = df["date"].dt.strftime("%Y-%m")
    sample = (df.groupby(["regime", "ym"], group_keys=False)
                .apply(lambda g: g.sample(min(SAMPLE_PER_STRATUM, len(g)), random_state=42)))
    X_sample = sample[qcols].to_numpy(np.float32)
    mean_raw, comp_raw, evr_raw = pca_svd(X_sample, N_PCS)
    np.savez(OUT / f"pc_basis_{tech.replace(' ', '_')}{suffix}.npz",
             mean=mean_raw, components=comp_raw, explained_variance_ratio=evr_raw)
    print(f"  raw EVR: {[round(float(x), 3) for x in evr_raw]}")

    # Functional SA: per-grid-point Fourier + DOW + regime regression; strip seasonal part
    X, _, seasonal_cols = design_matrix(df["date"])
    Y = df[qcols].to_numpy(np.float32)
    beta = fit_and_strip_seasonal(X, Y)
    seas_abs = np.abs(beta[seasonal_cols, :]).max()
    if seas_abs > 1e4:  # near-MCP curves are in [-H, +H], so coefficients should be O(10)
        raise RuntimeError(
            f"Seasonal coefficient blew up (max |b| = {seas_abs:.3g}); design ill-conditioned"
        )
    Y_seas = (X[:, seasonal_cols].astype(np.float64) @ beta[seasonal_cols, :]).astype(np.float32)
    Y_sa = Y - Y_seas
    print(f"  max |seasonal coef| = {seas_abs:.2f}; SA range [{Y_sa.min():.1f}, {Y_sa.max():.1f}]")
    df_sa = df[["date", "period", "entity", "regime", "ym"]].copy()
    df_sa[qcols] = Y_sa
    df_sa.to_parquet(OUT / f"quantile_curves_{tech.replace(' ', '_')}{suffix}_sa.parquet", index=False)

    # PCA on SA curves
    sample_sa = (df_sa.groupby(["regime", "ym"], group_keys=False)
                       .apply(lambda g: g.sample(min(SAMPLE_PER_STRATUM, len(g)), random_state=42)))
    X_sample_sa = sample_sa[qcols].to_numpy(np.float32)
    mean_sa, comp_sa, evr_sa = pca_svd(X_sample_sa, N_PCS)
    np.savez(OUT / f"pc_basis_{tech.replace(' ', '_')}{suffix}_sa.npz",
             mean=mean_sa, components=comp_sa, explained_variance_ratio=evr_sa)
    print(f"  SA EVR:  {[round(float(x), 3) for x in evr_sa]}")

    # Project all SA curves
    Xc = df_sa[qcols].to_numpy(np.float32) - mean_sa
    scores = Xc @ comp_sa.T
    out = df_sa[["date", "period", "entity", "regime", "ym"]].copy()
    for k in range(N_PCS):
        out[f"PC{k+1}"] = scores[:, k].astype(np.float32)
    out.to_parquet(OUT / f"pc_scores_{tech.replace(' ', '_')}{suffix}_sa.parquet", index=False)


# ============================================================================
# Step 4: Pairwise regressions on SA scores
# ============================================================================

def prepare_panel(tech: str):
    suffix = f"_nearmcp_H{int(H)}"
    fp = OUT / f"pc_scores_{tech.replace(' ', '_')}{suffix}_sa.parquet"
    if not fp.exists():
        return None
    df = pd.read_parquet(fp)
    df["date"] = pd.to_datetime(df["date"])
    df["mtu_minutes"] = np.where(df["period"] > 24, 15, 60)
    df["hour"] = np.where(df["mtu_minutes"] == 15,
                          ((df["period"] - 1) // 4).astype(int),
                          (df["period"] - 1).astype(int))
    df["quarter"] = np.where(df["mtu_minutes"] == 15,
                             ((df["period"] - 1) % 4 + 1).astype(int),
                             1)
    df["hour_class"] = df["hour"].apply(hour_class)
    df = df[df["hour_class"].isin(HOUR_CLASSES)].copy()
    df["ym"] = df["date"].dt.strftime("%Y-%m")
    df["regime"] = df["regime"].astype(str)
    if "firm" not in df.columns:
        unit_map = load_units().set_index("unit_code")["firm"]
        df["firm"] = df["entity"].map(unit_map).fillna("OTH")
    return df


def run_pairwise_one_pc(df_sub, y_col, post_regime):
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


def fit_pairwise(tech: str):
    df = prepare_panel(tech)
    if df is None or len(df) == 0:
        return None
    print(f"\n=== Pairwise SA (near-MCP) {tech}: {len(df):,} rows ===")
    rows = []
    for reform, pre, post in PAIRS:
        df_pair = df[df["regime"].isin([pre, post])]
        if len(df_pair) == 0:
            continue
        for firm in sorted(df_pair["firm"].unique()):
            df_sub = df_pair[df_pair["firm"] == firm]
            if len(df_sub) < 50:
                continue
            for k in range(1, N_PCS + 1):
                c, cF, cM = run_pairwise_one_pc(df_sub, f"PC{k}", post)
                rows.append({"tech": tech, "firm": firm, "reform": reform,
                             "PC": f"PC{k}", "hour_class": "Critical",
                             "coef": c, "n_rows": len(df_sub)})
                rows.append({"tech": tech, "firm": firm, "reform": reform,
                             "PC": f"PC{k}", "hour_class": "Flat",
                             "coef": (c + cF) if not np.isnan(c) else np.nan,
                             "n_rows": len(df_sub)})
                rows.append({"tech": tech, "firm": firm, "reform": reform,
                             "PC": f"PC{k}", "hour_class": "Midday",
                             "coef": (c + cM) if not np.isnan(c) else np.nan,
                             "n_rows": len(df_sub)})
            print(f"  {reform} | {firm}: done")
    return pd.DataFrame(rows)


# ============================================================================
# Step 5: Tables
# ============================================================================

def write_tables(all_coef: pd.DataFrame):
    suffix = f"_nearmcp_H{int(H)}_sa"
    # PC1 across hour-classes, 4 strategic techs
    for reform, _, _ in PAIRS:
        sub = all_coef[(all_coef["reform"] == reform)
                       & (all_coef["tech"].isin(STRATEGIC))
                       & (all_coef["firm"].isin(FIRMS_FOCUS))
                       & (all_coef["PC"] == "PC1")]
        piv = sub.pivot_table(index=["tech", "firm"], columns="hour_class", values="coef").round(2)
        rows = [r"\begin{tabular}{l l r r r}", r"\toprule",
                r"Tech & Firm & Critical & Flat & Midday \\", r"\midrule"]
        for (tech, firm), r in piv.iterrows():
            cells = [tech.replace("_", " "), firm]
            for hc in HOUR_CLASSES:
                v = r.get(hc, np.nan) if isinstance(r, pd.Series) else np.nan
                cells.append(f"{v:+.2f}" if not pd.isna(v) else "---")
            rows.append(" & ".join(cells) + r" \\")
        rows.extend([r"\bottomrule", r"\end{tabular}"])
        out = TEX / f"tab_fpca_pairwise_{reform.replace('/', '_').replace(' ', '_')}{suffix}.tex"
        out.write_text(f"% Near-MCP fPCA, H={H}, functional-SA, PC1 across hour-classes; units EUR/MWh on MCP-centred curve\n"
                       + "\n".join(rows))
        print(f"  wrote {out.name}")

    # PC1+PC2+PC3 critical across price-setting techs only (CCGT + hydro variants)
    all_techs_order = ["CCGT", "Hydro", "Hydro_pump"]
    for reform, _, _ in PAIRS:
        sub = all_coef[(all_coef["reform"] == reform)
                       & (all_coef["hour_class"] == "Critical")
                       & (all_coef["n_rows"] >= 200)
                       & (all_coef["tech"].isin(all_techs_order))]
        piv = sub.pivot_table(index=["tech", "firm"], columns="PC", values="coef")
        for col in ["PC1", "PC2", "PC3"]:
            if col not in piv.columns:
                piv[col] = np.nan
        piv = piv[["PC1", "PC2", "PC3"]].round(2)
        rows = [r"\begin{tabular}{l l r r r}", r"\toprule",
                r"Tech & Firm & PC1 & PC2 & PC3 \\", r"\midrule"]
        last_tech = None
        for tech in all_techs_order:
            firms_here = sorted({f for (t, f) in piv.index if t == tech})
            for firm in firms_here:
                if (tech, firm) not in piv.index:
                    continue
                r = piv.loc[(tech, firm)]
                tech_label = tech.replace("_", " ") if tech != last_tech else ""
                last_tech = tech
                cells = [tech_label, firm]
                for pc in ["PC1", "PC2", "PC3"]:
                    v = r.get(pc, np.nan)
                    cells.append(f"{v:+.2f}" if not pd.isna(v) else "---")
                rows.append(" & ".join(cells) + r" \\")
            if tech != all_techs_order[-1]:
                rows.append(r"\addlinespace")
        rows.extend([r"\bottomrule", r"\end{tabular}"])
        out = TEX / f"tab_fpca_pairwise_{reform.replace('/', '_').replace(' ', '_')}{suffix}_pc123.tex"
        out.write_text(f"% Near-MCP fPCA, H={H}, functional-SA, PC1+PC2+PC3 critical-hour across all techs; units EUR/MWh\n"
                       + "\n".join(rows))
        print(f"  wrote {out.name}")

    # EVR raw-vs-SA (price-setting techs only)
    rows = [r"\begin{tabular}{l r r r r r r r r r r}", r"\toprule",
            r" & \multicolumn{5}{c}{Raw curves} & \multicolumn{5}{c}{Functional-SA curves} \\",
            r"\cmidrule(lr){2-6}\cmidrule(lr){7-11}",
            r"Tech & PC1 & PC2 & PC3 & PC4 & PC5 & PC1 & PC2 & PC3 & PC4 & PC5 \\", r"\midrule"]
    evr_techs = ["CCGT", "Hydro", "Hydro_pump"]
    for tech in evr_techs:
        suff = f"_nearmcp_H{int(H)}"
        raw_p = OUT / f"pc_basis_{tech.replace(' ', '_')}{suff}.npz"
        sa_p = OUT / f"pc_basis_{tech.replace(' ', '_')}{suff}_sa.npz"
        if not raw_p.exists() or not sa_p.exists():
            continue
        e_raw = np.load(raw_p)["explained_variance_ratio"]
        e_sa = np.load(sa_p)["explained_variance_ratio"]
        cells = [tech.replace("_", " ")]
        for arr in (e_raw, e_sa):
            for v in arr[:5]:
                cells.append(f"{float(v):.2f}")
        rows.append(" & ".join(cells) + r" \\")
    rows.extend([r"\bottomrule", r"\end{tabular}"])
    out = TEX / f"tab_fpca_evr_nearmcp_H{int(H)}_raw_vs_sa.tex"
    out.write_text(f"% Near-MCP fPCA H={H} explained-variance ratios, raw vs functional-SA\n" + "\n".join(rows))
    print(f"  wrote {out.name}")


# ============================================================================
# Eigenfunctions figure
# ============================================================================

def plot_eigenfunctions():
    suff = f"_nearmcp_H{int(H)}_sa"
    q_grid = np.linspace(1, 99, 99)
    colors = ["#d62728", "#1f77b4", "#2ca02c", "#ff7f0e", "#9467bd"]
    for tech in TECHS:
        p = OUT / f"pc_basis_{tech.replace(' ', '_')}{suff}.npz"
        if not p.exists():
            continue
        b = np.load(p)
        mean, comp, evr = b["mean"], b["components"], b["explained_variance_ratio"]
        fig, axes = plt.subplots(1, 2, figsize=(11, 4))
        axes[0].plot(q_grid, mean, color="black", lw=2)
        axes[0].axhline(0, color="grey", lw=0.5)
        axes[0].set_xlabel("Quantile of in-band MW (1-99)")
        axes[0].set_ylabel(r"$p - $MCP (EUR/MWh)")
        axes[0].set_title(f"Mean near-MCP SA bid curve, {tech.replace('_',' ')} (H={int(H)})")
        axes[0].grid(alpha=0.3)
        for k in range(min(3, comp.shape[0])):
            axes[1].plot(q_grid, comp[k], color=colors[k], label=f"PC{k+1} ({evr[k]*100:.0f}%)")
        axes[1].axhline(0, color="black", lw=0.5)
        axes[1].set_xlabel("Quantile of in-band MW (1-99)")
        axes[1].set_ylabel("Eigenfunction value")
        axes[1].set_title(f"First 3 eigenfunctions (near-MCP SA), {tech.replace('_',' ')}")
        axes[1].legend(loc="best", fontsize=9)
        axes[1].grid(alpha=0.3)
        fig.tight_layout()
        fig.savefig(FIG / f"fpca_eigenfuncs_{tech.replace(' ', '_')}_nearmcp_H{int(H)}_sa.pdf")
        plt.close(fig)


# ============================================================================
# Main
# ============================================================================

def main():
    print(f"=== Near-MCP fPCA pipeline (H = {H} EUR/MWh) ===")
    # Step 1: build curves
    for tech in STRATEGIC:
        build_near_mcp_curves(tech, per_firm=False)
    for tech in NON_STRATEGIC:
        build_near_mcp_curves(tech, per_firm=True)
    # Steps 2-3: SA + PCA
    for tech in TECHS:
        sa_and_pca(tech)
    # Step 4: pairwise
    all_rows = []
    for tech in TECHS:
        r = fit_pairwise(tech)
        if r is not None and len(r):
            all_rows.append(r)
    all_coef = pd.concat(all_rows, ignore_index=True)
    all_coef.to_csv(OUT / f"coeffs_pairwise_nearmcp_H{int(H)}_sa.csv", index=False)
    print(f"\nTotal pairwise rows: {len(all_coef):,}")
    # Step 5: tables
    write_tables(all_coef)
    plot_eigenfunctions()
    print("\nAll done.")


if __name__ == "__main__":
    main()
