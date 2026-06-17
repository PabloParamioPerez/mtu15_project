# STATUS: ALIVE
# LAST-AUDIT: 2026-05-18
# FEEDS: descriptive_facts.tex §3.4 (fPCA descriptive of bid-shape changes)
# CLAIM: For each technology, build per-cell (or per-firm-cell for high-cardinality RES)
#        quantile representation of the bid supply curve, fit fPCA, project all
#        curves onto the basis. Output: per-cell PC scores parquet for Step 4
#        (score regressions).
#
# Memory-conscious design: process one tech at a time; chunked I/O; float32;
#   DuckDB for the heavy joins; pandas/numpy only for curve construction.
#
# Strategic techs (per-unit): CCGT, Hydro, Hydro_pump, Nuclear
# Non-strategic techs (per-firm): Wind, Solar PV, Cogen, Solar Thermal
#
# Output:
#   results/regressions/bid/fpca/
#     quantile_curves_<tech>.parquet      one row per (entity, date, period), 99 quantile cols
#     pc_basis_<tech>.npz                 eigenfunctions, mean curve, explained variance
#     pc_scores_<tech>.parquet            one row per (entity, date, period), 5 PC scores

from __future__ import annotations
from pathlib import Path
import duckdb
import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
OUT  = REPO / "results" / "regressions" / "bid" / "fpca"
OUT.mkdir(parents=True, exist_ok=True)

DET = REPO / "data/processed/omie/mercado_diario/ofertas/det_all.parquet"
CAB = REPO / "data/processed/omie/mercado_diario/ofertas/cab_all.parquet"
MP  = REPO / "data/processed/omie/mercado_diario/precios/marginalpdbc_all.parquet"
UNITS = REPO / "data/external/omie_reference/lista_unidades.csv"

START = "2024-06-14"
END   = "2026-01-31"
N_QUANTILES = 99      # grid 0.01, 0.02, ..., 0.99
N_PCS = 5
SAMPLE_PER_STRATUM = 1000   # for fPCA basis fitting

STRATEGIC = ["CCGT", "Hydro", "Hydro_pump", "Nuclear"]
NON_STRATEGIC = ["Wind", "Solar PV", "Cogen", "Solar Thermal"]


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


def load_units():
    raw = pd.read_csv(UNITS)
    raw["tech_group"] = raw["technology"].apply(map_tech)
    raw["firm"] = raw["owner_agent"].apply(map_firm)
    return raw[["unit_code", "tech_group", "firm"]].drop_duplicates("unit_code")


def build_quantile_curves(tech: str, per_firm: bool):
    """Materialise per-(entity, date, period) supply curve, compute 99-point quantile.
    entity = unit_code (strategic) or firm (aggregated).
    """
    print(f"\n=== Building quantile curves for {tech} (per_firm={per_firm}) ===")
    out_path = OUT / f"quantile_curves_{tech.replace(' ', '_')}.parquet"
    if out_path.exists():
        print(f"  {out_path} exists, skip")
        return out_path

    units = load_units()
    units_tech = units[units["tech_group"] == tech].copy()
    if len(units_tech) == 0:
        print(f"  no units for {tech}")
        return None
    print(f"  {len(units_tech)} units in {tech}")

    con = duckdb.connect()
    con.execute("PRAGMA threads=4")
    con.execute("SET memory_limit='4GB'")
    con.register("uft", units_tech[["unit_code", "tech_group", "firm"]])

    entity_col = "firm" if per_firm else "unit_code"

    # Materialise bid stack at the (entity, date, period) level
    sql = f"""
    WITH cab AS (
        SELECT date::DATE AS d, offer_code, version, unit_code,
               ROW_NUMBER() OVER (PARTITION BY date::DATE, offer_code, unit_code ORDER BY version DESC) AS rn
        FROM '{CAB}'
        WHERE buy_sell='V' AND date::DATE BETWEEN '{START}' AND '{END}'
    ),
    cab_l AS (SELECT * FROM cab WHERE rn=1),
    units_tech AS (SELECT * FROM uft),
    det AS (
        SELECT date::DATE AS d, offer_code, version, period,
               price_eur_mwh AS p, quantity_mw AS q
        FROM '{DET}'
        WHERE date::DATE BETWEEN '{START}' AND '{END}' AND quantity_mw > 0
    ),
    bids AS (
        SELECT c.d, c.unit_code, u.firm, dv.period, dv.p, dv.q
        FROM det dv
        JOIN cab_l c USING (d, offer_code, version)
        JOIN units_tech u USING (unit_code)
    )
    SELECT d AS date, period, {entity_col} AS entity, p, q
    FROM bids
    ORDER BY date, period, entity, p
    """
    print(f"  Materialising bid stack ({tech})...")
    df = con.execute(sql).df()
    print(f"  Got {len(df):,} rows")
    if len(df) == 0:
        return None
    df["date"] = pd.to_datetime(df["date"])
    # Reduce memory
    df["p"] = df["p"].astype(np.float32)
    df["q"] = df["q"].astype(np.float32)
    df["period"] = df["period"].astype(np.int16)
    df["entity"] = df["entity"].astype("category")

    # Group by (date, period, entity), compute quantile curve
    print(f"  Computing quantile curves (grid size = {N_QUANTILES})...")
    q_grid = np.linspace(1/(N_QUANTILES+1), N_QUANTILES/(N_QUANTILES+1), N_QUANTILES, dtype=np.float32)

    # Vectorized: for each group, sort by price (already done by ORDER BY),
    # compute cumulative quantity, then interpolate to quantile grid.
    def quantile_curve(group):
        prices = group["p"].to_numpy()
        qty = group["q"].to_numpy()
        cum = np.cumsum(qty)
        total = cum[-1]
        if total <= 0:
            return np.zeros(N_QUANTILES, dtype=np.float32)
        targets = q_grid * total
        # Find price at each target (first cum >= target)
        idx = np.searchsorted(cum, targets, side="left")
        idx = np.clip(idx, 0, len(prices) - 1)
        return prices[idx]

    grouped = df.groupby(["date", "period", "entity"], observed=True, sort=False)
    print(f"  {grouped.ngroups:,} curves to build")

    # Process in chunks (~500k groups per chunk to stay under memory)
    keys = []
    curves = []
    n_processed = 0
    for (d, p, e), group in grouped:
        keys.append((d, p, e))
        curves.append(quantile_curve(group))
        n_processed += 1
        if n_processed % 500_000 == 0:
            print(f"    processed {n_processed:,} / {grouped.ngroups:,}")
    print(f"  done {n_processed:,}")

    # Build output DataFrame
    keys_arr = np.array(keys, dtype=object)
    curves_arr = np.stack(curves)
    cols = [f"q{int(round(q*100)):02d}" for q in q_grid]
    out_df = pd.DataFrame(curves_arr, columns=cols)
    out_df["date"] = pd.to_datetime(keys_arr[:, 0])
    out_df["period"] = keys_arr[:, 1].astype(np.int16)
    out_df["entity"] = keys_arr[:, 2].astype(str)
    out_df = out_df[["date", "period", "entity"] + cols]
    out_df.to_parquet(out_path, index=False)
    print(f"  wrote {out_path} ({len(out_df):,} rows, {out_path.stat().st_size/1e6:.1f} MB)")
    del df, out_df, keys, curves, curves_arr
    import gc; gc.collect()
    return out_path


def pca_svd(X: np.ndarray, n_components: int):
    """PCA via numpy SVD. Returns (mean, components, explained_variance_ratio).
    components shape (n_components, n_features); rows are eigenfunctions."""
    mean = X.mean(axis=0)
    Xc = X - mean
    U, s, Vt = np.linalg.svd(Xc, full_matrices=False)
    components = Vt[:n_components]
    total_var = (s ** 2).sum() / max(X.shape[0] - 1, 1)
    explained_var = (s[:n_components] ** 2) / max(X.shape[0] - 1, 1)
    explained_var_ratio = explained_var / total_var
    return mean.astype(np.float32), components.astype(np.float32), explained_var_ratio.astype(np.float32)


def fit_fpca(tech: str):
    print(f"\n=== fPCA for {tech} ===")
    qpath = OUT / f"quantile_curves_{tech.replace(' ', '_')}.parquet"
    if not qpath.exists():
        print(f"  {qpath} missing, skip")
        return

    qcols = [f"q{i:02d}" for i in range(1, 100)]
    df = pd.read_parquet(qpath, columns=["date", "period", "entity"] + qcols)
    df["date"] = pd.to_datetime(df["date"])

    def regime(d):
        if d < pd.Timestamp("2024-06-14"): return "pre-IDA"
        if d <= pd.Timestamp("2024-11-30"): return "3-sess"
        if d <= pd.Timestamp("2025-03-18"): return "ISP15-win"
        if d <= pd.Timestamp("2025-04-27"): return "DA60/ID15 pre-blk"
        if d <= pd.Timestamp("2025-09-30"): return "DA60/ID15 post-blk"
        return "DA15/ID15"
    df["regime"] = df["date"].apply(regime)
    df["ym"] = df["date"].dt.strftime("%Y-%m")
    sample = (df.groupby(["regime", "ym"], group_keys=False)
                .apply(lambda g: g.sample(min(SAMPLE_PER_STRATUM, len(g)), random_state=42)))
    print(f"  Sample for fPCA: {len(sample):,} curves (from total {len(df):,})")

    X_sample = sample[qcols].to_numpy()
    mean, components, evr = pca_svd(X_sample, N_PCS)
    print(f"  Explained variance ratios: {evr.round(3).tolist()}")
    print(f"  Cumulative: {evr.cumsum().round(3).tolist()}")

    np.savez(OUT / f"pc_basis_{tech.replace(' ', '_')}.npz",
             mean=mean, components=components, explained_variance_ratio=evr)

    print(f"  Projecting all {len(df):,} curves...")
    Xc = df[qcols].to_numpy() - mean
    scores = Xc @ components.T
    out = df[["date", "period", "entity", "regime", "ym"]].copy()
    for k in range(N_PCS):
        out[f"PC{k+1}"] = scores[:, k].astype(np.float32)
    out_path = OUT / f"pc_scores_{tech.replace(' ', '_')}.parquet"
    out.to_parquet(out_path, index=False)
    print(f"  wrote {out_path} ({len(out):,} rows)")

    del df, sample, X_sample, scores, out, Xc
    import gc; gc.collect()


def main():
    for tech in STRATEGIC:
        build_quantile_curves(tech, per_firm=False)
        fit_fpca(tech)
    for tech in NON_STRATEGIC:
        build_quantile_curves(tech, per_firm=True)
        fit_fpca(tech)
    print("\nAll techs done.")


if __name__ == "__main__":
    main()
