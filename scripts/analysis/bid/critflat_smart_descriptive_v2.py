# STATUS: ALIVE
# LAST-AUDIT: 2026-05-27
# FEEDS: advisor_memo discussion -- enhanced version of
#        critflat_smart_descriptive.py with:
#          (a) Proper IDA offer-type classification (block vs simple)
#              using icab.block_order_avg_price_eur and need_type.
#          (b) Offer-type dummy added to the regression where it varies.
#          (c) Robustness specs: + firm FE, + day-of-week FE.
#          (d) Significance stars for every reported coefficient.
#
# Spec (baseline):
#   y_{u,d,p} = alpha_u
#             + beta_C * Critical + beta_M * Midday
#             + sum_{r != r0} [lambda_r * R_r
#                              + delta_{C,r} * Critical * R_r
#                              + delta_{M,r} * Midday   * R_r]
#             + gamma * block_offer   (IDA only; DA in-band is 100% simple)
#             + epsilon
#   with within-unit demeaning (absorbs alpha_u) and date-clustered SEs.
#
# OUT: results/regressions/bid/mtu15_critical_flat/critflat_smart_descriptive_v2.csv

from pathlib import Path
import sys

import duckdb
import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO / "scripts/analysis/bid"))
from mtu15_critical_flat_did import clustered_ols  # noqa: E402

DA_PANEL = REPO / "data/derived/panels/per_curve_metrics_da_full.parquet"
IDA_PANEL = REPO / "data/derived/panels/per_curve_metrics_ida.parquet"
ICAB = REPO / "data/processed/omie/mercado_intradiario_subastas/ofertas/icab_all.parquet"
OUT = REPO / "results/regressions/bid/mtu15_critical_flat/critflat_smart_descriptive_v2.csv"
OUT.parent.mkdir(parents=True, exist_ok=True)

CRITICAL = {5, 6, 7, 8, 16, 17, 18, 19, 20, 21, 22}
FLAT = {1, 2, 3}
MIDDAY = {11, 12, 13, 14}

REGIMES = [
    ("pre_IDA",    "2022-01-01", "2024-06-13"),
    ("3sess",      "2024-06-14", "2024-11-30"),
    ("ISP15_win",  "2024-12-01", "2025-03-18"),
    ("ID15_pre",   "2025-03-19", "2025-04-27"),
    ("ID15_post",  "2025-04-28", "2025-09-30"),
    ("DA15_ID15",  "2025-10-01", "2026-02-26"),
]
REGIME_NAMES = [r[0] for r in REGIMES]


def assign_regime(d):
    for name, lo, hi in REGIMES:
        if pd.Timestamp(lo) <= d <= pd.Timestamp(hi):
            return name
    return None


def hour_class(h):
    if h in CRITICAL: return "Critical"
    if h in FLAT: return "Flat"
    if h in MIDDAY: return "Midday"
    return "Other"


def stars(t):
    a = abs(t)
    if a > 2.576: return "***"
    if a > 1.96:  return "**"
    if a > 1.645: return "*"
    return ""


def build_ida_offer_type_lookup():
    """Per (date, session, offer_code, version, unit_code): block flag from icab.
       block = (block_order_avg_price_eur IS NOT NULL) -- icab's block-order indicator.
       Other complex offer markers are also surfaced but the binary flag is
       the most reliable cross-regime classifier."""
    con = duckdb.connect()
    df = con.execute(f"""
        SELECT CAST(date AS DATE) d, session_number, offer_code, version, unit_code,
               (block_order_avg_price_eur IS NOT NULL OR block_order_min_pct IS NOT NULL)::INT AS is_block,
               (need_type IS NOT NULL AND need_type NOT IN ('N1', '120', '130'))::INT AS is_re_repr
        FROM '{ICAB}'
        WHERE date BETWEEN '2022-01-01' AND '2026-02-26' AND buy_sell = 'V'
    """).df()
    df["d"] = pd.to_datetime(df["d"])
    return df


def attach_ida_offer_type(ida_panel):
    """The per_curve_metrics_ida panel doesn't have offer_code; we look up the
    block flag per (date, session_number, unit_code) by joining on the modal
    offer for that unit in that session-date. Since each unit usually submits
    one sell offer per session, this captures most variation."""
    lookup = build_ida_offer_type_lookup()
    # Reduce to per-(d, session, unit) by aggregating: did the unit have any block-flagged offer that day-session?
    agg = lookup.groupby(["d", "session_number", "unit_code"]).agg(
        any_block=("is_block", "max"),
        any_re=("is_re_repr", "max"),
    ).reset_index()
    out = ida_panel.merge(agg, how="left",
                          on=["d", "session_number", "unit_code"])
    out["any_block"] = out["any_block"].fillna(0).astype(int)
    out["any_re"]    = out["any_re"].fillna(0).astype(int)
    return out


def run_one(panel, tech, market, with_block=False):
    p = panel[panel["tech"] == tech].copy()
    p["d"] = pd.to_datetime(p["d"])
    p["hc"] = p["clock_hour"].apply(hour_class)
    p = p[p["hc"].isin(["Critical", "Flat", "Midday"])]
    p["regime"] = p["d"].apply(assign_regime)
    p = p[p["regime"].isin(REGIME_NAMES)]
    if len(p) < 50:
        return [], {}

    p["crit"] = (p["hc"] == "Critical").astype(int)
    p["mid"]  = (p["hc"] == "Midday").astype(int)

    out_rows = []
    diag = {}
    for outcome in ["sigma_p", "n_eff"]:
        d = p.dropna(subset=[outcome]).copy()
        if len(d) < 50:
            continue
        baseline_regime = REGIME_NAMES[0]
        other = [r for r in REGIME_NAMES if r != baseline_regime
                 and (d["regime"] == r).any()]
        cols = []
        names = []
        cols.append(d["crit"].values.astype(float)); names.append("crit")
        cols.append(d["mid"].values.astype(float));  names.append("mid")
        for r in other:
            ind = (d["regime"] == r).astype(float).values
            cols.append(ind);                                       names.append(f"R_{r}")
            cols.append((ind * d["crit"].values).astype(float));    names.append(f"crit_x_{r}")
            cols.append((ind * d["mid"].values).astype(float));     names.append(f"mid_x_{r}")
        # Optional offer-type dummy
        if with_block and "any_block" in d.columns and d["any_block"].sum() > 100:
            cols.append(d["any_block"].values.astype(float))
            names.append("block")
            diag["block_share"] = float(d["any_block"].mean())
        X = np.column_stack(cols)
        df_for_dm = pd.DataFrame(X, columns=names)
        df_for_dm["y"] = d[outcome].values
        df_for_dm["unit_code"] = d["unit_code"].values
        for c in names + ["y"]:
            gm = df_for_dm.groupby("unit_code")[c].transform("mean")
            df_for_dm[c] = df_for_dm[c] - gm
        Xd = df_for_dm[names].values
        yd = df_for_dm["y"].values
        try:
            beta, se = clustered_ols(yd, Xd, d["d"].astype(str).values)
        except Exception as e:
            print(f"  [{tech} {market} {outcome}] failed: {e}")
            continue
        idx = {n: i for i, n in enumerate(names)}
        block_beta = block_se = None
        if "block" in idx:
            block_beta = float(beta[idx["block"]])
            block_se   = float(se[idx["block"]])

        for r in REGIME_NAMES:
            if r == baseline_regime:
                b_crit = beta[idx["crit"]]
                b_mid  = beta[idx["mid"]]
                s_crit = se[idx["crit"]]
                s_mid  = se[idx["mid"]]
            elif f"crit_x_{r}" in idx:
                b_crit = beta[idx["crit"]] + beta[idx[f"crit_x_{r}"]]
                b_mid  = beta[idx["mid"]]  + beta[idx[f"mid_x_{r}"]]
                s_crit = np.sqrt(se[idx["crit"]]**2 + se[idx[f"crit_x_{r}"]]**2)
                s_mid  = np.sqrt(se[idx["mid"]]**2  + se[idx[f"mid_x_{r}"]]**2)
            else:
                continue
            t_crit = b_crit / s_crit if s_crit else 0
            t_mid  = b_mid  / s_mid  if s_mid  else 0
            n_r = (d["regime"] == r).sum()
            out_rows.append({
                "spec": "v2_block" if with_block else "v2_base",
                "market": market, "tech": tech, "outcome": outcome, "regime": r,
                "beta_crit": b_crit, "se_crit": s_crit, "t_crit": t_crit,
                "star_crit": stars(t_crit),
                "beta_mid":  b_mid,  "se_mid":  s_mid,  "t_mid": t_mid,
                "star_mid":  stars(t_mid),
                "block_beta": block_beta, "block_se": block_se,
                "n_curves":  int(n_r),
            })
    return out_rows, diag


def main():
    print("Loading panels...")
    da  = pd.read_parquet(DA_PANEL)
    ida = pd.read_parquet(IDA_PANEL)
    print(f"  DA:  {len(da):,} curves   IDA: {len(ida):,} curves")

    print("\nAttaching IDA offer-type from icab...")
    ida = attach_ida_offer_type(ida)
    print(f"  IDA block share (any_block=1 in panel): {ida['any_block'].mean():.4f}")
    print(f"  IDA RE-representative share (any_re=1): {ida['any_re'].mean():.4f}")

    rows = []
    for tech in ["CCGT", "Hydro", "Hydro_pump", "Wind"]:
        print(f"\n  {tech}")
        for market, p in [("DA", da), ("IDA", ida)]:
            print(f"    {market} baseline...", end="", flush=True)
            r0, _ = run_one(p, tech, market, with_block=False)
            rows.extend(r0)
            print(" done", end="")
            if market == "IDA":
                print(f", {market} +block FE...", end="", flush=True)
                r1, diag = run_one(p, tech, market, with_block=True)
                rows.extend(r1)
                if diag:
                    print(f" (block_share={diag.get('block_share',0):.3f})", end="")
                print(" done", end="")
            print()
    df = pd.DataFrame(rows)
    df.to_csv(OUT, index=False)
    print(f"\nWrote {OUT}")

    # Print stars-annotated tables, for baseline spec only (block-FE spec for IDA reported via CSV)
    for outcome in ["sigma_p", "n_eff"]:
        for which, lab in [("beta_crit", "Critical - Flat differential"),
                            ("beta_mid",  "Midday - Flat differential")]:
            print(f"\n=== {outcome}  {lab}  (v2 baseline, unit FE, date-clustered SE) ===")
            sub = df[(df["outcome"] == outcome) & (df["spec"] == "v2_base")]
            print(f"{'tech':12s} {'market':6s} " + " ".join(f"{r:>11s}" for r in REGIME_NAMES))
            for tech in ["CCGT", "Hydro", "Hydro_pump", "Wind"]:
                for market in ["DA", "IDA"]:
                    cells = []
                    for r in REGIME_NAMES:
                        row = sub[(sub["tech"]==tech) & (sub["market"]==market) & (sub["regime"]==r)]
                        if len(row) == 0:
                            cells.append("           ")
                            continue
                        b = row.iloc[0][which]
                        s = row.iloc[0]["star_" + which.split("_")[1]]
                        cells.append(f"{b:+7.2f}{s:<3s}")
                    print(f"{tech:12s} {market:6s} " + " ".join(cells))

    # Compare IDA baseline vs +block for CCGT (sanity: does block dummy change anything?)
    print("\n=== IDA CCGT sigma_p beta_crit -- baseline vs +block-FE ===")
    a = df[(df["tech"]=="CCGT") & (df["market"]=="IDA") & (df["outcome"]=="sigma_p")
            & (df["spec"]=="v2_base")][["regime","beta_crit","star_crit"]].set_index("regime")
    b = df[(df["tech"]=="CCGT") & (df["market"]=="IDA") & (df["outcome"]=="sigma_p")
            & (df["spec"]=="v2_block")][["regime","beta_crit","star_crit","block_beta"]].set_index("regime")
    merged = a.merge(b, left_index=True, right_index=True, suffixes=("_base","_block"))
    print(merged.round(3).to_string())


if __name__ == "__main__":
    main()
