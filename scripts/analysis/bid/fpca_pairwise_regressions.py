# STATUS: ALIVE
# LAST-AUDIT: 2026-05-18
# FEEDS: bidding_internal.tex §3.4 (pairwise reform-window fPCA regressions)
# CLAIM: Per (tech, firm, pairwise reform) run OLS of each PC score on a post
#        dummy × hour_class, with Fourier seasonal controls (sin/cos of doy
#        for k=1,2,3) plus calendar-month + day-of-week + hour + quarter FE.
#        Pairwise comparisons isolate each reform between its immediate pre/post
#        windows; the Fourier basis absorbs smooth annual seasonality more
#        flexibly than month FE alone.
#
# Pairwise reforms:
#   ISP15:       3-sess               (pre) -> ISP15-win           (post)
#   MTU15-IDA:   ISP15-win            (pre) -> DA60/ID15 pre-blk   (post)
#   MTU15-DA:    DA60/ID15 post-blk   (pre) -> DA15/ID15           (post)
#
# Output:
#   results/regressions/bid/fpca/
#     coeffs_pairwise.csv          long format (tech, firm, reform, PC, hour_class, coef)
#     tex/tab_fpca_pairwise_*.tex  per-reform headline tables
#   figures/working/
#     fpca_pairwise_curve_<tech>.pdf  per-(tech, reform) fitted curve shift

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
FIG  = REPO / "figures" / "working"

UNITS = REPO / "data/external/omie_reference/lista_unidades.csv"

N_PCS = 5
HOUR_CLASSES = ["Critical", "Flat", "Midday"]
TECHS = ["CCGT", "Hydro", "Hydro_pump", "Nuclear", "Wind", "Solar PV", "Cogen", "Solar Thermal"]

PAIRS = [
    ("ISP15",      "3-sess",             "ISP15-win"),
    ("MTU15-IDA",  "ISP15-win",          "DA60/ID15 pre-blk"),
    ("MTU15-DA",   "DA60/ID15 post-blk", "DA15/ID15"),
]

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
    if "acciona" in o: return "ACC"
    if "axpo" in o: return "AXPO"
    if "gesternova" in o: return "GST"
    return "OTH"


def load_units_firm_map():
    raw = pd.read_csv(UNITS)
    raw["firm"] = raw["owner_agent"].apply(map_firm)
    return raw[["unit_code", "firm"]].drop_duplicates("unit_code").set_index("unit_code")["firm"]


def fourier_basis(doy, K=3):
    """Cyclic Fourier basis on day-of-year [1..365]. Returns 2K columns."""
    arr = np.zeros((len(doy), 2 * K), dtype=np.float32)
    angle = 2 * np.pi * doy.values.astype(np.float32) / 365.25
    for k in range(1, K + 1):
        arr[:, 2*(k-1)]   = np.sin(k * angle)
        arr[:, 2*(k-1)+1] = np.cos(k * angle)
    return arr


def prepare_panel(tech: str):
    fp = IN / f"pc_scores_{tech.replace(' ', '_')}.parquet"
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
    df["dow"] = df["date"].dt.dayofweek
    df["doy"] = df["date"].dt.dayofyear
    df["ym"] = df["date"].dt.strftime("%Y-%m")
    df["regime"] = df["regime"].astype(str)
    if "firm" not in df.columns:
        unit_to_firm = load_units_firm_map()
        df["firm"] = df["entity"].map(unit_to_firm).fillna("OTH")
    return df


def run_pairwise_one_pc(df_sub, y_col, post_regime):
    """OLS of PC_k on:
       post + post×Flat + post×Midday + month FE + Fourier(doy) + dow FE + hour FE + quarter FE
    Returns: (coef_post, coef_post_x_Flat, coef_post_x_Midday).
    """
    if len(df_sub) < 50:
        return (np.nan, np.nan, np.nan)
    df_sub = df_sub.dropna(subset=["hour_class", y_col]).copy()
    df_sub["post"] = (df_sub["regime"] == post_regime).astype(np.float32)

    # Build design matrix
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
    # Columns 1, 2, 3 are post, post×Flat, post×Midday
    return (float(coef[1]), float(coef[2]), float(coef[3]))


def fit_pairwise(tech: str):
    df = prepare_panel(tech)
    if df is None or len(df) == 0:
        return None
    print(f"\n=== Pairwise regressions for {tech}: {len(df):,} rows ===")

    rows = []
    for reform, pre, post in PAIRS:
        df_pair = df[df["regime"].isin([pre, post])]
        if len(df_pair) == 0:
            continue
        firms_in_pair = sorted(df_pair["firm"].unique())
        for firm in firms_in_pair:
            df_sub = df_pair[df_pair["firm"] == firm]
            if len(df_sub) < 50:
                continue
            for k in range(1, N_PCS + 1):
                c_post, c_flat, c_mid = run_pairwise_one_pc(df_sub, f"PC{k}", post)
                # Compute hour-class-specific post effects
                # Effect on Critical = c_post (post main, reference hour-class)
                # Effect on Flat    = c_post + c_flat
                # Effect on Midday  = c_post + c_mid
                rows.append({"tech": tech, "firm": firm, "reform": reform,
                             "PC": f"PC{k}", "hour_class": "Critical",
                             "coef": c_post, "n_rows": len(df_sub)})
                rows.append({"tech": tech, "firm": firm, "reform": reform,
                             "PC": f"PC{k}", "hour_class": "Flat",
                             "coef": (c_post + c_flat) if not np.isnan(c_post) else np.nan,
                             "n_rows": len(df_sub)})
                rows.append({"tech": tech, "firm": firm, "reform": reform,
                             "PC": f"PC{k}", "hour_class": "Midday",
                             "coef": (c_post + c_mid) if not np.isnan(c_post) else np.nan,
                             "n_rows": len(df_sub)})
            print(f"  {reform} | {firm}: done")
    out_df = pd.DataFrame(rows)
    print(f"  -> {len(out_df)} coefficient rows for {tech}")
    return out_df


def plot_pairwise_curves(tech: str, all_coef: pd.DataFrame):
    basis_path = IN / f"pc_basis_{tech.replace(' ', '_')}.npz"
    if not basis_path.exists():
        return
    b = np.load(basis_path)
    components = b["components"]
    q_grid = np.linspace(1, 99, 99)
    sub = all_coef[all_coef["tech"] == tech]
    if len(sub) == 0:
        return

    fig, axes = plt.subplots(3, 3, figsize=(13, 10), sharex=True, sharey="row")
    firm_to_show = "GN" if tech == "CCGT" else ("IB" if tech in ["Hydro","Hydro_pump","Nuclear"] else "GST" if tech == "Wind" else "OTH")
    # If chosen firm has no data, pick the firm with most observations
    if len(sub[sub["firm"] == firm_to_show]) == 0:
        firms_by_n = sub.groupby("firm")["n_rows"].max().sort_values(ascending=False)
        firm_to_show = firms_by_n.index[0] if len(firms_by_n) else "OTH"

    colors = {"ISP15": "#7ed321", "MTU15-IDA": "#f5a623", "MTU15-DA": "#9013fe"}
    for i, reform in enumerate(["ISP15", "MTU15-IDA", "MTU15-DA"]):
        for j, hc in enumerate(HOUR_CLASSES):
            ax = axes[i, j]
            piv = sub[(sub["firm"] == firm_to_show) & (sub["reform"] == reform) & (sub["hour_class"] == hc)]
            if len(piv) == 0:
                ax.text(0.5, 0.5, "n/a", transform=ax.transAxes, ha="center", va="center")
                continue
            piv = piv.sort_values("PC")
            beta = piv["coef"].to_numpy()
            if np.any(np.isnan(beta)):
                ax.text(0.5, 0.5, "no fit", transform=ax.transAxes, ha="center", va="center")
                continue
            change = beta @ components
            ax.plot(q_grid, change, color=colors[reform], lw=2)
            ax.axhline(0, color="black", lw=0.5)
            ax.set_title(f"{reform} | {hc}", fontsize=9)
            if j == 0:
                ax.set_ylabel(f"Δ price (€/MWh)")
            if i == 2:
                ax.set_xlabel("Quantile of cumulative MW")
            ax.grid(alpha=0.3)
    fig.suptitle(f"Pairwise reform-window fitted bid-curve shifts — {tech}, firm: {firm_to_show}\n"
                 f"(each panel: post-pre coefficient projected onto the curve domain via fPCA)")
    fig.tight_layout(rect=[0, 0, 1, 0.95])
    fig.savefig(FIG / f"fpca_pairwise_curve_{tech.replace(' ', '_')}.pdf")
    plt.close(fig)


def write_tex_table(all_coef: pd.DataFrame):
    """One tex table per reform: rows = (tech, firm), cols = (PC1..5) × (Critical, Flat, Midday)
    Simplification: report only PC1 across the 3 hour-classes, for the 4 strategic techs.
    """
    for reform, _, _ in PAIRS:
        sub = all_coef[(all_coef["reform"] == reform)
                       & (all_coef["tech"].isin(["CCGT", "Hydro", "Hydro_pump", "Nuclear"]))
                       & (all_coef["firm"].isin(FIRMS_FOCUS))]
        # pivot: index=(tech, firm), cols=(PC, hour_class) with PC1 only
        sub1 = sub[sub["PC"] == "PC1"].copy()
        piv = sub1.pivot_table(index=["tech", "firm"], columns="hour_class", values="coef")
        piv = piv[["Critical", "Flat", "Midday"]] if all(c in piv.columns for c in ["Critical","Flat","Midday"]) else piv
        piv = piv.round(1)
        rows = []
        rows.append(r"\begin{tabular}{l l r r r}")
        rows.append(r"\toprule")
        rows.append(r"Tech & Firm & Critical & Flat & Midday \\")
        rows.append(r"\midrule")
        for (tech, firm), r in piv.iterrows():
            tech_label = tech.replace("_", " ")
            row = [tech_label, firm]
            for hc in ["Critical", "Flat", "Midday"]:
                v = r.get(hc, np.nan) if isinstance(r, pd.Series) else np.nan
                row.append(f"{v:+.1f}" if not pd.isna(v) else "---")
            rows.append(" & ".join(row) + r" \\")
        rows.append(r"\bottomrule")
        rows.append(r"\end{tabular}")
        out = TEX / f"tab_fpca_pairwise_{reform.replace('/', '_').replace(' ', '_')}.tex"
        out.write_text(f"% auto-built — pairwise {reform} effect on PC1 score\n" + "\n".join(rows))
        print(f"  wrote {out}")


def main():
    all_rows = []
    for tech in TECHS:
        out = fit_pairwise(tech)
        if out is not None and len(out):
            all_rows.append(out)
    all_coef = pd.concat(all_rows, ignore_index=True)
    all_coef.to_csv(IN / "coeffs_pairwise.csv", index=False)
    print(f"\nTotal: {len(all_coef):,} coefficient rows")

    # Tex tables
    write_tex_table(all_coef)

    # Figures
    for tech in TECHS:
        plot_pairwise_curves(tech, all_coef)
    print("All pairwise regressions done.")


if __name__ == "__main__":
    main()
