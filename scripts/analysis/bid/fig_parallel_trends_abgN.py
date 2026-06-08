# STATUS: ALIVE
# LAST-AUDIT: 2026-06-06
# FEEDS: thesis/paper/thesis.tex --- visual parallel-trends figure
#        for the Spec C bid-shape DiD on the CURRENT outcome quadruple
#        (alpha, beta, gamma, N_eff), mirroring fig_parallel_trends_sigma_p
#        which uses the legacy sigma_p outcome.
#
# OUT: figures/thesis/fig_parallel_trends_abgN_{alpha,beta,gamma,neff}.{pdf,png}

from pathlib import Path

import duckdb
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
CAB  = REPO / "data/processed/omie/mercado_diario/ofertas/cab_all.parquet"
DET  = REPO / "data/processed/omie/mercado_diario/ofertas/det_all.parquet"
ICAB = REPO / "data/processed/omie/mercado_intradiario_subastas/ofertas/icab_all.parquet"
IDET = REPO / "data/processed/omie/mercado_intradiario_subastas/ofertas/idet_all.parquet"
MCPDA  = REPO / "data/processed/omie/mercado_diario/precios/marginalpdbc_all.parquet"
MCPIDA = REPO / "data/processed/omie/mercado_intradiario_subastas/precios/marginalpibc_all.parquet"
UMAP = REPO / "data/derived/panels/bid_shape_critical_flat/_unit_map.parquet"
OUT_DIR = REPO / "figures/thesis"

H_BAND     = 150.0
DATE_START = "2024-06-14"
DATE_END   = "2025-12-31"
CRIT_HOURS = "(5,6,7,8,16,17,18,19,20,21,22)"
FLAT_HOURS = "(1,2,3)"
ID15 = "2025-03-19"
BLK  = "2025-04-28"
DA15 = "2025-10-01"


def fetch_bands(con, mode):
    """Return the per-curve in-band tranches for CCGT, with tranche price p and MW q."""
    if mode == "da":
        offers_src = f"""
          SELECT CAST(c.date AS DATE) AS d, c.offer_code, c.version, c.unit_code,
                 d.period, d.price_eur_mwh AS p, d.quantity_mw AS q
          FROM '{CAB}' c JOIN '{DET}' d
            ON c.date = d.date AND c.offer_code = d.offer_code AND c.version = d.version
          WHERE c.buy_sell = 'V'
            AND d.price_eur_mwh IS NOT NULL AND d.quantity_mw IS NOT NULL AND d.quantity_mw > 0
            AND c.date BETWEEN '{DATE_START}' AND '{DATE_END}'
        """
        # DA cab has no block/simple flag; cab rows are all price-quantity tranche style.
        mcp_src = f"""
          SELECT CAST(date AS DATE) AS d, period, price_es_eur_mwh AS mcp
          FROM '{MCPDA}'
          WHERE date BETWEEN '{DATE_START}' AND '{DATE_END}'
            AND price_es_eur_mwh IS NOT NULL
        """
    else:  # ida
        # Restrict to simple SIDC offers; exclude block orders (~2% of CCGT sells)
        # whose multi-period flat pricing collapses the per-curve q-spread.
        offers_src = f"""
          SELECT CAST(c.date AS DATE) AS d, c.offer_code, c.version, c.unit_code,
                 d.period, d.price_eur_mwh AS p, d.quantity_mw AS q
          FROM '{ICAB}' c JOIN '{IDET}' d
            ON c.date = d.date AND c.offer_code = d.offer_code AND c.version = d.version
          WHERE c.buy_sell = 'V'
            AND c.block_order_avg_price_eur IS NULL
            AND d.price_eur_mwh IS NOT NULL AND d.quantity_mw IS NOT NULL AND d.quantity_mw > 0
            AND c.date BETWEEN '{DATE_START}' AND '{DATE_END}'
        """
        mcp_src = f"""
          SELECT CAST(date AS DATE) AS d, period, price_es_eur_mwh AS mcp,
                 ROW_NUMBER() OVER (PARTITION BY date::DATE, period
                                     ORDER BY mtu_minutes ASC) AS rn
          FROM '{MCPIDA}'
          WHERE date BETWEEN '{DATE_START}' AND '{DATE_END}'
            AND price_es_eur_mwh IS NOT NULL
        """
    mcp_filter = "WHERE rn=1" if mode == "ida" else ""
    q = f"""
    WITH u AS (SELECT unit_code FROM '{UMAP}' WHERE tech_group = 'CCGT'),
         mcp_raw AS ({mcp_src}),
         mcp AS (SELECT d, period, mcp FROM mcp_raw {mcp_filter}),
         offers AS ({offers_src}),
         banded AS (
           SELECT o.d, o.offer_code, o.version, o.unit_code, o.period, o.p, o.q,
                  CASE WHEN o.period <= 24 THEN o.period
                       ELSE CAST(CEIL(o.period / 4.0) AS INT) END AS hour
           FROM offers o JOIN mcp m USING (d, period) JOIN u USING (unit_code)
           WHERE ABS(o.p - m.mcp) <= {H_BAND}
         )
    SELECT * FROM banded
    """
    return con.execute(q).df()


def fit_quadratic_per_curve(df):
    """Compute alpha, beta, gamma, N_eff per (d, unit_code, period, hour) curve."""
    df = df.sort_values(["d","unit_code","period","p"]).reset_index(drop=True)
    df["q_cum"] = df.groupby(["d","unit_code","period"])["q"].cumsum()
    df["q_mid"] = df["q_cum"] - df["q"]/2.0   # midpoint MW for each tranche

    def fit_one(g):
        n = len(g); p = g["p"].values; q = g["q_mid"].values; w = g["q"].values
        if n < 2:
            return pd.Series({"alpha": p.mean() if n else np.nan, "beta": np.nan,
                              "gamma": np.nan, "n_eff": 1.0 if n==1 else 0.0,
                              "hhi": 1.0,
                              "n_tranche": float(n), "q_spread": 0.0})
        # Two-stage: linear OLS for alpha, beta; quadratic with FWL residual for gamma
        x = q - q.mean()
        denom = (w * x * x).sum()
        if denom < 1e-9:
            beta = 0.0
        else:
            beta = (w * x * (p - (w*p).sum()/w.sum())).sum() / denom
        alpha = (w*p).sum()/w.sum() - beta * q.mean()
        if n >= 3:
            resid = p - (alpha + beta*q)
            x2 = (q - q.mean())**2
            x2c = x2 - (w*x2).sum()/w.sum()
            denom2 = (w*x2c*x2c).sum()
            gamma = (w*x2c*resid).sum() / denom2 if denom2 > 1e-12 else 0.0
        else:
            gamma = 0.0
        sum_q = w.sum(); sum_q2 = (w*w).sum()
        n_eff = (sum_q * sum_q) / sum_q2 if sum_q2 > 0 else 0.0
        hhi = 1.0 / n_eff if n_eff > 0 else 1.0
        return pd.Series({"alpha": alpha, "beta": beta, "gamma": gamma,
                          "n_eff": n_eff, "hhi": hhi,
                          "n_tranche": float(n), "q_spread": float(q.max() - q.min())})

    res = (df.groupby(["d","unit_code","period","hour"], group_keys=True)
             .apply(fit_one, include_groups=False).reset_index())
    return res


def aggregate_weekly(res, min_n_tranche=4, min_q_spread=20.0):
    """Aggregate per-curve outcomes to weekly MEDIAN by hour-class.

    For beta and gamma, restrict to identifiable curves: actual tranche count
    >= min_n_tranche AND q-spread >= min_q_spread MW. Single- and two-tranche
    curves (~33% and ~6% of DA curves at h=150) cannot identify a slope; including
    them via the previous n_eff Herfindahl-based filter let too much noise through.

    Median across the surviving subset is robust to remaining OLS outliers.
    """
    res = res.copy()
    res["d"] = pd.to_datetime(res["d"])
    res["week"] = res["d"] - pd.to_timedelta(res["d"].dt.weekday, unit="D")
    crit = set(range(5,9)) | set(range(16,23))
    flat = {1,2,3}
    res["hour_class"] = res["hour"].apply(lambda h: "critical" if h in crit else ("flat" if h in flat else None))
    res = res.dropna(subset=["hour_class"])

    # Identification floor for beta/gamma: real tranche count AND meaningful q-spread.
    keep = (res["n_tranche"] >= min_n_tranche) & (res["q_spread"] >= min_q_spread)
    slope_part = res[keep]

    g_alpha_neff = (res.groupby(["week","hour_class"], observed=True)
                    .agg(alpha=("alpha","median"),
                         hhi=("hhi","median"),
                         n=("alpha","size")).reset_index())
    g_slope = (slope_part.groupby(["week","hour_class"], observed=True)
                    .agg(beta=("beta","median"),
                         gamma=("gamma","median"),
                         n_slope=("beta","size")).reset_index())
    weekly = g_alpha_neff.merge(g_slope, on=["week","hour_class"], how="left")
    return weekly


def plot_panel(ax, w, outcome, title, ylabel):
    for cls, col in [("critical", "tab:red"), ("flat", "tab:blue")]:
        sub = w[w["hour_class"] == cls].sort_values("week")
        ax.plot(sub["week"], sub[outcome], lw=1.5, color=col,
                label=cls.capitalize(), marker="o", markersize=2.0)
    for date_str, label, color in [(ID15, "ID15", "tab:purple"),
                                   (BLK, "Blackout", "tab:gray"),
                                   (DA15, "DA15", "tab:green")]:
        d = pd.to_datetime(date_str)
        ax.axvline(d, color=color, ls="--", lw=0.9, alpha=0.7)
        ax.text(d, ax.get_ylim()[1] * 0.97, "  " + label,
                rotation=90, va="top", ha="left", fontsize=7, color=color)
    ax.set_title(title, fontsize=10, weight="bold")
    ax.set_ylabel(ylabel, fontsize=9)
    ax.grid(alpha=0.3)
    ax.tick_params(labelsize=7)
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
    ax.legend(loc="best", fontsize=7, frameon=False)


def plot_diff(ax, w_da, w_ida, outcome, title, ylabel):
    """Plot critical-minus-flat differential for DA and IDA on the same axis."""
    for w, lab, col in [(w_da, "DA", "tab:cyan"), (w_ida, "IDA", "tab:orange")]:
        crit = w[w["hour_class"]=="critical"].set_index("week")[outcome]
        flat = w[w["hour_class"]=="flat"].set_index("week")[outcome]
        diff = (crit - flat).sort_index()
        ax.plot(diff.index, diff.values, lw=1.5, color=col,
                label=lab, marker="o", markersize=2.0)
    ax.axhline(0, color="black", lw=0.5, alpha=0.5)
    for date_str, label, color in [(ID15, "ID15", "tab:purple"),
                                   (BLK, "Blackout", "tab:gray"),
                                   (DA15, "DA15", "tab:green")]:
        d = pd.to_datetime(date_str)
        ax.axvline(d, color=color, ls="--", lw=0.9, alpha=0.7)
        ax.text(d, ax.get_ylim()[1] * 0.97, "  " + label,
                rotation=90, va="top", ha="left", fontsize=7, color=color)
    ax.set_title(title, fontsize=10, weight="bold")
    ax.set_ylabel(ylabel, fontsize=9)
    ax.grid(alpha=0.3)
    ax.tick_params(labelsize=7)
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
    ax.legend(loc="best", fontsize=7, frameon=False)


def main():
    con = duckdb.connect(); con.execute("SET threads=4; SET memory_limit='6GB'")
    print("Fetching DA in-band tranches ...", flush=True)
    da_raw = fetch_bands(con, "da")
    print(f"  DA tranches: {len(da_raw):,}", flush=True)
    print("Fitting per-curve alpha/beta/gamma/N_eff (DA) ...", flush=True)
    da_fit = fit_quadratic_per_curve(da_raw)
    print(f"  DA curves: {len(da_fit):,}", flush=True)

    print("Fetching IDA in-band tranches ...", flush=True)
    ida_raw = fetch_bands(con, "ida")
    print(f"  IDA tranches: {len(ida_raw):,}", flush=True)
    print("Fitting per-curve alpha/beta/gamma/N_eff (IDA) ...", flush=True)
    ida_fit = fit_quadratic_per_curve(ida_raw)
    print(f"  IDA curves: {len(ida_fit):,}", flush=True)
    con.close()

    da_w  = aggregate_weekly(da_fit)
    ida_w = aggregate_weekly(ida_fit)

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    outcomes = [("alpha", "$\\hat\\alpha$ (EUR/MWh)"),
                ("beta",  "$\\hat\\beta$ (EUR/MWh per MW)"),
                ("gamma", "$\\hat\\gamma$ (EUR/MWh per MW$^2$)"),
                ("hhi",   "$\\mathrm{HHI}_p$")]
    # Composite: 4 outcomes (rows) x 3 columns (DA levels | IDA levels | DA & IDA differentials)
    fig, axes = plt.subplots(4, 3, figsize=(16, 14), sharex=True)
    for i, (outc, ylab) in enumerate(outcomes):
        plot_panel(axes[i,0], da_w, outc, f"DA market: {outc}", ylab)
        plot_panel(axes[i,1], ida_w, outc, f"IDA market: {outc}", ylab)
        plot_diff(axes[i,2], da_w, ida_w, outc,
                  f"Critical $-$ Flat differential: {outc}", "$\\Delta$ " + ylab)
    for ax in axes[-1,:]:
        ax.set_xlabel("Week", fontsize=9)
    fig.tight_layout()
    COMP = OUT_DIR / "fig_parallel_trends_abgN"
    plt.savefig(f"{COMP}.pdf", bbox_inches="tight")
    plt.savefig(f"{COMP}.png", bbox_inches="tight", dpi=130)
    plt.close(fig)
    print(f"saved {COMP}.pdf (composite, 4x3)")

    # Also save per-outcome individual figures (smaller, for callouts if needed)
    for outcome, ylab in outcomes:
        fig, axes = plt.subplots(2, 1, figsize=(11, 6.6), sharex=True)
        plot_panel(axes[0], da_w, outcome,
                   f"CCGT day-ahead per-curve {outcome} (critical = ramp hours, flat = overnight)",
                   ylab)
        plot_panel(axes[1], ida_w, outcome,
                   f"CCGT intraday-auction per-curve {outcome}", ylab)
        axes[1].set_xlabel("Week", fontsize=9)
        fig.tight_layout()
        OUT = OUT_DIR / f"fig_parallel_trends_abgN_{outcome}"
        plt.savefig(f"{OUT}.pdf", bbox_inches="tight")
        plt.savefig(f"{OUT}.png", bbox_inches="tight", dpi=130)
        plt.close(fig)
        print(f"saved {OUT}.pdf")


if __name__ == "__main__":
    main()
