# STATUS: ALIVE
# LAST-AUDIT: 2026-05-26
# FEEDS: advisor_memo.tex sec 4 -- event-time visualization of the BSTS
#        headline findings. Shows observed series, model counterfactual,
#        and pointwise treatment effect with 95% credible bands. Robustness
#        check after peer-review flagged the IDA price magnitude.
#
# OUT: figures/working/fig_bsts_event_time_id15_ida_price.pdf
#      figures/working/fig_bsts_event_time_da15_ccgt.pdf

from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
RES = REPO / "results/regressions/bid/mtu15_critical_flat/pointwise"
FIG = REPO / "figures/working"
FIG.mkdir(parents=True, exist_ok=True)


def plot_event_time(pw_path, cutover, title, ylabel, out_path):
    df = pd.read_csv(pw_path)
    df["date"] = pd.to_datetime(df["date"])
    cutover = pd.Timestamp(cutover)

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(8, 6.5), sharex=True,
                                     gridspec_kw={"height_ratios": [1.2, 1]})

    ax1.plot(df["date"], df["response"], color="black", linewidth=1.2,
              label="Observed")
    ax1.plot(df["date"], df["point.pred"], color="#c0392b", linewidth=1.2,
              linestyle="--", label="Counterfactual (BSTS)")
    ax1.fill_between(df["date"], df["point.pred.lower"],
                      df["point.pred.upper"], color="#c0392b", alpha=0.15,
                      label="95% credible band")
    ax1.axvline(cutover, color="black", linewidth=0.8, linestyle=":")
    ax1.set_ylabel(ylabel)
    ax1.set_title(title)
    ax1.legend(loc="best", frameon=False, fontsize=9)
    ax1.grid(True, alpha=0.3)

    ax2.axhline(0, color="black", linewidth=0.6)
    ax2.plot(df["date"], df["point.effect"], color="#1f4e79", linewidth=1.1,
              label="Pointwise effect")
    ax2.fill_between(df["date"], df["point.effect.lower"],
                      df["point.effect.upper"], color="#1f4e79", alpha=0.18)
    ax2.axvline(cutover, color="black", linewidth=0.8, linestyle=":")
    ax2.set_ylabel(f"Effect ({ylabel})")
    ax2.set_xlabel("Date")
    ax2.grid(True, alpha=0.3)

    plt.tight_layout()
    fig.savefig(out_path, bbox_inches="tight")
    plt.close(fig)
    print(f"Wrote {out_path}")


def main():
    plot_event_time(
        RES / "bsts_daily_pointwise_ID15_IDA_price.csv",
        cutover="2025-03-19",
        title="ID15: IDA clearing price, observed vs. BSTS counterfactual",
        ylabel="EUR/MWh",
        out_path=FIG / "fig_bsts_event_time_id15_ida_price.pdf",
    )
    plot_event_time(
        RES / "bsts_daily_pointwise_DA15_q_ccgt_da.csv",
        cutover="2025-10-01",
        title="DA15: CCGT auction-cleared energy, observed vs. BSTS counterfactual",
        ylabel="GWh / day",
        out_path=FIG / "fig_bsts_event_time_da15_ccgt.pdf",
    )
    plot_event_time(
        RES / "bsts_daily_pointwise_DA15_DA_price.csv",
        cutover="2025-10-01",
        title="DA15: DA clearing price, observed vs. BSTS counterfactual",
        ylabel="EUR/MWh",
        out_path=FIG / "fig_bsts_event_time_da15_da_price.pdf",
    )


if __name__ == "__main__":
    main()
