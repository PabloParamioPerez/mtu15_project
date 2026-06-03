# STATUS: ALIVE
# LAST-AUDIT: 2026-05-15
# FEEDS: thesis/paper/paper.tex §3.6 — reforzada-confound figure
# CLAIM: RT2 ("operación reforzada") intensity is continuous from the April
#        2025 blackout onward; it spans both the DA60/ID15 and DA15/ID15
#        regimes, so D_MTU15-DA and D_reforzada are not collinear in panels
#        including the 2025-03-19 → 2025-04-27 sub-window.

from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd
import matplotlib.pyplot as plt

REPO = Path(__file__).resolve().parents[3]
P = REPO / "data" / "processed" / "esios" / "indicators" / "indicators_all.parquet"
OUT = REPO / "figures" / "thesis" / "fig_reforzada_intensity"

REFORMS = [
    ("ISP15",       "2024-12-09", "C0"),
    ("MTU15-IDA",   "2025-03-19", "C2"),
    ("Blackout",    "2025-04-28", "k"),
    ("MTU15-DA",    "2025-10-01", "C3"),
]


def load_monthly(indicator_id: int) -> pd.DataFrame:
    df = duckdb.execute(f"""
        SELECT date_trunc('month', ts_local)::DATE AS month, AVG(value) AS value
        FROM '{P}'
        WHERE indicator_id = {indicator_id}
          AND geo_id IN (3, 8741)
          AND date::DATE BETWEEN DATE '2024-01-01' AND DATE '2026-05-31'
        GROUP BY 1 ORDER BY 1
    """).df()
    df["month"] = pd.to_datetime(df["month"])
    return df


def main() -> None:
    rt2_abs = load_monthly(10052)       # Volumen absoluto RT en tiempo real
    rf_needs = load_monthly(1880)       # Necesidades reserva adicional en TR

    fig, axes = plt.subplots(2, 1, figsize=(12, 6.5), sharex=True)

    # Panel 1: RT2 absolute intensity
    ax1 = axes[0]
    ax1.plot(rt2_abs["month"], rt2_abs["value"], color="C0", linewidth=1.6,
             label="RT2 absolute energy (ESIOS id 10052)")
    ax1.fill_between(rt2_abs["month"], 0, rt2_abs["value"], color="C0", alpha=0.15)
    ax1.set_ylabel("MW (monthly avg)")
    ax1.set_title("Restricciones técnicas en tiempo real — absolute energy assigned")
    ax1.grid(alpha=0.3)

    # Panel 2: Reforzada-specific needs (post-2021 coverage)
    ax2 = axes[1]
    ax2.plot(rf_needs["month"], rf_needs["value"], color="C3", linewidth=1.6,
             label="Reforzada needs (ESIOS id 1880)")
    ax2.fill_between(rf_needs["month"], 0, rf_needs["value"], color="C3", alpha=0.15)
    ax2.set_ylabel("MW (monthly avg)")
    ax2.set_xlabel("Month")
    ax2.set_title("Additional reserve needs identified in real time ('operación reforzada')")
    ax2.grid(alpha=0.3)

    # Reform / event markers
    for ax in axes:
        ymin, ymax = ax.get_ylim()
        for label, date_str, color in REFORMS:
            d = pd.to_datetime(date_str)
            ax.axvline(d, color=color, linestyle="--", linewidth=1.0, alpha=0.8)
            ax.text(d, ymax * 0.95, label, rotation=90, va="top", ha="right",
                    fontsize=8, color=color, alpha=0.9)

    fig.suptitle("Reforzada / RT2 intensity, January 2024 -- May 2026",
                 fontsize=13, y=0.995)
    fig.tight_layout(rect=[0, 0, 1, 0.97])

    for ext in ("pdf", "png"):
        fig.savefig(f"{OUT}.{ext}", bbox_inches="tight", dpi=140 if ext == "png" else None)
        print(f"saved {OUT}.{ext}")
    plt.close(fig)

    # Also save the data
    csv_out = REPO / "results" / "regressions" / "regulatory" / "reforzada_intensity_monthly.csv"
    csv_out.parent.mkdir(parents=True, exist_ok=True)
    merged = rt2_abs.rename(columns={"value": "rt2_abs_mw"}).merge(
        rf_needs.rename(columns={"value": "reforzada_needs_mw"}), on="month", how="outer"
    ).sort_values("month")
    merged.to_csv(csv_out, index=False)
    print(f"saved {csv_out}")


if __name__ == "__main__":
    main()
