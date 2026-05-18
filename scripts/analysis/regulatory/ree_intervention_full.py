# STATUS: ALIVE
# LAST-AUDIT: 2026-05-18
# FEEDS: bidding_internal.tex §6 / §7 (full REE-intervention picture beyond RT2 strict)
# CLAIM: System-aggregate monthly REE-coordinated energy by channel.
#        ESIOS archive 28 (totalrp48preccierre). Channel labels use ONLY
#        codes that appear in the project-canonical reference (parser
#        docstring + docs/notes/SPANISH_MARKET_STRUCTURE.md §13). Codes
#        not in that reference (19, 22, 23, 24, 32, 38, 65, 66, 80, 82,
#        85, 89, 95, 96) are reported separately as "Uncategorised". This
#        differs from the previous version, which used unverified labels.
#        Code 23 in particular (very large volumes, only up-direction) is
#        excluded from the energy-delivered plot because it appears to be
#        a capacity-reservation volume (MW × hours, PO 7.2 banda-secundaria
#        reserve auction), not delivered energy — methodologically not
#        comparable to the activated-energy channels on the same axis.

from __future__ import annotations

from pathlib import Path
import glob

import duckdb
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
OUTDIR = REPO / "results" / "regressions" / "regulatory" / "ree_intervention_full"
OUTDIR.mkdir(parents=True, exist_ok=True)
FIGDIR = REPO / "figures" / "working"

START = "2024-01-01"
END   = "2026-03-01"
MTU15_IDA = pd.Timestamp("2025-03-19")
BLACKOUT  = pd.Timestamp("2025-04-28")
MTU15_DA  = pd.Timestamp("2025-10-01")

# Channel groupings of tipo_redespacho codes — PROJECT-CANONICAL labels only.
# Source: src/mtu/parsing/esios/totalrp48preccierre.py docstring (lines 7-15)
# and docs/notes/SPANISH_MARKET_STRUCTURE.md §13 (lines 727-737). Codes
# without a project-documented label go into UNCATEGORISED below.
CHANNELS = {
    "RT general (33)":                                ["33"],
    "RT inter-zonal / network (34)":                   ["34"],
    "RT system-security RZ, PO 3.2 (61)":              ["61"],
    "Reserve management (68)":                          ["68"],
    "Voltage control / black-start (69)":              ["69"],
    "Catch-all 'other' bucket (81)":                    ["81"],
    "mFRR activation (92)":                             ["92"],
    "System balancing / residual imbalance (94)":       ["94"],
}
# Codes NOT in project-canonical reference; their meanings are unverified.
# We aggregate them into a single "Uncategorised" bucket rather than guess.
# Code 23 in particular has very large up-only volumes (~10 TWh over 2 yr)
# consistent with PO 7.2 aFRR capacity reservation (MW × hours), which is
# a forward commitment, NOT delivered energy. Plotting it on the same axis
# as activated-energy codes would be apples-to-oranges; we DROP it.
UNCATEGORISED_CODES = ["19", "22", "24", "32", "38", "65", "66", "80",
                       "82", "85", "89", "95", "96"]
RESERVATION_CODES = ["23"]  # excluded from delivered-energy plot
for c in UNCATEGORISED_CODES:
    CHANNELS.setdefault("Uncategorised (project doc silent)", []).append(c)
CODE2CHANNEL = {c: ch for ch, codes in CHANNELS.items() for c in codes}

CHANNEL_COLORS = {
    "RT general (33)":                                "tab:red",
    "RT inter-zonal / network (34)":                   "tab:pink",
    "RT system-security RZ, PO 3.2 (61)":              "tab:orange",
    "Reserve management (68)":                          "tab:olive",
    "Voltage control / black-start (69)":              "tab:purple",
    "Catch-all 'other' bucket (81)":                    "tab:brown",
    "mFRR activation (92)":                             "tab:cyan",
    "System balancing / residual imbalance (94)":       "tab:green",
    "Uncategorised (project doc silent)":               "tab:grey",
}


def main():
    files = sorted(glob.glob(str(REPO / "data" / "processed" / "esios" / "restricciones" /
                                  "totalrp48preccierre_*.parquet")))
    files = [f for f in files if "_all" not in f]
    print(f"reading {len(files)} monthly files")
    con = duckdb.connect()
    q = f"""
    SELECT date_trunc('month', period_start_utc) AS month,
           tipo_redespacho,
           SUM(COALESCE(qty_up_mwh, 0))   AS up_mwh,
           SUM(COALESCE(qty_down_mwh, 0)) AS dn_mwh
    FROM read_parquet({files})
    WHERE period_start_utc >= TIMESTAMP '{START}'
      AND period_start_utc < TIMESTAMP '{END}'
    GROUP BY 1, 2
    """
    df = con.execute(q).df()
    df["month"] = pd.to_datetime(df["month"]).dt.tz_localize(None)
    df["code_str"] = df["tipo_redespacho"].astype(str)
    # Drop reservation-volume codes (code 23) — apples-to-oranges with delivered energy.
    df_reservation = df[df["code_str"].isin(RESERVATION_CODES)].copy()
    df = df[~df["code_str"].isin(RESERVATION_CODES)].copy()
    df["channel"] = df["code_str"].map(CODE2CHANNEL).fillna("Uncategorised (project doc silent)")
    df["up_gwh"] = df["up_mwh"] / 1000.0
    df["dn_gwh"] = df["dn_mwh"] / 1000.0
    df.to_csv(OUTDIR / "monthly_by_channel_code.csv", index=False)
    if len(df_reservation):
        df_reservation["up_gwh"] = df_reservation["up_mwh"] / 1000.0
        df_reservation["dn_gwh"] = df_reservation["dn_mwh"] / 1000.0
        df_reservation.to_csv(OUTDIR / "monthly_reservation_codes_excluded.csv", index=False)
        print(f"  (excluded {len(df_reservation)} reservation-code rows; see monthly_reservation_codes_excluded.csv)")

    by_chan = df.groupby(["month", "channel"], as_index=False)[["up_gwh", "dn_gwh"]].sum()
    by_chan.to_csv(OUTDIR / "monthly_by_channel.csv", index=False)

    print("\n=== Full-period totals by channel (GWh) ===")
    tot = by_chan.groupby("channel", as_index=False)[["up_gwh", "dn_gwh"]].sum().round(0)
    print(tot.to_string(index=False))

    # Stacked area plots: UP and DOWN
    fig, axes = plt.subplots(2, 1, figsize=(13, 8), sharex=True)
    channels = list(CHANNELS.keys())
    pivot_up = by_chan.pivot_table(index="month", columns="channel", values="up_gwh",
                                     fill_value=0).reindex(columns=channels)
    pivot_dn = by_chan.pivot_table(index="month", columns="channel", values="dn_gwh",
                                     fill_value=0).reindex(columns=channels)
    months = pivot_up.index
    cum_up = np.zeros(len(months))
    cum_dn = np.zeros(len(months))
    for ch in channels:
        u = pivot_up[ch].values
        axes[0].fill_between(months, cum_up, cum_up + u, color=CHANNEL_COLORS[ch],
                              alpha=0.75, label=ch)
        cum_up = cum_up + u
        d = pivot_dn[ch].values
        axes[1].fill_between(months, -cum_dn, -(cum_dn + d), color=CHANNEL_COLORS[ch],
                              alpha=0.75)
        cum_dn = cum_dn + d
    for ax in axes:
        ax.axvline(MTU15_IDA, color="gray", ls=":", lw=0.9)
        ax.axvline(BLACKOUT,  color="black", ls="-.", lw=0.9)
        ax.axvline(MTU15_DA,  color="red",   ls="--", lw=1.0)
        ax.axhline(0, color="black", lw=0.5)
        ax.grid(alpha=0.3)
        ax.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
        plt.setp(ax.get_xticklabels(), rotation=45, ha="right")
    axes[0].set_ylabel("UP-redispatch GWh / month")
    axes[1].set_ylabel("DOWN-redispatch GWh / month")
    axes[0].legend(loc="upper left", fontsize=8, frameon=False, ncol=2)
    fig.suptitle("Full REE post-clearing intervention by channel (ESIOS archive 28). " +
                  "Labels follow project-canonical reference (parser docstring + " +
                  "SPANISH\\_MARKET\\_STRUCTURE.md §13). Code 23 (capacity reservation) excluded.",
                  fontsize=10, y=1.00)
    plt.tight_layout(rect=[0, 0, 1, 0.97])
    out = FIGDIR / "fig_ree_intervention_full"
    plt.savefig(f"{out}.pdf", bbox_inches="tight")
    plt.savefig(f"{out}.png", bbox_inches="tight", dpi=130)
    plt.close(fig)
    print(f"saved {out}.pdf")


if __name__ == "__main__":
    main()
