# STATUS: ALIVE
# LAST-AUDIT: 2026-04-27
# FEEDS: F9 + new F19/F20 candidates from per-BSP aFRR
# CLAIM: Per-firm aFRR up-reserve activation share, evolution 2015-2025
"""Per-firm aFRR settlement analysis using ESIOS liquicierre data.

Builds on the existing 52.6M-row per-BSP aFRR settlement panel and the
new BSP -> firm mapping at data/external/esios_reference/bsp_to_firm.csv.

Key info codes:
  RMRSP / RMRSN  — secondary reserve margin up / down (volume of band assigned)
  EnAcSuTo       — total energy activated up
  EnAcBaTo       — total energy activated down
  PreSubPo       — price for upward activation
  PreBajPo       — price for downward activation
  COEFPAR        — participation coefficient (share of band the BSP carries)

Outputs (one CSV each):
  per_firm_afrr_band_share_yearly.csv     # who provides the band each year
  per_firm_afrr_activation_yearly.csv     # who actually got activated
  per_firm_afrr_blackout_split.csv        # pre vs post 2025-04-28 blackout

NOTE on confidence: BSP -> firm mapping is high-confidence for IB
(IMA/IGN/IGR), Naturgy (GN/GN3/GN4/GNE), Endesa (END/ENC/EV), HC.
Subsidiary BSPs (EN1/IGS/IGE/GST/AC2/AX2/...) are medium-confidence and
small in volume. Several small BSPs are unmapped ("Other").
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd

PROJECT = Path(__file__).resolve().parents[3]


def main() -> None:
    map_df = pd.read_csv(PROJECT / "data/external/esios_reference/bsp_to_firm.csv")
    bsp_to_firm = dict(zip(map_df["bsp"], map_df["firm"]))

    cols = ["bsp", "info", "period_start_utc", "ctd", "precio", "date"]
    df = pd.read_parquet(PROJECT / "data/processed/esios/reservas/liquicierre_all.parquet",
                         columns=cols)
    df["firm"] = df["bsp"].map(bsp_to_firm).fillna("Unmapped")
    df["year"] = pd.to_datetime(df["date"]).dt.year

    # 1. RMRSP — band assigned, upward direction
    print("=" * 72)
    print("Per-firm aFRR up-reserve BAND assigned (RMRSP, MWh/MTU equivalent)")
    print("Aggregate: sum of ctd (15-min MW values, MTU)")
    print("=" * 72)
    band = df[df["info"] == "RMRSP"]
    band_yr = band.groupby(["year", "firm"])["ctd"].sum().div(1e3).round(0)  # GWh-equiv
    pivot_band = band_yr.unstack("firm").fillna(0)
    big4 = ["IB", "GE", "GN", "HC"]
    cols_show = [f for f in big4 if f in pivot_band.columns] + \
                [f for f in pivot_band.columns if f not in big4]
    print(pivot_band[cols_show].to_string())

    # 2. Activation — actual energy delivered up
    print()
    print("=" * 72)
    print("Per-firm aFRR UP-activation energy (EnAcSuTo info code, MWh)")
    print("=" * 72)
    actv = df[df["info"] == "EnAcSuTo"]
    actv_yr = actv.groupby(["year", "firm"])["ctd"].sum().round(0)
    pivot_actv = actv_yr.unstack("firm").fillna(0)
    if len(pivot_actv) > 0:
        cols_show2 = [f for f in big4 if f in pivot_actv.columns] + \
                     [f for f in pivot_actv.columns if f not in big4]
        print(pivot_actv[cols_show2].to_string())
    else:
        print("(EnAcSuTo not in panel — only RMRSP/RMRSN/etc. data; check info code names)")

    # 3. Pre-/post-blackout split — use EnAcSuTo (post-2024-12 archive code)
    # Note: liquicierre uses RMRSP/etc; liquicierresrs uses EnAcSuTo/etc.
    # For Apr-Jun 2025 (post-blackout) only liquicierresrs is available.
    print()
    print("=" * 72)
    print("Pre vs Post 2025-04-28 blackout: per-firm aFRR UP-activation MWh")
    print("(using EnAcSuTo info code — only post-2024-12 archive has it)")
    print("=" * 72)
    actv_only = df[df["info"] == "EnAcSuTo"].copy()
    actv_only["ts"] = pd.to_datetime(actv_only["date"])
    pre = actv_only[(actv_only.ts >= "2025-03-19") & (actv_only.ts < "2025-04-28")]
    post = actv_only[(actv_only.ts >= "2025-04-28") & (actv_only.ts < "2025-10-01")]
    da15 = actv_only[(actv_only.ts >= "2025-10-01")]
    rows = []
    for firm in sorted(actv_only["firm"].unique()):
        pre_mwh = pre[pre.firm == firm]["ctd"].sum()
        post_mwh = post[post.firm == firm]["ctd"].sum()
        da15_mwh = da15[da15.firm == firm]["ctd"].sum()
        total = pre_mwh + post_mwh + da15_mwh
        if total > 100:
            rows.append({
                "firm": firm,
                "DA60_PRE_blackout_MWh": round(pre_mwh, 0),
                "DA60_POST_blackout_MWh": round(post_mwh, 0),
                "DA15_ID15_MWh": round(da15_mwh, 0),
                "post_share_%": round(100 * post_mwh / post["ctd"].sum(), 2) if post["ctd"].sum() > 0 else 0,
            })
    out = pd.DataFrame(rows).sort_values("DA60_POST_blackout_MWh", ascending=False)
    print(out.to_string(index=False))

    # 4. Per-firm aFRR up-REVENUE by reform regime (F20)
    print()
    print("=" * 72)
    print("Per-firm aFRR up-activation REVENUE 2025 by reform regime (EUR M)")
    print("Joining PreSubPo (€/MWh) × EnAcSuTo (MWh) on (bsp, period_start_utc)")
    print("=" * 72)
    prc = df[df["info"] == "PreSubPo"][["bsp", "period_start_utc", "precio", "firm", "date"]].rename(
        columns={"precio": "price_eur_mwh"})
    qty = df[df["info"] == "EnAcSuTo"][["bsp", "period_start_utc", "ctd"]].rename(
        columns={"ctd": "mwh"})
    rev_df = prc.merge(qty, on=["bsp", "period_start_utc"], how="inner")
    rev_df = rev_df[rev_df["mwh"] > 0]
    rev_df["rev_eur"] = rev_df["mwh"] * rev_df["price_eur_mwh"]
    rev_df["ts"] = pd.to_datetime(rev_df["date"])

    def regime(d):
        if d < pd.Timestamp("2025-03-19"):
            return "pre-MTU15"
        if d < pd.Timestamp("2025-04-28"):
            return "DA60_PRE_blackout"
        if d < pd.Timestamp("2025-10-01"):
            return "DA60_POST_blackout"
        return "DA15/ID15"

    rev_df["reg"] = rev_df["ts"].apply(regime)
    rev = rev_df.groupby(["reg", "firm"])["rev_eur"].sum().div(1e6).round(2)
    pivot_rev = rev.unstack("firm").fillna(0)
    cols_rev = [f for f in big4 if f in pivot_rev.columns] + \
               [f for f in pivot_rev.columns if f not in big4]
    print(pivot_rev[cols_rev].to_string())

    # Save outputs
    out_dir = PROJECT / "data/derived/results"
    out_dir.mkdir(parents=True, exist_ok=True)
    pivot_band.to_csv(out_dir / "per_firm_afrr_band_share_yearly.csv")
    if len(pivot_actv) > 0:
        pivot_actv.to_csv(out_dir / "per_firm_afrr_activation_yearly.csv")
    out.to_csv(out_dir / "per_firm_afrr_blackout_split.csv", index=False)
    pivot_rev.to_csv(out_dir / "per_firm_afrr_revenue_by_regime.csv")
    print(f"\nwrote {out_dir}/per_firm_afrr_*.csv")


if __name__ == "__main__":
    main()
