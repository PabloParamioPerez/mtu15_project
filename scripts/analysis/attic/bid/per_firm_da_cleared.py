# STATUS: ALIVE
# LAST-AUDIT: 2026-05-29
# FEEDS: thesis/provisional/advisor_memo.tex sec 4.A -- per-firm decomposition
#        of the surviving DA15 day-ahead CCGT cleared-volume scale-up.
#
# For each (firm_class) within CCGT, compute daily DA cleared GWh in pre and
# post windows of the DA15 reform, with a 2024 same-calendar placebo.
# Reports placebo-net (post - pre, real - placebo) per firm class.

from pathlib import Path
import duckdb
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
PDBC = REPO / "data/processed/omie/mercado_diario/programas/pdbc_all.parquet"
UMAP = REPO / "data/derived/panels/bid_shape_critical_flat/_unit_map.parquet"
OUT  = REPO / "results/regressions/bid/mtu15_critical_flat/per_firm_da_cleared.csv"

# DA15 + ID15 windows (real and 2024 same-calendar placebo). For ID15 the
# placebo lives in 2024 with the same calendar offset.
WINDOWS = {
    "DA15_real":    ("2025-04-28", "2025-09-30", "2025-10-01", "2025-12-31"),
    "DA15_placebo": ("2024-04-28", "2024-09-30", "2024-10-01", "2024-12-31"),
    "ID15_real":    ("2024-12-11", "2025-03-18", "2025-03-19", "2025-04-27"),
    "ID15_placebo": ("2023-12-11", "2024-03-18", "2024-03-19", "2024-04-27"),
}


def compute(label, pre_lo, pre_hi, post_lo, post_hi):
    con = duckdb.connect()
    sql = f"""
    WITH q AS (
        SELECT CAST(date AS DATE) d, unit_code,
               SUM(assigned_power_mw * COALESCE(mtu_minutes,60)/60.0) / 1000 gwh
        FROM '{PDBC}'
        WHERE date BETWEEN '{pre_lo}' AND '{post_hi}'
          AND assigned_power_mw > 0
        GROUP BY 1, 2
    )
    SELECT q.d, u.firm_class, SUM(q.gwh) gwh
    FROM q JOIN '{UMAP}' u ON q.unit_code = u.unit_code
    WHERE u.tech_group = 'CCGT'
    GROUP BY 1, 2
    """
    df = con.execute(sql).fetchdf()
    df["d"] = pd.to_datetime(df["d"])
    df["era"] = df["d"].apply(
        lambda x: "pre" if x < pd.Timestamp(post_lo) else "post")
    summ = df.groupby(["firm_class", "era"]).agg(
        gwh_per_day=("gwh", "mean"), n_days=("gwh", "count")).reset_index()
    summ["label"] = label
    return summ


parts = []
for lbl, (pre_lo, pre_hi, post_lo, post_hi) in WINDOWS.items():
    parts.append(compute(lbl, pre_lo, pre_hi, post_lo, post_hi))
df = pd.concat(parts, ignore_index=True)

# Pivot to wide and compute placebo-net deltas
piv = df.pivot_table(index="firm_class",
                     columns=["label", "era"],
                     values="gwh_per_day", aggfunc="first")
piv.columns = [f"{a}_{b}" for a, b in piv.columns]
piv["DA15_delta_real"]    = piv["DA15_real_post"]    - piv["DA15_real_pre"]
piv["DA15_delta_placebo"] = piv["DA15_placebo_post"] - piv["DA15_placebo_pre"]
piv["DA15_placebo_net"]   = piv["DA15_delta_real"]   - piv["DA15_delta_placebo"]
piv["ID15_delta_real"]    = piv["ID15_real_post"]    - piv["ID15_real_pre"]
piv["ID15_delta_placebo"] = piv["ID15_placebo_post"] - piv["ID15_placebo_pre"]
piv["ID15_placebo_net"]   = piv["ID15_delta_real"]   - piv["ID15_delta_placebo"]

print("=== Per-firm-class CCGT DA cleared GWh/day, placebo-net per reform ===")
print("(post - pre, real - same-calendar placebo)")
print(piv[["DA15_placebo_net", "ID15_placebo_net"]].round(3).to_string())

OUT.parent.mkdir(parents=True, exist_ok=True)
piv.to_csv(OUT)
print(f"\nWrote {OUT}")
