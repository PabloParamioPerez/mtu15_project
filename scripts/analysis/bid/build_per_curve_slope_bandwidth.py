# STATUS: ALIVE
# LAST-AUDIT: 2026-06-05
# FEEDS: Spec C bandwidth-robustness for (alpha, beta, gamma, N_eff).
#        Rebuilds per-curve slope/intercept/curvature panels at
#        wider bandwidths delta = {100, 140, 200} EUR/MWh, for the four
#        headline (reform, market) cells (ID15 IDA, ID15 DA, DA15 DA, DA15 IDA).
#
# OUT: data/derived/panels/per_curve_slope_windowed/
#        slope_{LABEL}_h{H}.parquet
#      for LABEL in {ID15_real_DA, ID15_real_IDA, DA15_real_DA, DA15_real_IDA}
#      and H in {100, 140, 200}.

from pathlib import Path
import build_per_curve_slope as m

WIDE = [100, 140, 200]
CONFIG = [
    # (label,         market, lo,           hi)
    ("ID15_real_DA",  "da",   "2024-06-14", "2025-04-27"),
    ("ID15_real_IDA", "ida",  "2024-06-14", "2025-04-27"),
    ("DA15_real_DA",  "da",   "2025-04-28", "2026-03-06"),
    ("DA15_real_IDA", "ida",  "2025-04-28", "2026-03-06"),
]

if __name__ == "__main__":
    for label, market, lo, hi in CONFIG:
        for h in WIDE:
            out = m.OUT_DIR / f"slope_{label}_h{h}.parquet"
            m.build(lo, hi, market, h, out)
