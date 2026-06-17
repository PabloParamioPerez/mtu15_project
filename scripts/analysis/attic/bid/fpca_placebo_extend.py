# STATUS: ALIVE
# LAST-AUDIT: 2026-05-18
# FEEDS: descriptive_facts.tex §3.4 (parallel-trends descriptive check, DA-side)
# CLAIM: Extend per-(entity, date, period) PC-score panel back to 2022-01-01
#        to enable descriptive parallel-trends checks. Place pre-existing
#        fPCA basis (pc_basis_<tech>.npz, fit on 2024-2026 sample) is reused
#        — we DO NOT re-fit. Curves are projected onto the existing basis.
#
# This is DA-only. IDA bid-format changed at 2024-06-14 (6 MIBEL → 3 SIDC
# sessions), so pre-2024-06-14 IDA placebo regressions are not comparable.
#
# Output:
#   results/regressions/bid/fpca/
#     pc_scores_<tech>_placebo.parquet      2022-01-01 to 2024-06-13 cells
#       Columns mirror pc_scores_<tech>.parquet so the two can be concatenated.

from __future__ import annotations
from pathlib import Path
import sys
import gc
import duckdb
import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
OUT  = REPO / "results" / "regressions" / "bid" / "fpca"

DET = REPO / "data/processed/omie/mercado_diario/ofertas/det_all.parquet"
CAB = REPO / "data/processed/omie/mercado_diario/ofertas/cab_all.parquet"
UNITS = REPO / "data/external/omie_reference/lista_unidades.csv"

PLACEBO_START = "2022-01-01"
PLACEBO_END   = "2024-06-13"

N_QUANTILES = 99
N_PCS = 5
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


def load_basis(tech: str):
    path = OUT / f"pc_basis_{tech.replace(' ', '_')}.npz"
    if not path.exists():
        raise FileNotFoundError(f"basis missing: {path}")
    b = np.load(path)
    return b["mean"].astype(np.float32), b["components"].astype(np.float32)


def regime_label(d: pd.Timestamp) -> str:
    # Placebo regimes — calendar-shifted analogues of the named reform windows.
    # 2022 and 2023 calendar windows that mirror the 2024-2025 transitions.
    if d < pd.Timestamp("2022-06-14"): return "pre-2022-Jun"
    if d <= pd.Timestamp("2022-11-30"): return "3-sess-2022"
    if d <= pd.Timestamp("2023-03-18"): return "ISP15-win-2023"
    if d <= pd.Timestamp("2023-04-27"): return "DA60_ID15-preblk-2023"
    if d <= pd.Timestamp("2023-09-30"): return "DA60_ID15-postblk-2023"
    if d <= pd.Timestamp("2023-12-31"): return "DA15_ID15-2023"
    if d <= pd.Timestamp("2024-03-18"): return "ISP15-win-2024"
    if d <= pd.Timestamp("2024-04-27"): return "DA60_ID15-preblk-2024"
    if d <= pd.Timestamp("2024-06-13"): return "DA60_ID15-postblk-2024"
    return "real"


def fetch_bids(tech: str, per_firm: bool, units_tech: pd.DataFrame,
               date_start: str, date_end: str) -> pd.DataFrame:
    con = duckdb.connect()
    con.execute("PRAGMA threads=4")
    con.execute("SET memory_limit='6GB'")
    con.register("uft", units_tech[["unit_code", "tech_group", "firm"]])
    entity_col = "firm" if per_firm else "unit_code"
    sql = f"""
    WITH cab AS (
        SELECT date::DATE AS d, offer_code, version, unit_code,
               ROW_NUMBER() OVER (PARTITION BY date::DATE, offer_code, unit_code ORDER BY version DESC) AS rn
        FROM '{CAB}'
        WHERE buy_sell='V' AND date::DATE BETWEEN '{date_start}' AND '{date_end}'
    ),
    cab_l AS (SELECT * FROM cab WHERE rn=1),
    det AS (
        SELECT date::DATE AS d, offer_code, version, period,
               price_eur_mwh AS p, quantity_mw AS q
        FROM '{DET}'
        WHERE date::DATE BETWEEN '{date_start}' AND '{date_end}' AND quantity_mw > 0
    ),
    bids AS (
        SELECT c.d, c.unit_code, u.firm, dv.period, dv.p, dv.q
        FROM det dv
        JOIN cab_l c USING (d, offer_code, version)
        JOIN uft u USING (unit_code)
    )
    SELECT d AS date, period, {entity_col} AS entity, p, q
    FROM bids
    ORDER BY date, period, entity, p
    """
    df = con.execute(sql).df()
    if len(df) == 0:
        return df
    df["date"] = pd.to_datetime(df["date"])
    df["p"] = df["p"].astype(np.float32)
    df["q"] = df["q"].astype(np.float32)
    df["period"] = df["period"].astype(np.int16)
    df["entity"] = df["entity"].astype("category")
    return df


def compute_quantile_curves(df: pd.DataFrame, q_grid: np.ndarray):
    """Return (keys[(date, period, entity)], curves[N x 99])."""
    keys = []
    curves = []
    grouped = df.groupby(["date", "period", "entity"], observed=True, sort=False)
    for (d, p, e), g in grouped:
        prices = g["p"].to_numpy()
        qty = g["q"].to_numpy()
        cum = np.cumsum(qty)
        total = cum[-1]
        if total <= 0:
            continue
        targets = q_grid * total
        idx = np.searchsorted(cum, targets, side="left")
        idx = np.clip(idx, 0, len(prices) - 1)
        keys.append((d, p, e))
        curves.append(prices[idx])
    if not curves:
        return [], np.zeros((0, len(q_grid)), dtype=np.float32)
    return keys, np.stack(curves)


def project_one_chunk(tech: str, per_firm: bool, units_tech: pd.DataFrame,
                      date_start: str, date_end: str,
                      mean: np.ndarray, components: np.ndarray,
                      q_grid: np.ndarray) -> pd.DataFrame | None:
    print(f"    chunk {date_start} → {date_end}")
    df = fetch_bids(tech, per_firm, units_tech, date_start, date_end)
    if len(df) == 0:
        return None
    print(f"      {len(df):,} bid rows")
    keys, curves = compute_quantile_curves(df, q_grid)
    if len(curves) == 0:
        return None
    print(f"      {len(curves):,} curves projected")
    scores = (curves - mean) @ components.T
    keys_arr = np.array(keys, dtype=object)
    out = pd.DataFrame({
        "date": pd.to_datetime(keys_arr[:, 0]),
        "period": keys_arr[:, 1].astype(np.int16),
        "entity": keys_arr[:, 2].astype(str),
    })
    for k in range(N_PCS):
        out[f"PC{k+1}"] = scores[:, k].astype(np.float32)
    return out


def run_tech(tech: str, per_firm: bool):
    out_path = OUT / f"pc_scores_{tech.replace(' ', '_')}_placebo.parquet"
    print(f"\n=== Placebo PC scores for {tech} (per_firm={per_firm}) ===")
    if out_path.exists():
        print(f"  {out_path} exists, skip")
        return

    mean, components = load_basis(tech)
    print(f"  basis loaded: mean {mean.shape}, components {components.shape}")

    units = load_units()
    units_tech = units[units["tech_group"] == tech].copy()
    if len(units_tech) == 0:
        print(f"  no units for {tech}, skip")
        return
    print(f"  {len(units_tech)} units in {tech}")

    q_grid = np.linspace(1/(N_QUANTILES+1), N_QUANTILES/(N_QUANTILES+1),
                         N_QUANTILES, dtype=np.float32)

    # Process in 3-month chunks
    chunks = []
    cur = pd.Timestamp(PLACEBO_START)
    end = pd.Timestamp(PLACEBO_END)
    while cur <= end:
        nxt = min(cur + pd.DateOffset(months=3) - pd.Timedelta(days=1), end)
        chunks.append((cur.strftime("%Y-%m-%d"), nxt.strftime("%Y-%m-%d")))
        cur = nxt + pd.Timedelta(days=1)

    print(f"  {len(chunks)} chunks")
    all_out = []
    for a, b in chunks:
        out = project_one_chunk(tech, per_firm, units_tech, a, b,
                                mean, components, q_grid)
        if out is not None:
            all_out.append(out)
        gc.collect()

    if not all_out:
        print(f"  no data for {tech}")
        return

    df = pd.concat(all_out, ignore_index=True)
    df["regime"] = df["date"].apply(regime_label)
    df["ym"] = df["date"].dt.strftime("%Y-%m")
    df.to_parquet(out_path, index=False)
    print(f"  wrote {out_path} ({len(df):,} rows, {out_path.stat().st_size/1e6:.1f} MB)")


def main():
    techs_arg = sys.argv[1:] if len(sys.argv) > 1 else ["CCGT"]
    for t in techs_arg:
        per_firm = t in NON_STRATEGIC
        run_tech(t, per_firm)


if __name__ == "__main__":
    main()
