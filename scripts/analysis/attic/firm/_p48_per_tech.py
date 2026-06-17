# Helper for fetching P48 (final operational program) per technology from
# ESIOS. P48 is the most-downstream firm-program checkpoint REE publishes;
# it includes everything in PHF + continuous market + per-round REE
# adjustments + real-time technical restrictions + balancing (RR / aFRR /
# mFRR / SRAD).
#
# Per-tech indicators (from `https://api.esios.ree.es/indicators?text=P48`):
#   CCGT (Ciclo combinado)            79
#   Nuclear                           74
#   Hydro UGH (large reservoir)       71
#   Hydro no UGH (small / run-of-river) 72
#   Turbinación bombeo (pump gen)     73
#   Consumo bombeo (pump load)        95
#   Wind (combined)                10010
#   Solar PV                          84
#   Cogen (combined)               10011
#   Coal (combined)                10008
#   Biomass                           91
#   Hybrid                          2131
#
# All indicators are 15-min, Peninsula-only, native unit MW.
#
# Output: weekly GWh per (tech, week_start), positive for generation,
# *signed* for Pump_load (we negate Consumo bombeo so demand sits below 0).
#
# Cache at data/derived/panels/p48_weekly_by_tech.parquet.

from pathlib import Path
import os
import time

import pandas as pd
import requests

REPO = Path(__file__).resolve().parents[3]
CACHE = REPO / "data/derived/panels/p48_weekly_by_tech.parquet"

# Tech -> list of ESIOS indicator IDs to sum (positive contribution by default).
# Pump_load uses indicator 95 (Consumo bombeo) but reported as NEGATIVE (load).
TECH_TO_INDICATORS = {
    # main figure
    "CCGT":               [79],
    "Nuclear":            [74],
    "Hydro":              [71, 72],     # large + small conventional hydro
    "Hydro_pump":         [73],         # turbinación bombeo (generation mode)
    "Wind":               [10010],
    "Solar PV":           [84],
    # appendix figure
    "Cogen":              [10011],
    "Coal":               [10008],
    "Biomass":            [91],
    "Hydro_RES":          [72],         # hidráulica no UGH (small RES hydro)
    "Pump_load":          [95],         # consumo bombeo — sign-flipped below
    "Hybrid_RES_storage": [2131],
}
NEG_TECHS = set()  # ESIOS already returns Consumo bombeo (95) as negative MW


def _load_token() -> str:
    env = (REPO / ".env").read_text()
    for line in env.splitlines():
        if line.strip().startswith("ESIOS_TOKEN"):
            return line.split("=", 1)[1].strip().strip("'").strip('"')
    raise RuntimeError("ESIOS_TOKEN not found in .env")


def _fetch_indicator(ind_id: int, start: str, end: str, token: str) -> pd.DataFrame:
    """Fetch one ESIOS indicator for the [start, end] window."""
    hdr = {"x-api-key": token,
           "Accept": "application/json; application/vnd.esios-api-v1+json"}
    rows = []
    # 3-month chunks: ESIOS occasionally drops mid-stream on long pulls; smaller
    # chunks fail less often and resume cheaply.
    s = pd.Timestamp(start); e = pd.Timestamp(end)
    cur = s
    while cur < e:
        nxt = min(cur + pd.DateOffset(months=3), e)
        params = {
            "start_date": cur.strftime("%Y-%m-%dT00:00"),
            "end_date":   nxt.strftime("%Y-%m-%dT23:59"),
        }
        chunk_rows = None
        for attempt in range(5):
            try:
                r = requests.get(f"https://api.esios.ree.es/indicators/{ind_id}",
                                  params=params, headers=hdr, timeout=180)
                r.raise_for_status()
                chunk_rows = r.json()["indicator"]["values"]
                break
            except (requests.exceptions.ChunkedEncodingError,
                    requests.exceptions.ConnectionError,
                    requests.exceptions.Timeout,
                    requests.exceptions.HTTPError) as exc:
                wait = 2 ** attempt
                print(f"    retry {attempt+1} for {ind_id} {cur.date()}: {exc.__class__.__name__} (sleep {wait}s)", flush=True)
                time.sleep(wait)
        if chunk_rows is None:
            raise RuntimeError(f"failed to fetch indicator {ind_id} starting {cur.date()}")
        rows.extend(chunk_rows)
        cur = nxt + pd.Timedelta(seconds=1)
    if not rows:
        return pd.DataFrame(columns=["ts_local", "value"])
    df = pd.DataFrame(rows)
    df["ts_local"] = (pd.to_datetime(df["datetime"], utc=True)
                        .dt.tz_convert("Europe/Madrid")
                        .dt.tz_localize(None))
    df = df[["ts_local", "value"]]
    return df


def build_p48_weekly(start: str = "2024-01-01", end: str = "2026-03-01",
                     use_cache: bool = True) -> pd.DataFrame:
    """Return weekly (tech, week_start, p48_gwh) DataFrame.

    p48_gwh is signed: positive for generators, negative for Pump_load.
    """
    if use_cache and CACHE.exists():
        return pd.read_parquet(CACHE)

    token = _load_token()
    print(f"Fetching P48 indicators for {start} -> {end}", flush=True)
    # First, fetch each unique indicator once into a dict
    all_inds = {i for ids in TECH_TO_INDICATORS.values() for i in ids}
    ind_data = {}
    for ind_id in sorted(all_inds):
        print(f"  ESIOS {ind_id}...", flush=True)
        ind_data[ind_id] = _fetch_indicator(ind_id, start, end, token)

    # Combine per tech: sum the relevant indicators per timestamp
    parts = []
    for tech, ids in TECH_TO_INDICATORS.items():
        frames = [ind_data[i] for i in ids if not ind_data[i].empty]
        if not frames:
            continue
        merged = frames[0].copy()
        for f in frames[1:]:
            merged = merged.merge(f, on="ts_local", how="outer", suffixes=("", "_x"))
            merged["value"] = merged["value"].fillna(0) + merged["value_x"].fillna(0)
            merged = merged.drop(columns=["value_x"])
        merged["tech"] = tech
        if tech in NEG_TECHS:
            merged["value"] = -merged["value"]
        parts.append(merged)
    raw = pd.concat(parts, ignore_index=True)

    # MW (15-min point) -> GWh per quarter-hour, then sum to weekly
    raw["gwh"] = raw["value"] * 0.25 / 1000.0
    raw["d"] = raw["ts_local"].dt.normalize()
    raw["week_start"] = raw["d"] - pd.to_timedelta(raw["d"].dt.weekday, unit="D")
    weekly = (raw.groupby(["tech", "week_start"], as_index=False)["gwh"]
                .sum()
                .rename(columns={"gwh": "p48_gwh"}))

    CACHE.parent.mkdir(parents=True, exist_ok=True)
    weekly.to_parquet(CACHE, index=False)
    print(f"  cached -> {CACHE}", flush=True)
    return weekly


if __name__ == "__main__":
    df = build_p48_weekly(use_cache=False)
    print(df.groupby("tech")["p48_gwh"].agg(["count", "mean", "min", "max"]).to_string())
