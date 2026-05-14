"""Parse ESIOS /indicators JSON dumps into normalized long-format parquet.

The /indicators endpoint returns one JSON per call (default per-month chunk
by our `00_download_indicator.py`):

    {"indicator": {
        "id": 634, "name": "...", "short_name": "...",
        "magnitud": [{"name": "Precio €/MW", "id": 239}],
        "tiempo":   [{"name": "Hora", "id": 4}],
        "geos":     [{"geo_id": 8741, "geo_name": "Península"}, ...],
        "values":   [{"value": 18.52,
                      "datetime":     "2025-06-01T00:00:00.000+02:00",
                      "datetime_utc": "2025-05-31T22:00:00Z",
                      "tz_time":      "2025-05-31T22:00:00.000Z",
                      "geo_id": 8741, "geo_name": "Península"}, ...]}}

Conventions
-----------
- Timestamps are kept in UTC (`ts_utc`) and Europe/Madrid local (`ts_local`).
- `period_15min` is the 1-based 15-min position within the local-time day,
  computed from `ts_local`. For 24h days this runs 1..96; on DST-spring days
  it runs 1..92, on DST-fall it runs 1..100. Series at lower-than-15-min
  granularity (hourly, daily, monthly) leave `period_15min` set to the
  position of the period's *start* in the 15-min grid (e.g. an hourly value
  for 03:00–04:00 has period_15min=13).
- `geo_id`/`geo_name` are preserved verbatim. Spain peninsula is geo_id 8741
  for many series; `geo_id=3` ("España") for market clearing prices.
- Output schema (long format) — see `INDICATOR_SCHEMA` below.
- No filtering happens here. Callers downstream pick the geo they need.
"""
from __future__ import annotations

import json
from datetime import date as date_type
from pathlib import Path

import pandas as pd

INDICATOR_SCHEMA = {
    "indicator_id":   "int32",
    "ts_utc":         "datetime64[ns, UTC]",
    "ts_local":       "datetime64[ns, Europe/Madrid]",
    "date":           "object",     # python date — keep as object
    "hour":           "int16",
    "period_15min":   "int16",
    "geo_id":         "int32",
    "geo_name":       "object",
    "value":          "float64",
    "source_file":    "object",
}


def parse_indicator_json(path: Path) -> pd.DataFrame:
    """Parse one ESIOS indicator JSON file into long-format rows.

    Returns an empty DataFrame with the standard schema if the JSON has
    no `indicator.values` array (the API returns this when the date range
    has no data, e.g. discontinued series).
    """
    with open(path, "r", encoding="utf-8") as f:
        payload = json.load(f)

    ind = payload.get("indicator") or {}
    indicator_id = int(ind.get("id", 0))
    values = ind.get("values") or []

    if not values:
        return _empty_frame()

    df = pd.DataFrame(values)
    # ESIOS occasionally omits datetime_utc on legacy series; fall back to
    # `tz_time` (UTC) or to `datetime` (local) parsed with utc=True.
    if "datetime_utc" in df.columns:
        df["ts_utc"] = pd.to_datetime(df["datetime_utc"], utc=True, errors="coerce")
    elif "tz_time" in df.columns:
        df["ts_utc"] = pd.to_datetime(df["tz_time"], utc=True, errors="coerce")
    else:
        df["ts_utc"] = pd.to_datetime(df["datetime"], utc=True, errors="coerce")
    df = df[df["ts_utc"].notna()].copy()

    df["ts_local"] = df["ts_utc"].dt.tz_convert("Europe/Madrid")
    df["date"] = df["ts_local"].dt.date
    df["hour"] = df["ts_local"].dt.hour.astype("int16")

    # 15-min position within the local-time day. Compute as (minutes since
    # midnight local) // 15 + 1. Handles DST springs (92) and falls (100).
    minutes_since_midnight = (
        df["ts_local"].dt.hour * 60 + df["ts_local"].dt.minute
    )
    df["period_15min"] = (minutes_since_midnight // 15 + 1).astype("int16")

    df["indicator_id"] = indicator_id
    df["geo_id"] = df.get("geo_id", pd.Series([0] * len(df))).fillna(0).astype("int32")
    df["geo_name"] = df.get("geo_name", "").astype(str)
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df["source_file"] = path.name

    return df[list(INDICATOR_SCHEMA.keys())].reset_index(drop=True)


def _empty_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "indicator_id":   pd.Series([], dtype="int32"),
            "ts_utc":         pd.Series([], dtype="datetime64[ns, UTC]"),
            "ts_local":       pd.Series([], dtype="datetime64[ns, Europe/Madrid]"),
            "date":           pd.Series([], dtype="object"),
            "hour":           pd.Series([], dtype="int16"),
            "period_15min":   pd.Series([], dtype="int16"),
            "geo_id":         pd.Series([], dtype="int32"),
            "geo_name":       pd.Series([], dtype="object"),
            "value":          pd.Series([], dtype="float64"),
            "source_file":    pd.Series([], dtype="object"),
        }
    )


def parse_indicator_dir(raw_dir: Path) -> pd.DataFrame:
    """Concatenate every JSON in `raw_dir` (one indicator's monthly dumps)
    into a single DataFrame, deduplicated on (ts_utc, geo_id).

    Deduplication: when month chunks overlap (e.g. month boundary timestamp
    appears in both adjacent files), keep the row from the lexicographically
    later filename (newer download → newer values_updated_at).
    """
    parts = []
    for p in sorted(raw_dir.glob("indicator_*.json")):
        try:
            df = parse_indicator_json(p)
        except (json.JSONDecodeError, KeyError, ValueError) as e:
            print(f"  [WARN] {p.name}: {e}")
            continue
        if not df.empty:
            parts.append(df)
    if not parts:
        return _empty_frame()
    out = pd.concat(parts, ignore_index=True)
    # Sort then drop duplicates keeping the LAST occurrence (later source_file).
    out = out.sort_values(["ts_utc", "geo_id", "source_file"])
    out = out.drop_duplicates(subset=["ts_utc", "geo_id"], keep="last")
    return out.reset_index(drop=True)
