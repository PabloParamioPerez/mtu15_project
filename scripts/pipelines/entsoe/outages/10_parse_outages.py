"""Parse ENTSO-E A80 generation unavailability documents.

A80 returns one TimeSeries per outage event with: business_type
(A53=planned, A54=forced), unit info, start/end timestamps, available
capacity vs nominal capacity, reason. We extract one row per outage
event (not per-ISP, since outages are durations).

Each XML contains all outages PUBLISHED in that month, but the outage
itself may span past months. We deduplicate by mRID across files.

Output:
  data/processed/entsoe/outages/outages_planned_all.parquet
  data/processed/entsoe/outages/outages_forced_all.parquet
"""
from __future__ import annotations

import xml.etree.ElementTree as ET
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[4]
NS = {
    "u": "urn:iec62325.351:tc57wg16:451-n:unavailibilityofgenerationdocument:7:1",
    "u6": "urn:iec62325.351:tc57wg16:451-n:unavailibilityofgenerationdocument:6:0",
    "uany": "urn:iec62325.351:tc57wg16:451-n:unavailibilityofgenerationdocument",
}


def _localname(tag: str) -> str:
    return tag.split("}", 1)[1] if "}" in tag else tag


def _walk(elem):
    """Iter all descendants by local name (ignores namespace)."""
    for child in elem.iter():
        yield child


def parse_one(path: Path) -> list[dict]:
    """Parse Unavailability documents (potentially merged via <entsoe_merged>).

    Each <TimeSeries> is one outage event with metadata + period(s) of
    available capacity. We extract one row per event.
    """
    try:
        tree = ET.parse(path)
    except ET.ParseError:
        return []
    root = tree.getroot()

    out: list[dict] = []

    # Find all Unavailability_MarketDocument under root (whether root IS one or wraps several)
    docs = []
    if _localname(root.tag) == "Unavailability_MarketDocument":
        docs = [root]
    else:
        for d in root.iter():
            if _localname(d.tag) == "Unavailability_MarketDocument":
                docs.append(d)

    for doc in docs:
        # Document-level mRID + start/end (the publication window)
        doc_mrid = None
        for child in doc:
            if _localname(child.tag) == "mRID":
                doc_mrid = child.text
                break

        # Iterate TimeSeries inside this document
        for ts in doc:
            if _localname(ts.tag) != "TimeSeries":
                continue
            rec = {"doc_mrid": doc_mrid}
            start_date = start_time = end_date = end_time = None
            min_avail_mw = None
            for el in _walk(ts):
                ln = _localname(el.tag)
                txt = el.text
                if ln == "mRID":
                    rec.setdefault("ts_mrid", f"{doc_mrid}_{txt}")
                elif ln == "businessType":
                    rec["business_type"] = txt
                elif ln == "production_RegisteredResource.mRID":
                    rec["unit_eic"] = txt
                elif ln == "production_RegisteredResource.name":
                    rec["unit_name"] = txt
                elif ln == "production_RegisteredResource.location.name":
                    rec["location"] = txt
                elif ln == "production_RegisteredResource.pSRType.psrType":
                    rec["psr_type"] = txt
                elif ln == "production_RegisteredResource.pSRType.powerSystemResources.nominalP":
                    try:
                        rec["nominal_mw"] = float(txt)
                    except (ValueError, TypeError):
                        pass
                elif ln == "start_DateAndOrTime.date":
                    start_date = txt
                elif ln == "start_DateAndOrTime.time":
                    start_time = txt
                elif ln == "end_DateAndOrTime.date":
                    end_date = txt
                elif ln == "end_DateAndOrTime.time":
                    end_time = txt
                elif ln == "quantity":
                    # In Available_Period/Point — captures the curtailed capacity
                    try:
                        v = float(txt)
                        if min_avail_mw is None or v < min_avail_mw:
                            min_avail_mw = v
                    except (ValueError, TypeError):
                        pass

            if start_date and start_time:
                rec["start_utc"] = f"{start_date}T{start_time}"
            if end_date and end_time:
                rec["end_utc"] = f"{end_date}T{end_time}"
            if min_avail_mw is not None:
                rec["min_avail_mw"] = min_avail_mw

            # Reason
            for r in ts.iter():
                if _localname(r.tag) != "Reason":
                    continue
                for rc in r:
                    rln = _localname(rc.tag)
                    if rln == "code":
                        rec["reason_code"] = rc.text
                    elif rln == "text":
                        rec["reason_text"] = rc.text
                break
            if rec.get("business_type") and rec.get("unit_eic"):
                out.append(rec)
    return out


def parse_dir(d: Path, label: str) -> pd.DataFrame:
    rows: list[dict] = []
    for f in sorted(d.glob("*.xml")):
        if f.stat().st_size < 50:
            continue
        try:
            head = f.read_bytes()[:8]
        except Exception:
            continue
        if head == b"<empty/>":
            continue
        rows.extend(parse_one(f))
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    # Dedupe by ts_mrid (same outage event re-published in successive months)
    if "ts_mrid" in df.columns:
        df = df.drop_duplicates(subset=["ts_mrid"], keep="last")
    df["start_utc"] = pd.to_datetime(df["start_utc"], errors="coerce", utc=True).dt.tz_localize(None)
    df["end_utc"] = pd.to_datetime(df["end_utc"], errors="coerce", utc=True).dt.tz_localize(None)
    df["duration_h"] = (df["end_utc"] - df["start_utc"]).dt.total_seconds() / 3600
    df["category"] = label
    return df


def main() -> None:
    out_dir = PROJECT_ROOT / "data/processed/entsoe/outages"
    out_dir.mkdir(parents=True, exist_ok=True)
    for sub, label in [
        ("outages_generation_planned", "planned"),
        ("outages_generation_forced", "forced"),
    ]:
        d = PROJECT_ROOT / f"data/raw/entsoe/{sub}"
        if not d.exists():
            print(f"skip {sub} (not found)")
            continue
        df = parse_dir(d, label)
        if df.empty:
            print(f"{sub}: empty")
            continue
        out = out_dir / f"outages_{label}_all.parquet"
        df.to_parquet(out, index=False)
        print(f"{sub}: {len(df):,} outage events → {out}")
        # Quick summary
        if "psr_type" in df.columns:
            print(f"  psr_types: {df.psr_type.value_counts().head(8).to_dict()}")


if __name__ == "__main__":
    main()
