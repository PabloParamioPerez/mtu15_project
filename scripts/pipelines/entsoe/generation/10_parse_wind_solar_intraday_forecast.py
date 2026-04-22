from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[4]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from mtu.parsing.entsoe.wind_solar_forecast import parse_file  # noqa: E402
from mtu.parsing.omie_common import (  # noqa: E402
    append_csv_row,
    ensure_dir,
    utc_now_iso,
    visible_files,
)

MARKET = "entsoe_generation"
CATEGORY = "wind_solar_intraday_forecast"
FILE_FAMILY = "wind_solar_intraday_forecast"
PARSER_NAME = "entsoe.wind_solar_forecast.parse_file"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=(
            "Parse ENTSO-E A69 wind & solar intraday-forecast (processType "
            "A18) XMLs into per-month Parquet."
        )
    )
    p.add_argument("--overwrite", action="store_true")
    return p.parse_args()


def main() -> None:
    args = parse_args()

    raw_dir = PROJECT_ROOT / "data/raw/entsoe/generation/wind_solar_intraday_forecast"
    processed_dir = PROJECT_ROOT / "data/processed/entsoe/generation/wind_solar_intraday_forecast"
    ingestion_log = PROJECT_ROOT / "data/metadata/ingestion_log.csv"
    ensure_dir(processed_dir)

    files = visible_files(raw_dir)
    if not files:
        print(f"No raw XMLs found in {raw_dir}")
        return

    counts = {"success": 0, "skipped": 0, "failed": 0, "empty": 0}

    for raw_path in files:
        if not raw_path.name.endswith(".xml"):
            continue

        out_name = raw_path.stem + ".parquet"
        out_path = processed_dir / out_name

        if out_path.exists() and not args.overwrite:
            if out_path.stat().st_mtime >= raw_path.stat().st_mtime:
                counts["skipped"] += 1
                continue

        status = "success"
        error_message = ""
        rows_output = 0

        try:
            df = parse_file(raw_path)
            rows_output = len(df)
            if df.empty:
                counts["empty"] += 1
                if out_path.exists():
                    out_path.unlink()
            else:
                df.to_parquet(out_path, index=False)
                counts["success"] += 1
        except Exception as e:
            status = "failed"
            error_message = f"{type(e).__name__}: {e}"
            counts["failed"] += 1
            print(f"[FAIL] {raw_path.name} -> {error_message}")

        append_csv_row(
            ingestion_log,
            {
                "ingested_at": utc_now_iso(),
                "market": MARKET,
                "category": CATEGORY,
                "file_family": FILE_FAMILY,
                "filename": raw_path.name,
                "parser_name": PARSER_NAME,
                "raw_file_kind": "xml",
                "rows_read": rows_output,
                "rows_output": rows_output,
                "status": status,
                "output_path": str(out_path.relative_to(PROJECT_ROOT)),
                "error_message": error_message,
            },
        )

    print(
        f"Done. success={counts['success']}, empty={counts['empty']}, "
        f"skipped={counts['skipped']}, failed={counts['failed']}"
    )


if __name__ == "__main__":
    main()
