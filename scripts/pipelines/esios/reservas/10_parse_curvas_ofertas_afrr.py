"""Parse curvas_ofertas_afrr daily .xls files into per-month Parquet.

Output:
    data/processed/esios/reservas/curvas_ofertas_afrr_<yyyymm>.parquet
"""
from __future__ import annotations

import argparse
import sys
from collections import defaultdict
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[4]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

import pandas as pd  # noqa: E402

from mtu.parsing.esios.curvas_ofertas_afrr import parse_curvas_ofertas_afrr_xls  # noqa: E402
from mtu.parsing.omie_common import (  # noqa: E402
    append_csv_row,
    ensure_dir,
    utc_now_iso,
)

MARKET = "esios_reservas"
CATEGORY = "regulacion_secundaria"
FILE_FAMILY = "curvas_ofertas_afrr"
PARSER_NAME = "esios.curvas_ofertas_afrr.parse_curvas_ofertas_afrr_xls"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--overwrite", action="store_true")
    p.add_argument("--start-month", default=None, help="YYYY-MM")
    p.add_argument("--end-month", default=None, help="YYYY-MM")
    return p.parse_args()


def main() -> None:
    args = parse_args()

    raw_root = PROJECT_ROOT / "data/raw/esios/reservas/curvas_ofertas_afrr"
    processed_dir = PROJECT_ROOT / "data/processed/esios/reservas/curvas_ofertas_afrr"
    ingestion_log = PROJECT_ROOT / "data/metadata/ingestion_log.csv"
    ensure_dir(processed_dir)

    if not raw_root.exists():
        print(f"No raw root: {raw_root}")
        return

    by_month: dict[str, list[Path]] = defaultdict(list)
    for day_dir in sorted(raw_root.iterdir()):
        if not day_dir.is_dir():
            continue
        name = day_dir.name
        if not name.isdigit() or len(name) != 8:
            continue
        yyyymm = name[:6]
        if args.start_month and yyyymm < args.start_month.replace("-", ""):
            continue
        if args.end_month and yyyymm > args.end_month.replace("-", ""):
            continue
        by_month[yyyymm].append(day_dir)

    counts = {"success": 0, "skipped": 0, "failed": 0, "empty": 0}

    for yyyymm, day_dirs in sorted(by_month.items()):
        out_path = processed_dir / f"{FILE_FAMILY}_{yyyymm}.parquet"
        if out_path.exists() and not args.overwrite:
            counts["skipped"] += 1
            continue

        status = "success"
        error_message = ""
        rows_output = 0
        try:
            parts = []
            for d in day_dirs:
                # Find the .xls file directly in the day-dir (no extracted/ subdir for .xls payloads)
                for xls in sorted(d.glob("*.xls")):
                    sub = parse_curvas_ofertas_afrr_xls(xls)
                    if not sub.empty:
                        parts.append(sub)
            if not parts:
                df = pd.DataFrame()
            else:
                df = pd.concat(parts, ignore_index=True)
            rows_output = len(df)
            if df.empty:
                counts["empty"] += 1
                if out_path.exists():
                    out_path.unlink()
            else:
                df.to_parquet(out_path, index=False)
                counts["success"] += 1
                print(
                    f"[OK]      {FILE_FAMILY}_{yyyymm}: {len(df):,} rows, "
                    f"{len(day_dirs)} days"
                )
        except Exception as e:
            status = "failed"
            error_message = f"{type(e).__name__}: {e}"
            counts["failed"] += 1
            print(f"[FAIL] {yyyymm} -> {error_message}")

        append_csv_row(
            ingestion_log,
            {
                "ingested_at": utc_now_iso(),
                "market": MARKET,
                "category": CATEGORY,
                "file_family": FILE_FAMILY,
                "filename": f"{FILE_FAMILY}_{yyyymm}",
                "parser_name": PARSER_NAME,
                "raw_file_kind": "xls-daily",
                "status": status,
                "error_message": error_message,
                "rows_output": rows_output,
            },
        )

    print(
        f"\nDone. success={counts['success']}, skipped={counts['skipped']}, "
        f"empty={counts['empty']}, failed={counts['failed']}"
    )


if __name__ == "__main__":
    main()
