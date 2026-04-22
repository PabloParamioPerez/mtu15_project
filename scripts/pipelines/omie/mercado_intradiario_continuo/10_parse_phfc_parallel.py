from __future__ import annotations

import csv
import sys
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[4]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from mtu.parsing.mercado_intradiario_continuo.phfc import (  # noqa: E402
    FILENAME_RE,
    parse_phfc_file,
    write_parquet_for_file,
)
from mtu.parsing.omie_common import ensure_dir, utc_now_iso, visible_files  # noqa: E402

PARSER_NAME = "mtu.parsing.mercado_intradiario_continuo.phfc.parse_phfc_file:v2"
N_WORKERS = 8


def _parse_one(args: tuple[Path, Path]) -> dict:
    path, processed_dir = args
    out_path = processed_dir / f"{path.name}.parquet"

    if not FILENAME_RE.match(path.name):
        return {
            "filename": path.name,
            "status": "skipped",
            "rows_read": "",
            "rows_output": 0,
            "output_path": "",
            "error_message": "Filename does not match phfc pattern",
        }

    if out_path.exists():
        return {
            "filename": path.name,
            "status": "skipped",
            "rows_read": "",
            "rows_output": 0,
            "output_path": str(out_path),
            "error_message": "Output parquet already exists",
        }

    try:
        df = parse_phfc_file(path)

        if df.empty:
            return {
                "filename": path.name,
                "status": "skipped",
                "rows_read": 0,
                "rows_output": 0,
                "output_path": "",
                "error_message": "Empty file (no data rows)",
            }

        write_parquet_for_file(df, processed_dir, path.name)

        return {
            "filename": path.name,
            "status": "success",
            "rows_read": len(df),
            "rows_output": len(df),
            "output_path": str(out_path),
            "error_message": "",
        }

    except Exception as e:
        return {
            "filename": path.name,
            "status": "failed",
            "rows_read": "",
            "rows_output": 0,
            "output_path": "",
            "error_message": str(e),
        }


def _append_log_rows(csv_path: Path, rows: list[dict]) -> None:
    if not rows:
        return
    with csv_path.open("r", newline="", encoding="utf-8") as f:
        header = next(csv.reader(f))
    with csv_path.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=header)
        for row in rows:
            writer.writerow({col: row.get(col, "") for col in header})


def main() -> None:
    raw_dir = PROJECT_ROOT / "data/raw/omie/mercado_intradiario_continuo/programas/phfc"
    processed_dir = PROJECT_ROOT / "data/processed/omie/mercado_intradiario_continuo/programas/phfc"
    ingestion_log_csv = PROJECT_ROOT / "data/metadata/ingestion_log.csv"

    ensure_dir(processed_dir)

    files = visible_files(raw_dir)
    args = [(path, processed_dir) for path in files]
    total = len(args)
    print(f"Files to process: {total}  |  workers: {N_WORKERS}")

    log_rows: list[dict] = []
    counts = {"success": 0, "skipped": 0, "failed": 0}
    done = 0

    ingested_at = utc_now_iso()

    with ProcessPoolExecutor(max_workers=N_WORKERS) as executor:
        futures = {executor.submit(_parse_one, a): a[0].name for a in args}
        for future in as_completed(futures):
            result = future.result()
            status = result["status"]
            counts[status] = counts.get(status, 0) + 1
            done += 1

            if status == "success":
                log_rows.append({
                    "ingested_at": ingested_at,
                    "market": "mercado_intradiario_continuo",
                    "category": "programas",
                    "file_family": "phfc",
                    "filename": result["filename"],
                    "parser_name": PARSER_NAME,
                    "raw_file_kind": "omie_text",
                    "rows_read": result["rows_read"],
                    "rows_output": result["rows_output"],
                    "status": "success",
                    "output_path": result["output_path"],
                    "error_message": "",
                })
            elif status == "failed":
                log_rows.append({
                    "ingested_at": ingested_at,
                    "market": "mercado_intradiario_continuo",
                    "category": "programas",
                    "file_family": "phfc",
                    "filename": result["filename"],
                    "parser_name": PARSER_NAME,
                    "raw_file_kind": "omie_text",
                    "rows_read": "",
                    "rows_output": 0,
                    "status": "failed",
                    "output_path": "",
                    "error_message": result["error_message"],
                })
                print(f"  FAILED: {result['filename']}: {result['error_message']}")

            if done % 500 == 0 or done == total:
                pct = 100 * done / total
                print(f"  {done}/{total} ({pct:.1f}%)  success={counts['success']}  failed={counts['failed']}  skipped={counts['skipped']}")

    _append_log_rows(ingestion_log_csv, log_rows)

    print(f"\nDone. success={counts['success']}, failed={counts['failed']}, skipped={counts['skipped']}")


if __name__ == "__main__":
    main()
