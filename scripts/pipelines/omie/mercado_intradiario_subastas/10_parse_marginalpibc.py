from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[4]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

import pandas as pd  # noqa: E402
from mtu.parsing.marginalpibc import parse_folder_and_write  # noqa: E402


def main() -> None:
    raw_dir = PROJECT_ROOT / "data/raw/omie/mercado_intradiario_subastas/precios/marginalpibc"
    processed_dir = PROJECT_ROOT / "data/processed/omie/mercado_intradiario_subastas/precios/marginalpibc"
    ingestion_log_csv = PROJECT_ROOT / "data/metadata/ingestion_log.csv"

    print(f"Raw dir:       {raw_dir}")
    print(f"Processed dir: {processed_dir}")
    print(f"Ingestion log: {ingestion_log_csv}")
    print()

    summary = parse_folder_and_write(
        raw_dir=raw_dir,
        processed_dir=processed_dir,
        ingestion_log_csv=ingestion_log_csv,
    )

    if summary.empty:
        print("No files found.")
        return

    print(summary.to_string(index=False))

    n_success = (summary["status"] == "success").sum()
    n_failed = (summary["status"] == "failed").sum()
    n_skipped = (summary["status"] == "skipped").sum()

    print()
    print(f"Done. success={n_success}, failed={n_failed}, skipped={n_skipped}")


if __name__ == "__main__":
    main()
