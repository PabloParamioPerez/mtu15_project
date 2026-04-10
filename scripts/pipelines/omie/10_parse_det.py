from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[3]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from mtu.parsing.det import parse_folder_and_write  # noqa: E402


def main() -> None:
    raw_dir = PROJECT_ROOT / "data/raw/omie/mercado_diario/ofertas/det"
    processed_dir = PROJECT_ROOT / "data/processed/omie/mercado_diario/ofertas/det"
    ingestion_log_csv = PROJECT_ROOT / "data/metadata/ingestion_log.csv"

    summary = parse_folder_and_write(
        raw_dir=raw_dir,
        processed_dir=processed_dir,
        ingestion_log_csv=ingestion_log_csv,
    )

    if summary.empty:
        print("No files found.")
        return

    print(summary.to_string(index=False))
    counts = summary["status"].value_counts(dropna=False).to_dict()
    print(
        f"\nDone. success={counts.get('success', 0)}, "
        f"failed={counts.get('failed', 0)}, "
        f"skipped={counts.get('skipped', 0)}"
    )


if __name__ == "__main__":
    main()
