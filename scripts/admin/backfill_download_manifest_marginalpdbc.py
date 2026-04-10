from __future__ import annotations

import csv
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from mtu.parsing.marginalpdbc import build_download_manifest_row_for_existing_file  # noqa: E402
from mtu.parsing.omie_common import append_csv_row, visible_files  # noqa: E402


def load_existing_filenames(manifest_csv: Path) -> set[str]:
    if not manifest_csv.exists():
        return set()
    with manifest_csv.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return {row["filename"] for row in reader if row.get("filename")}


def main() -> None:
    raw_dir = PROJECT_ROOT / "data/raw/omie/mercado_diario/precios/marginalpdbc"
    manifest_csv = PROJECT_ROOT / "data/metadata/download_manifest.csv"

    files = visible_files(raw_dir)
    existing = load_existing_filenames(manifest_csv)

    added = 0
    skipped = 0

    for path in files:
        if path.name in existing:
            skipped += 1
            continue

        row = build_download_manifest_row_for_existing_file(path)
        append_csv_row(manifest_csv, row)
        added += 1

    print(f"Backfill complete. added={added}, skipped={skipped}")


if __name__ == "__main__":
    main()
