from __future__ import annotations

import csv
import hashlib
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def visible_files(folder: Path) -> list[Path]:
    """Return non-hidden files in a folder (skip .DS_Store, etc.)."""
    if not folder.exists():
        return []
    return sorted(
        p for p in folder.iterdir()
        if p.is_file() and not p.name.startswith(".")
    )


def read_text_lines(path: Path) -> list[str]:
    """Read text file lines with a couple of common encodings."""
    encodings = ["utf-8-sig", "utf-8", "latin-1"]
    last_error = None
    for enc in encodings:
        try:
            with path.open("r", encoding=enc) as f:
                return [line.rstrip("\n\r") for line in f]
        except UnicodeDecodeError as e:
            last_error = e
            continue
    raise UnicodeDecodeError(
        "unknown",
        b"",
        0,
        1,
        f"Could not decode {path} with tried encodings. Last error: {last_error}"
    )


def parse_decimal(text: str) -> float:
    """
    Parse numeric strings robustly:
    - '123.45' -> 123.45
    - '123,45' -> 123.45
    - '1.234,56' -> 1234.56
    """
    s = text.strip()
    if s == "":
        raise ValueError("Empty numeric field")

    # If both separators appear, assume European thousands '.' and decimal ','
    if "." in s and "," in s:
        s = s.replace(".", "").replace(",", ".")
    else:
        s = s.replace(",", ".")

    return float(s)


def sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def append_csv_row(csv_path: Path, row: dict) -> None:
    """
    Append one row to a CSV using existing header order.
    Assumes CSV already exists and has a header row (as you created).
    """
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV file not found: {csv_path}")

    with csv_path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader, None)

    if not header:
        raise ValueError(f"CSV file has no header: {csv_path}")

    ordered_row = {col: row.get(col, "") for col in header}

    with csv_path.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=header)
        writer.writerow(ordered_row)