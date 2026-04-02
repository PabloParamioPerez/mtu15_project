from __future__ import annotations

import argparse
import re
from pathlib import Path

import pandas as pd

# OMIE "Nombre del fichero" patterns from the manual.
# We use lowercase matching on filenames.
FAMILY_PATTERNS: list[tuple[str, re.Pattern[str]]] = [
    ("marginalpdbc", re.compile(r"^marginalpdbc_\d{8}\.\d+$", re.I)),
    ("marginalpibc", re.compile(r"^marginalpibc_\d{10}\.\d+$", re.I)),
    ("pdbc", re.compile(r"^pdbc_\d{8}\.\d+$", re.I)),
    ("pibca", re.compile(r"^pibca_\d{10}\.\d+$", re.I)),
    ("pibci", re.compile(r"^pibci_\d{10}\.\d+$", re.I)),
    ("pibcie", re.compile(r"^pibcie_\d{10}\.\d+$", re.I)),
    ("precios_pibcic", re.compile(r"^precios_pibcic_\d{8}\.\d+$", re.I)),
    ("precios_pibcic_ronda", re.compile(r"^precios_pibcic_ronda_\d{8}\.\d+$", re.I)),
]

DATE_RE = re.compile(r"_(\d{8})(\d{0,2})\.(\d+)$")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Compact OMIE raw coverage summary by documented family")
    p.add_argument(
        "--root-dir",
        default="data/raw/omie",
        help="Root OMIE raw-data folder to scan recursively (default: data/raw/omie)",
    )
    p.add_argument(
        "--show-paths",
        action="store_true",
        help="Also show the inferred family directory path",
    )
    return p.parse_args()


def iter_candidate_files(root_dir: Path) -> list[Path]:
    files: list[Path] = []
    for p in sorted(root_dir.rglob("*")):
        if not p.is_file():
            continue
        if any(part.startswith(".") for part in p.parts):
            continue
        if "archives" in p.parts:
            continue
        files.append(p)
    return files


def infer_family(path: Path) -> str:
    name = path.name.lower()

    for family, pattern in FAMILY_PATTERNS:
        if pattern.match(name):
            return family

    # Fallback: raw folder names in this repo are already family-specific.
    parent = path.parent.name.lower()
    if parent:
        return parent

    return "unknown"


def try_parse_int(x: str):
    x = x.strip()
    if x == "":
        return None
    try:
        return int(x)
    except Exception:
        return None


def inspect_file(path: Path) -> dict:
    out = {
        "family": infer_family(path),
        "family_dir": str(path.parent),
        "filename_date": None,
        "version_suffix": None,
        "content_date_min": None,
        "content_date_max": None,
    }

    m = DATE_RE.search(path.name)
    if m:
        yyyymmdd, _session_part, version_suffix = m.groups()
        dt = pd.to_datetime(yyyymmdd, format="%Y%m%d", errors="coerce")
        if pd.notna(dt):
            out["filename_date"] = dt.date()
        out["version_suffix"] = int(version_suffix)

    content_dates = []

    try:
        with path.open("r", encoding="latin-1", errors="replace") as f:
            first_nonempty_seen = False
            for raw in f:
                line = raw.strip()
                if not line:
                    continue

                if not first_nonempty_seen:
                    first_nonempty_seen = True
                    continue

                if line == "*":
                    continue

                parts = line.split(";")
                if parts and parts[-1] == "":
                    parts = parts[:-1]

                if len(parts) >= 3:
                    y = try_parse_int(parts[0])
                    mth = try_parse_int(parts[1])
                    d = try_parse_int(parts[2])
                    if y is not None and mth is not None and d is not None:
                        try:
                            content_dates.append(pd.Timestamp(year=y, month=mth, day=d).date())
                        except Exception:
                            pass
    except Exception:
        pass

    if content_dates:
        out["content_date_min"] = min(content_dates)
        out["content_date_max"] = max(content_dates)

    return out


def fmt_date(x) -> str:
    return str(x) if x is not None and pd.notna(x) else "NA"


def main() -> None:
    args = parse_args()
    root_dir = Path(args.root_dir)

    if not root_dir.exists():
        raise FileNotFoundError(f"Root dir does not exist: {root_dir}")
    if not root_dir.is_dir():
        raise NotADirectoryError(f"Not a directory: {root_dir}")

    files = iter_candidate_files(root_dir)
    if not files:
        print("No raw files found.")
        return

    rows = [inspect_file(p) for p in files]
    df = pd.DataFrame(rows)

    grouped = []
    for family, g in df.groupby("family", dropna=False):
        filename_dates = [x for x in g["filename_date"].tolist() if pd.notna(x)]
        content_mins = [x for x in g["content_date_min"].tolist() if pd.notna(x)]
        content_maxs = [x for x in g["content_date_max"].tolist() if pd.notna(x)]
        versions = sorted(set(int(x) for x in g["version_suffix"].dropna().tolist()))
        family_dirs = sorted(set(g["family_dir"].tolist()))

        grouped.append(
            {
                "family": family,
                "files": len(g),
                "filename_date_min": min(filename_dates) if filename_dates else None,
                "filename_date_max": max(filename_dates) if filename_dates else None,
                "content_date_min": min(content_mins) if content_mins else None,
                "content_date_max": max(content_maxs) if content_maxs else None,
                "version_suffixes": ",".join(str(v) for v in versions[:10]) + ("..." if len(versions) > 10 else ""),
                "family_dir": family_dirs[0] if family_dirs else "",
            }
        )

    out = pd.DataFrame(grouped).sort_values("family").reset_index(drop=True)

    for _, r in out.iterrows():
        line = (
            f"{r['family']} | "
            f"files={r['files']} | "
            f"filename_dates={fmt_date(r['filename_date_min'])}..{fmt_date(r['filename_date_max'])} | "
            f"content_dates={fmt_date(r['content_date_min'])}..{fmt_date(r['content_date_max'])}"
        )
        if pd.notna(r["version_suffixes"]) and r["version_suffixes"] != "":
            line += f" | versions={r['version_suffixes']}"
        print(line)
        if args.show_paths:
            print(f"  path={r['family_dir']}")


if __name__ == "__main__":
    main()
