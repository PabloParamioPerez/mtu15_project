from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Compact OMIE processed coverage summary by family")
    p.add_argument(
        "--root-dir",
        default="data/processed/omie",
        help="Root processed OMIE folder to scan (default: data/processed/omie)",
    )
    p.add_argument(
        "--sample-per-file-parquets",
        type=int,
        default=10,
        help="When no combined parquet exists, inspect up to this many per-file parquets (default: 10)",
    )
    return p.parse_args()


def is_auxiliary_parquet(path: Path) -> bool:
    name = path.name
    return ("_STALE_" in name) or name.endswith("_with_session_date.parquet")


def read_date_range_from_parquet(path: Path):
    try:
        df = pd.read_parquet(path, columns=["date"])
    except Exception:
        return None, None

    if "date" not in df.columns or df.empty:
        return None, None

    vals = pd.to_datetime(df["date"], errors="coerce").dropna()
    if vals.empty:
        return None, None

    return vals.min().date(), vals.max().date()


def fmt_date(x) -> str:
    return str(x) if x is not None and pd.notna(x) else "NA"


def main() -> None:
    args = parse_args()
    root_dir = Path(args.root_dir)

    if not root_dir.exists():
        raise FileNotFoundError(f"Root dir does not exist: {root_dir}")
    if not root_dir.is_dir():
        raise NotADirectoryError(f"Not a directory: {root_dir}")

    all_by_family = {}
    for p in sorted(root_dir.rglob("*_all.parquet")):
        if p.parent.name.endswith("_monthly"):
            continue
        if is_auxiliary_parquet(p):
            continue
        family = p.name.removesuffix("_all.parquet")
        all_by_family[family] = p

    monthly_by_family = {}
    for d in sorted(root_dir.rglob("*_monthly")):
        if not d.is_dir():
            continue
        family = d.name.removesuffix("_monthly")
        monthly_files = sorted(
            p for p in d.glob(f"{family}_*_all.parquet")
            if not is_auxiliary_parquet(p)
        )
        if monthly_files:
            monthly_by_family[family] = monthly_files

    family_dirs = {}
    for p in sorted(root_dir.rglob("*.parquet")):
        if p.name.endswith("_all.parquet"):
            continue
        if p.parent.name.endswith("_monthly"):
            continue
        if is_auxiliary_parquet(p):
            continue

        parent_name = p.parent.name
        if parent_name in {"precios", "programas"}:
            continue

        fam = parent_name
        family_dirs.setdefault(fam, p.parent)

    families = sorted(set(all_by_family) | set(monthly_by_family) | set(family_dirs))

    if not families:
        print("No processed parquet families found.")
        return

    rows = []

    for family in families:
        family_dir = family_dirs.get(family)
        all_path = all_by_family.get(family)
        monthly_files = monthly_by_family.get(family, [])

        per_file_parquets = 0
        if family_dir is not None:
            per_file_parquets = len(list(family_dir.glob("*.parquet")))

        monthly_count = len(monthly_files)

        date_min = None
        date_max = None
        build_status = "none"
        combined_name = ""

        if all_path is not None:
            date_min, date_max = read_date_range_from_parquet(all_path)
            build_status = "all"
            combined_name = all_path.name
        elif monthly_files:
            mins = []
            maxs = []
            for p in monthly_files:
                dmin, dmax = read_date_range_from_parquet(p)
                if dmin is not None:
                    mins.append(dmin)
                if dmax is not None:
                    maxs.append(dmax)
            if mins:
                date_min = min(mins)
            if maxs:
                date_max = max(maxs)
            build_status = "monthly"
            combined_name = f"{family}_monthly/{monthly_count} files"
        elif family_dir is not None:
            sample_files = sorted(family_dir.glob("*.parquet"))[: args.sample_per_file_parquets]
            mins = []
            maxs = []
            for p in sample_files:
                dmin, dmax = read_date_range_from_parquet(p)
                if dmin is not None:
                    mins.append(dmin)
                if dmax is not None:
                    maxs.append(dmax)
            if mins:
                date_min = min(mins)
            if maxs:
                date_max = max(maxs)
            build_status = "per_file_only"

        rows.append(
            {
                "family": family,
                "per_file_parquets": per_file_parquets,
                "monthly_parquets": monthly_count,
                "build_status": build_status,
                "combined_name": combined_name,
                "processed_date_min": date_min,
                "processed_date_max": date_max,
            }
        )

    df = pd.DataFrame(rows).sort_values("family").reset_index(drop=True)

    for _, r in df.iterrows():
        line = (
            f"{r['family']} | "
            f"per_file_parquets={r['per_file_parquets']} | "
            f"monthly_parquets={r['monthly_parquets']} | "
            f"build_status={r['build_status']} | "
            f"processed_dates={fmt_date(r['processed_date_min'])}..{fmt_date(r['processed_date_max'])}"
        )
        if r["combined_name"]:
            line += f" | combined={r['combined_name']}"
        print(line)


if __name__ == "__main__":
    main()
