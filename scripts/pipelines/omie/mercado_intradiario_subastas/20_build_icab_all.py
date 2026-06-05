from __future__ import annotations

from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[4]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from mtu.parsing.build_helpers import build_or_append  # noqa: E402


def main() -> None:
    processed_dir = PROJECT_ROOT / "data/processed/omie/mercado_intradiario_subastas/ofertas/icab"
    output_path = processed_dir.parent / "icab_all.parquet"

    con = build_or_append(
        processed_dir=processed_dir,
        output_path=output_path,
        family="ICAB",
    )

    stats = con.execute(f"""
        SELECT COUNT(*) AS total_rows, MIN(date) AS date_min, MAX(date) AS date_max
        FROM read_parquet('{output_path}')
    """).fetchone()

    rows_by_session = con.execute(f"""
        SELECT session_number, COUNT(*) AS n_rows
        FROM read_parquet('{output_path}')
        GROUP BY session_number ORDER BY session_number
    """).df()

    rows_per_file = con.execute(f"""
        SELECT MIN(cnt) AS min_rows, MAX(cnt) AS max_rows, AVG(cnt) AS avg_rows
        FROM (
            SELECT source_file, COUNT(*) AS cnt
            FROM read_parquet('{output_path}')
            GROUP BY source_file
        ) t
    """).fetchone()

    session_dist = {int(r[0]): int(r[1]) for r in rows_by_session.itertuples(index=False)}

    print(f"Output rows:             {stats[0]:,}")
    print(f"Date range:              {stats[1]} -> {stats[2]}")
    print(f"Output file:             {output_path}")
    print(f"Rows by session_number:  {session_dist}")
    print(
        f"Rows/file summary:       min={int(rows_per_file[0])}, "
        f"max={int(rows_per_file[1])}, mean={rows_per_file[2]:.2f}"
    )


if __name__ == "__main__":
    main()
