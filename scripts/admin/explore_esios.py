from __future__ import annotations

"""Search ESIOS archives and indicators by keyword.

Usage examples:
    uv run scripts/admin/explore_esios.py restriccion
    uv run scripts/admin/explore_esios.py rampa
    uv run scripts/admin/explore_esios.py desvio
    uv run scripts/admin/explore_esios.py restriccion --archives-only
    uv run scripts/admin/explore_esios.py 680 --indicators-only
"""

import argparse
import os
import sys
from pathlib import Path

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

BASE_URL = "https://api.esios.ree.es"
DEMO_TOKEN = "96c56fcd69dd5c29f569ab3ea9298b37151a1ee488a1830d353babad3ec90fd7"


def get_token() -> str:
    token = os.environ.get("ESIOS_TOKEN", "").strip()
    if not token:
        print("[WARN] ESIOS_TOKEN not set; using demo key")
    return token or DEMO_TOKEN


def make_session(token: str) -> requests.Session:
    s = requests.Session()
    s.headers.update(
        {
            "Accept": "application/json; application/vnd.esios-api-v1+json",
            "Content-Type": "application/json",
            "x-api-key": token,
        }
    )
    return s


def fetch_all(session: requests.Session, endpoint: str, key: str, timeout: int) -> list[dict]:
    r = session.get(f"{BASE_URL}/{endpoint}", timeout=timeout)
    r.raise_for_status()
    return r.json().get(key, [])


def matches(item: dict, keywords: list[str]) -> bool:
    name = item.get("name", "").lower()
    return all(kw.lower() in name for kw in keywords)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Search ESIOS archives and indicators by keyword")
    p.add_argument("keywords", nargs="+", help="Keywords to search (all must appear in name, case-insensitive)")
    p.add_argument("--archives-only", action="store_true", help="Only search archives")
    p.add_argument("--indicators-only", action="store_true", help="Only search indicators")
    p.add_argument("--timeout", type=int, default=60)
    return p.parse_args()


def main() -> None:
    args = parse_args()
    session = make_session(get_token())

    if not args.indicators_only:
        print("Fetching archives...")
        archives = fetch_all(session, "archives", "archives", args.timeout)
        hits = [a for a in archives if matches(a, args.keywords)]
        print(f"\n=== Archives matching {args.keywords!r}  ({len(hits)}/{len(archives)}) ===\n")
        for a in hits:
            terms = ", ".join(t["name"] for t in a.get("taxonomy_terms", []))
            print(f"  id={a['id']:<7}  type={a.get('archive_type', ''):6}  name={a.get('name', '')}")
            if terms:
                print(f"           taxonomy={terms}")

    if not args.archives_only:
        print("\nFetching indicators...")
        indicators = fetch_all(session, "indicators", "indicators", args.timeout)
        hits = [i for i in indicators if matches(i, args.keywords)]
        print(f"\n=== Indicators matching {args.keywords!r}  ({len(hits)}/{len(indicators)}) ===\n")
        for ind in hits:
            print(f"  id={ind['id']:<7}  name={ind.get('name', '')}")


if __name__ == "__main__":
    main()
