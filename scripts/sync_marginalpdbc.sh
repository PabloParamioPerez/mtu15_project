#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

uv run python scripts/download_marginalpdbc.py --recent-days 7
uv run python scripts/parse_marginalpdbc.py

uv run python scripts/build_marginalpdbc_all.py
