"""ENTSO-E File Library client (skeleton).

The individual mFRR balancing-energy-bids feed (GL EB 12.3.B) and the
detailed redispatching feed (TR 13.1.a) — i.e. the "Balancing Energy bids"
view shown in the Transparency Platform UI for SCA|ES with 47k bids/day —
are NOT exposed via the public REST API at web-api.tp.entsoe.eu (the one
our ENTSOE_TOKEN authenticates against).

They are only available via the File Library service at fms.tp.entsoe.eu,
which requires:
  1. A registered Transparency Platform user account (TP_USERNAME / TP_PASSWORD)
  2. Keycloak OAuth password-grant authentication via
     keycloak.tp.entsoe.eu/realms/tp/protocol/openid-connect/token
  3. Bearer-token authorization on subsequent listFileMetadata / downloadFile calls

Rate limit: 100 requests/min/user (10-min ban on overshoot).

This skeleton implements the auth + listing + download flow. To use:
  1. Add to .env:
       TP_USERNAME=your_tp_account_email@example.com
       TP_PASSWORD=your_tp_password
  2. uv run python scripts/pipelines/entsoe/_file_library_client.py \\
       --path "/TP_export/BalancingEnergyBids_GL_EB_12.3.B/" \\
       --start 2025-04 --end 2026-04 \\
       --area 10YES-REE------0 \\
       --out-subdir balancing_energy_bids_individual

The exact File Library path for individual bids and redispatching is set
empirically (the path strings below are likely candidates; verify against
the File Library Guide and the data-view UI on transparency.entsoe.eu).
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]

KEYCLOAK_URL = "https://keycloak.tp.entsoe.eu/realms/tp/protocol/openid-connect/token"
FMS_URL = "https://fms.tp.entsoe.eu"

# Best-guess paths from File Library Guide / TP UI naming. Adjust after probe.
KNOWN_PATHS = {
    "balancing_energy_bids":     "/TP_export/BalancingEnergyBids_12.3.h_v1_r3/",
    "balancing_bid_conversion":  "/TP_export/BidConversion_12.3.c_v1_r3/",
    "aggregated_bid_offers":     "/TP_export/AggregatedBids_12.3.e_v1_r3/",
    "redispatching":             "/TP_export/Redispatching_13.1.A_v1_r3/",
    "countertrading":            "/TP_export/Countertrading_13.1.B_v1_r3/",
    "congestion_costs":          "/TP_export/CongestionCosts_13.1.C_v1_r3/",
    "accepted_aggregated_offers": "/TP_export/AcceptedAggregatedOffers_17.1.D/",
}


def load_creds() -> tuple[str, str]:
    """Load TP_USERNAME + TP_PASSWORD from env or project-root .env."""
    user = os.environ.get("TP_USERNAME")
    pw = os.environ.get("TP_PASSWORD")
    env_path = PROJECT_ROOT / ".env"
    if (not user or not pw) and env_path.exists():
        for raw in env_path.read_text(encoding="utf-8").splitlines():
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            v = v.split("#", 1)[0].strip().strip('"').strip("'")
            if k.strip() == "TP_USERNAME" and not user:
                user = v
            elif k.strip() == "TP_PASSWORD" and not pw:
                pw = v
    if not user or not pw:
        raise RuntimeError(
            "TP_USERNAME / TP_PASSWORD not set. Add them to .env or environment.\n"
            "Register a free account at https://transparency.entsoe.eu first."
        )
    return user, pw


def get_token(user: str, password: str) -> str:
    r = requests.post(
        KEYCLOAK_URL,
        data={
            "client_id": "tp-fms-public",
            "grant_type": "password",
            "username": user,
            "password": password,
        },
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        timeout=30,
    )
    r.raise_for_status()
    return r.json()["access_token"]


def list_files(token: str, path: str, page_size: int = 5000) -> list[dict]:
    """List file metadata under a File Library path."""
    r = requests.post(
        f"{FMS_URL}/listFileMetadata",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json={
            "topLevelFolder": "TP_export",
            "typeSpecificAttributeMap": {"path": path},
            "sorterList": [{"key": "lastUpdatedTimestamp", "ascending": False}],
            "pageInfo": {"pageIndex": 0, "pageSize": page_size},
        },
        timeout=60,
    )
    r.raise_for_status()
    return r.json().get("contentItemList", [])


def download_file(token: str, file_path: str, out_path: Path) -> int:
    r = requests.post(
        f"{FMS_URL}/downloadFile",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json={"path": file_path},
        timeout=300,
    )
    r.raise_for_status()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_bytes(r.content)
    return len(r.content)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--path", help="File Library path (or shortcut from KNOWN_PATHS)")
    p.add_argument("--shortcut", choices=list(KNOWN_PATHS.keys()),
                   help="Shortcut name for KNOWN_PATHS")
    p.add_argument("--out-subdir", default="file_library_dump",
                   help="Subdir under data/raw/entsoe/")
    p.add_argument("--list-only", action="store_true", help="List metadata; do not download")
    p.add_argument("--max-files", type=int, default=200)
    args = p.parse_args()

    user, pw = load_creds()
    token = get_token(user, pw)
    print(f"[FMS] auth OK; token len={len(token)}")

    path = args.path or KNOWN_PATHS.get(args.shortcut or "balancing_energy_bids")
    print(f"[FMS] listing {path} ...")
    items = list_files(token, path)
    print(f"[FMS] {len(items)} files in {path}")
    for it in items[:5]:
        print(f"  {it.get('name')!r:50}  {it.get('size','?')!s:>12} bytes  {it.get('lastUpdatedTimestamp','?')}")

    if args.list_only or not items:
        return

    out_dir = PROJECT_ROOT / "data/raw/entsoe" / args.out_subdir
    n_dl = 0
    for it in items[: args.max_files]:
        name = it.get("name") or ""
        if not name:
            continue
        out = out_dir / name
        if out.exists() and out.stat().st_size > 0:
            continue
        try:
            n = download_file(token, it.get("path") or f"{path}{name}", out)
            print(f"  OK {name} ({n} bytes)")
            n_dl += 1
        except Exception as e:
            print(f"  FAIL {name}: {e}")

    print(f"[FMS] downloaded {n_dl}; saved under {out_dir}")


if __name__ == "__main__":
    sys.exit(main() or 0)
