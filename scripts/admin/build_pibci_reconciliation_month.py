from __future__ import annotations

import argparse
import re
from pathlib import Path

import pandas as pd


SNAP_RE = re.compile(r"^(?:pibci|pibca)_(\d{8})(\d{2})\.(\d+)$")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--month", required=True)
    p.add_argument(
        "--pibci-dir",
        default="data/processed/omie/mercado_intradiario_subastas/programas/pibci",
    )
    p.add_argument(
        "--pibca-dir",
        default="data/processed/omie/mercado_intradiario_subastas/programas/pibca",
    )
    p.add_argument(
        "--out-dir",
        default="data/metadata/reconciliation",
    )
    return p.parse_args()


def discover_files(base_dir: Path, family: str, month: str) -> list[Path]:
    out: list[Path] = []
    for p in sorted(base_dir.glob("*.parquet")):
        stem = p.stem
        if stem == f"{family}_all":
            continue
        if stem == f"{family}_{month}":
            continue
        if stem.startswith(f"{family}_{month}"):
            out.append(p)
    return out


def parse_snapshot_parts(source_file: str) -> tuple[str, int, int, str]:
    m = SNAP_RE.match(source_file)
    if not m:
        raise ValueError(f"Unexpected source_file format: {source_file}")
    trade_date, trade_session, version = m.groups()
    snapshot_token = f"{trade_date}{trade_session}.{version}"
    return trade_date, int(trade_session), int(version), snapshot_token


def load_family(files: list[Path]) -> pd.DataFrame:
    parts: list[pd.DataFrame] = []
    for p in files:
        df = pd.read_parquet(p).copy()
        if "source_file" not in df.columns:
            df["source_file"] = p.stem
        parsed = df["source_file"].map(parse_snapshot_parts)
        df["snapshot_trade_date"] = parsed.map(lambda x: x[0])
        df["snapshot_trade_session"] = parsed.map(lambda x: x[1])
        df["snapshot_version_num"] = parsed.map(lambda x: x[2])
        df["snapshot_token"] = parsed.map(lambda x: x[3])
        df["date"] = df["date"].astype(str)
        parts.append(df)
    if not parts:
        return pd.DataFrame()
    return pd.concat(parts, ignore_index=True)


def main() -> None:
    args = parse_args()

    pibci_files = discover_files(Path(args.pibci_dir), "pibci", args.month)
    pibca_files = discover_files(Path(args.pibca_dir), "pibca", args.month)

    if not pibci_files:
        raise SystemExit(f"No PIBCI files found for month={args.month}")
    if not pibca_files:
        raise SystemExit(f"No PIBCA files found for month={args.month}")

    pibci = load_family(pibci_files)
    pibca = load_family(pibca_files)

    snapshot_key = ["snapshot_token", "date", "period", "unit_code"]
    state_key = ["date", "period", "unit_code"]

    pibci_net = (
        pibci.groupby(
            snapshot_key + ["snapshot_trade_date", "snapshot_trade_session", "snapshot_version_num"],
            dropna=False,
        )
        .agg(
            pibci_raw_rows=("assigned_power_mw", "size"),
            pibci_net_power_mw=("assigned_power_mw", "sum"),
            pibci_offer_type_nunique=("offer_type", "nunique"),
            pibci_unused_zero_nunique=("unused_zero", "nunique"),
        )
        .reset_index()
    )

    pibca_level = (
        pibca.groupby(
            snapshot_key + ["snapshot_trade_date", "snapshot_trade_session", "snapshot_version_num"],
            dropna=False,
        )
        .agg(
            pibca_rows=("assigned_power_mw", "size"),
            pibca_power_mw=("assigned_power_mw", "sum"),
        )
        .reset_index()
    )

    pibca_level = pibca_level.sort_values(
        state_key + ["snapshot_trade_date", "snapshot_trade_session", "snapshot_version_num"]
    ).reset_index(drop=True)

    pibca_level["prev_snapshot_token"] = (
        pibca_level.groupby(state_key, dropna=False)["snapshot_token"].shift(1)
    )
    pibca_level["pibca_prev_power_mw"] = (
        pibca_level.groupby(state_key, dropna=False)["pibca_power_mw"].shift(1)
    )
    pibca_level["pibca_delta_from_prev_mw"] = (
        pibca_level["pibca_power_mw"] - pibca_level["pibca_prev_power_mw"]
    )

    recon = pibci_net.merge(
        pibca_level,
        on=[
            "snapshot_token",
            "date",
            "period",
            "unit_code",
            "snapshot_trade_date",
            "snapshot_trade_session",
            "snapshot_version_num",
        ],
        how="outer",
        indicator=True,
    )

    recon["delta_error_mw"] = recon["pibci_net_power_mw"] - recon["pibca_delta_from_prev_mw"]
    recon["abs_error_mw"] = recon["delta_error_mw"].abs()

    def classify(row) -> str:
        if row["_merge"] == "left_only":
            return "only_pibci"
        if row["_merge"] == "right_only":
            return "only_pibca"
        if pd.isna(row["pibca_prev_power_mw"]):
            return "first_observation"
        if abs(row["delta_error_mw"]) <= 1e-9:
            return "exact"
        if abs(row["delta_error_mw"]) <= 0.1:
            return "close"
        return "mismatch"

    recon["recon_status"] = recon.apply(classify, axis=1)

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    out_parquet = out_dir / f"pibci_pibca_reconciliation_{args.month}.parquet"
    out_csv = out_dir / f"pibci_pibca_reconciliation_{args.month}.csv"
    summary_csv = out_dir / f"pibci_pibca_reconciliation_summary_{args.month}.csv"

    recon.to_parquet(out_parquet, index=False)
    recon.to_csv(out_csv, index=False)

    summary = (
        recon.groupby("recon_status", dropna=False)
        .agg(
            n_rows=("recon_status", "size"),
            mean_abs_error_mw=("abs_error_mw", "mean"),
            max_abs_error_mw=("abs_error_mw", "max"),
        )
        .reset_index()
        .sort_values("n_rows", ascending=False)
    )
    summary.to_csv(summary_csv, index=False)

    print(f"wrote: {out_parquet}")
    print(f"wrote: {out_csv}")
    print(f"wrote: {summary_csv}")
    print()
    print(summary.to_string(index=False))


if __name__ == "__main__":
    main()
