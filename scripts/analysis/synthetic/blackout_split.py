# STATUS: ALIVE
# LAST-AUDIT: 2026-04-27
# FEEDS: F7, F8 (blackout-confound check)
# CLAIM: Pre vs post Iberian blackout (2025-04-28) split of F7 IB market-power and F8 hydro Q4 concentration
"""Iberian blackout (2025-04-28) confound check.

The 2025-04-28 Iberian blackout led to "operación reforzada" — REE
forced increased CCGT/nuclear commitment for system security. The
post-blackout period is contained entirely within DA60/ID15 (until
MTU15-DA on 2025-10-01) and continues into DA15/ID15 thereafter.
~5 of 6 DA60/ID15 months are post-blackout.

This script splits the F7 per-ISP synthetic-clearing data and the F8
hydro Q4 concentration around the blackout date, asking:
  (a) Is IB's price-setting power present in the clean pre-blackout
      DA60/ID15 window (2025-03-19 → 2025-04-27)?
  (b) Does the rent intensify post-blackout (consistent with operación
      reforzada amplifying CCGT/hydro rents) or is it stable across?
  (c) Does DA15/ID15 (post-MTU15-DA, also post-blackout) show the
      pattern persisting?

If IB's market power is materially different pre vs post blackout
within DA60/ID15, the reform attribution must distinguish reform-
amplified rent from blackout-amplified rent.
"""
from __future__ import annotations

from pathlib import Path
import duckdb
import pandas as pd

PROJECT = Path(__file__).resolve().parents[3]
ISP_FILE = PROJECT / "data" / "derived" / "results" / "synthetic_firm_per_firm_isp.csv"
PDBCE = PROJECT / "data" / "processed" / "omie" / "mercado_diario" / "programas" / "pdbce_all.parquet"
PRICE = PROJECT / "data" / "processed" / "omie" / "mercado_diario" / "precios" / "marginalpdbc_all.parquet"
REF = PROJECT / "data" / "external" / "omie_reference" / "lista_unidades.csv"
OUT = PROJECT / "data" / "derived" / "results" / "blackout_split.csv"

BLACKOUT = pd.Timestamp("2025-04-28")
MTU15_IDA = pd.Timestamp("2025-03-19")
MTU15_DA = pd.Timestamp("2025-10-01")


def split_label(d):
    d = pd.Timestamp(d)
    if d < MTU15_IDA:
        return "pre-MTU15-IDA"
    if d < BLACKOUT:
        return "DA60/ID15 PRE-blackout"
    if d < MTU15_DA:
        return "DA60/ID15 POST-blackout"
    return "DA15/ID15 (post-blackout)"


def part_a_f7_ib_transfer() -> None:
    print("=" * 100)
    print("PART A — F7 IB market-power transfer, split by blackout")
    print("=" * 100)
    if not ISP_FILE.exists():
        print(f"  {ISP_FILE} not found; skipping.")
        return
    df = pd.read_csv(ISP_FILE)
    df["date"] = pd.to_datetime(df["date"])
    df["era"] = df["date"].apply(split_label)
    df = df.dropna(subset=["mp_IB"]).copy()

    print()
    print(f"{'era':<35}  {'n ISPs':>8}  {'mean p_actual':>13}  {'mean MP_IB':>12}  {'rel MP':>8}  {'~transfer M€':>14}")
    rows = []
    for era in [
        "DA60/ID15 PRE-blackout",
        "DA60/ID15 POST-blackout",
        "DA15/ID15 (post-blackout)",
    ]:
        sub = df[df["era"] == era]
        if len(sub) == 0:
            continue
        mean_p = sub["p_actual"].mean()
        mean_mp = sub["mp_IB"].mean()
        rel = mean_mp / mean_p * 100 if mean_p > 0 else 0
        # Transfer estimate: 25/4 GWh per 15-min ISP (post-MTU15-IDA all 15-min)
        tr = (sub["mp_IB"] * 25_000 / 4).sum() / 1e6
        rows.append({
            "era": era, "n_isps": len(sub),
            "mean_p_actual": mean_p, "mean_mp_IB": mean_mp,
            "rel_mp_pct": rel, "transfer_eur_M": tr,
        })
        print(f"{era:<35}  {len(sub):>8,}  {mean_p:>13.2f}  {mean_mp:>+12.3f}  {rel:>+7.2f}%  {tr:>+14.1f}")

    return pd.DataFrame(rows)


def part_b_f8_hydro_concentration() -> None:
    print()
    print("=" * 100)
    print("PART B — F8 IB hydro Q4 concentration, split by blackout")
    print("=" * 100)
    ref = pd.read_csv(REF, encoding="latin1")
    ref["tech_low"] = ref["technology"].fillna("").astype(str).str.lower()
    hydro_units = ref[
        ref["tech_low"].str.contains("hidr", regex=False)
        | ref["tech_low"].str.contains("hydro", regex=False)
    ]["unit_code"].tolist()

    con = duckdb.connect()
    con.execute("SET memory_limit='2GB'")
    con.execute("SET threads=4")
    con.register("hydro_units", pd.DataFrame({"unit_code": hydro_units}))

    print("[query] DA hourly price + within-month price quartile + hydro cleared MWh by firm...")
    con.execute(f"""
        CREATE TEMP TABLE px AS
        SELECT date,
               CASE WHEN mtu_minutes = 15 THEN CEIL(period / 4.0)::INTEGER
                    ELSE period END AS hour,
               AVG(price_es_eur_mwh) AS p_da
        FROM '{PRICE}'
        WHERE price_es_eur_mwh IS NOT NULL
          AND CAST(date AS DATE) >= DATE '2025-03-19'
        GROUP BY 1, 2
    """)
    con.execute("""
        CREATE TEMP TABLE px_q AS
        SELECT date, hour, p_da,
               DATE_TRUNC('month', CAST(date AS DATE)) AS month,
               NTILE(4) OVER (PARTITION BY DATE_TRUNC('month', CAST(date AS DATE))
                              ORDER BY p_da) AS price_q_in_month
        FROM px
    """)
    df = con.sql(f"""
        SELECT p.unit_code,
               CASE WHEN p.grupo_empresarial IN ('GE','IB','GN','HC') THEN p.grupo_empresarial ELSE 'Fringe' END AS firm_group,
               CAST(p.date AS DATE) AS p_date,
               CASE WHEN p.mtu_minutes = 15 THEN CEIL(p.period / 4.0)::INTEGER
                    ELSE p.period END AS p_hour,
               q.price_q_in_month,
               SUM(p.assigned_power_mw)
                 / CASE WHEN p.mtu_minutes = 15 THEN 4.0 ELSE 1.0 END AS q_mwh
        FROM '{PDBCE}' p
        JOIN hydro_units h USING (unit_code)
        JOIN px_q q ON CAST(p.date AS DATE) = q.date
                  AND (CASE WHEN p.mtu_minutes = 15 THEN CEIL(p.period / 4.0)::INTEGER ELSE p.period END) = q.hour
        WHERE p.offer_type = 1
          AND p.assigned_power_mw > 0
          AND CAST(p.date AS DATE) >= DATE '2025-03-19'
        GROUP BY p.unit_code, firm_group, p_date, p_hour, q.price_q_in_month, p.mtu_minutes
    """).df()
    df = df.rename(columns={"p_date": "date", "p_hour": "hour"})
    df["date"] = pd.to_datetime(df["date"])
    df["era"] = df["date"].apply(split_label)

    print()
    print(f"{'firm':<10}  {'era':<32}  {'Q4 share':>10}  {'gap vs Fringe':>15}")
    rows = []
    for era in [
        "DA60/ID15 PRE-blackout",
        "DA60/ID15 POST-blackout",
        "DA15/ID15 (post-blackout)",
    ]:
        # Compute Fringe Q4 share for this era (control)
        fringe = df[(df["firm_group"] == "Fringe") & (df["era"] == era)]
        fringe_total = fringe["q_mwh"].sum()
        fringe_q4 = fringe.loc[fringe["price_q_in_month"] == 4, "q_mwh"].sum()
        fringe_q4_share = fringe_q4 / fringe_total * 100 if fringe_total > 0 else 0
        for fg in ["IB", "GE", "GN", "HC"]:
            sub = df[(df["firm_group"] == fg) & (df["era"] == era)]
            if len(sub) == 0:
                continue
            tot = sub["q_mwh"].sum()
            q4 = sub.loc[sub["price_q_in_month"] == 4, "q_mwh"].sum()
            q4_share = q4 / tot * 100 if tot > 0 else 0
            gap = q4_share - fringe_q4_share
            rows.append({
                "firm": fg, "era": era,
                "q4_share_pct": q4_share, "fringe_q4_share_pct": fringe_q4_share,
                "gap_pp": gap,
            })
            print(f"{fg:<10}  {era:<32}  {q4_share:>9.1f}%  {gap:>+14.1f} pp")

    return pd.DataFrame(rows)


def part_c_s6_monthly_split() -> None:
    print()
    print("=" * 100)
    print("PART C — S6 A87 NET fiscal surplus monthly, split by blackout")
    print("=" * 100)
    A87 = PROJECT / "data" / "processed" / "entsoe" / "balancing" / "financial_balance_all.parquet"
    df = pd.read_parquet(A87)
    df["month"] = pd.to_datetime(df["month"])
    a02 = df[df["direction_code"] == "A02"][["month", "amount_eur"]].rename(columns={"amount_eur": "a02"})
    a01 = df[df["direction_code"] == "A01"][["month", "amount_eur"]].rename(columns={"amount_eur": "a01"})
    m = a02.merge(a01, on="month", how="outer").fillna(0)
    m["net_eur"] = m["a02"] - m["a01"]
    m["net_M"] = m["net_eur"] / 1e6

    # Same-calendar-month pre-IDA baseline (months 2018-2023 same calendar month)
    pre = m[m["month"] < MTU15_IDA].copy()
    pre["cal_m"] = pre["month"].dt.month
    baseline = pre.groupby("cal_m")["net_M"].mean().to_dict()

    post = m[m["month"] >= MTU15_IDA].copy()
    post["cal_m"] = post["month"].dt.month
    post["baseline_M"] = post["cal_m"].map(baseline)
    post["excess_M"] = post["net_M"] - post["baseline_M"]
    post["era"] = post["month"].apply(split_label)

    print()
    print(f"{'month':<10}  {'net (M€)':>10}  {'baseline':>10}  {'excess (M€)':>12}  {'era':<35}")
    for _, r in post.iterrows():
        print(f"{r['month'].strftime('%Y-%m'):<10}  {r['net_M']:>10.1f}  {r['baseline_M']:>10.1f}  {r['excess_M']:>+12.1f}  {r['era']:<35}")

    print()
    print("Aggregate by era:")
    agg = post.groupby("era")["excess_M"].agg(["sum", "mean", "count"]).reset_index()
    print(agg.to_string(index=False))


def main() -> None:
    df_a = part_a_f7_ib_transfer()
    df_b = part_b_f8_hydro_concentration()
    part_c_s6_monthly_split()

    OUT.parent.mkdir(parents=True, exist_ok=True)
    if df_a is not None and df_b is not None:
        out_df = pd.concat([df_a.assign(part="A_F7_IB"), df_b.assign(part="B_F8_hydro")],
                            ignore_index=True, sort=False)
        out_df.to_csv(OUT, index=False)
        print(f"\nwrote {OUT}")


if __name__ == "__main__":
    main()
