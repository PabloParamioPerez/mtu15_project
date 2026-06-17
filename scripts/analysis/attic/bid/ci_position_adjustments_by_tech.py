# STATUS: ALIVE
# LAST-AUDIT: 2026-05-19
# CLAIM: Per-technology continuous-intraday (XBID) position adjustments
#        across the reform window. For each (delivery_date, tech) we sum the
#        gross MWh transacted in the continuous market on the SELL side and
#        on the BUY side; the difference is the net repositioning.
#        Aggregated per regime to give a per-day average per tech.
# FEEDS: descriptive_facts.tex (continuous-intraday per-tech repositioning)
# OUT:
#   results/regressions/bid/ci_position_adjustments/per_tech_regime.csv
#   results/regressions/bid/ci_position_adjustments/per_tech_regime.tex
#
# Data:
#   trades_all.parquet — every matched XBID transaction with buyer_unit and
#     seller_unit. quantity_mw is the contracted power; mtu_minutes is the
#     delivery duration (60 pre-MTU15-IDA, 15 post). Energy = MW * mtu/60.
#   lista_unidades.csv — unit_code → technology (raw OMIE strings).

from __future__ import annotations
from pathlib import Path

import duckdb
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
TRADES = REPO / "data/processed/omie/mercado_intradiario_continuo/transacciones/trades_all.parquet"
UNITS = REPO / "data/external/omie_reference/lista_unidades.csv"
OUT_DIR = REPO / "results/regressions/bid/ci_position_adjustments"


def tech_bucket(t: str | None) -> str:
    if t is None:
        return "unknown"
    t = str(t)
    if "Solar Fotov" in t:
        return "Solar PV"
    if "Solar Térm" in t:
        return "Solar Thermal"
    if "Eólica" in t:
        return "Wind"
    if "Bombeo" in t:
        return "Hydro pump"
    if "Hidráulica" in t or "Hidraulic" in t:
        return "Hydro"
    if "Ciclo Combinado" in t:
        return "CCGT"
    if "Nuclear" in t:
        return "Nuclear"
    if "Térmica Renovable" in t:
        return "Biomass / RE thermal"
    if "Térmica no Renovab" in t:
        return "Thermal non-RE"
    if "Almacenamiento" in t:
        return "Battery / storage"
    if "Cogen" in t.lower():
        return "Cogen"
    if "Comercializ" in t or "Compra" in t or "Consumo" in t:
        return "Demand side"
    return "Other"


def regime_of(date) -> str:
    d = pd.to_datetime(date).date()
    if d < pd.Timestamp("2024-06-13").date():
        return "pre-IDA"
    if d < pd.Timestamp("2024-12-01").date():
        return "3-sess (MTU60)"
    if d < pd.Timestamp("2025-03-19").date():
        return "ISP15-win (MTU60)"
    if d < pd.Timestamp("2025-04-28").date():
        return "MTU15-IDA pre-blk"
    if d < pd.Timestamp("2025-10-01").date():
        return "MTU15-IDA post-blk"
    return "MTU15-DA (DA15/ID15)"


REGIME_ORDER = [
    "pre-IDA",
    "3-sess (MTU60)",
    "ISP15-win (MTU60)",
    "MTU15-IDA pre-blk",
    "MTU15-IDA post-blk",
    "MTU15-DA (DA15/ID15)",
]


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    units = pd.read_csv(UNITS)[["unit_code", "technology"]]
    units["tech"] = units["technology"].apply(tech_bucket)

    con = duckdb.connect()
    con.execute("SET memory_limit='10GB'")
    con.execute("SET threads=4")
    con.register("units", units[["unit_code", "tech"]])

    # Restrict to 2024-01-01 onward to keep the comparison reform-window focused.
    sql = """
    WITH t AS (
      SELECT
        CAST(delivery_date AS DATE) AS d,
        mtu_minutes,
        seller_unit,
        buyer_unit,
        quantity_mw * mtu_minutes/60.0 AS mwh
      FROM read_parquet(?)
      WHERE delivery_date >= '2024-01-01'
        AND delivery_date <= '2026-05-15'
    ),
    sells AS (
      SELECT t.d, u.tech, SUM(t.mwh) AS sells_mwh
      FROM t JOIN units u ON t.seller_unit = u.unit_code
      WHERE t.seller_unit <> ''
      GROUP BY t.d, u.tech
    ),
    buys AS (
      SELECT t.d, u.tech, SUM(t.mwh) AS buys_mwh
      FROM t JOIN units u ON t.buyer_unit = u.unit_code
      WHERE t.buyer_unit <> ''
      GROUP BY t.d, u.tech
    ),
    j AS (
      SELECT
        COALESCE(s.d, b.d) AS d,
        COALESCE(s.tech, b.tech) AS tech,
        COALESCE(s.sells_mwh, 0) AS sells_mwh,
        COALESCE(b.buys_mwh, 0) AS buys_mwh
      FROM sells s FULL OUTER JOIN buys b
        ON s.d = b.d AND s.tech = b.tech
    )
    SELECT * FROM j ORDER BY d, tech
    """
    daily = con.execute(sql, [str(TRADES)]).fetchdf()
    daily["regime"] = daily["d"].apply(regime_of)
    daily["gross_mwh"] = daily["sells_mwh"] + daily["buys_mwh"]
    daily["net_mwh"] = daily["sells_mwh"] - daily["buys_mwh"]

    # Per-regime per-tech: mean GWh/day across the regime's days.
    per_regime = (
        daily.groupby(["regime", "tech"], dropna=False)
        .agg(
            sells_gwh_day=("sells_mwh", lambda x: x.mean() / 1000),
            buys_gwh_day=("buys_mwh", lambda x: x.mean() / 1000),
            gross_gwh_day=("gross_mwh", lambda x: x.mean() / 1000),
            net_gwh_day=("net_mwh", lambda x: x.mean() / 1000),
            n_active_days=("d", "nunique"),
        )
        .reset_index()
    )
    per_regime["regime"] = pd.Categorical(per_regime["regime"], categories=REGIME_ORDER, ordered=True)
    per_regime = per_regime.sort_values(["regime", "tech"]).reset_index(drop=True)

    # System-wide CI volume per regime (sum across techs for the sells side).
    system_per_regime = (
        per_regime.groupby("regime", observed=True)["sells_gwh_day"].sum().round(1)
    )
    print("System-wide CI sell volume per regime (GWh/day, sum across techs):")
    print(system_per_regime.to_string())

    out_csv = OUT_DIR / "per_tech_regime.csv"
    per_regime.to_csv(out_csv, index=False)
    print(f"\nwrote {out_csv}: {len(per_regime)} rows")

    # Pivot to a tex-friendly layout: rows = tech (generator-side techs only),
    # cols = regime. Drop "Demand side" and "Other" / "unknown" to keep the
    # focus on production technologies.
    keep_techs = [
        "CCGT", "Nuclear", "Hydro", "Hydro pump", "Wind", "Solar PV",
        "Solar Thermal", "Biomass / RE thermal", "Thermal non-RE", "Battery / storage", "Cogen",
    ]
    p_gross = per_regime.pivot_table(
        index="tech", columns="regime", values="gross_gwh_day", observed=True
    ).reindex(index=keep_techs, columns=REGIME_ORDER)
    p_sells = per_regime.pivot_table(
        index="tech", columns="regime", values="sells_gwh_day", observed=True
    ).reindex(index=keep_techs, columns=REGIME_ORDER)
    p_buys = per_regime.pivot_table(
        index="tech", columns="regime", values="buys_gwh_day", observed=True
    ).reindex(index=keep_techs, columns=REGIME_ORDER)
    p_net = per_regime.pivot_table(
        index="tech", columns="regime", values="net_gwh_day", observed=True
    ).reindex(index=keep_techs, columns=REGIME_ORDER)

    print("\n--- Gross CI activity (GWh/day) per tech × regime ---")
    print(p_gross.round(1).to_string())
    print("\n--- Net CI position (GWh/day = sells - buys) per tech × regime ---")
    print(p_net.round(1).to_string())

    # Write a LaTeX table: gross activity per tech across the 6 regimes.
    # The narrative table — short regime labels.
    short_labels = {
        "pre-IDA": "pre-IDA",
        "3-sess (MTU60)": "3-sess",
        "ISP15-win (MTU60)": "ISP15-win",
        "MTU15-IDA pre-blk": "MTU15 pre-blk",
        "MTU15-IDA post-blk": "MTU15 post-blk",
        "MTU15-DA (DA15/ID15)": "DA15/ID15",
    }
    p = p_gross.rename(columns=short_labels)
    # Format as 1-decimal GWh/day; use $-$ for NaN.
    tex_rows = []
    tex_rows.append(r"\begin{tabular}{l " + "r " * len(p.columns) + "}")
    tex_rows.append(r"\toprule")
    tex_rows.append(
        " & " + " & ".join(f"\\textbf{{{c}}}" for c in p.columns) + r" \\"
    )
    tex_rows.append(r"\midrule")
    for tech, row in p.iterrows():
        vals = []
        for v in row.values:
            if pd.isna(v):
                vals.append("--")
            else:
                vals.append(f"{v:.1f}")
        tex_rows.append(f"{tech} & " + " & ".join(vals) + r" \\")
    tex_rows.append(r"\bottomrule")
    tex_rows.append(r"\end{tabular}")
    tex = "\n".join(tex_rows) + "\n"
    out_tex = OUT_DIR / "per_tech_gross_gwh_day.tex"
    out_tex.write_text(tex)
    print(f"wrote {out_tex}")

    # Also write net (sells - buys) per tech × regime, GWh/day. Generator-side net.
    p_net_short = p_net.rename(columns=short_labels)
    tex_rows = []
    tex_rows.append(r"\begin{tabular}{l " + "r " * len(p_net_short.columns) + "}")
    tex_rows.append(r"\toprule")
    tex_rows.append(
        " & " + " & ".join(f"\\textbf{{{c}}}" for c in p_net_short.columns) + r" \\"
    )
    tex_rows.append(r"\midrule")
    for tech, row in p_net_short.iterrows():
        vals = []
        for v in row.values:
            if pd.isna(v):
                vals.append("--")
            else:
                sign = "+" if v >= 0 else "$-$"
                vals.append(f"{sign}{abs(v):.1f}")
        tex_rows.append(f"{tech} & " + " & ".join(vals) + r" \\")
    tex_rows.append(r"\bottomrule")
    tex_rows.append(r"\end{tabular}")
    tex = "\n".join(tex_rows) + "\n"
    out_tex2 = OUT_DIR / "per_tech_net_gwh_day.tex"
    out_tex2.write_text(tex)
    print(f"wrote {out_tex2}")


if __name__ == "__main__":
    main()
