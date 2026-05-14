# STATUS: ALIVE
# LAST-AUDIT: 2026-05-15
# FEEDS: thesis paper.tex §3.6 (parallel-trends defense: marginal-tech composition)
# CLAIM: Per clock-hour share of marginal-MW provided by each technology,
#        Oct-Dec 2024 and Oct-Dec 2025. A tranche is "at the margin" if its
#        bid price is within 0.5 EUR/MWh of the DA Spanish clearing price
#        for that (date, period). Share is MW-weighted and sums to 100% per
#        clock-hour by construction.

from __future__ import annotations

from pathlib import Path
import sys

import duckdb
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO / "src"))
from mtu.classification.units import firm_unit_panel  # noqa: E402

DET   = REPO / "data" / "processed" / "omie" / "mercado_diario" / "ofertas" / "det_all.parquet"
CAB   = REPO / "data" / "processed" / "omie" / "mercado_diario" / "ofertas" / "cab_all.parquet"
MPDBC = REPO / "data" / "processed" / "omie" / "mercado_diario" / "precios" / "marginalpdbc_all.parquet"
UNITS_CSV = REPO / "data" / "external" / "omie_reference" / "lista_unidades.csv"

OUTDIR = REPO / "results" / "regressions" / "firm" / "marginal_tech"
OUTDIR.mkdir(parents=True, exist_ok=True)
TABDIR = REPO / "thesis" / "paper" / "tables"
TABDIR.mkdir(parents=True, exist_ok=True)

CRIT = (5, 6, 7, 8, 16, 17, 18, 19, 20, 21, 22)
FLAT = (1, 2, 3)
MID  = (11, 12, 13, 14)

TOL = 0.5   # EUR/MWh tolerance for "at the margin"


def marginal_shares(window_start, window_end, tol=TOL):
    """MW-weighted share of marginal-tranche activity per (clock-hour, tech)."""
    units = firm_unit_panel(csv_path=str(UNITS_CSV), scheme="short", mode="primary_owner")
    con = duckdb.connect(); con.execute("PRAGMA threads=4"); con.execute("SET memory_limit='10GB'")
    con.register("uft", units[["unit_code", "tech_group"]])
    q = f"""
    WITH prices AS (
        SELECT date::DATE AS d, period, price_es_eur_mwh AS p_clear
        FROM '{MPDBC}'
        WHERE date::DATE BETWEEN DATE '{window_start}' AND DATE '{window_end}'
          AND price_es_eur_mwh IS NOT NULL
    ),
    cab AS (
        SELECT date::DATE AS d, offer_code, version, unit_code,
               ROW_NUMBER() OVER (PARTITION BY date::DATE, offer_code, unit_code
                                  ORDER BY version DESC) AS rn
        FROM '{CAB}'
        WHERE buy_sell = 'V'
          AND date::DATE BETWEEN DATE '{window_start}' AND DATE '{window_end}'
    ),
    cab_l AS (SELECT * FROM cab WHERE rn = 1),
    det AS (
        SELECT date::DATE AS d, offer_code, version, period,
               price_eur_mwh AS p_bid, quantity_mw AS q_mw
        FROM '{DET}'
        WHERE date::DATE BETWEEN DATE '{window_start}' AND DATE '{window_end}'
          AND price_eur_mwh IS NOT NULL AND quantity_mw > 0
    ),
    margin AS (
        SELECT pr.d, pr.period,
               CAST(CASE WHEN EXTRACT(year FROM pr.d) <= 2024 THEN pr.period - 1
                         ELSE FLOOR((pr.period - 1) / 4) END AS INT) AS hour,
               u.tech_group, d.q_mw
        FROM prices pr
        JOIN det d   ON d.d = pr.d AND d.period = pr.period
        JOIN cab_l c ON c.d = d.d AND c.offer_code = d.offer_code AND c.version = d.version
        JOIN uft u   ON u.unit_code = c.unit_code
        WHERE ABS(d.p_bid - pr.p_clear) <= {tol}
    )
    SELECT hour, tech_group, SUM(q_mw) AS marg_mw, COUNT(*) AS n_tranches
    FROM margin
    WHERE hour BETWEEN 0 AND 23
    GROUP BY 1, 2
    ORDER BY 1, 3 DESC
    """
    df = con.execute(q).df()
    total_per_hour = df.groupby("hour")["marg_mw"].sum().rename("hour_total")
    df = df.merge(total_per_hour, on="hour")
    df["share"] = df["marg_mw"] / df["hour_total"]
    pivot = df.pivot_table(index="hour", columns="tech_group", values="share", fill_value=0.0)
    return pivot


def summarise(pivot):
    return pd.DataFrame({
        "Critical": pivot.loc[list(CRIT)].mean(),
        "Midday":   pivot.loc[list(MID)].mean(),
        "Flat":     pivot.loc[list(FLAT)].mean(),
    }).pipe(lambda d: d.assign(**{"Crit - Flat": d["Critical"] - d["Flat"]}))


def write_tex(summary, out_path, caption_window):
    s = (summary * 100).round(1)
    # Keep techs with at least 1% share in any class
    s = s[s[["Critical", "Midday", "Flat"]].max(axis=1) >= 1.0]
    s = s.sort_values("Critical", ascending=False)
    lines = [
        r"\begin{tabular}{l r r r r}",
        r"\toprule",
        r"Technology & Critical & Midday & Flat & Crit$-$Flat \\",
        r"\midrule",
    ]
    for tech, row in s.iterrows():
        tech_esc = tech.replace("_", r"\_")
        lines.append(
            f"{tech_esc} & {row['Critical']:.1f} & {row['Midday']:.1f} & "
            f"{row['Flat']:.1f} & {row['Crit - Flat']:+.1f} \\\\"
        )
    lines += [r"\bottomrule", r"\end{tabular}"]
    Path(out_path).write_text("\n".join(lines) + "\n")
    print(f"  saved {out_path}")


def main():
    print("=== Marginal-tech composition (MW-weighted share at clearing price ±0.5 EUR/MWh) ===\n")
    for label, (lo, hi) in [
        ("Oct-Dec 2024 (pre-MTU15-DA)",  ("2024-10-01", "2024-12-31")),
        ("Oct-Dec 2025 (post-MTU15-DA)", ("2025-10-01", "2025-12-31")),
    ]:
        pivot = marginal_shares(lo, hi)
        pivot.to_csv(OUTDIR / f"marginal_tech_by_hour_{lo[:7]}_{hi[:7]}.csv")
        summary = summarise(pivot)
        print(f"--- {label} ---")
        print((summary * 100).round(1).to_string(), "\n")
        slug = "pre" if "2024" in lo else "post"
        write_tex(summary, TABDIR / f"tab_marginal_tech_by_hour_{slug}.tex", label)


if __name__ == "__main__":
    main()
