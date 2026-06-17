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

EPS = 0.01  # EUR/MWh tolerance for "p_bid exactly equal to p_clear"


# Map OMIE technology field → consolidated tech_group, covering ALL ~3,950 units
# in lista_unidades.csv (not just the 511 in firm_unit_panel).
def map_tech(tech_str: str) -> str:
    if not isinstance(tech_str, str):
        return "Other"
    t = tech_str.lower()
    if "ciclo combinado" in t: return "CCGT"
    if "nuclear" in t: return "Nuclear"
    if "carbón" in t or "carbon" in t or "hulla" in t: return "Coal"
    if "fuel" in t: return "Fuel/Gas"
    if t.strip() == "gas" or "gas natural" in t or "turbina de gas" in t: return "Fuel/Gas"
    if "bombeo mixto" in t or "consumo bombeo" in t or "consumo de bombeo" in t: return "Pump_load"
    if "bombeo puro" in t or ("bombeo" in t and ("turbin" in t or "hidráulica" in t or "hidraulica" in t)): return "Hydro_pump"
    if "hidráulica generación" in t or "hidraulica generacion" in t: return "Hydro"
    if "re mercado hidráulica" in t or "re mercado hidraulica" in t: return "Hydro_RES"
    if "re mercado eólica" in t or "re mercado eolica" in t: return "Wind"
    if "re mercado solar fotovolt" in t: return "Solar PV"
    if "re mercado solar térmica" in t or "re mercado solar termica" in t: return "Solar Thermal"
    if "re mercado térmica renovable" in t or "re mercado termica renovable" in t: return "Biomass"
    if "re mercado térmica no renovab" in t or "re mercado termica no renovab" in t: return "Cogen"
    if "re mercado geotérmica" in t or "re mercado geotermica" in t: return "Geothermal"
    if "híbrida" in t or "hibrida" in t or "hibridación" in t or "hibridacion" in t: return "Hybrid_RES"
    if "re tar" in t or "re tarifa" in t: return "RE_Tarifa_CUR"
    if "almacenamiento" in t: return "Storage"
    if "comercializador" in t or "compras comercializaci" in t: return "Retailer"
    if "compras consumo directo" in t or "consumidor directo" in t or "cons. directo" in t: return "Direct_consumer"
    if "consumo de productores" in t or "consumos auxiliares" in t: return "Aux_consumption"
    if "porfolio" in t or "portfolio" in t: return "Portfolio"
    if "import" in t or "contrato internacional" in t: return "Import"
    if "rep. de" in t or "rep. consum" in t or "representante" in t: return "Representative"
    if "unidad generica" in t or "genérica" in t or "generica" in t: return "Generic"
    if "distribuidor" in t: return "Distributor"
    if "agente vendedor" in t: return "Reg_Especial_other"
    return "Other"


def marginal_shares(window_start, window_end):
    """Share of (date, period) cells where each tech is THE price-setting tech.

    Uniform-price auction: the price-setting tranche per period is the single
    highest-priced accepted (p_bid ≤ p_clear) sell tranche. We additionally
    require that |max_accepted_p_bid − p_clear| ≤ {EPS} to be confident the
    matched tranche actually sets the price; otherwise the clearing was set
    by a unit outside our matched panel (Portuguese zone, interconnector,
    demand-side bid, or BSP-only unit) and we flag the period as UNMATCHED.

    The returned pivot has shares computed over MATCHED cells only; the
    auxiliary unmatched_pivot reports per-hour unmatched cell share.
    """
    # Use the FULL lista_unidades (~3,950 rows) with our own tech mapper,
    # so we cover the small RE-Mercado aggregator portfolios + retailers
    # that often set the clearing price. firm_unit_panel only returns ~511
    # units (project-relevant firms) and would miss the rest.
    import pandas as pd
    raw = pd.read_csv(UNITS_CSV)
    raw["tech_group"] = raw["technology"].apply(map_tech)
    units_all = raw[["unit_code", "tech_group"]].drop_duplicates("unit_code")
    con = duckdb.connect(); con.execute("PRAGMA threads=4"); con.execute("SET memory_limit='10GB'")
    con.register("uft", units_all)
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
               price_eur_mwh AS p_bid
        FROM '{DET}'
        WHERE date::DATE BETWEEN DATE '{window_start}' AND DATE '{window_end}'
          AND price_eur_mwh IS NOT NULL AND quantity_mw > 0
    ),
    accepted AS (
        SELECT pr.d, pr.period, u.tech_group, d.p_bid, pr.p_clear,
               RANK() OVER (PARTITION BY pr.d, pr.period ORDER BY d.p_bid DESC) AS rk
        FROM prices pr
        JOIN det d   ON d.d = pr.d AND d.period = pr.period
        JOIN cab_l c ON c.d = d.d AND c.offer_code = d.offer_code AND c.version = d.version
        JOIN uft u   ON u.unit_code = c.unit_code
        WHERE d.p_bid <= pr.p_clear
    ),
    -- For each (d, period): highest accepted bid (rank=1). Match flag tells
    -- us whether this top bid is actually at the clearing price (price-setter)
    -- or strictly below (clearing was set by a unit outside our panel).
    top_per_period AS (
        SELECT d, period, tech_group, p_bid, p_clear,
               (p_clear - MAX(p_bid) OVER (PARTITION BY d, period)) AS gap_to_clear
        FROM accepted
        WHERE rk = 1
    ),
    -- Per-period marginal contribution: 1/k split among rank=1 ties, ONLY
    -- if matched. Unmatched cells contribute nothing to the tech share but
    -- are counted separately.
    per_cell AS (
        SELECT d, period, tech_group,
               CASE WHEN gap_to_clear <= {EPS}
                    THEN 1.0 / COUNT(*) OVER (PARTITION BY d, period)
                    ELSE 0.0 END AS w,
               CASE WHEN gap_to_clear <= {EPS} THEN 1 ELSE 0 END AS matched
        FROM top_per_period
        GROUP BY d, period, tech_group, p_bid, p_clear, gap_to_clear
    ),
    with_hour AS (
        SELECT pc.d, pc.period,
               CAST(CASE WHEN EXTRACT(year FROM pc.d) <= 2024 THEN pc.period - 1
                         ELSE FLOOR((pc.period - 1) / 4) END AS INT) AS hour,
               pc.tech_group, pc.w, pc.matched
        FROM per_cell pc
    ),
    -- (d, period)-level matched flag: 1 if any of its rank-1 rows is matched
    -- (which is symmetric: matched is set together for all rank-1 rows of a period).
    period_matched AS (
        SELECT d, period, MAX(matched) AS matched
        FROM with_hour GROUP BY 1, 2
    ),
    hour_totals AS (
        SELECT CAST(CASE WHEN EXTRACT(year FROM p.d) <= 2024 THEN p.period - 1
                         ELSE FLOOR((p.period - 1) / 4) END AS INT) AS hour,
               COUNT(*) AS n_all
        FROM prices p
        GROUP BY 1
    ),
    hour_matched_count AS (
        SELECT CAST(CASE WHEN EXTRACT(year FROM pm.d) <= 2024 THEN pm.period - 1
                         ELSE FLOOR((pm.period - 1) / 4) END AS INT) AS hour,
               SUM(pm.matched) AS n_matched
        FROM period_matched pm
        GROUP BY 1
    )
    SELECT wh.hour, wh.tech_group,
           SUM(wh.w) / hmc.n_matched AS share,
           ht.n_all, hmc.n_matched
    FROM with_hour wh
    JOIN hour_totals       ht  ON wh.hour = ht.hour
    JOIN hour_matched_count hmc ON wh.hour = hmc.hour
    WHERE wh.hour BETWEEN 0 AND 23
    GROUP BY wh.hour, wh.tech_group, ht.n_all, hmc.n_matched
    """
    df = con.execute(q).df()
    pivot = df.pivot_table(index="hour", columns="tech_group", values="share", fill_value=0.0)
    # Diagnostic: matched fraction per hour
    diag = (df.groupby("hour").agg(n_all=("n_all", "max"),
                                    n_matched=("n_matched", "max"))
              .assign(unmatched_share=lambda d: 1 - d["n_matched"] / d["n_all"]))
    return pivot, diag


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
    print("=== Price-setting tech composition (share of MATCHED periods where each tech is the price-setter) ===\n")
    for label, (lo, hi) in [
        ("Oct-Dec 2024 (pre-MTU15-DA)",  ("2024-10-01", "2024-12-31")),
        ("Oct-Dec 2025 (post-MTU15-DA)", ("2025-10-01", "2025-12-31")),
    ]:
        pivot, diag = marginal_shares(lo, hi)
        pivot.to_csv(OUTDIR / f"marginal_tech_by_hour_{lo[:7]}_{hi[:7]}.csv")
        diag.to_csv(OUTDIR / f"marginal_tech_unmatched_{lo[:7]}_{hi[:7]}.csv")
        summary = summarise(pivot)
        print(f"--- {label} ---")
        print((summary * 100).round(1).to_string(), "\n")
        # Print unmatched diagnostic
        CRIT = (5, 6, 7, 8, 16, 17, 18, 19, 20, 21, 22)
        FLAT = (1, 2, 3)
        MID  = (11, 12, 13, 14)
        unmatched_crit = diag.loc[list(CRIT), "unmatched_share"].mean()
        unmatched_flat = diag.loc[list(FLAT), "unmatched_share"].mean()
        unmatched_mid  = diag.loc[list(MID),  "unmatched_share"].mean()
        print(f"  Unmatched-period share (clearing set by unit outside our panel):")
        print(f"    Critical: {100*unmatched_crit:.1f}%")
        print(f"    Midday:   {100*unmatched_mid:.1f}%")
        print(f"    Flat:     {100*unmatched_flat:.1f}%")
        print()
        slug = "pre" if "2024" in lo else "post"
        write_tex(summary, TABDIR / f"tab_marginal_tech_by_hour_{slug}.tex", label)


if __name__ == "__main__":
    main()
