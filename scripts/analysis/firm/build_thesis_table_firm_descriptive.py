# STATUS: ALIVE
# LAST-AUDIT: 2026-05-11
# FEEDS: thesis paper.tex Table 1 + Appendix B.2 (firm-descriptive)
# CLAIM: For each parent firm in the thesis sample, report the main
#        technologies, an installed-capacity proxy (max offered MW summed
#        across units, share-weighted), the firm's Iberian day-ahead
#        market share (PDBF DA-cleared + bilateral, share-weighted),
#        and DA-spot net position (sell-vs-buy turnover in PDBC).
#        Sources: OMIE unit register, cab_all, pdbc_all, pdbf_all.

from __future__ import annotations

from pathlib import Path
import sys

import duckdb
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO / "src"))
# Centralized firm classification: see src/mtu/classification/units.py and
# notebooks/memos/_firm_classification_audit.md.
from mtu.classification.units import (  # noqa: E402
    firm_unit_panel,
    TREATMENT_PARENTS_BROAD as DOMINANT_OPERATORS_SET,
    PLACEBO_PARENTS_BROAD as OTHER_OPERATORS_SET,
)

UNIT_REF = REPO / "data" / "external" / "omie_reference" / "lista_unidades.csv"
PDBC = REPO / "data" / "processed" / "omie" / "mercado_diario" / "programas" / "pdbc_all.parquet"
PDBF = REPO / "data" / "processed" / "omie" / "mercado_diario" / "programas" / "pdbf_all.parquet"
PDBCE = REPO / "data" / "processed" / "omie" / "mercado_diario" / "programas" / "pdbce_all.parquet"
PIBCA = REPO / "data" / "processed" / "omie" / "mercado_intradiario_subastas" / "programas" / "pibca_all.parquet"
CAB = REPO / "data" / "processed" / "omie" / "mercado_diario" / "ofertas" / "cab_all.parquet"
OUT_TEX = REPO / "thesis" / "paper" / "tables" / "tab_firm_descriptive.tex"
OUT_TEX_MAIN = REPO / "thesis" / "paper" / "tables" / "tab_firm_descriptive_main.tex"

# Two-block ordering: dominant operators (CNMC tier) above the rule, other
# large operators below it. No "Treatment / Placebo" column — that framing
# describes the within-day DiD on hours, not on firms.
DOMINANT_OPERATORS = ["Iberdrola", "Endesa", "Naturgy", "EDP-Spain", "EDP-Portugal"]
OTHER_OPERATORS    = ["Repsol", "Engie España", "TotalEnergies", "Moeve"]
FIRM_ORDER = DOMINANT_OPERATORS + OTHER_OPERATORS
# Sanity-check that the explicit order above matches the centralized constants
assert set(DOMINANT_OPERATORS) == DOMINANT_OPERATORS_SET
assert set(OTHER_OPERATORS) == OTHER_OPERATORS_SET

# Generation-technology buckets (order matters: first match wins).
# Retail / portfolio / storage / import entries are bucketed separately and
# excluded from the "Main generation technologies" column.
GEN_BUCKETS = [
    ("CCGT",    ["CICLO COMBINADO"]),
    ("Hydro",   ["HIDRÁULICA", "HIDRAULICA", "BOMBEO"]),
    ("Wind",    ["EÓLICA", "EOLICA"]),
    ("Solar",   ["SOLAR FOTOVOLT", "TÉRMICA NO RENOVAB", "TERMICA NO RENOVAB"]),
    ("Nuclear", ["NUCLEAR"]),
    ("Coal",    ["CARBÓN", "CARBON", "FUEL"]),
    ("Cogen",   ["COGENERAC"]),
    ("Biomass", ["BIOMASA", "BIOGÁS", "BIOGAS"]),
    ("Hybrid",  ["HÍBRIDA", "HIBRIDA"]),
]
RETAIL_TOKENS = (
    "COMERCIALIZ", "COMPRA", "PORFOLIO", "REPRESENT", "REP.",
    "TARIFA CUR", "IMPORT", "CONSUMO", "GENERICA",
    "ALMACENAMIENTO", "STORAGE",
)


def tech_bucket(tech) -> str | None:
    """Map OMIE technology string -> generation bucket, or 'Retail' for
    non-generation entries (commercializer, portfolio, storage, etc.)."""
    if tech is None or (isinstance(tech, float) and pd.isna(tech)):
        return None
    t = str(tech).upper()
    for bucket, needles in GEN_BUCKETS:
        if any(n in t for n in needles):
            return bucket
    if any(tok in t for tok in RETAIL_TOKENS):
        return "Retail"
    return "Other"


def main():
    # all_owners mode: keep one row per (unit_code, owner) so joint-owned
    # plants (Almaraz, Trillo, ...) carry the per-stakeholder ownership share.
    # All downstream aggregates MUST weight quantities by `share` to avoid the
    # multi-attribution bug (see notebooks/memos/_firm_classification_audit.md).
    keep = firm_unit_panel(csv_path=str(UNIT_REF), scheme="broad",
                            mode="all_owners")
    keep["tech_bucket"] = keep["technology"].apply(tech_bucket)

    # Per-firm: generation-unit count, generation-tech mix (top-3 by unit count).
    firm_rows: list[dict] = []
    for firm in FIRM_ORDER:
        sub = keep[keep["parent"] == firm]
        if sub.empty:
            firm_rows.append({"parent": firm, "n_gen_units": 0, "techs": "—"})
            continue
        gen = sub[~sub["tech_bucket"].isin({"Retail", "Other", None})]
        top_techs = gen["tech_bucket"].value_counts().head(3).index.tolist()
        firm_rows.append({
            "parent": firm,
            "n_gen_units": int(len(gen)),
            "techs": ", ".join(top_techs) if top_techs else "—",
        })

    fdf = pd.DataFrame(firm_rows).set_index("parent").reindex(FIRM_ORDER).reset_index()

    con = duckdb.connect()
    # Pass the ownership share with the parent label so downstream sums weight
    # joint-owned units (mainly nuclear) by each firm's actual stake.
    con.register("units_map", keep[["unit_code", "parent", "share"]])

    # Installed-capacity proxy: per unit, take the maximum `max_power_mw` ever
    # offered in 2025 (from cab headers); sum to parent firm WEIGHTED BY OWNERSHIP.
    cap = con.execute(f"""
        WITH per_unit AS (
            SELECT unit_code, MAX(max_power_mw) AS max_mw
            FROM '{CAB}'
            WHERE date::DATE >= DATE '2025-01-01' AND date::DATE < DATE '2026-01-01'
              AND buy_sell = 'V'
            GROUP BY 1
        )
        SELECT u.parent, SUM(p.max_mw * u.share) / 1000.0 AS capacity_gw
        FROM per_unit p JOIN units_map u USING (unit_code)
        GROUP BY 1
    """).df()

    # Channel-segregated programmed-volume shares for 2025, share-weighted across
    # joint-owned plants. Everything is taken from PDBF (Programa Diario Base
    # Final) so denominators are internally consistent:
    #   - DA auction:   PDBF offer_type=1  (matched in the DA auction)
    #   - DA bilateral: PDBF offer_type=4  (bilateral contract executions)
    #   - IDA:          PIBCA accumulated, last session per (date, period, unit)
    #
    # Note on PDBC vs PDBF: PDBC has more rows than PDBF.offer_type=1 because
    # it reports the DA-clearing snapshot for every unit regardless of whether
    # the same unit also has bilateral activity. PDBF aggregates by unit-period
    # and stores the DA-cleared portion under offer_type=1. Using PDBF
    # consistently keeps the denominators clean.
    da_auction = con.execute(f"""
        WITH p AS (
            SELECT unit_code, assigned_power_mw * (mtu_minutes/60.0) AS mwh
            FROM '{PDBF}'
            WHERE date::DATE >= DATE '2025-01-01' AND date::DATE < DATE '2026-01-01'
              AND assigned_power_mw > 0 AND offer_type = 1
        )
        SELECT u.parent, SUM(p.mwh * u.share) AS twh
        FROM p JOIN units_map u USING (unit_code) GROUP BY 1
    """).df()

    da_bilateral = con.execute(f"""
        WITH p AS (
            SELECT unit_code, assigned_power_mw * (mtu_minutes/60.0) AS mwh
            FROM '{PDBF}'
            WHERE date::DATE >= DATE '2025-01-01' AND date::DATE < DATE '2026-01-01'
              AND assigned_power_mw > 0 AND offer_type = 4
        )
        SELECT u.parent, SUM(p.mwh * u.share) AS twh
        FROM p JOIN units_map u USING (unit_code) GROUP BY 1
    """).df()

    # IDA cumulative: take last session per (date, period, unit_code) since
    # pibca is "accumulated" — later sessions update the running total.
    ida_cum = con.execute(f"""
        WITH max_session AS (
            SELECT date, period, unit_code, MAX(session_number) AS s
            FROM '{PIBCA}'
            WHERE date::DATE >= DATE '2025-01-01' AND date::DATE < DATE '2026-01-01'
            GROUP BY 1,2,3
        ),
        p AS (
            SELECT p.unit_code,
                   p.assigned_power_mw * (p.mtu_minutes/60.0) AS mwh
            FROM '{PIBCA}' p
            JOIN max_session m
              ON p.date = m.date AND p.period = m.period
             AND p.unit_code = m.unit_code AND p.session_number = m.s
            WHERE p.assigned_power_mw > 0
        )
        SELECT u.parent, SUM(p.mwh * u.share) AS twh
        FROM p JOIN units_map u USING (unit_code) GROUP BY 1
    """).df()

    # Spain totals for the denominators (Iberian DA + bilateral; Iberian IDA cumulative)
    tot_da_auct = con.execute(f"""
        SELECT SUM(assigned_power_mw * (mtu_minutes/60.0)) FROM '{PDBF}'
        WHERE date::DATE >= DATE '2025-01-01' AND date::DATE < DATE '2026-01-01'
          AND assigned_power_mw > 0 AND offer_type = 1
    """).df().iloc[0, 0]
    tot_da_bilat = con.execute(f"""
        SELECT SUM(assigned_power_mw * (mtu_minutes/60.0)) FROM '{PDBF}'
        WHERE date::DATE >= DATE '2025-01-01' AND date::DATE < DATE '2026-01-01'
          AND assigned_power_mw > 0 AND offer_type = 4
    """).df().iloc[0, 0]
    tot_ida_cum = con.execute(f"""
        WITH max_session AS (
            SELECT date, period, unit_code, MAX(session_number) AS s
            FROM '{PIBCA}'
            WHERE date::DATE >= DATE '2025-01-01' AND date::DATE < DATE '2026-01-01'
            GROUP BY 1,2,3
        )
        SELECT SUM(p.assigned_power_mw * (p.mtu_minutes/60.0))
        FROM '{PIBCA}' p
        JOIN max_session m
          ON p.date = m.date AND p.period = m.period
         AND p.unit_code = m.unit_code AND p.session_number = m.s
        WHERE p.assigned_power_mw > 0
    """).df().iloc[0, 0]

    da_auction["da_auct_pct"]   = 100.0 * da_auction["twh"]   / tot_da_auct
    da_bilateral["da_bilat_pct"] = 100.0 * da_bilateral["twh"] / tot_da_bilat
    ida_cum["ida_pct"]           = 100.0 * ida_cum["twh"]      / tot_ida_cum

    # Headline Market share = DA auction + DA bilateral (combined Iberian
    # day-ahead programmed sell share). Reproduces the previous Iberdrola=25%.
    combined = da_auction[["parent", "twh"]].rename(columns={"twh": "da_auct_twh"}).merge(
        da_bilateral[["parent", "twh"]].rename(columns={"twh": "da_bilat_twh"}),
        on="parent", how="outer"
    ).fillna(0.0)
    combined["da_total_twh"] = combined["da_auct_twh"] + combined["da_bilat_twh"]
    combined["da_share_pct"] = 100.0 * combined["da_total_twh"] / (tot_da_auct + tot_da_bilat)

    # Net position from pdbc, post-MTU15-IDA only (post 2025-03-19) so the
    # buy-side data isn't dominated by pre-reform rule-28.8 artefacts. PDBC
    # uses a single offer_type=1 (the unit holds a sell slot) but reports
    # cleared SALES as positive `assigned_power_mw` and cleared BUYS (pump-load,
    # retail-arm consumption, storage charging) as negative.
    net = con.execute(f"""
        WITH d AS (
            SELECT unit_code,
                   GREATEST(assigned_power_mw, 0)  * (mtu_minutes/60.0) AS sell_mwh,
                   GREATEST(-assigned_power_mw, 0) * (mtu_minutes/60.0) AS buy_mwh
            FROM '{PDBC}'
            WHERE date::DATE >= DATE '2025-04-01' AND date::DATE < DATE '2026-01-01'
        )
        SELECT u.parent,
               SUM(d.sell_mwh * u.share) AS sell_mwh,
               SUM(d.buy_mwh  * u.share) AS buy_mwh
        FROM d JOIN units_map u USING (unit_code)
        GROUP BY 1
    """).df()

    def classify_net(row) -> str:
        s, b = row.get("sell_mwh", 0.0) or 0.0, row.get("buy_mwh", 0.0) or 0.0
        total = s + b
        if total <= 1.0:
            return "—"
        share_sell = s / total
        if share_sell >= 0.60:
            return "Net seller"
        if share_sell <= 0.40:
            return "Net buyer"
        return "Mixed"
    net["net_position"] = net.apply(classify_net, axis=1)

    fdf = fdf.merge(cap[["parent", "capacity_gw"]], on="parent", how="left")
    fdf = fdf.merge(combined[["parent", "da_share_pct"]], on="parent", how="left")
    fdf = fdf.merge(da_auction[["parent", "da_auct_pct"]], on="parent", how="left")
    fdf = fdf.merge(da_bilateral[["parent", "da_bilat_pct"]], on="parent", how="left")
    fdf = fdf.merge(ida_cum[["parent", "ida_pct"]], on="parent", how="left")
    fdf = fdf.merge(net[["parent", "net_position"]], on="parent", how="left")
    for c in ["da_share_pct", "da_auct_pct", "da_bilat_pct", "ida_pct"]:
        fdf[c] = fdf[c].fillna(0.0)
    fdf["net_position"] = fdf["net_position"].fillna("—")

    def fmt_share(x: float) -> str:
        if pd.isna(x) or x == 0:
            return "—"
        return f"{x:.2f}"

    def fmt_cap(x) -> str:
        if x is None or pd.isna(x) or x == 0:
            return "—"
        return f"{x:.2f}"

    dominant = fdf[fdf["parent"].isin(DOMINANT_OPERATORS)]
    other    = fdf[fdf["parent"].isin(OTHER_OPERATORS)]

    # ---- Main-text table: simple. Parent | techs | capacity | market share -----
    main_lines: list[str] = []
    main_lines.append(r"\begin{tabular}{l p{4.8cm} r r}")
    main_lines.append(r"\toprule")
    main_lines.append(
        r"Parent firm & Main generation technologies & "
        r"\makecell{Capacity\\(GW)} & \makecell{Market share\\(\%)} \\"
    )
    main_lines.append(r"\midrule")
    def emit_main(rows, header: str) -> list[str]:
        out = [r"\multicolumn{4}{l}{\textit{" + header + r"}} \\"]
        for r in rows.itertuples():
            out.append(
                f"{r.parent} & {r.techs} & {fmt_cap(r.capacity_gw)} & "
                f"{fmt_share(r.da_share_pct)} \\\\"
            )
        return out
    main_lines.extend(emit_main(dominant, "Dominant operators in Spanish generation (CNMC tier)"))
    main_lines.append(r"\midrule")
    main_lines.extend(emit_main(other, "Other large operators (non-dominant)"))
    main_lines.append(r"\bottomrule")
    main_lines.append(r"\end{tabular}")
    OUT_TEX_MAIN.write_text("\n".join(main_lines))
    print(f"wrote: {OUT_TEX_MAIN}")

    # ---- Appendix table: channel-segregated breakdown -----
    # Columns: Parent | Gen.units | Capacity | DA auction % | DA bilateral % |
    #          IDA cumulative % | Net position
    lines: list[str] = []
    lines.append(r"\begin{tabular}{l r r r r r l}")
    lines.append(r"\toprule")
    lines.append(
        r" & \makecell{Gen.\\units} & \makecell{Capacity\\(GW)} & "
        r"\makecell{DA auction\\share (\%)} & \makecell{DA bilateral\\share (\%)} & "
        r"\makecell{IDA cumul.\\share (\%)} & \\"
    )
    lines.append(
        r"Parent firm & & & & & & Net position \\"
    )
    lines.append(r"\midrule")
    def emit_appx(rows, header: str) -> list[str]:
        out = [r"\multicolumn{7}{l}{\textit{" + header + r"}} \\"]
        for r in rows.itertuples():
            out.append(
                f"{r.parent} & {r.n_gen_units} & {fmt_cap(r.capacity_gw)} & "
                f"{fmt_share(r.da_auct_pct)} & {fmt_share(r.da_bilat_pct)} & "
                f"{fmt_share(r.ida_pct)} & {r.net_position} \\\\"
            )
        return out
    lines.extend(emit_appx(dominant, "Dominant operators in Spanish generation (CNMC tier)"))
    lines.append(r"\midrule")
    lines.extend(emit_appx(other, "Other large operators (non-dominant)"))
    lines.append(r"\bottomrule")
    lines.append(r"\end{tabular}")
    OUT_TEX.write_text("\n".join(lines))
    print(f"wrote: {OUT_TEX}")

    print(fdf[['parent','n_gen_units','techs','capacity_gw',
               'da_auct_pct','da_bilat_pct','ida_pct','da_share_pct','net_position']]
          .to_string(index=False))


if __name__ == "__main__":
    main()
