# STATUS: ALIVE
# LAST-AUDIT: 2026-05-15
# FEEDS: provisional.tex §6 (REE post-clearing intervention)
# CLAIM: Generalised Path A — per-firm × per-tech monthly PHF − PDBF
#        intervention. Extends the CCGT-only script. Output: a
#        (tech × firm) × (pre, post) intervention table.

from __future__ import annotations

import sys
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO / "src"))
from mtu.classification.units import firm_unit_panel  # noqa: E402

PDBF = REPO / "data" / "processed" / "omie" / "mercado_diario" / "programas" / "pdbf_all.parquet"
PHF  = REPO / "data" / "processed" / "omie" / "mercado_intradiario_subastas" / "programas" / "phf_all.parquet"
UNITS_CSV = REPO / "data" / "external" / "omie_reference" / "lista_unidades.csv"
OUTDIR = REPO / "results" / "regressions" / "regulatory" / "pdbf_to_phf_alltech"
OUTDIR.mkdir(parents=True, exist_ok=True)

PIVOTAL = ("IB", "GE", "GN", "HC")
TECHS = ("CCGT", "Hydro", "Hydro_pump", "Nuclear", "Wind", "Solar PV")
WINDOW = ("2024-01-01", "2026-03-01")

PRE_W  = ("2024-01-01", "2025-04-01")   # pre-blackout (excl. blackout April)
POST_W = ("2025-05-01", "2026-02-01")   # post-blackout (reforzada)


def _month_iter(start: str, end: str):
    s = pd.Timestamp(start); e = pd.Timestamp(end)
    cur = pd.Timestamp(s.year, s.month, 1)
    while cur < e:
        nxt = cur + pd.offsets.MonthBegin(1)
        yield cur.date(), nxt.date()
        cur = nxt


def units_for(techs):
    u = firm_unit_panel(csv_path=str(UNITS_CSV), scheme="short", mode="primary_owner")
    return u[u["parent"].isin(PIVOTAL) & u["tech_group"].isin(techs)][
        ["unit_code", "parent", "tech_group"]
    ].rename(columns={"parent": "firm"})


def build_monthly_panel() -> pd.DataFrame:
    panel = units_for(TECHS)
    con = duckdb.connect(); con.execute("PRAGMA threads=4"); con.execute("SET memory_limit='6GB'")
    con.register("ups", panel)
    rows = []
    for m0, m1 in _month_iter(*WINDOW):
        q_pdbf = f"""
        SELECT u.firm, u.tech_group,
               SUM(p.assigned_power_mw * (p.mtu_minutes / 60.0)) AS pdbf_mwh
        FROM '{PDBF}' p JOIN ups u ON p.unit_code = u.unit_code
        WHERE p.date::DATE >= DATE '{m0}' AND p.date::DATE < DATE '{m1}'
          AND p.assigned_power_mw IS NOT NULL
        GROUP BY 1, 2
        """
        pdbf = con.execute(q_pdbf).df()

        q_phf = f"""
        WITH lat AS (
            SELECT date::DATE AS d, period, unit_code,
                   assigned_power_mw, mtu_minutes,
                   ROW_NUMBER() OVER (PARTITION BY date::DATE, period, unit_code
                                      ORDER BY session_number DESC) AS rn
            FROM '{PHF}'
            WHERE date::DATE >= DATE '{m0}' AND date::DATE < DATE '{m1}'
              AND assigned_power_mw IS NOT NULL
        )
        SELECT u.firm, u.tech_group,
               SUM(lat.assigned_power_mw * (lat.mtu_minutes / 60.0)) AS phf_mwh
        FROM lat JOIN ups u ON lat.unit_code = u.unit_code
        WHERE lat.rn = 1
        GROUP BY 1, 2
        """
        phf = con.execute(q_phf).df()
        m = pdbf.merge(phf, on=["firm", "tech_group"], how="outer").fillna(0.0)
        m["month"] = pd.Timestamp(m0)
        rows.append(m)
        print(f"  {m0} -> {m1}: {len(m)} (firm,tech) cells", flush=True)
    df = pd.concat(rows, ignore_index=True)
    df["intervention_mwh"] = df["phf_mwh"] - df["pdbf_mwh"]
    return df


def pre_post_table(monthly: pd.DataFrame) -> pd.DataFrame:
    pre  = monthly[(monthly["month"] >= PRE_W[0])  & (monthly["month"] < PRE_W[1])]
    post = monthly[(monthly["month"] >= POST_W[0]) & (monthly["month"] < POST_W[1])]
    pre_g  = pre.groupby(["firm", "tech_group"], as_index=False)["intervention_mwh"].mean()\
                 .rename(columns={"intervention_mwh": "pre_gwh_per_month"})
    pre_g["pre_gwh_per_month"] /= 1e3
    post_g = post.groupby(["firm", "tech_group"], as_index=False)["intervention_mwh"].mean()\
                  .rename(columns={"intervention_mwh": "post_gwh_per_month"})
    post_g["post_gwh_per_month"] /= 1e3
    t = pre_g.merge(post_g, on=["firm", "tech_group"], how="outer")
    t["delta_gwh"] = t["post_gwh_per_month"] - t["pre_gwh_per_month"]
    t["ratio"] = t["post_gwh_per_month"] / t["pre_gwh_per_month"]
    return t.sort_values(["tech_group", "firm"]).reset_index(drop=True)


def emit_latex(t: pd.DataFrame) -> str:
    pretty = {"IB": "IB", "GE": "GE", "GN": "GN", "HC": "HC"}
    techs = [tg for tg in TECHS if tg in t["tech_group"].unique()]
    lines = [
        r"\begin{tabular}{l l r r r r}",
        r"\toprule",
        r"Tech & Firm & Pre (GWh/mo) & Post (GWh/mo) & $\Delta$ (GWh/mo) & Ratio \\",
        r"\midrule",
    ]
    def tex_escape(s):
        return s.replace("_", r"\_")
    for tg in techs:
        sub = t[t["tech_group"] == tg]
        tg_disp = tex_escape(tg)
        for i, row in enumerate(sub.itertuples(index=False)):
            firm = pretty.get(row.firm, row.firm)
            pre  = f"{row.pre_gwh_per_month:.0f}"   if np.isfinite(row.pre_gwh_per_month)  else "--"
            post = f"{row.post_gwh_per_month:.0f}"  if np.isfinite(row.post_gwh_per_month) else "--"
            delta = f"{row.delta_gwh:+.0f}"         if np.isfinite(row.delta_gwh)          else "--"
            ratio = f"{row.ratio:.2f}"              if np.isfinite(row.ratio) and abs(row.ratio) < 100 else "--"
            tg_cell = tg_disp if i == 0 else ""
            lines.append(f"{tg_cell} & {firm} & {pre} & {post} & {delta} & {ratio} \\\\")
        lines.append(r"\addlinespace")
    lines += [r"\bottomrule", r"\end{tabular}"]
    return "\n".join(lines)


def main():
    print("building monthly panel for all techs...")
    monthly = build_monthly_panel()
    monthly.to_csv(OUTDIR / "monthly_alltech.csv", index=False)

    t = pre_post_table(monthly)
    t.to_csv(OUTDIR / "pre_post_summary.csv", index=False)
    print("\n=== pre/post-blackout summary, per firm × tech (GWh/mo) ===")
    print(t.to_string(index=False))

    tex = emit_latex(t)
    out_tex = OUTDIR / "tab_pdbf_to_phf_alltech.tex"
    out_tex.write_text(tex)
    print(f"\nLaTeX table at {out_tex}")


if __name__ == "__main__":
    main()
