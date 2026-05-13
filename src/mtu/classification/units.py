"""Unit-level classification: firm_class, tech_group, tech_strategic_role,
plus the thesis firm-panel builder (parent + ownership share).

Single source of truth for unit-to-firm mapping across the whole project.
All thesis analysis scripts MUST import `parent_of()` and `firm_unit_panel()`
from this module; never copy/paste the rules inline.

## Two firm-naming schemes

This module produces firm labels under TWO schemes:

- `scheme='short'` — 2-letter codes used in DiD scripts:
  IB, GE, GN, HC (Big-4 generation), EDP-PT (EDP-Portugal),
  Repsol, Engie, TotalEnergies, Moeve. Used by
  `critical_hours_did_thesis.py`, `da_cleared_did_thesis.py`, etc.

- `scheme='broad'` — full firm names used in descriptive tables:
  Iberdrola, Endesa, Naturgy, EDP-Spain, EDP-Portugal, Repsol,
  Engie España, TotalEnergies, Moeve.

Both schemes use the SAME underlying substring-matching rules (same keywords,
same exclusions), so a unit_code that maps to IB under 'short' will always
map to Iberdrola under 'broad'.

## Joint ownership — two output modes

OMIE registers joint-owned plants (mostly Spanish nuclear: Almaraz, Trillo,
Ascó 2, Vandellós II) with one row per (unit_code, owner), and `ownership_pct`
< 100. `firm_unit_panel()` exposes this via `mode`:

- `mode='all_owners'` — one row per (unit_code, owner) with the `share` column
  (ownership_pct/100). Use when downstream computes weighted aggregates
  (total cleared MWh per firm = SUM(unit_mwh * share)). Joins against PDBC
  will multi-count joint-owned units; downstream code MUST weight by share.

- `mode='primary_owner'` — one row per unit_code, assigned to the largest-share
  owner. Use when downstream cannot apply share weighting (e.g. unit-period
  panel regressions: each (date, period, unit) is one observation). Joint
  nuclear plants are attributed to the majority shareholder.

For unit-period bidding-behavior regressions, use 'primary_owner'.
For aggregate-volume / capacity / share computations, use 'all_owners' and
weight by `share`.

## Schema produced by classify_units()

Legacy entry point. For each `unit_code` in the OMIE register (~3,950 units),
produce three flat fields:

- `firm_class ∈ {IB, GE, GN, HC, Fringe}`
  Big-4 dominant generation firms (IB=Iberdrola, GE=Endesa, GN=Naturgy,
  HC=EDP-España) plus a Fringe catch-all for everyone else.

  NOTE: classify_units() uses the STRICT scheme — only the generation-arm
  owner_agent strings, no EDP-Portugal / Engie / Repsol / etc. For the
  full thesis firm partition, use `firm_unit_panel(scheme='short')` instead.

- `tech_group ∈ {CCGT, Hydro, Hydro_pump, Hydro_RES, Nuclear, Coal, Wind,
   Solar PV, Solar Thermal, Biomass, Cogen, Other_RES, Hybrid_RES,
   Hybrid_RES_storage, Hybrid_RES_thermal, Storage_buy, Storage_sell,
   Pump_load, Retailer, Direct_consumer, Generic, Import, Other}`
  Operational technology, derived from OMIE `technology` field via the
  `_tech_map` below.

- `tech_strategic_role ∈ {flexible_strategic, price_taking_in_DA,
   demand_side, other}`
  The economically meaningful classification for strategic-conduct work:
  * `flexible_strategic`: CCGT, Coal, reservoir Hydro, Hydro_pump (sell),
    Hybrid_RES_thermal. Units that can withhold or shape bids strategically.
  * `price_taking_in_DA`: Wind, Solar PV/Thermal, Biomass, Hydro_RES (run-of-
    river), Cogen, Other_RES, Hybrid_RES, Hybrid_RES_storage, base Nuclear.
    Marginal cost ~0 or operationally inflexible; bids price-takingly.
  * `demand_side`: Retailer, Direct_consumer, Pump_load (buy), Storage_buy.
  * `other`: Generic, Import, Storage_sell-as-arbitrage.

## Why pivotality is NOT a tier here

The `firm_class` × `tech_strategic_role` partition captures *capacity for*
strategic conduct (firm has market power, tech is flexible). *Realised*
pivotality (RSI<1) is a separate empirical question better handled per
unit-period using the pivotal indicator panel (see
`data/derived/panels/bid_shape_critical_flat/pivotal_indicator_*.parquet`).
Conditioning on `pct_pivotal > X%` directly is more flexible than baking
a "strict tier" into this module.

## CNMC reference

Resolución de la CNMC sobre operadores dominantes en el sector eléctrico:
intermediate tier (>10% generation share) = Iberdrola, Endesa, Naturgy
(formerly Gas Natural Fenosa), EDP. The "operador principal" lax tier
also includes Acciona and Repsol. Source:
https://www.cnmc.es/sites/default/files/editor_contenidos/Energia/...
"""
from __future__ import annotations

from typing import Iterable

import pandas as pd

DOMINANT_FIRMS: tuple[str, ...] = ("IB", "GE", "GN", "HC")

# Operational technology grouping. Maps OMIE `technology` string → tech_group.
TECH_GROUPS: dict[str, str] = {
    # CCGT / OCGT
    "Ciclo Combinado": "CCGT",
    "Gas": "CCGT",  # legacy entry, may overlap with OCGT
    # Coal
    "Hulla Antracita": "Coal",
    "Carbón de Importación": "Coal",
    # Nuclear
    "Nuclear": "Nuclear",
    # Hydro flavours
    "Hidráulica Generación": "Hydro",         # reservoir / pondage
    "Hidráulica de Bombeo Puro": "Hydro_pump",  # pumped storage
    "RE Mercado Hidráulica": "Hydro_RES",     # run-of-river under RE regime
    "RE Tar. CUR Hidráulica": "Hydro_RES",
    # Wind
    "RE Mercado Eólica": "Wind",
    "RE Tar. CUR Eólica": "Wind",
    "RE Mercado Eólica Marina": "Wind",
    "RE Tar. CUR Eólica": "Wind",
    # Solar
    "RE Mercado Solar Fotovoltáica": "Solar PV",
    "RE Tar. CUR Solar Fotovoltáica": "Solar PV",
    "RE Mercado Solar Térmica": "Solar Thermal",
    "RE Tar. CUR Solar Térmica": "Solar Thermal",
    # Biomass / cogen / other RES
    "RE Mercado Térmica Renovable": "Biomass",
    "RE Tar. CUR Térmica Renovable": "Biomass",
    "RE Mercado Térmica no Renovab.": "Cogen",
    "RE Tar. CUR Térmica no Renov.": "Cogen",
    "RE Mercado Geotérmica": "Other_RES",
    # Hybrid
    "Híbrida Renovable": "Hybrid_RES",
    "Híbrida Renov.-Almacenamiento": "Hybrid_RES_storage",
    "Híbrida Renov.-Térmica": "Hybrid_RES_thermal",
    # Storage / pumping load
    "Almacenamiento Compra": "Storage_buy",
    "Almacenamiento Venta": "Storage_sell",
    "Consumo Bombeo Mixto": "Pump_load",
    "Consumo de bombeo": "Pump_load",
    "Consumo Bombeo Puro": "Pump_load",
    # Retailers / direct consumers
    "Comercializador": "Retailer",
    "Comercializador no residente": "Retailer",
    "Comercializador ultimo recurso": "Retailer",
    "Compras Comercialización": "Retailer",
    "Compra Comercializador Balance": "Retailer",
    "Porfolio Comerc. Compra": "Retailer",
    "Porfolio Comerc. Venta": "Retailer",
    "Rep. de comercializadores": "Retailer",
    "Compras Cons. Directo Balance": "Direct_consumer",
    "Compras Consumo Directo": "Direct_consumer",
    "Consumidor directo": "Direct_consumer",
    "Compras Consumos Auxiliares": "Direct_consumer",
    "Consumo de productores": "Direct_consumer",
    "Rep. de consumidores directos": "Direct_consumer",
    "Rep. Consumos Auxiliares": "Direct_consumer",
    # Generic / cross-border
    "Unidad Generica": "Generic",
    "VENTA GENERICA": "Generic",
    "Porfolio Produccion Compra": "Generic",
    "Porfolio Produccion Venta": "Generic",
    "Agente vendedor Reg. Especial": "Generic",
    "Import. de agentes externos": "Import",
    "Import. de comercializadoras": "Import",
}

FLEXIBLE_STRATEGIC_TECHS: frozenset[str] = frozenset({
    "CCGT",
    "Coal",
    "Hydro",            # reservoir/pondage hydro — strategic dispatch
    "Hydro_pump",       # pumped storage on the sell side
    "Hybrid_RES_thermal",
})

PRICE_TAKING_TECHS: frozenset[str] = frozenset({
    "Wind",
    "Solar PV",
    "Solar Thermal",
    "Biomass",
    "Hydro_RES",        # run-of-river — physical not strategic
    "Cogen",
    "Other_RES",
    "Hybrid_RES",
    "Hybrid_RES_storage",
    "Nuclear",          # base-load, must-run; price-taking in DA
})

DEMAND_SIDE_TECHS: frozenset[str] = frozenset({
    "Retailer",
    "Direct_consumer",
    "Pump_load",
    "Storage_buy",
})


def _classify_firm(owner_agent: str | None) -> str:
    """Legacy strict firm classifier. Use parent_of() for new code.

    Big-4 generation arms only. Other utilities (Acciona, Repsol, EDP-Portugal,
    Engie, retailers) → 'Fringe'.
    """
    if owner_agent is None:
        return "Fringe"
    o = owner_agent.upper()
    if "IBERDROLA ENERG" in o:
        return "IB"
    if o.startswith("ENDESA GENERAC"):
        return "GE"
    if o.startswith("NATURGY ") or "GAS NATURAL" in o:
        return "GN"
    if "EDP ESPAÑA" in o and "GENERACI" in o:
        return "HC"
    return "Fringe"


# =========================================================================
# Thesis firm partition (broad scheme): the canonical owner_agent → parent
# map used by the within-day DiD analysis and the thesis descriptive tables.
# =========================================================================
#
# Substring matching on owner_agent (uppercased). Order matters: first match
# wins. Broad enough to capture both generation-arm and retail-arm strings of
# the same group, so net-position computations can aggregate the full firm.
#
# Audited 2026-05-11 against lista_unidades.csv (3,950 units) and
# lista_agentes.csv (1,479 agents). See
# notebooks/memos/_firm_classification_audit.md for the audit trail.
FIRM_RULES_BROAD: tuple[tuple[str, str], ...] = (
    ("IBERDROLA",        "Iberdrola"),
    ("ENDESA",           "Endesa"),
    ("NATURGY",          "Naturgy"),
    ("GAS NATURAL",      "Naturgy"),     # legacy Gas Natural Fenosa handle for Naturgy CCGT fleet
    ("EDP ESPAÑA",       "EDP-Spain"),
    ("EDP CLIENTES",     "EDP-Spain"),
    ("EDP COMERCIAL",    "EDP-Spain"),
    ("IBERENERGIA",      "EDP-Spain"),   # legacy JV vehicle holding EDP-Spain's 15.5% Trillo stake
    ("EDP GEM PORTUGAL", "EDP-Portugal"),
    ("REPSOL",           "Repsol"),
    ("ENGIE",            "Engie España"),
    ("TOTALENERGIES",    "TotalEnergies"),
    ("MOEVE",            "Moeve"),
    ("CEPSA",            "Moeve"),       # Moeve = formerly Cepsa
)

# Same rules under the 2-letter short scheme used in DiD scripts. Keyword
# matching is identical; only the output label differs.
FIRM_RULES_SHORT: tuple[tuple[str, str], ...] = (
    ("IBERDROLA",        "IB"),
    ("ENDESA",           "GE"),
    ("NATURGY",          "GN"),
    ("GAS NATURAL",      "GN"),
    ("EDP ESPAÑA",       "HC"),
    ("EDP CLIENTES",     "HC"),
    ("EDP COMERCIAL",    "HC"),
    ("IBERENERGIA",      "HC"),
    ("EDP GEM PORTUGAL", "EDP-PT"),
    ("REPSOL",           "Repsol"),
    ("ENGIE",            "Engie"),
    ("TOTALENERGIES",    "TotalEnergies"),
    ("MOEVE",            "Moeve"),
    ("CEPSA",            "Moeve"),
)

# Exact owner_agent strings (uppercased, stripped) that match the rules above
# but should NOT be folded into the parent firm. Verified against
# lista_agentes.agent_type and the technology mix of the units they hold.
#
#  - REPSOL SERVICIOS RENOVABLES, S.A. is a REPRESENTANTE agent (REPSB) that
#    aggregates ~42 third-party small renewable plants for OMIE market
#    participation. The plants are not Repsol-owned assets.
#  - ENGIE GLOBAL MARKETS, BELGIAN BRANCH (FR_GS) is a Belgian trading desk,
#    not part of Engie España's Spanish portfolio.
OWNER_EXCLUDE: frozenset[str] = frozenset({
    "REPSOL SERVICIOS RENOVABLES, S.A.",
    "ENGIE GLOBAL MARKETS, BELGIAN BRANCH",
})

# Subset memberships used by DiD scripts to assign units to treatment vs
# negative-control samples.
TREATMENT_PARENTS_SHORT: frozenset[str] = frozenset({"IB", "GE", "GN", "HC", "EDP-PT"})
PLACEBO_PARENTS_SHORT:   frozenset[str] = frozenset({"Repsol", "Engie", "TotalEnergies", "Moeve"})
TREATMENT_PARENTS_BROAD: frozenset[str] = frozenset({"Iberdrola", "Endesa", "Naturgy", "EDP-Spain", "EDP-Portugal"})
PLACEBO_PARENTS_BROAD:   frozenset[str] = frozenset({"Repsol", "Engie España", "TotalEnergies", "Moeve"})


def parent_of(owner_agent, *, scheme: str = "short") -> str | None:
    """Map an OMIE `owner_agent` description string to a thesis parent-firm label.

    Parameters
    ----------
    owner_agent : str or None
        The free-text owner string from `lista_unidades.csv`. None / NaN /
        non-string inputs return None.
    scheme : {'short', 'broad'}
        'short' returns 2-letter codes (IB, GE, GN, HC, EDP-PT, Repsol, Engie,
        TotalEnergies, Moeve) used in DiD scripts. 'broad' returns full names
        (Iberdrola, Endesa, …, Engie España) used in descriptive tables.

    Returns
    -------
    str or None
        Parent-firm label, or None if the owner is outside the thesis firm set
        (including units excluded via OWNER_EXCLUDE).
    """
    if not isinstance(owner_agent, str):
        return None
    u = owner_agent.strip().upper()
    if not u:
        return None
    if u in OWNER_EXCLUDE:
        return None
    rules = FIRM_RULES_SHORT if scheme == "short" else FIRM_RULES_BROAD
    for needle, label in rules:
        if needle in u:
            return label
    return None


def firm_unit_panel(
    csv_path: str = "data/external/omie_reference/lista_unidades.csv",
    *,
    scheme: str = "short",
    mode: str = "all_owners",
    unit_ref: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Build a clean unit-to-firm panel with ownership share.

    This is the canonical join key for downstream analysis. Every script that
    aggregates by parent firm MUST use this function rather than reimplementing
    the rules.

    Parameters
    ----------
    csv_path : str
        Path to the OMIE register CSV. Default
        `data/external/omie_reference/lista_unidades.csv`.
    scheme : {'short', 'broad'}
        Firm-naming scheme; see `parent_of`.
    mode : {'all_owners', 'primary_owner'}
        How to handle joint-owned units (mostly Spanish nuclear):

        - 'all_owners': keep one row per (unit_code, owner_agent). For
          joint-owned plants (Almaraz, Trillo, Ascó 2, Vandellós II), this
          yields multiple rows per unit_code; the `share` column carries the
          ownership fraction. Downstream MUST weight aggregate quantities by
          share, otherwise the same MWh is multi-counted across firms.

        - 'primary_owner': deduplicate to one row per unit_code, assigning
          each to the largest-share owner. `share` is set to 1.0 (i.e. the
          unit is fully attributed to its primary operator). Use this for
          unit-period panel regressions where one observation = one unit.
    unit_ref : DataFrame, optional
        Pre-loaded register. If None, read from `csv_path`.

    Returns
    -------
    DataFrame with columns:
        unit_code, parent, share, owner_agent, ownership_pct
        (and the original register columns: description, unit_type, zone,
         technology, tech_group, tech_strategic_role).
    """
    if unit_ref is None:
        unit_ref = pd.read_csv(csv_path, sep=None, engine="python")

    df = unit_ref.copy()
    df["parent"] = df["owner_agent"].apply(lambda o: parent_of(o, scheme=scheme))
    df["share"] = df["ownership_pct"].fillna(100.0) / 100.0
    df["tech_group"] = df["technology"].map(TECH_GROUPS).fillna("Other")
    # Description-based override: some hybrid PV+battery and wind+battery
    # UOs carry the OMIE classification of the dominant resource (Solar PV
    # or Wind) even though physically they mix technologies. Reclassify
    # those so single-tech figures and stratifications do not pool them.
    desc = df["description"].astype(str).str.upper()
    is_hybrid_storage = desc.str.contains(r"\bHIB\b|HIBRID") & desc.str.contains("BAT")
    is_hybrid = desc.str.contains(r"\bHIB\b|HIBRID") & ~is_hybrid_storage
    df.loc[is_hybrid_storage, "tech_group"] = "Hybrid_RES_storage"
    df.loc[is_hybrid & (df["tech_group"] != "Hybrid_RES_storage"), "tech_group"] = "Hybrid_RES"
    df["tech_strategic_role"] = df["tech_group"].map(_classify_strategic_role)

    keep = df[df["parent"].notna()].copy()

    if mode == "primary_owner":
        keep = (keep
                .sort_values(["unit_code", "share"], ascending=[True, False])
                .drop_duplicates("unit_code", keep="first")
                .copy())
        keep["share"] = 1.0  # full attribution to the primary owner
    elif mode != "all_owners":
        raise ValueError(f"mode must be 'all_owners' or 'primary_owner', got {mode!r}")

    return keep.reset_index(drop=True)


def _classify_strategic_role(tech_group: str) -> str:
    if tech_group in FLEXIBLE_STRATEGIC_TECHS:
        return "flexible_strategic"
    if tech_group in PRICE_TAKING_TECHS:
        return "price_taking_in_DA"
    if tech_group in DEMAND_SIDE_TECHS:
        return "demand_side"
    return "other"


def classify_units(
    unit_ref: pd.DataFrame | None = None,
    *,
    csv_path: str = "data/external/omie_reference/lista_unidades.csv",
    keep_columns: Iterable[str] | None = None,
) -> pd.DataFrame:
    """Read OMIE unit register and return classification table.

    Parameters
    ----------
    unit_ref : DataFrame, optional
        Pre-loaded register. If None, read from `csv_path`.
    csv_path : str
        Path to the OMIE register CSV. Default
        `data/external/omie_reference/lista_unidades.csv`.
    keep_columns : iterable of str, optional
        Columns to retain in the output beyond the classification fields.
        Default keeps `unit_code, owner_agent, technology, max_power_mw`
        (where present) plus the new classification fields.

    Returns
    -------
    DataFrame with columns:
        unit_code, firm_class, tech_group, tech_strategic_role,
        + any retained context columns from the register.
    """
    if unit_ref is None:
        unit_ref = pd.read_csv(csv_path, sep=None, engine="python")

    df = unit_ref.copy()
    df["firm_class"] = df["owner_agent"].map(_classify_firm)
    df["tech_group"] = df["technology"].map(TECH_GROUPS).fillna("Other")
    df["tech_strategic_role"] = df["tech_group"].map(_classify_strategic_role)

    if keep_columns is None:
        cols = ["unit_code", "firm_class", "tech_group", "tech_strategic_role"]
        for extra in ("owner_agent", "technology", "max_power_mw", "unit_type"):
            if extra in df.columns:
                cols.append(extra)
        df = df[cols]
    else:
        df = df[list(keep_columns)]

    return df
