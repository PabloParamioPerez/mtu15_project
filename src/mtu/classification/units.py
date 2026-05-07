"""Unit-level classification: firm_class, tech_group, tech_strategic_role.

Replaces inline `firm_class` / `tech_group` definitions previously duplicated
across `scripts/analysis/`. Single source of truth.

## Schema produced

For each `unit_code` in the OMIE register (~3,950 units), produce three flat
fields:

- `firm_class ∈ {IB, GE, GN, HC, Fringe}`
  Big-4 dominant generation firms (IB=Iberdrola, GE=Endesa, GN=Naturgy,
  HC=EDP-España) plus a Fringe catch-all for everyone else (incl.
  Acciona, Repsol, EDP-Portugal, Engie, retailers, direct consumers, etc.).
  This is the **CNMC operador-dominante-en-generación intermediate tier**:
  firms with >10% generation share. The "lax" CNMC operador-principal tier
  (which adds ACC, REP) is descriptive-only and not produced as a separate
  field — Acciona and Repsol units are in `Fringe`.

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
    """Map OMIE `owner_agent` string → firm_class.

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
