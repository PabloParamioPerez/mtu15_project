"""Unit tests for the centralized firm classification module.

Verifies that the rules in `src/mtu/classification/units.py` correctly handle
the four known-tricky cases discovered in the 2026-05-11 audit:

1. Joint-owned nuclear (ALZ1 owned by IB 52.7%, GE 36.0%, GN 11.3%).
2. REPSOL SERVICIOS RENOVABLES (REPRESENTANTE aggregator, must be excluded).
3. ENGIE GLOBAL MARKETS, BELGIAN BRANCH (Belgian trading desk, must be excluded).
4. IBERENERGIA, S.A. (legacy JV vehicle holding EDP-Spain's 15.5% Trillo stake).

Run:  uv run pytest tests/test_classification.py -v
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import pytest

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "src"))

from mtu.classification.units import (  # noqa: E402
    FIRM_RULES_BROAD,
    FIRM_RULES_SHORT,
    OWNER_EXCLUDE,
    PLACEBO_PARENTS_BROAD,
    PLACEBO_PARENTS_SHORT,
    TREATMENT_PARENTS_BROAD,
    TREATMENT_PARENTS_SHORT,
    firm_unit_panel,
    parent_of,
)


# ----------------------------------------------------------------------------
# parent_of()
# ----------------------------------------------------------------------------

class TestParentOfShort:
    """parent_of() under the 2-letter scheme used in DiD scripts."""

    def test_iberdrola(self):
        assert parent_of("IBERDROLA ENERGÍA ESPAÑA S..A.") == "IB"
        assert parent_of("Iberdrola Clientes Portugal, Unipessoal, LDA") == "IB"

    def test_endesa(self):
        assert parent_of("ENDESA GENERACIÓN, S.A.") == "GE"
        assert parent_of("Endesa Energía, S.A.U.") == "GE"
        assert parent_of("Endesa Energía Renovable, S.L.U.") == "GE"

    def test_naturgy_modern_name(self):
        assert parent_of("NATURGY IBERIA S.A.") == "GN"

    def test_naturgy_legacy_gas_natural(self):
        # Most Naturgy generation is registered under the legacy handle.
        assert parent_of("GAS NATURAL COMERCIALIZADORA") == "GN"

    def test_edp_spain(self):
        assert parent_of("EDP ESPAÑA, S.A.U. (GENERACIÓN)") == "HC"
        assert parent_of("EDP CLIENTES, S.A.U.") == "HC"
        assert parent_of("EDP COMERCIAL COMERCIALIZACAO DE ENERGIA SA") == "HC"

    def test_edp_portugal_is_distinct(self):
        assert parent_of("EDP GEM PORTUGAL S.A.") == "EDP-PT"

    def test_iberenergia_folds_into_edp_spain(self):
        # IBERENERGIA, S.A. is the JV vehicle holding EDP-Spain's 15.5% Trillo
        # nuclear stake. It must NOT fall into Iberdrola despite the "IBER"
        # prefix; the matching rule explicitly resolves this.
        assert parent_of("IBERENERGIA,S.A.") == "HC"
        assert parent_of("IBERENERGIA, S.A.") == "HC"

    def test_repsol_main_arms(self):
        assert parent_of("REPSOL GENERACIÓN ELÉCTRICA, S.A") == "Repsol"
        assert parent_of("REPSOL FUELS SA") == "Repsol"
        assert parent_of("Repsol Comercializadora de Electricidad y Gas SLU") == "Repsol"

    def test_repsol_servicios_renovables_excluded(self):
        # REPRESENTANTE aggregator: ~42 third-party renewables, not Repsol-owned.
        assert parent_of("REPSOL SERVICIOS RENOVABLES, S.A.") is None

    def test_engie_main_arms(self):
        assert parent_of("ENGIE ESPAÑA,S.L.U") == "Engie"
        assert parent_of("ENGIE CARTAGENA S.L") == "Engie"
        assert parent_of("ENGIE CASTELNOU") == "Engie"

    def test_engie_global_markets_excluded(self):
        # Belgian trading desk, not Spanish operations.
        assert parent_of("ENGIE GLOBAL MARKETS, BELGIAN BRANCH") is None

    def test_totalenergies(self):
        assert parent_of("TOTALENERGIES GAS AND POWER LIMITED") == "TotalEnergies"

    def test_moeve(self):
        assert parent_of("MOEVE GAS AND POWER, S.A.U") == "Moeve"
        # Legacy Cepsa name (Moeve = formerly Cepsa).
        assert parent_of("CEPSA GAS Y ELECTRICIDAD S.A.-SUCURSAL EM PORTUGAL") == "Moeve"

    def test_outside_firm_set(self):
        assert parent_of("ACCIONA GREEN ENERGY DEVELOPMENTS") is None
        assert parent_of("IGNIS ENERGIA S.L.") is None
        assert parent_of("GALP ENERGÍA ESPAÑA, S.A.U.") is None
        assert parent_of("ENEL GREEN POWER ESPAÑA SL (ACT: COM RE)") is None

    def test_none_and_empty(self):
        assert parent_of(None) is None
        assert parent_of("") is None
        assert parent_of("   ") is None
        assert parent_of(float("nan")) is None  # pandas NaN should not crash

    def test_whitespace_stripped(self):
        assert parent_of("  IBERDROLA ENERGÍA ESPAÑA S..A.  ") == "IB"


class TestParentOfBroad:
    """parent_of() under the full-name scheme used in descriptive tables."""

    def test_iberdrola(self):
        assert parent_of("IBERDROLA ENERGÍA ESPAÑA S..A.", scheme="broad") == "Iberdrola"

    def test_iberenergia_under_broad(self):
        assert parent_of("IBERENERGIA, S.A.", scheme="broad") == "EDP-Spain"

    def test_engie_full_name(self):
        assert parent_of("ENGIE ESPAÑA,S.L.U", scheme="broad") == "Engie España"

    def test_exclusions_apply(self):
        assert parent_of("REPSOL SERVICIOS RENOVABLES, S.A.", scheme="broad") is None
        assert parent_of("ENGIE GLOBAL MARKETS, BELGIAN BRANCH", scheme="broad") is None


# ----------------------------------------------------------------------------
# firm_unit_panel() — joint-ownership handling
# ----------------------------------------------------------------------------

@pytest.fixture(scope="module")
def panel_all_owners():
    return firm_unit_panel(scheme="short", mode="all_owners")


@pytest.fixture(scope="module")
def panel_primary_owner():
    return firm_unit_panel(scheme="short", mode="primary_owner")


class TestPanelAllOwners:
    """In 'all_owners' mode, joint-owned units appear once per stakeholder."""

    def test_alz1_has_three_rows(self, panel_all_owners):
        rows = panel_all_owners[panel_all_owners["unit_code"] == "ALZ1"]
        assert len(rows) == 3, "Almaraz 1 has 3 thesis-firm stakeholders"
        parents = set(rows["parent"])
        assert parents == {"IB", "GE", "GN"}

    def test_alz1_shares_sum_close_to_full(self, panel_all_owners):
        rows = panel_all_owners[panel_all_owners["unit_code"] == "ALZ1"]
        # IB 52.687% + GE 36.021% + GN 11.292% = 100.000%
        assert abs(rows["share"].sum() - 1.0) < 1e-3

    def test_trl1_includes_edp_spain_via_iberenergia(self, panel_all_owners):
        # Trillo ownership: IB 49% + GN 34.5% + IBERENERGIA→HC 15.5% + GE 1%
        rows = panel_all_owners[panel_all_owners["unit_code"] == "TRL1"]
        assert set(rows["parent"]) == {"IB", "GN", "HC", "GE"}

        hc_row = rows[rows["parent"] == "HC"]
        assert abs(hc_row["share"].iloc[0] - 0.155) < 1e-3, \
            "EDP-Spain's Trillo stake comes through IBERENERGIA at 15.5%"

    def test_cofrentes_single_owner(self, panel_all_owners):
        # Cofrentes is 100% Iberdrola, no joint ownership.
        rows = panel_all_owners[panel_all_owners["unit_code"] == "COF1"]
        assert len(rows) == 1
        assert rows["parent"].iloc[0] == "IB"
        assert abs(rows["share"].iloc[0] - 1.0) < 1e-9

    def test_no_repsol_renovables_units(self, panel_all_owners):
        # The exclusion should remove all units owned by REPSB.
        owners = panel_all_owners["owner_agent"].str.strip().str.upper().unique()
        assert "REPSOL SERVICIOS RENOVABLES, S.A." not in owners

    def test_no_engie_global_markets(self, panel_all_owners):
        owners = panel_all_owners["owner_agent"].str.strip().str.upper().unique()
        assert "ENGIE GLOBAL MARKETS, BELGIAN BRANCH" not in owners

    def test_all_parents_in_thesis_set(self, panel_all_owners):
        valid = TREATMENT_PARENTS_SHORT | PLACEBO_PARENTS_SHORT
        seen = set(panel_all_owners["parent"])
        assert seen.issubset(valid), f"Unexpected parents: {seen - valid}"


class TestPanelPrimaryOwner:
    """In 'primary_owner' mode, each unit_code appears exactly once."""

    def test_unique_unit_codes(self, panel_primary_owner):
        assert panel_primary_owner["unit_code"].is_unique

    def test_alz1_goes_to_iberdrola(self, panel_primary_owner):
        # Iberdrola holds 52.687% — the largest stake.
        rows = panel_primary_owner[panel_primary_owner["unit_code"] == "ALZ1"]
        assert len(rows) == 1
        assert rows["parent"].iloc[0] == "IB"

    def test_asc2_goes_to_endesa(self, panel_primary_owner):
        # Ascó 2 is Endesa 85%, Iberdrola 15%.
        rows = panel_primary_owner[panel_primary_owner["unit_code"] == "ASC2"]
        assert rows["parent"].iloc[0] == "GE"

    def test_trl1_goes_to_iberdrola(self, panel_primary_owner):
        # Trillo: IB 49% > GN 34.5% > HC 15.5% > GE 1%.
        rows = panel_primary_owner[panel_primary_owner["unit_code"] == "TRL1"]
        assert rows["parent"].iloc[0] == "IB"

    def test_van2_goes_to_endesa(self, panel_primary_owner):
        # Vandellós II: Endesa 72%, Iberdrola 28%.
        rows = panel_primary_owner[panel_primary_owner["unit_code"] == "VAN2"]
        assert rows["parent"].iloc[0] == "GE"

    def test_share_is_one(self, panel_primary_owner):
        # In primary_owner mode the unit is fully attributed.
        assert (panel_primary_owner["share"] == 1.0).all()


# ----------------------------------------------------------------------------
# Constants integrity
# ----------------------------------------------------------------------------

class TestConstants:
    def test_short_and_broad_rules_have_same_keywords(self):
        short_keys = [k for k, _ in FIRM_RULES_SHORT]
        broad_keys = [k for k, _ in FIRM_RULES_BROAD]
        assert short_keys == broad_keys, \
            "Short and broad rules must use identical keyword ordering"

    def test_partition_blocks_disjoint(self):
        assert TREATMENT_PARENTS_SHORT.isdisjoint(PLACEBO_PARENTS_SHORT)
        assert TREATMENT_PARENTS_BROAD.isdisjoint(PLACEBO_PARENTS_BROAD)

    def test_owner_exclude_is_uppercased(self):
        # The OWNER_EXCLUDE set is matched against the uppercased+stripped
        # owner_agent string. Each entry must already be uppercase.
        for s in OWNER_EXCLUDE:
            assert s == s.upper().strip(), f"{s!r} not uppercase/stripped"
