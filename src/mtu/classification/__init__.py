"""Centralised unit and hour classification for MTU15 project analyses.

The two modules here replace inline classification code that was previously
duplicated across `scripts/analysis/` (firm_class definitions in 5+ scripts;
critical-hours hard-coded as `[7,8,16,17,18]` in 4+ scripts).

Modules:
- `units` — unit_code → (firm_class, tech_group, tech_strategic_role)
- `critical_hours` — hour → critical/flat/other under multiple definitions
"""
from mtu.classification.units import (
    classify_units,
    DOMINANT_FIRMS,
    TECH_GROUPS,
    FLEXIBLE_STRATEGIC_TECHS,
    PRICE_TAKING_TECHS,
)
from mtu.classification.critical_hours import (
    classify_hour,
    CRITICAL_HOURS_SUPPLY_RAMP,
    FLAT_HOURS,
)

__all__ = [
    "classify_units",
    "DOMINANT_FIRMS",
    "TECH_GROUPS",
    "FLEXIBLE_STRATEGIC_TECHS",
    "PRICE_TAKING_TECHS",
    "classify_hour",
    "CRITICAL_HOURS_SUPPLY_RAMP",
    "FLAT_HOURS",
]
