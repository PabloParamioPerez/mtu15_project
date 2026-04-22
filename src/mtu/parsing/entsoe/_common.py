"""Shared XML helpers for ENTSO-E parsers.

The balancing (A84, A85, A86) and publication (A69) document families
share the same `TimeSeries > Period > Point` skeleton, so tag-stripping,
timestamp parsing, and resolution-code mapping live here.
"""

from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from datetime import datetime, timezone

_LOCAL = re.compile(r"^\{[^}]+\}")


def local_tag(el: ET.Element) -> str:
    return _LOCAL.sub("", el.tag)


def find_child(el: ET.Element, name: str) -> ET.Element | None:
    for child in el:
        if local_tag(child) == name:
            return child
    return None


def find_children(el: ET.Element, name: str) -> list[ET.Element]:
    return [c for c in el if local_tag(c) == name]


def text_of(el: ET.Element | None) -> str:
    if el is None or el.text is None:
        return ""
    return el.text.strip()


def parse_iso_utc(ts: str) -> datetime:
    """ENTSO-E emits timestamps like `2025-03-19T00:00Z` (always UTC)."""
    t = ts.rstrip("Z")
    dt = datetime.fromisoformat(t)
    return dt.replace(tzinfo=timezone.utc)


_RESOLUTION_MIN = {
    "PT15M": 15,
    "PT30M": 30,
    "PT60M": 60,
    "PT1H": 60,
}


def resolution_minutes(code: str) -> int:
    if code not in _RESOLUTION_MIN:
        raise ValueError(f"Unsupported resolution code: {code!r}")
    return _RESOLUTION_MIN[code]
