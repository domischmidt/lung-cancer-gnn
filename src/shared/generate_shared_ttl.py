"""
Generate graph_shared.ttl with entities reused across EEA, ECIS, and OECD.

Only creates instances that do NOT already exist in the ontology:
- CalendarYear: 2023, 2024 (1990-2022 already exist)
- Countries: only those not in the existing 171
- Chemicals: PM10 variants (PM2.5, BaP, C6H6, NO2, O3, NOx already exist)

Output: data/processed/graph_shared.ttl
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "shared"))
from ttl_utils import (
    PREFIXES, EXISTING_CALENDAR_YEARS, EXISTING_CHEMICALS,
    CHEMICAL_LABELS, CHEMICAL_IDS,
    calendar_year_uri, chemical_uri,
)

BASE = os.path.join(os.path.dirname(__file__), "..", "..")
PROCESSED = os.path.join(BASE, "data", "processed")
os.makedirs(PROCESSED, exist_ok=True)

# New calendar years needed by our datasets
NEW_YEARS = {2023, 2024} - EXISTING_CALENDAR_YEARS

# New chemicals: only PM10 variants
NEW_CHEMICALS = {
    cid: label
    for cid, label in CHEMICAL_LABELS.items()
    if cid not in EXISTING_CHEMICALS
}


def generate_shared():
    lines = [PREFIXES]

    # ── New CalendarYear instances ────────────────────────────────────────
    lines.append("# ── New CalendarYear instances ──")
    lines.append("")
    for year in sorted(NEW_YEARS):
        lines.append(f'{calendar_year_uri(year)} a <http://snomed.info/id/277267003> ;')
        lines.append(f'    rdfs:label "{year}" ;')
        lines.append(f'    dcterms:identifier "{year}" .')
        lines.append("")
    print(f"  {len(NEW_YEARS)} new CalendarYear instances: {sorted(NEW_YEARS)}")

    # ── New Chemical instances ───────────────────────────────────────────
    lines.append("# ── New Chemical instances (PM10 variants) ──")
    lines.append("")
    for chem_id, label in sorted(NEW_CHEMICALS.items()):
        lines.append(f"{chemical_uri(chem_id)} a ncit:C48807 ;")
        lines.append(f'    rdfs:label "{label}" ;')
        lines.append(f'    dcterms:identifier "{chem_id}" .')
        lines.append("")
    print(f"  {len(NEW_CHEMICALS)} new Chemical instances: {sorted(NEW_CHEMICALS.keys())}")

    out_path = os.path.join(PROCESSED, "graph_shared.ttl")
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    print(f"  Saved: {out_path}")


if __name__ == "__main__":
    generate_shared()