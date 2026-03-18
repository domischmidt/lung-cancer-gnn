"""
Convert EEA final CSV to TTL format for LUCIA ontology.

Fixes per ontology review:
- Source entity NOT redefined (already exists)
- Correct UMLS chemical IDs; existing chemicals only referenced
- City type: ncit:C0008848 (not C61066)
- City URI includes year, has population + CalendarYear link
- Existing Countries only referenced, not redefined

Input:  data/processed/eea_final.csv
Output: data/processed/graph_EEA.ttl
"""
import pandas as pd
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "shared"))
from ttl_utils import (
    PREFIXES, CHEMICAL_IDS, EXISTING_COUNTRIES, EXISTING_CHEMICALS,
    cla_uri, cla_id, source_uri, chemical_uri, city_uri, city_identifier,
    country_uri, units_uri, calendar_year_uri,
)

BASE = os.path.join(os.path.dirname(__file__), "..", "..")
PROCESSED = os.path.join(BASE, "data", "processed")

SOURCE = "EEA-2025"
CATEGORY = "POLLUTIONEXP"


def eea_to_ttl():
    df = pd.read_csv(os.path.join(PROCESSED, "eea_final.csv"))
    print(f"Loaded {len(df)} rows from eea_final.csv")

    lines = [PREFIXES]

    # ── Source: only reference, do NOT redefine ──────────────────────────
    # (EEA-2025 already exists in ontology)

    # ── Country entities (only NEW ones) ─────────────────────────────────
    new_countries = 0
    countries_seen = set()
    for _, row in df.iterrows():
        cc = row["CountryCode"]
        if cc not in countries_seen:
            countries_seen.add(cc)
            if cc not in EXISTING_COUNTRIES:
                lines.append(f"{country_uri(cc)} a ncit:C25464 ;")
                lines.append(f'    rdfs:label "{row["CountryName"]}" ;')
                lines.append(f'    dcterms:identifier "{cc}" .')
                lines.append("")
                new_countries += 1
    print(f"  {new_countries} new Country entities ({len(countries_seen)} total)")

    # ── City entities (with year, population, CalendarYear link) ─────────
    cities_seen = set()
    for _, row in df.iterrows():
        cc = row["CountryCode"]
        city = row["CityName"]
        year = int(row["Year"])
        pop = row.get("Population", "")

        loc_key = (cc, city, year)
        if loc_key not in cities_seen:
            cities_seen.add(loc_key)
            c_uri = city_uri(cc, city, year)
            ident = city_identifier(cc, city, year)

            lines.append(f"{c_uri} a sio:SIO_000415 ;")
            lines.append(f'    rdfs:label "{city}" ;')
            lines.append(f'    dcterms:identifier "{ident}" ;')
            if pd.notna(pop) and str(pop).strip():
                lines.append(f'    sem-lucia:population "{int(pop)}"^^xsd:integer ;')
            lines.append(f"    sio:SIO_000061 {country_uri(cc)} ;")
            lines.append(f"    sio:SIO_000679 {calendar_year_uri(year)} .")
            lines.append("")
    print(f"  {len(cities_seen)} City entities")

    # ── ChemicalLocationAssociation instances ────────────────────────────
    count = 0
    for _, row in df.iterrows():
        cc = row["CountryCode"]
        city = row["CityName"]
        chem_name = row["ChemicalID"]
        value = row["Value"]
        year = int(row["Year"])

        chem_id = CHEMICAL_IDS.get(chem_name, f"UNKNOWN_{chem_name}")
        identifier = cla_id(SOURCE, chem_id, cc, city, year)
        c_uri = city_uri(cc, city, year)

        lines.append(f"{cla_uri(identifier)} a sem-lucia:ChemicalLocationAssociation ;")
        lines.append(f'    dcterms:identifier "{identifier}" ;')
        lines.append(f"    sio:SIO_000253 {source_uri(SOURCE)} ;")
        lines.append(f"    sio:SIO_000628 {chemical_uri(chem_id)} ,")
        lines.append(f"        {c_uri} ;")
        lines.append(f"    sio:SIO_000008 {units_uri('microgram-per-cubic-meter')} ;")
        lines.append(f"    sio:SIO_000679 {calendar_year_uri(year)} ;")
        lines.append(f'    sem-lucia:category "{CATEGORY}" ;')
        lines.append(f'    sem-lucia:value "{value}"^^xsd:decimal .')
        lines.append("")
        count += 1

    out_path = os.path.join(PROCESSED, "graph_EEA.ttl")
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    print(f"\n  {count} ChemicalLocationAssociation instances")
    print(f"  Saved: {out_path}")


if __name__ == "__main__":
    eea_to_ttl()