"""
Convert EEA final CSV to TTL format for LUCIA ontology.

Fixes per ontology review:
- Source entity NOT redefined (already exists)
- Correct UMLS chemical IDs; existing chemicals only referenced
- City type: sio:SIO_000415 (Geopolitical Region)
- City URI includes year, has CalendarYear link
- Population as separate entity (sio:SIO_001061) per ontology
- Existing Countries only referenced, not redefined

Input:  data/processed/eea_final.csv
Output: data/processed/graph_EEA.ttl
"""
import pandas as pd
import hashlib
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "shared"))
from ttl_utils import (
    PREFIXES, CHEMICAL_IDS, EXISTING_COUNTRIES, EXISTING_CHEMICALS,
    cla_uri, cla_id, source_uri, chemical_uri, city_uri, city_identifier,
    country_uri, units_uri, frequency_uri, calendar_year_uri,
    normalize_ascii,
)

BASE = os.path.join(os.path.dirname(__file__), "..", "..")
PROCESSED = os.path.join(BASE, "data", "processed")

SOURCE = "EEA-2025"
CATEGORY = "POLLUTIONEXP"


def population_uri(country_code, city_name, year):
    """URI for a Population entity: lucia:#population/{CC}_{slug}_{hash}_{year}"""
    norm = normalize_ascii(city_name)
    short = norm[:5].replace(" ", "")
    h = hashlib.md5(norm.encode("utf-8")).hexdigest()[:4]
    return f"<http://medal.ctb.upm.es/projects/LUCIA/res/sem-lucia#population/{country_code}_{short}_{h}_{year}>"


def eea_to_ttl():
    df = pd.read_csv(os.path.join(PROCESSED, "eea_final.csv"))
    print(f"Loaded {len(df)} rows from eea_final.csv")

    lines = [PREFIXES]

    # ── Source: only reference, do NOT redefine ──────────────────────────

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

    # ── City entities (with year) + Population entities ──────────────────
    cities_seen = set()
    pop_count = 0
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

            has_pop = (
                pd.notna(pop)
                and str(pop).strip() != ""
                and str(pop).strip() != "nan"
            )

            lines.append(f"{c_uri} a sio:SIO_000415 ;")
            lines.append(f'    rdfs:label "{city}" ;')
            lines.append(f'    dcterms:identifier "{ident}" ;')
            if has_pop:
                pop_u = population_uri(cc, city, year)
                lines.append(f"    sio:SIO_000216 {pop_u} ;")
            lines.append(f"    sio:SIO_000061 {country_uri(cc)} ;")
            lines.append(f"    sio:SIO_000679 {calendar_year_uri(year)} .")
            lines.append("")

            # Population entity (separate, per ontology)
            if has_pop:
                try:
                    pop_val = int(float(pop))
                    pop_u = population_uri(cc, city, year)
                    lines.append(f"{pop_u} a sio:SIO_001061 ;")
                    lines.append(f'    rdfs:label "Population of {city} ({year})" ;')
                    lines.append(f'    sio:SIO_000300 "{pop_val}"^^xsd:integer .')
                    lines.append("")
                    pop_count += 1
                except (ValueError, TypeError):
                    pass

    print(f"  {len(cities_seen)} City entities")
    print(f"  {pop_count} Population entities")

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
