import pandas as pd
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "shared"))
from ttl_utils import (
    PREFIXES, CHEMICAL_IDS, EXISTING_COUNTRIES,
    cla_uri, cla_id, source_uri, chemical_uri,
    country_uri, units_uri, frequency_uri, calendar_year_uri,
    slugify,
)

BASE = os.path.join(os.path.dirname(__file__), "..", "..")
PROCESSED = os.path.join(BASE, "data", "processed")

SOURCE = "EEA-2025"
CATEGORY = "POLLUTIONEXP"
BASE_URI = "http://medal.ctb.upm.es/projects/LUCIA/res/sem-lucia"
POP_YEAR = 2024  # Eurostat LAU reference date


def city_uri_noyear(country_code, city_name):
    """Geopolitical Region URI without year: lucia:#country/gpr/{CC}_{slug}"""
    slug = slugify(city_name)
    return f"<{BASE_URI}#country/gpr/{country_code}_{slug}>"


def city_identifier_noyear(country_code, city_name):
    """Geopolitical Region dcterms:identifier without year."""
    slug = slugify(city_name)
    return f"{country_code}_{slug}"


def pop_uri(city_name):
    """Population URI: lucia:#country/gpr/population/{slug}_{year}"""
    slug = slugify(city_name)
    return f"<{BASE_URI}#country/gpr/population/{slug}_{POP_YEAR}>"


def eea_to_ttl():
    df = pd.read_csv(os.path.join(PROCESSED, "eea_final.csv"))
    print(f"Loaded {len(df)} rows from eea_final.csv")

    lines = [PREFIXES]

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

    cities_seen = set()
    pop_count = 0

    pop_df = df[df["Population"].notna()].copy()
    pop_df["Population"] = pd.to_numeric(pop_df["Population"], errors="coerce")
    pop_df = pop_df.dropna(subset=["Population"])
    pop_lookup = {}
    for _, r in pop_df.groupby(["CountryCode", "CityName"]).first().reset_index().iterrows():
        pop_lookup[(r["CountryCode"], r["CityName"])] = int(r["Population"])

    for _, row in df.iterrows():
        cc = row["CountryCode"]
        city = row["CityName"]
        city_key = (cc, city)

        if city_key not in cities_seen:
            cities_seen.add(city_key)
            c_uri = city_uri_noyear(cc, city)
            c_ident = city_identifier_noyear(cc, city)

            has_pop = city_key in pop_lookup

            lines.append(f"{c_uri} a sio:SIO_000415 ;")
            lines.append(f'    rdfs:label "{city}" ;')
            lines.append(f'    dcterms:identifier "{c_ident}" ;')
            if has_pop:
                p_uri = pop_uri(city)
                lines.append(f"    sio:SIO_000216 {p_uri} ;")
            lines.append(f"    sio:SIO_000061 {country_uri(cc)} .")
            lines.append("")

            
            if has_pop:
                pop_val = pop_lookup[city_key]
                p_uri = pop_uri(city)
                lines.append(f"{p_uri} a sio:SIO_001061 ;")
                lines.append(f'    rdfs:label "Population of {city} ({POP_YEAR})" ;')
                lines.append(f"    sio:SIO_000679 {calendar_year_uri(POP_YEAR)} ;")
                lines.append(f'    sio:SIO_000300 "{pop_val}"^^xsd:integer .')
                lines.append("")
                pop_count += 1

    print(f"  {len(cities_seen)} City entities")
    print(f"  {pop_count} Population entities")

    count = 0
    for _, row in df.iterrows():
        cc = row["CountryCode"]
        city = row["CityName"]
        chem_name = row["ChemicalID"]
        value = row["Value"]
        year = int(row["Year"])

        chem_id = CHEMICAL_IDS.get(chem_name, f"UNKNOWN_{chem_name}")
        c_uri = city_uri_noyear(cc, city)
        identifier = cla_id(SOURCE, chem_id, cc, city, year)

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