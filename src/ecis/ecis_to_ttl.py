"""
Convert ECIS final CSV to TTL format for LUCIA ontology.

Structure per ontology:
- Regional registries → Geopolitical Region (SIO_000415) → is located in → Country
- National registries → VitalStats links directly to Country
- VitalStats → is located in → Region or Country
- Disease → SIO_000300 → all VitalStatistics

Input:  data/processed/ecis_final.csv
Output: data/processed/graph_ECIS.ttl
"""
import pandas as pd
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "shared"))
from ttl_utils import (
    PREFIXES, EXISTING_COUNTRIES, EXISTING_CALENDAR_YEARS,
    calendar_year_uri, disease_uri, people_uri, country_uri,
    slugify,
)

BASE = os.path.join(os.path.dirname(__file__), "..", "..")
PROCESSED = os.path.join(BASE, "data", "processed")

ETHNICITY = "undefined"
BASE_URI = "http://medal.ctb.upm.es/projects/LUCIA/res/sem-lucia"


def registry_uri(country_code, registry_name):
    """Geopolitical Region URI for a registry: country/gpr/{CC}_{slug}"""
    slug = slugify(registry_name)
    return f"<{BASE_URI}#country/gpr/{country_code}_{slug}>"


def vstat_uri(country_code, age, gender, ethnicity, disease, year, registry):
    """VitalStatistics URI including registry for uniqueness."""
    reg_slug = slugify(registry)
    ag = age.replace("+", "%2B")
    return f"<{BASE_URI}#vitalstatistics/vstat_{disease}_{country_code}_{reg_slug}_{ag}_{gender}_{ethnicity}_{year}>"


def ecis_to_ttl():
    df = pd.read_csv(os.path.join(PROCESSED, "ecis_final.csv"))
    print(f"Loaded {len(df)} rows from ecis_final.csv")

    lines = [PREFIXES]

    disease_code = df["DiseaseCode"].iloc[0]
    disease_name = df["DiseaseName"].iloc[0]

    # ── People entities ──────────────────────────────────────────────────
    people_seen = set()
    for _, row in df.iterrows():
        gender = str(row["Gender"]).strip().capitalize()
        key = (row["AgeGroup"], gender)
        if key not in people_seen:
            people_seen.add(key)
            p_uri = people_uri(row["AgeGroup"], gender, ETHNICITY)
            lines.append(f"{p_uri} a ncit:C95553 ;")
            lines.append(f'    sem-lucia:age "{row["AgeGroup"]}" ;')
            lines.append(f'    sem-lucia:gender "{gender}" ;')
            lines.append(f'    sem-lucia:ethnicity "{ETHNICITY}" .')
            lines.append("")
    print(f"  {len(people_seen)} People entities")

    # ── Country entities (only NEW ones) ─────────────────────────────────
    new_countries = 0
    countries_seen = set()
    for _, row in df.iterrows():
        code = row["CountryCode"]
        if code not in countries_seen:
            countries_seen.add(code)
            if code not in EXISTING_COUNTRIES:
                lines.append(f"{country_uri(code)} a ncit:C25464 ;")
                lines.append(f'    rdfs:label "{row["Country"]}" ;')
                lines.append(f'    dcterms:identifier "{code}" .')
                lines.append("")
                new_countries += 1
    print(f"  {new_countries} new Country entities ({len(countries_seen)} total)")

    # ── Registry/Region entities (only for regional registries) ──────────
    registries_seen = set()
    registry_count = 0
    for _, row in df.iterrows():
        code = row["CountryCode"]
        registry = str(row["Registry"]).strip()
        country = str(row["Country"]).strip()
        is_national = (registry == country)

        if not is_national:
            reg_key = (code, registry)
            if reg_key not in registries_seen:
                registries_seen.add(reg_key)
                r_uri = registry_uri(code, registry)
                r_slug = slugify(registry)
                lines.append(f"{r_uri} a sio:SIO_000415 ;")
                lines.append(f'    rdfs:label "{registry}" ;')
                lines.append(f'    dcterms:identifier "{code}_{r_slug}" ;')
                lines.append(f"    sio:SIO_000061 {country_uri(code)} .")
                lines.append("")
                registry_count += 1
    print(f"  {registry_count} Registry/Region entities")

    # ── VitalStatistics instances ────────────────────────────────────────
    vstat_uris = []
    count = 0

    for _, row in df.iterrows():
        code = row["CountryCode"]
        gender = str(row["Gender"]).strip().capitalize()
        age = row["AgeGroup"]
        year = int(row["Year"])
        registry = str(row["Registry"]).strip()
        country = str(row["Country"]).strip()
        inc = row.get("Incidence", None)
        mort = row.get("MortalityRate", None)

        has_inc = pd.notna(inc)
        has_mort = pd.notna(mort)
        if not has_inc and not has_mort:
            continue

        is_national = (registry == country)

        vs = vstat_uri(code, age, gender, ETHNICITY, disease_code, year, registry)
        vstat_uris.append(vs)

        # Location: regional → Registry entity, national → Country directly
        if is_national:
            location_uri = country_uri(code)
        else:
            location_uri = registry_uri(code, registry)

        lines.append(f"{vs} a ncit:C17258 ;")
        lines.append(f"    sio:SIO_000679 {calendar_year_uri(year)} ;")
        if has_inc:
            lines.append(f'    sem-lucia:incidence "{float(inc)}"^^xsd:float ;')
        if has_mort:
            lines.append(f'    sem-lucia:mortalityrate "{float(mort)}"^^xsd:float ;')
        lines.append(f"    sio:SIO_000229 {people_uri(age, gender, ETHNICITY)} ;")
        lines.append(f"    sio:SIO_000061 {location_uri} .")
        lines.append("")
        count += 1

    print(f"  {count} VitalStatistics instances")

    # ── Disease entity with sio:SIO_000300 links ─────────────────────────
    lines.append(f"{disease_uri(disease_code)} a ncit:C7057 ;")
    lines.append(f'    rdfs:label "{disease_name}" ;')
    lines.append(f'    dcterms:identifier "{disease_code}" ;')
    for i, vs in enumerate(vstat_uris):
        comma = " ," if i < len(vstat_uris) - 1 else " ."
        if i == 0:
            lines.append(f"    sio:SIO_000300 {vs}{comma}")
        else:
            lines.append(f"        {vs}{comma}")
    lines.append("")

    out_path = os.path.join(PROCESSED, "graph_ECIS.ttl")
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    print(f"  Saved: {out_path}")


if __name__ == "__main__":
    ecis_to_ttl()