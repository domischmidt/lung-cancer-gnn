"""
Convert ECIS final CSV to TTL format for LUCIA ontology.

Supports:
- Historical data (registry-level, 1953-2023, Age-specific Rates)
- 2024 estimates (country-level, Incidence + Mortality)
- Registry included in VitalStatistics URI for uniqueness

Input:  data/processed/ecis_final.csv
Output: data/processed/graph_ECIS.ttl
"""
import pandas as pd
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "shared"))
from ttl_utils import (
    PREFIXES, EXISTING_COUNTRIES, EXISTING_CALENDAR_YEARS,
    vstat_uri, calendar_year_uri, disease_uri, people_uri, country_uri,
    slugify,
)

BASE = os.path.join(os.path.dirname(__file__), "..", "..")
PROCESSED = os.path.join(BASE, "data", "processed")

ETHNICITY = "undefined"
BASE_URI = "http://medal.ctb.upm.es/projects/LUCIA/res/sem-lucia"


def vstat_uri_registry(country_code, age, gender, ethnicity, disease, year, registry):
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

    # ── VitalStatistics instances ────────────────────────────────────────
    vstat_uris = []
    count = 0

    for _, row in df.iterrows():
        code = row["CountryCode"]
        gender = str(row["Gender"]).strip().capitalize()
        age = row["AgeGroup"]
        year = int(row["Year"])
        registry = str(row["Registry"]).strip()
        inc = row.get("Incidence", None)
        mort = row.get("MortalityRate", None)

        has_inc = pd.notna(inc)
        has_mort = pd.notna(mort)
        if not has_inc and not has_mort:
            continue

        vs = vstat_uri_registry(code, age, gender, ETHNICITY, disease_code, year, registry)
        vstat_uris.append(vs)

        lines.append(f"{vs} a ncit:C17258 ;")
        lines.append(f"    sio:SIO_000679 {calendar_year_uri(year)} ;")
        if has_inc:
            lines.append(f'    sem-lucia:incidence "{float(inc)}"^^xsd:float ;')
        if has_mort:
            lines.append(f'    sem-lucia:mortalityrate "{float(mort)}"^^xsd:float ;')
        lines.append(f"    sio:SIO_000229 {people_uri(age, gender, ETHNICITY)} ;")
        lines.append(f"    sio:SIO_000061 {country_uri(code)} .")
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