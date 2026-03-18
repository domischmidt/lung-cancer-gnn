"""
Convert ECIS final CSV to TTL format for LUCIA ontology.

Fixes per ontology review:
- Year included in VitalStatistics URI
- sio:SIO_000300 on Disease instance (not on VitalStatistics)
- Gender capitalized: "Male", "Female"
- Existing Countries/CalendarYears only referenced, not redefined

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
)

BASE = os.path.join(os.path.dirname(__file__), "..", "..")
PROCESSED = os.path.join(BASE, "data", "processed")

ETHNICITY = "undefined"


def ecis_to_ttl():
    df = pd.read_csv(os.path.join(PROCESSED, "ecis_final.csv"))
    print(f"Loaded {len(df)} rows from ecis_final.csv")

    lines = [PREFIXES]

    disease_code = df["DiseaseCode"].iloc[0]
    disease_name = df["DiseaseName"].iloc[0]

    # ── People entities (unique combinations) ────────────────────────────
    people_seen = set()
    for _, row in df.iterrows():
        gender = row["Gender"]  # Already "Male"/"Female" from pipeline
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
    print(f"  {new_countries} new Country entities ({len(countries_seen)} total referenced)")

    # ── VitalStatistics instances ────────────────────────────────────────
    # Collect all vstat URIs for the Disease link
    vstat_uris = []
    count = 0

    for _, row in df.iterrows():
        code = row["CountryCode"]
        gender = row["Gender"]
        age = row["AgeGroup"]
        year = int(row["Year"])
        inc = row["Incidence"]
        mort = row["MortalityRate"]

        vs = vstat_uri(code, age, gender, ETHNICITY, disease_code, year)
        vstat_uris.append(vs)

        lines.append(f"{vs} a ncit:C17258 ;")
        lines.append(f"    sio:SIO_000679 {calendar_year_uri(year)} ;")
        lines.append(f'    sem-lucia:incidence "{inc}"^^xsd:float ;')
        lines.append(f'    sem-lucia:mortalityrate "{mort}"^^xsd:float ;')
        lines.append(f"    sio:SIO_000229 {people_uri(age, gender, ETHNICITY)} ;")
        lines.append(f"    sio:SIO_000061 {country_uri(code)} .")
        lines.append("")
        count += 1

    print(f"  {count} VitalStatistics instances")

    # ── Disease entity with sio:SIO_000300 links to all VitalStatistics ──
    lines.append(f"{disease_uri(disease_code)} a ncit:C7057 ;")
    lines.append(f'    rdfs:label "{disease_name}" ;')
    lines.append(f'    dcterms:identifier "{disease_code}" ;')
    # Link disease → all vital statistics
    for i, vs in enumerate(vstat_uris):
        sep = " ;" if i == 0 else ""
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