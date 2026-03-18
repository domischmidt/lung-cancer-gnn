"""
Convert CDC Final CSV to TTL format for LUCIA ontology.

Fixes per Virginia's Arreglos document:
- Each row becomes ONE VitalStatistics instance (one year, one value)
- VitalStatistics URI includes year
- sio:SIO_000300 on Disease (not on VitalStatistics)
- Gender capitalized: "Male", "Female"
- Ethnicity preserved (Hispanic, Non-Hispanic)
- Country US_PRI already exists, only referenced

Input:  data/raw/CDC_Final.csv
Output: data/processed/graph_CDC.ttl
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
RAW = os.path.join(BASE, "data", "raw")
PROCESSED = os.path.join(BASE, "data", "processed")
os.makedirs(PROCESSED, exist_ok=True)

DISEASE_CODE = "C0242379"
DISEASE_NAME = "Malignant neoplasm of lung"


def cdc_to_ttl():
    df = pd.read_csv(os.path.join(RAW, "CDC_Final.csv"))
    print(f"Loaded {len(df)} rows from CDC_Final.csv")

    # Clean data
    df = df[df["Count"].notna()].copy()
    df["Year"] = df["Year"].astype(int)
    df["Count"] = df["Count"].astype(int)
    df["Sex"] = df["Sex"].str.strip().str.capitalize()  # "Male", "Female"
    df["age_code"] = df["age_code"].str.strip()
    df["Ethnicity"] = df["Ethnicity"].str.strip()

    # Ethnicity slug for URI (no spaces, lowercase)
    df["eth_slug"] = df["Ethnicity"].str.lower().str.replace("-", "").str.replace(" ", "")

    lines = [PREFIXES]

    # ── Disease entity with SIO_000300 links ─────────────────────────────
    # Will be added at the end after collecting all vstat URIs

    # ── People entities (unique combinations) ────────────────────────────
    people_seen = set()
    for _, row in df.iterrows():
        key = (row["age_code"], row["Sex"], row["eth_slug"])
        if key not in people_seen:
            people_seen.add(key)
            p_uri = people_uri(row["age_code"], row["Sex"], row["eth_slug"])
            lines.append(f"{p_uri} a ncit:C95553 ;")
            lines.append(f'    sem-lucia:age "{row["age_code"]}" ;')
            lines.append(f'    sem-lucia:gender "{row["Sex"]}" ;')
            lines.append(f'    sem-lucia:ethnicity "{row["eth_slug"]}" .')
            lines.append("")
    print(f"  {len(people_seen)} People entities")

    # ── Country: US_PRI already exists, only reference ───────────────────

    # ── VitalStatistics instances (one per row = one per year) ───────────
    vstat_uris = []
    count = 0
    for _, row in df.iterrows():
        year = row["Year"]
        age = row["age_code"]
        gender = row["Sex"]
        eth = row["eth_slug"]
        incidence = row["Count"]
        country = row["country_id"]

        vs = vstat_uri(country, age, gender, eth, DISEASE_CODE, year)
        vstat_uris.append(vs)

        lines.append(f"{vs} a ncit:C17258 ;")
        lines.append(f"    sio:SIO_000679 {calendar_year_uri(year)} ;")
        lines.append(f'    sem-lucia:incidence "{float(incidence)}"^^xsd:float ;')
        lines.append(f"    sio:SIO_000229 {people_uri(age, gender, eth)} ;")
        lines.append(f"    sio:SIO_000061 {country_uri(country)} .")
        lines.append("")
        count += 1

    print(f"  {count} VitalStatistics instances")

    # ── Disease entity with sio:SIO_000300 links to all VitalStatistics ──
    lines.append(f"{disease_uri(DISEASE_CODE)} a ncit:C7057 ;")
    lines.append(f'    rdfs:label "{DISEASE_NAME}" ;')
    lines.append(f'    dcterms:identifier "{DISEASE_CODE}" ;')
    for i, vs in enumerate(vstat_uris):
        comma = " ," if i < len(vstat_uris) - 1 else " ."
        if i == 0:
            lines.append(f"    sio:SIO_000300 {vs}{comma}")
        else:
            lines.append(f"        {vs}{comma}")
    lines.append("")

    out_path = os.path.join(PROCESSED, "graph_CDC.ttl")
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    print(f"  Saved: {out_path}")


if __name__ == "__main__":
    cdc_to_ttl()