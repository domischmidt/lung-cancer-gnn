"""
Convert ECIS final CSVs to TTL format for LUCIA ontology.

Combines:
  - data/raw/ecis_final_2022.csv  (old format, 256 rows)
  - data/processed/ecis_final.csv (2024 format, 324 rows)
Output: data/processed/graph_ECIS.ttl

Fixes per ontology review:
- Year included in VitalStatistics URI
- sio:SIO_000300 on Disease instance, linking to all VitalStatistics
- Gender capitalized: "Male", "Female"
- Existing Countries/CalendarYears only referenced, not redefined
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

ETHNICITY = "undefined"


def load_2022():
    """Load old 2022 ECIS data and normalize to common format."""
    path = os.path.join(RAW, "ecis_final_2022.csv")
    if not os.path.exists(path):
        print("  [WARN] ecis_final_2022.csv not found – skipping 2022 data.")
        return pd.DataFrame()

    df = pd.read_csv(path)
    df["gender"] = df["gender"].str.strip().str.capitalize()
    df["age"] = df["age"].str.strip()
    df["ethnicity"] = "undefined"
    df["year"] = 2022

    # Rename to common columns
    out = df.rename(columns={
        "country_id": "CountryCode",
        "Country": "CountryName",
        "gender": "Gender",
        "age": "AgeGroup",
        "Incidence": "Incidence",
        "Mortality": "MortalityRate",
        "year": "Year",
    })[["CountryName", "CountryCode", "Gender", "AgeGroup", "Incidence", "MortalityRate", "Year"]]

    print(f"  2022: {len(out)} rows, {out['CountryCode'].nunique()} countries")
    return out


def load_2024():
    """Load 2024 ECIS data (from ecis_pipeline.py output)."""
    path = os.path.join(PROCESSED, "ecis_final.csv")
    if not os.path.exists(path):
        print("  [WARN] ecis_final.csv not found – skipping 2024 data.")
        return pd.DataFrame()

    df = pd.read_csv(path)
    df = df.rename(columns={"Country": "CountryName"})
    print(f"  2024: {len(df)} rows, {df['CountryCode'].nunique()} countries")
    return df


def ecis_to_ttl():
    print("Loading ECIS data...")
    df_2022 = load_2022()
    df_2024 = load_2024()

    df = pd.concat([df_2022, df_2024], ignore_index=True)
    print(f"  Combined: {len(df)} rows, years {sorted(df['Year'].unique())}")

    lines = [PREFIXES]

    disease_code = "C0242379"
    disease_name = "Malignant neoplasm of lung"

    # ── People entities (unique combinations) ────────────────────────────
    people_seen = set()
    for _, row in df.iterrows():
        gender = row["Gender"]
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
                lines.append(f'    rdfs:label "{row["CountryName"]}" ;')
                lines.append(f'    dcterms:identifier "{code}" .')
                lines.append("")
                new_countries += 1
    print(f"  {new_countries} new Country entities ({len(countries_seen)} total referenced)")

    # ── VitalStatistics instances ────────────────────────────────────────
    vstat_uris = []
    count = 0

    for _, row in df.iterrows():
        code = row["CountryCode"]
        gender = row["Gender"]
        age = row["AgeGroup"]
        year = int(row["Year"])
        inc = row["Incidence"]
        mort = row.get("MortalityRate", None)

        vs = vstat_uri(code, age, gender, ETHNICITY, disease_code, year)
        vstat_uris.append(vs)

        lines.append(f"{vs} a ncit:C17258 ;")
        lines.append(f"    sio:SIO_000679 {calendar_year_uri(year)} ;")
        if pd.notna(inc):
            lines.append(f'    sem-lucia:incidence "{float(inc)}"^^xsd:float ;')
        if pd.notna(mort):
            lines.append(f'    sem-lucia:mortalityrate "{float(mort)}"^^xsd:float ;')
        lines.append(f"    sio:SIO_000229 {people_uri(age, gender, ETHNICITY)} ;")
        lines.append(f"    sio:SIO_000061 {country_uri(code)} .")
        lines.append("")
        count += 1

    print(f"  {count} VitalStatistics instances")

    # ── Disease entity with sio:SIO_000300 links to all VitalStatistics ──
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