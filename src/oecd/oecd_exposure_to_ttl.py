"""
Convert OECD exposure final CSV to TTL format for LUCIA ontology.

Input:  data/processed/oecd_exposure_final.csv
Output: data/processed/graph_OECD.ttl

Handles multiple years, population as separate entity (per ontology),
and region-level geographic entities (SIO_000414).
"""
import pandas as pd
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "shared"))
from ttl_utils import (
    PREFIXES, CHEMICAL_IDS, EXISTING_COUNTRIES, EXISTING_CHEMICALS,
    cla_uri, cla_id, source_uri, chemical_uri, city_uri, city_identifier,
    country_uri, units_uri, frequency_uri, calendar_year_uri,
)

BASE = os.path.join(os.path.dirname(__file__), "..", "..")
PROCESSED = os.path.join(BASE, "data", "processed")

SOURCE = "OECD-2025"
CATEGORY = "POLLUTIONEXP"


def population_uri(country_code, region_name, year):
    """URI for a Population entity: lucia:#population/{CC}_{region}_{year}"""
    from ttl_utils import normalize_ascii
    import hashlib
    norm = normalize_ascii(region_name)
    short = norm[:5].replace(" ", "")
    h = hashlib.md5(norm.encode("utf-8")).hexdigest()[:4]
    return f"<http://medal.ctb.upm.es/projects/LUCIA/res/sem-lucia#population/{country_code}_{short}_{h}_{year}>"


def oecd_exposure_to_ttl():
    df = pd.read_csv(os.path.join(PROCESSED, "oecd_exposure_final.csv"))
    print(f"Loaded {len(df)} rows from oecd_exposure_final.csv")

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
                lines.append(f'    rdfs:label "{row["Country"]}" ;')
                lines.append(f'    dcterms:identifier "{cc}" .')
                lines.append("")
                new_countries += 1
    print(f"  {new_countries} new Country entities ({len(countries_seen)} total)")

    # ── Region entities (per year) + Population entities ─────────────────
    regions_seen = set()
    pop_count = 0
    for _, row in df.iterrows():
        cc = row["CountryCode"]
        region = row["RegionName"]
        year = int(row["Year"])
        pop = row.get("Population", "")

        loc_key = (cc, region, year)
        if loc_key not in regions_seen:
            regions_seen.add(loc_key)
            c_uri = city_uri(cc, region, year)
            ident = city_identifier(cc, region, year)

            # Region entity (Geographic Region per ontology)
            has_pop = (
                pd.notna(pop)
                and str(pop).strip() != ""
                and str(pop).strip() != "nan"
            )

            lines.append(f"{c_uri} a sio:SIO_000414 ;")
            lines.append(f'    rdfs:label "{region}" ;')
            lines.append(f'    dcterms:identifier "{ident}" ;')
            if has_pop:
                # Link region to population entity via has_measurement_value
                pop_uri = population_uri(cc, region, year)
                lines.append(f"    sio:SIO_000216 {pop_uri} ;")
            lines.append(f"    sio:SIO_000061 {country_uri(cc)} ;")
            lines.append(f"    sio:SIO_000679 {calendar_year_uri(year)} .")
            lines.append("")

            # Population entity (separate, per ontology)
            if has_pop:
                try:
                    pop_val = int(float(pop))
                    pop_uri = population_uri(cc, region, year)
                    lines.append(f"{pop_uri} a sio:SIO_001061 ;")
                    lines.append(f'    rdfs:label "Population of {region} ({year})" ;')
                    lines.append(f'    sio:SIO_000300 "{pop_val}"^^xsd:integer .')
                    lines.append("")
                    pop_count += 1
                except (ValueError, TypeError):
                    pass

    print(f"  {len(regions_seen)} Region entities (across {len(df['Year'].unique())} years)")
    print(f"  {pop_count} Population entities")

    # ── ChemicalLocationAssociation instances ────────────────────────────
    count = 0
    for _, row in df.iterrows():
        cc = row["CountryCode"]
        region = row["RegionName"]
        chem_name = row["Chemical"]
        value = row["Value"]
        year = int(row["Year"])

        chem_id = CHEMICAL_IDS.get(chem_name, f"UNKNOWN_{chem_name}")
        c_uri = city_uri(cc, region, year)
        identifier = cla_id(SOURCE, chem_id, cc, region, year)

        lines.append(f"{cla_uri(identifier)} a sem-lucia:ChemicalLocationAssociation ;")
        lines.append(f'    dcterms:identifier "{identifier}" ;')
        lines.append(f"    sio:SIO_000253 {source_uri(SOURCE)} ;")
        lines.append(f"    sio:SIO_000628 {chemical_uri(chem_id)} ,")
        lines.append(f"        {c_uri} ;")
        lines.append(f"    sio:SIO_000008 {units_uri('microgram-per-cubic-meter')} ,")
        lines.append(f"        {frequency_uri('Annual')} ;")
        lines.append(f"    sio:SIO_000679 {calendar_year_uri(year)} ;")
        lines.append(f'    sem-lucia:category "{CATEGORY}" ;')
        lines.append(f'    sem-lucia:value "{value}"^^xsd:decimal .')
        lines.append("")
        count += 1

    out_path = os.path.join(PROCESSED, "graph_OECD.ttl")
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    print(f"\n  {count} ChemicalLocationAssociation instances")
    print(f"  Saved: {out_path}")


if __name__ == "__main__":
    oecd_exposure_to_ttl()