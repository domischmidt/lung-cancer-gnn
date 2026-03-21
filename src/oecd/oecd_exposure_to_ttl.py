"""
Convert OECD exposure final CSV to TTL format for LUCIA ontology.

Per Virginia's review:
- Geographic Region: 444 instances, NO year in URI
  URI: lucia:#country/gr/<CountryCode>_<GRName>
- Population: ≤444 instances, one per region (latest year only)
  URI: lucia:#country/gr/population/<GRName>_<Year>
- CLA: 14,001 instances, links to year-free Region, has own CalendarYear
- Country: only new ones (not in EXISTING_COUNTRIES)

Input:  data/processed/oecd_exposure_final.csv
Output: data/processed/graph_OECD.ttl
"""
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

SOURCE = "OECD-2025"
CATEGORY = "POLLUTIONEXP"
BASE_URI = "http://medal.ctb.upm.es/projects/LUCIA/res/sem-lucia"


def gr_uri(country_code, region_name):
    """Geographic Region URI (no year): lucia:#country/gr/{CC}_{slug}"""
    slug = slugify(region_name)
    return f"<{BASE_URI}#country/gr/{country_code}_{slug}>"


def gr_identifier(country_code, region_name):
    """Geographic Region dcterms:identifier."""
    slug = slugify(region_name)
    return f"{country_code}_{slug}"


def pop_uri(region_name, year):
    """Population URI: lucia:#country/gr/population/{slug}_{year}"""
    slug = slugify(region_name)
    return f"<{BASE_URI}#country/gr/population/{slug}_{year}>"


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

    # ── Geographic Region entities (444, no year in URI) ─────────────────
    # ── Population entities (≤444, latest year only) ─────────────────────
    regions_seen = set()
    pop_count = 0

    # Find latest population per region
    pop_df = df[df["Population"].notna() & (df["Population"] != "")].copy()
    pop_df["Population"] = pd.to_numeric(pop_df["Population"], errors="coerce")
    pop_df = pop_df.dropna(subset=["Population"])
    latest_pop = (
        pop_df.sort_values("Year")
        .groupby(["CountryCode", "RegionName"])
        .last()
        .reset_index()[["CountryCode", "RegionName", "Population", "Year"]]
    )
    pop_lookup = {}
    for _, r in latest_pop.iterrows():
        pop_lookup[(r["CountryCode"], r["RegionName"])] = (int(r["Population"]), int(r["Year"]))

    for _, row in df.iterrows():
        cc = row["CountryCode"]
        region = row["RegionName"]
        region_key = (cc, region)

        if region_key not in regions_seen:
            regions_seen.add(region_key)
            r_uri = gr_uri(cc, region)
            r_ident = gr_identifier(cc, region)

            has_pop = region_key in pop_lookup

            lines.append(f"{r_uri} a sio:SIO_000414 ;")
            lines.append(f'    rdfs:label "{region}" ;')
            lines.append(f'    dcterms:identifier "{r_ident}" ;')
            if has_pop:
                pop_val, pop_year = pop_lookup[region_key]
                p_uri = pop_uri(region, pop_year)
                lines.append(f"    sio:SIO_000216 {p_uri} ;")
            lines.append(f"    sio:SIO_000061 {country_uri(cc)} .")
            lines.append("")

            # Population entity (latest year only)
            if has_pop:
                pop_val, pop_year = pop_lookup[region_key]
                p_uri = pop_uri(region, pop_year)
                lines.append(f"{p_uri} a sio:SIO_001061 ;")
                lines.append(f'    rdfs:label "Population of {region} ({pop_year})" ;')
                lines.append(f'    sio:SIO_000300 "{pop_val}"^^xsd:integer .')
                lines.append("")
                pop_count += 1

    print(f"  {len(regions_seen)} Geographic Region entities")
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
        r_uri = gr_uri(cc, region)
        identifier = cla_id(SOURCE, chem_id, cc, region, year)

        lines.append(f"{cla_uri(identifier)} a sem-lucia:ChemicalLocationAssociation ;")
        lines.append(f'    dcterms:identifier "{identifier}" ;')
        lines.append(f"    sio:SIO_000253 {source_uri(SOURCE)} ;")
        lines.append(f"    sio:SIO_000628 {chemical_uri(chem_id)} ,")
        lines.append(f"        {r_uri} ;")
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