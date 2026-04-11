import pandas as pd
import os

BASE = os.path.join(os.path.dirname(__file__), "..", "..")
RAW = os.path.join(BASE, "data", "raw")
PROCESSED = os.path.join(BASE, "data", "processed")
os.makedirs(PROCESSED, exist_ok=True)

EXCLUDE = {"OECD", "OECDE", "OECDA", "OECDSO", "EU27_2020", "EA20", "G7", "G20", "WXOECD"}

CHEMICAL_MAP = {
    "Fine particulate matter (PM2.5)": "PM2.5",
    "Particulates (PM10)": "PM10",
    "Nitrogen dioxide (NO2)": "NO2",
}

PREFIX_TO_COUNTRY = {
    "AT": ("Austria", "AT"),
    "AU": ("Australia", "AU"),
    "BE": ("Belgium", "BE"),
    "BG": ("Bulgaria", "BG"),
    "CA": ("Canada", "CA"),
    "CH": ("Switzerland", "CH"),
    "CL": ("Chile", "CL"),
    "CO": ("Colombia", "CO"),
    "CR": ("Costa Rica", "CR"),
    "CZ": ("Czechia", "CZ"),
    "DE": ("Germany", "DE"),
    "DK": ("Denmark", "DK"),
    "EE": ("Estonia", "EE"),
    "EL": ("Greece", "GR"),
    "ES": ("Spain", "ES"),
    "FI": ("Finland", "FI"),
    "FR": ("France", "FR"),
    "HR": ("Croatia", "HR"),
    "HU": ("Hungary", "HU"),
    "IE": ("Ireland", "IE"),
    "IS": ("Iceland", "IS"),
    "IT": ("Italy", "IT"),
    "JP": ("Japan", "JP"),
    "KR": ("Korea", "KR"),
    "LT": ("Lithuania", "LT"),
    "LU": ("Luxembourg", "LU"),
    "LV": ("Latvia", "LV"),
    "ME": ("Mexico", "MX"),
    "NL": ("Netherlands", "NL"),
    "NO": ("Norway", "NO"),
    "NZ": ("New Zealand", "NZ"),
    "PL": ("Poland", "PL"),
    "PT": ("Portugal", "PT"),
    "RO": ("Romania", "RO"),
    "SE": ("Sweden", "SE"),
    "SI": ("Slovenia", "SI"),
    "SK": ("Slovakia", "SK"),
    "TR": ("Türkiye", "TR"),
    "UK": ("United Kingdom", "GB"),
    "US": ("United States", "US"),
}


def get_country_info(ref_area, ref_name):
    prefix = ref_area[:2]
    if prefix in PREFIX_TO_COUNTRY:
        name, iso2 = PREFIX_TO_COUNTRY[prefix]
        return name, iso2, ref_name
    return ref_name, prefix, ref_name


def load_population():
    pop_path = os.path.join(RAW, "oecd_population.csv")
    if not os.path.exists(pop_path):
        print("  [WARN] oecd_population.csv not found – Population column will be empty.")
        return {}

    pop = pd.read_csv(pop_path)
    pop = pop[pop["MEASURE"] == "POP"]  # safety filter

    pop_lookup = {}
    for _, r in pop.iterrows():
        key = (r["REF_AREA"], int(r["TIME_PERIOD"]))
        pop_lookup[key] = int(r["OBS_VALUE"])

    years = sorted(pop["TIME_PERIOD"].unique())
    print(f"  Population loaded: {len(pop_lookup)} entries ({pop['REF_AREA'].nunique()} regions × {len(years)} years)")
    return pop_lookup


def oecd_exposure_pipeline():
    df = pd.read_csv(os.path.join(RAW, "oecd_exposure.csv"))
    print(f"Loaded {len(df)} rows from oecd_exposure.csv")

    before = len(df)
    df = df[~df["REF_AREA"].isin(EXCLUDE)]
    print(f"  After removing aggregates: {len(df)} (removed {before - len(df)})")

    pop_lookup = load_population()

    rows = []
    pop_matched = 0
    pop_missing = 0

    for _, r in df.iterrows():
        ref_area = r["REF_AREA"]
        year = int(r["TIME_PERIOD"])
        country_name, country_code, region_name = get_country_info(
            ref_area, r["Reference area"]
        )

        population = pop_lookup.get((ref_area, year))

        if population is not None:
            pop_matched += 1
        else:
            pop_missing += 1

        rows.append({
            "Country": country_name,
            "CountryCode": country_code,
            "RegionName": region_name,
            "Chemical": CHEMICAL_MAP.get(r["Pollutant"], r["Pollutant"]),
            "Measure": r["Measure"],
            "Units": "µg/m³",
            "Frequency": r["Frequency of observation"],
            "Year": year,
            "Value": round(r["OBS_VALUE"], 4),
            "Population": population if population is not None else "",
        })

    final = pd.DataFrame(rows)
    final = final.dropna(subset=["Value"])
    final = final.sort_values(["Year", "Country", "RegionName", "Chemical"]).reset_index(drop=True)

    print(f"\n  {final['Country'].nunique()} unique countries")
    print(f"  {final['RegionName'].nunique()} unique regions")
    print(f"  Chemicals: {sorted(final['Chemical'].unique())}")
    print(f"  Years: {sorted(final['Year'].unique())}")
    print(f"  Population matched: {pop_matched} rows ({pop_matched*100//(pop_matched+pop_missing)}%)")
    print(f"  Population missing:  {pop_missing} rows")
    print(f"  {len(final)} total rows")

    out_path = os.path.join(PROCESSED, "oecd_exposure_final.csv")
    final.to_csv(out_path, index=False)
    print(f"  Saved: {out_path}")


if __name__ == "__main__":
    oecd_exposure_pipeline()