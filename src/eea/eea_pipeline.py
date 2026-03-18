"""
EEA Pipeline: Merge air quality measurements with Eurostat LAU population data.

Reads:  data/interim/eea_all_results.csv
        data/raw/eurostat_lau.xlsx
        data/raw/eea_metadata.csv

Output: data/processed/eea_final.csv

Columns: ChemicalID, CountryName, CountryCode, CityName, Value, Units, Population, Year

Supports multiple years (2013-2024).
"""
import pandas as pd
import re
import unicodedata
import os
from collections import defaultdict

BASE = os.path.join(os.path.dirname(__file__), "..", "..")
RAW = os.path.join(BASE, "data", "raw")
INTERIM = os.path.join(BASE, "data", "interim")
PROCESSED = os.path.join(BASE, "data", "processed")
os.makedirs(PROCESSED, exist_ok=True)

COUNTRIES_LAU = [
    "BE", "BG", "CZ", "DK", "DE", "EE", "IE", "EL", "ES", "FR", "HR", "IT",
    "CY", "LV", "LT", "LU", "HU", "MT", "NL", "AT", "PL", "PT", "RO", "SI",
    "SK", "FI", "SE", "LI", "NO", "CH", "MK", "TR",
]

POLLUTANT_MAP = {
    "PM2.5": "PM2.5",
    "PM10": "PM10",
    "NO2": "NO2",
    "O3": "O3",
    "BaP": "BaP",
    "C6H6": "C6H6",
    "As in PM10": "As_PM10",
    "Cd in PM10": "Cd_PM10",
    "Pb in PM10": "Pb_PM10",
    "Ni in PM10": "Ni_PM10",
}

COUNTRY_NAMES = {
    "AT": "Austria", "BE": "Belgium", "BG": "Bulgaria", "CY": "Cyprus",
    "CZ": "Czechia", "DE": "Germany", "DK": "Denmark", "EE": "Estonia",
    "ES": "Spain", "FI": "Finland", "FR": "France", "GR": "Greece",
    "HR": "Croatia", "HU": "Hungary", "IE": "Ireland", "IT": "Italy",
    "LT": "Lithuania", "LU": "Luxembourg", "LV": "Latvia", "MK": "North Macedonia",
    "MT": "Malta", "NL": "Netherlands", "NO": "Norway", "PL": "Poland",
    "PT": "Portugal", "RO": "Romania", "SE": "Sweden", "SI": "Slovenia",
    "SK": "Slovakia", "TR": "Türkiye", "CH": "Switzerland", "GB": "United Kingdom",
    "RS": "Serbia", "AL": "Albania", "BA": "Bosnia and Herzegovina",
    "ME": "Montenegro", "XK": "Kosovo", "IS": "Iceland", "LI": "Liechtenstein",
}

DE_ABBREVS = {
    "a.d.": "an der", "a. d.": "an der", "a.": "am",
    "i.": "in", "i. ": "in ", "i.d.": "in der",
    "b.": "bei", "b. ": "bei ",
    "o.d.": "ob der", "a.inn": "am inn",
    "i.t.": "in tirol", "i.bay.": "in bayern",
    "i. bay.": "in bayern", "i. t.": "in tirol",
    "i.opf.": "in der oberpfalz",
}


# ═════════════════════════════════════════════════════════════════════════════
#  Name normalization helpers (unchanged)
# ═════════════════════════════════════════════════════════════════════════════

def normalize(name, country=None):
    if pd.isna(name):
        return []
    s = str(name).strip()
    s = " ".join(s.split())
    s = re.sub(r"^\d{3,6}\s+", "", s)
    s = s.lower()

    for old, new in [
        ("İ", "i"), ("ı", "i"), ("i̇", "i"), ("ş", "s"),
        ("ç", "c"), ("ğ", "g"), ("ü", "u"), ("ö", "o"),
    ]:
        s = s.replace(old, new)

    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    s = s.replace("=", " ").replace("/", " ").replace("'", "").replace('"', "").replace("c/o", "")
    s = re.sub(r"\s+", " ", s).strip()

    variants = {s, s.replace("-", " "), s.replace(" ", "-")}

    no_parens = re.sub(r"\s*\([^)]*\)", "", s).strip()
    if no_parens:
        variants.add(no_parens)

    m = re.search(r"\(([^)]+)\)", s)
    if m and len(m.group(1).strip()) > 2:
        inner, outer = m.group(1).strip(), no_parens
        variants.update([f"{inner} {outer}", inner])

    arr = re.match(r"^(.+?)\s+\d+e?\s*arrondissement", s)
    if arr:
        variants.add(arr.group(1))

    if country == "IT":
        words = re.sub(r"\s*\([^)]*\)", "", s).strip().split()
        if len(words) > 2 and words[0] in (
            "via", "piazza", "corso", "viale", "largo", "piazzale", "localita", "loc", "str",
        ):
            variants.add(words[-1])
            if len(words) > 3:
                variants.add(" ".join(words[-2:]))

    if country in ("DE", "AT"):
        expanded = s
        for abbr, full in sorted(DE_ABBREVS.items(), key=lambda x: -len(x[0])):
            expanded = expanded.replace(abbr, full)
        variants.add(re.sub(r"\s+", " ", expanded).strip())
        for abbr in DE_ABBREVS:
            if abbr in s:
                prefix = s.split(abbr)[0].strip()
                if len(prefix) > 2:
                    variants.add(prefix)

    if country == "ES":
        m2 = re.match(r"^(.+?)\s*\((el|la|los|las|l)\)$", s)
        if m2:
            variants.add(f"{m2.group(2)} {m2.group(1)}")
            if m2.group(2) == "l":
                variants.add(f"l{m2.group(1)}")

    if country == "BE":
        parts = s.split()
        if len(parts) == 2:
            variants.update(parts)

    cleaned = re.sub(r"^\d[\d\s]*\d\s+", "", s)
    if cleaned != s and cleaned:
        variants.add(cleaned)

    for suffix in [" stadt", " am main", " an der ", " im ", " ob der "]:
        if suffix in s:
            variants.add(s.split(suffix)[0])

    for v in list(variants):
        variants.update([v.replace("-", " "), v.replace(" ", "-")])

    return [v for v in variants if v and len(v) > 1]


def extract_base_city(name, country):
    if pd.isna(name):
        return name
    s = str(name).strip()

    if country == "FR":
        arr = re.match(r"^(.+?)\s+\d+e[r]?\s*arrondissement", s, re.IGNORECASE)
        if arr:
            return arr.group(1).strip()
        numbered = re.match(r"^(.+?)\s+\d+e?$", s, re.IGNORECASE)
        if numbered:
            return numbered.group(1).strip()

    if country == "IT":
        m = re.search(r"-\s*([A-Za-zÀ-ÿ\s]+?)\s*\([A-Z]{2}\)\s*$", s)
        if m:
            return m.group(1).strip().title()
        m = re.match(r"^([A-Za-zÀ-ÿ][\w\s]*?),\s+", s)
        if m and len(m.group(1).strip()) > 2:
            return m.group(1).strip().title()
        m = re.match(r"^([A-Za-zÀ-ÿ]+)\s+(?:Via|Piazza|Viale|Corso)\b", s)
        if m and len(m.group(1)) > 2:
            return m.group(1).strip().title()

    if country == "AT":
        s = re.sub(r"^\d{4,5}\s+", "", s)
        s = re.sub(r",?\s*KG\s+\w+", "", s).strip()
        s = re.split(r"[,]", s)[0].strip()
        known = {"Graz", "Wien", "Leoben", "Innsbruck", "Salzburg", "Klagenfurt"}
        words = s.split()
        if len(words) > 1 and words[0] in known:
            return words[0]
        return s

    if country == "BE":
        s = s.upper()
        s = re.sub(r"BRUSSEL[= ]*BRUXELLES", "BRUXELLES", s)
        s = s.replace("BRUSSEL", "BRUXELLES")
        return s

    s = re.sub(r"^\d{3,6}\s+", "", s)
    return s


# ═════════════════════════════════════════════════════════════════════════════
#  1. Load Eurostat LAU population reference
# ═════════════════════════════════════════════════════════════════════════════
print("Loading LAU population data...")
lau_all = []
for c in COUNTRIES_LAU:
    try:
        df = pd.read_excel(os.path.join(RAW, "eurostat_lau.xlsx"), sheet_name=c)
        df["country_lau"] = c
        lau_all.append(df)
    except Exception as e:
        print(f"  Skip {c}: {e}")

lau = pd.concat(lau_all, ignore_index=True)
lau["country_lau"] = lau["country_lau"].replace({"EL": "GR"})
print(f"  {len(lau)} municipalities loaded")

lau_lookup = defaultdict(dict)
for _, row in lau.iterrows():
    c = row["country_lau"]
    pop = row.get("POPULATION")
    if pd.isna(pop):
        continue
    pop = int(pop)
    for field in ["LAU NAME LATIN", "LAU NAME NATIONAL"]:
        name = row.get(field)
        if pd.isna(name):
            continue
        for v in normalize(name, c):
            if v not in lau_lookup[c] or pop > lau_lookup[c][v]:
                lau_lookup[c][v] = pop

print(f"  {sum(len(v) for v in lau_lookup.values())} lookup entries")


# ═════════════════════════════════════════════════════════════════════════════
#  2. Load EEA measurement results
# ═════════════════════════════════════════════════════════════════════════════
print("\nLoading EEA results...")
result_files = [
    os.path.join(INTERIM, "eea_all_results.csv"),
]

frames = []
for f in result_files:
    if os.path.exists(f):
        try:
            tmp = pd.read_csv(f, low_memory=False)
            frames.append(tmp)
            print(f"  {os.path.basename(f)}: {len(tmp)} rows")
        except Exception:
            pass

if not frames:
    print("ERROR: No result files found. Run 02_download_parquets.py first.")
    exit(1)

eea = pd.concat(frames, ignore_index=True)

# Handle legacy format: annual_mean_2024 → annual_mean + year
if "annual_mean_2024" in eea.columns and "annual_mean" not in eea.columns:
    eea.rename(columns={"annual_mean_2024": "annual_mean"}, inplace=True)
if "year" not in eea.columns:
    eea["year"] = 2024

eea["year"] = eea["year"].astype(int)

# Clean municipality names: strip trailing semicolons, whitespace
if "municipality" in eea.columns:
    eea["municipality"] = eea["municipality"].astype(str).str.strip().str.rstrip(";").str.strip()
    eea["municipality"] = eea["municipality"].replace({"nan": None, "": None})

# Also clean station_name
if "station_name" in eea.columns:
    eea["station_name"] = eea["station_name"].astype(str).str.strip().str.rstrip(";").str.strip()

# Filter: only verified data (2013-2024), exclude 2025+ (unverified UTD)
before = len(eea)
eea = eea[(eea["year"] >= 2013) & (eea["year"] <= 2024)]
if before > len(eea):
    print(f"  Filtered to 2013-2024: {before} → {len(eea)} (removed {before - len(eea)} rows)")

# Deduplicate per pollutant+country+station+year
if "sampling_point" in eea.columns:
    before = len(eea)
    eea = eea.drop_duplicates(subset=["pollutant", "country", "sampling_point", "year"], keep="first")
    if before > len(eea):
        print(f"  Deduplicated: {before} → {len(eea)}")

print(f"  Total: {len(eea)} records")
print(f"  Years: {sorted(eea['year'].unique())}")


# ═════════════════════════════════════════════════════════════════════════════
#  3. Clean pollutant names
# ═════════════════════════════════════════════════════════════════════════════
eea["pollutant"] = eea["pollutant"].map(POLLUTANT_MAP).fillna(eea["pollutant"])
print(f"\n  Pollutants: {sorted(eea['pollutant'].unique())}")


# ═════════════════════════════════════════════════════════════════════════════
#  4. Match stations to LAU population
# ═════════════════════════════════════════════════════════════════════════════
print("\nMatching population...")
populations = []
for _, row in eea.iterrows():
    country = row.get("country")
    municipality = row.get("municipality")
    if pd.isna(municipality) or pd.isna(country):
        populations.append(None)
        continue

    matched = False
    for v in normalize(municipality, country):
        if v in lau_lookup.get(country, {}):
            populations.append(lau_lookup[country][v])
            matched = True
            break

    if not matched:
        base_variants = normalize(municipality, country)
        base_str = base_variants[0] if base_variants else ""
        best = None
        for lau_name, lau_pop in lau_lookup.get(country, {}).items():
            if len(lau_name) > 3 and (lau_name in base_str or base_str in lau_name):
                if best is None or lau_pop > best:
                    best = lau_pop
        populations.append(best)

eea["population"] = populations
matched_n = eea["population"].notna().sum()
total_n = eea["municipality"].notna().sum()
print(f"  Matched: {matched_n}/{total_n} ({matched_n/total_n*100:.1f}%)")


# ═════════════════════════════════════════════════════════════════════════════
#  5. Normalize city names + aggregate PER YEAR
# ═════════════════════════════════════════════════════════════════════════════
print("\nNormalizing city names...")

eea["city_clean"] = eea.apply(
    lambda r: extract_base_city(r["municipality"], r["country"]), axis=1
)

# Normalize case + accents to merge near-duplicates (KARABÜK/KARABUK, KAKANJ/Kakanj)
def normalize_city_label(name):
    if pd.isna(name):
        return name
    s = str(name).strip()
    # Strip trailing semicolons (from comma→semicolon replacement in download)
    s = s.rstrip(";").strip()
    # Turkish special chars
    for old, new in [("İ", "I"), ("ı", "i"), ("ş", "s"), ("ç", "c"), ("ğ", "g")]:
        s = s.replace(old, new)
    # Strip accents
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    # Title case
    s = s.title()
    return s

eea["city_clean"] = eea["city_clean"].apply(normalize_city_label)

before = len(eea)
eea = (
    eea.groupby(["pollutant", "country", "city_clean", "unit", "year"])
    .agg(annual_mean=("annual_mean", "mean"), population=("population", "max"))
    .reset_index()
)
eea.rename(columns={"city_clean": "municipality"}, inplace=True)
print(f"  {before} → {len(eea)} rows ({before - len(eea)} sub-entries merged)")


# ═════════════════════════════════════════════════════════════════════════════
#  6. Build final output
# ═════════════════════════════════════════════════════════════════════════════
print("\nBuilding final dataset...")
final = eea[["pollutant", "country", "municipality", "annual_mean", "unit", "population", "year"]].copy()
final.columns = ["ChemicalID", "CountryCode", "CityName", "Value", "Units", "Population", "Year"]
final["CountryName"] = final["CountryCode"].map(COUNTRY_NAMES).fillna(final["CountryCode"])
final["Value"] = final["Value"].round(4)
final["Population"] = final["Population"].astype("Int64")

# Reorder columns
final = final[["ChemicalID", "CountryName", "CountryCode", "CityName", "Value", "Units", "Population", "Year"]]

before = len(final)
final = final.dropna(subset=["Value"])
print(f"  Incomplete rows dropped: {before - len(final)}")

before = len(final)
final = final.drop_duplicates(subset=["ChemicalID", "CountryCode", "CityName", "Year"])
final = final.sort_values(["Year", "ChemicalID", "CountryName", "CityName"]).reset_index(drop=True)
print(f"  Duplicates removed: {before - len(final)}")
print(f"  {len(final)} rows | {final['CountryCode'].nunique()} countries | {final['CityName'].nunique()} cities")
print(f"  Pollutants: {sorted(final['ChemicalID'].unique())}")
print(f"  Years: {sorted(final['Year'].unique())}")

out_path = os.path.join(PROCESSED, "eea_final.csv")
final.to_csv(out_path, index=False)
print(f"\nSaved: {out_path}")