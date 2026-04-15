import hashlib
import unicodedata
import re

BASE = "http://medal.ctb.upm.es/projects/LUCIA/res/sem-lucia"

PREFIXES = """\
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix dcterms: <http://purl.org/dc/terms/> .
@prefix sio: <http://semanticscience.org/resource/> .
@prefix ncit: <http://ncicb.nci.nih.gov/xml/owl/EVS/Thesaurus.owl#> .
@prefix sem-lucia: <https://w3id.org/LUCIA/sem-lucia#> .
@prefix lucia: <http://medal.ctb.upm.es/projects/LUCIA/res/sem-lucia#> .

"""

EXISTING_COUNTRIES = {
    "AE", "AF", "AL", "AM", "AO", "AR", "AT", "AU", "AZ", "BA", "BD", "BE",
    "BF", "BG", "BH", "BI", "BJ", "BN", "BO", "BR", "BT", "BW", "BY", "CA",
    "CF", "CG", "CH", "CHN", "CI", "CL", "CM", "CO", "COD", "CR", "CU", "CY",
    "CZ", "DE", "DK", "DO", "DZ", "EA", "EC", "EE", "EG", "ER", "ES", "ET",
    "EU", "FI", "FJ", "FR", "GA", "GB", "GE", "GH", "GM", "GN", "GQ", "GR",
    "GT", "GW", "HKG", "HN", "HR", "HT", "HU", "ID", "IE", "IL", "IN", "IQ",
    "IR", "IS", "IT", "JM", "JO", "JP", "KE", "KG", "KH", "KOR", "KP", "KW",
    "KZ", "LA", "LB", "LI", "LK", "LR", "LS", "LT", "LU", "LV", "LY", "MA",
    "MAC", "MD", "ME", "MG", "MK", "ML", "MM", "MN", "MR", "MT", "MU", "MW",
    "MX", "MY", "MZ", "NE", "NG", "NI", "NL", "NO", "NP", "NZ", "OECD",
    "OECDE", "OM", "PA", "PE", "PG", "PH", "PK", "PL", "PSE", "PT", "PY",
    "QA", "RO", "RS", "RUS", "RW", "SA", "SD", "SE", "SG", "SI", "SK", "SL",
    "SN", "SO", "SV", "SY", "SZ", "TD", "TG", "TH", "TJ", "TL", "TM", "TN",
    "TR", "TT", "TWN", "TZ", "UA", "UG", "US", "US_PRI", "UY", "UZ", "VE",
    "VN", "WLD", "YE", "ZA", "ZM", "ZW",
}

EXISTING_CALENDAR_YEARS = {
    1990, 1995, 1999, 2000, 2001, 2002, 2003, 2004, 2005, 2006, 2007, 2008,
    2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020,
    2021, 2022,
}

EXISTING_CHEMICALS = {
    "C5890534",   # PM2.5
    "C0005052",   # BaP (Benzopyrene)
    "C0005036",   # C6H6 (Benzene)
    "C0028160",   # NO2 (Nitrogen Dioxide)
    "C0030106",   # O3 (Ozone)
    "C0028167",   # NOx (Nitrogen Oxides)
}

EXISTING_SOURCES = {"EEA-2025", "OECD-2025"}


CHEMICAL_IDS = {
    "PM2.5":    "C5890534",
    "BaP":      "C0005052",
    "C6H6":     "C0005036",
    "NO2":      "C0028160",
    "O3":       "C0030106",
    "Nitrogen oxides": "C0028167",
    "PM10":     "C1720884_10",
    "As_PM10":  "C1720884_10_As",
    "Cd_PM10":  "C1720884_10_Cd",
    "Ni_PM10":  "C1720884_10_Ni",
    "Pb_PM10":  "C1720884_10_Pb",
}

CHEMICAL_LABELS = {
    "C1720884_10":    "Particulate matter (PM10)",
    "C1720884_10_As": "Particulate matter (PM10, Arsenic)",
    "C1720884_10_Cd": "Particulate matter (PM10, Cadmium)",
    "C1720884_10_Ni": "Particulate matter (PM10, Nickel)",
    "C1720884_10_Pb": "Particulate matter (PM10, Lead)",
}

def uri(path):
    return f"<{BASE}#{path}>"

def country_uri(code):
    return uri(f"country/{code}")

def city_uri(country_code, city_name, year):
    """City URI includes year: country/city/{CC}_{slug}_{year}"""
    slug = slugify(city_name)
    return uri(f"country/city/{country_code}_{slug}_{year}")

def city_identifier(country_code, city_name, year):
    """City dcterms:identifier value."""
    slug = slugify(city_name)
    return f"{country_code}_{slug}_{year}"

def calendar_year_uri(year):
    return uri(f"calendaryear/{year}")

def people_uri(age_group, gender, ethnicity="undefined"):
    """e.g. lucia:#people/people_60-74_Male_undefined"""
    return uri(f"people/people_{age_group}_{gender}_{ethnicity}")

def disease_uri(cui="C0242379"):
    return uri(f"disease/{cui}")

def chemical_uri(chem_id):
    return uri(f"chemical/{chem_id}")

def source_uri(source):
    return uri(f"cla/source/{source}")

def units_uri(unit_key):
    return uri(f"units/{unit_key}")

def frequency_uri(freq="Annual"):
    return uri(f"frequency/{freq}")

def vstat_uri(country_code, age_group, gender, ethnicity, disease, year):
    """Vital statistics URI includes year."""
    ag = age_group.replace("+", "%2B")
    return uri(f"vitalstatistics/vstat_{disease}_{country_code}_{ag}_{gender}_{ethnicity}_{year}")

def cla_uri(identifier):
    """ChemicalLocationAssociation instance URI."""
    return uri(f"cla/{identifier}")

def cla_id(*parts):
    """Generate deterministic CLA identifier from parts."""
    raw = "_".join(str(p) for p in parts)
    return "CLA" + hashlib.sha256(raw.encode("utf-8")).hexdigest()


def slugify(name):
    """Convert name to URI-safe slug: lowercase, underscores, alphanumeric only."""
    s = str(name).strip().lower()
    for old, new in [("İ","i"),("ı","i"),("ş","s"),("ç","c"),("ğ","g")]:
        s = s.replace(old, new)
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    s = re.sub(r"[^a-z0-9]+", "_", s)
    s = s.strip("_")
    return s

def normalize_ascii(name):
    """Lowercase ASCII normalization."""
    s = str(name).lower().strip()
    for old, new in [("İ","i"),("ı","i"),("ş","s"),("ç","c"),("ğ","g")]:
        s = s.replace(old, new)
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    s = re.sub(r"[^a-z0-9 ]", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

COUNTRY_CODES = {
    "Austria": "AT", "Belgium": "BE", "Bulgaria": "BG", "Croatia": "HR",
    "Cyprus": "CY", "Czechia": "CZ", "Denmark": "DK", "Estonia": "EE",
    "Finland": "FI", "France": "FR", "Germany": "DE", "Greece": "GR",
    "Hungary": "HU", "Ireland": "IE", "Italy": "IT", "Latvia": "LV",
    "Lithuania": "LT", "Luxembourg": "LU", "Malta": "MT", "Netherlands": "NL",
    "Poland": "PL", "Portugal": "PT", "Romania": "RO", "Slovakia": "SK",
    "Slovenia": "SI", "Spain": "ES", "Sweden": "SE",
    "Norway": "NO", "Switzerland": "CH", "Iceland": "IS",
    "Liechtenstein": "LI", "Turkey": "TR", "Türkiye": "TR",
    "United Kingdom": "GB", "Serbia": "RS", "North Macedonia": "MK",
    "Albania": "AL", "Bosnia and Herzegovina": "BA", "Montenegro": "ME",
    "Kosovo": "XK",
}