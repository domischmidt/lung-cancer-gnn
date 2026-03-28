"""
ECIS Pipeline: Combine historical ECIS data + 2024 estimates.

Inputs:
  data/raw/ecis_historical/*.csv  (32 country CSVs, semicolon-separated)
  data/raw/ecis_incidence_*.csv   (2024 estimates, 6 age groups)
  data/raw/ecis_mortality_*.csv   (2024 estimates, 6 age groups)
Output:
  data/processed/ecis_final.csv

Columns: DiseaseName, DiseaseCode, Country, CountryCode, Registry,
         Gender, AgeGroup, Incidence, MortalityRate, Year
"""
import pandas as pd
import os

BASE = os.path.join(os.path.dirname(__file__), "..", "..")
RAW = os.path.join(BASE, "data", "raw")
PROCESSED = os.path.join(BASE, "data", "processed")
os.makedirs(PROCESSED, exist_ok=True)

# File code → ISO2 country code (where they differ)
FILE_TO_COUNTRY = {
    "BH": "BA",  # Bosnia Herzegovina
    "IR": "IE",  # Ireland
    "PO": "PL",  # Poland
}

# Registry → Country name (for display)
COUNTRY_NAMES = {
    "AT": "Austria", "BA": "Bosnia and Herzegovina", "BE": "Belgium",
    "BG": "Bulgaria", "CH": "Switzerland", "CY": "Cyprus", "CZ": "Czechia",
    "DE": "Germany", "DK": "Denmark", "EE": "Estonia", "ES": "Spain",
    "FI": "Finland", "FR": "France", "GB": "United Kingdom", "HR": "Croatia",
    "IE": "Ireland", "IS": "Iceland", "IT": "Italy", "LI": "Liechtenstein",
    "LT": "Lithuania", "LV": "Latvia", "MT": "Malta", "NL": "Netherlands",
    "NO": "Norway", "PL": "Poland", "PT": "Portugal", "RO": "Romania",
    "RS": "Serbia", "SE": "Sweden", "SI": "Slovenia", "SK": "Slovakia",
    "UA": "Ukraine",
}

# Age group mapping: historical 75-89 and 90-95+ → 75-85+
AGE_MAP = {
    "0-14": "0-14",
    "15-29": "15-29",
    "30-44": "30-44",
    "45-59": "45-59",
    "60-74": "60-74",
    "75-89": "75-85+",
    "90-95+": "75-85+",
    "75-85+": "75-85+",  # 2024 format
}

# 2024 estimates: age group labels
AGE_GROUPS_2024 = ["0_14", "15_29", "30_44", "45_59", "60_74", "75_85"]
AGE_LABELS_2024 = {
    "0_14": "0-14", "15_29": "15-29", "30_44": "30-44",
    "45_59": "45-59", "60_74": "60-74", "75_85": "75-85+",
}

COUNTRY_CODES_2024 = {
    "Austria": "AT", "Belgium": "BE", "Bulgaria": "BG", "Croatia": "HR",
    "Cyprus": "CY", "Czechia": "CZ", "Denmark": "DK", "Estonia": "EE",
    "Finland": "FI", "France": "FR", "Germany": "DE", "Greece": "GR",
    "Hungary": "HU", "Ireland": "IE", "Italy": "IT", "Latvia": "LV",
    "Lithuania": "LT", "Luxembourg": "LU", "Malta": "MT", "Netherlands": "NL",
    "Poland": "PL", "Portugal": "PT", "Romania": "RO", "Slovakia": "SK",
    "Slovenia": "SI", "Spain": "ES", "Sweden": "SE",
}


def load_historical():
    """Load all historical CSVs from data/raw/ecis_historical/."""
    hist_dir = os.path.join(RAW, "ecis_historical")
    if not os.path.exists(hist_dir):
        print("  [WARN] ecis_historical/ not found – skipping historical data.")
        return pd.DataFrame()

    all_rows = []
    for f in sorted(os.listdir(hist_dir)):
        if not f.endswith(".csv"):
            continue
        cc_file = f.replace(".csv", "")
        cc = FILE_TO_COUNTRY.get(cc_file, cc_file)

        df = pd.read_csv(os.path.join(hist_dir, f), sep=";")
        df = df[df["Indicator"].isin(["Incidence", "Mortality"])].copy()

        for _, row in df.iterrows():
            age_raw = str(row["Age"]).strip()
            age_mapped = AGE_MAP.get(age_raw, age_raw)
            indicator = row["Indicator"]
            raw_value = str(row["Age-specific Rate"]).strip()
            if raw_value in ("-", "", "nan", "None"):
                continue
            try:
                value = float(raw_value)
            except ValueError:
                continue
            registry = str(row["Registry"]).strip().rstrip("(*)")

            entry = {
                "Country": COUNTRY_NAMES.get(cc, registry),
                "CountryCode": cc,
                "Registry": registry,
                "Gender": str(row["Sex"]).strip().capitalize(),
                "AgeGroup": age_mapped,
                "Year": int(row["Year"]),
            }
            if indicator == "Incidence":
                entry["Incidence"] = value
                entry["MortalityRate"] = None
            else:
                entry["Incidence"] = None
                entry["MortalityRate"] = value

            all_rows.append(entry)

    if not all_rows:
        return pd.DataFrame()

    df = pd.DataFrame(all_rows)

    # Merge Incidence + Mortality rows for same key
    # (some countries have both, some only one)
    group_cols = ["Country", "CountryCode", "Registry", "Gender", "AgeGroup", "Year"]
    merged = df.groupby(group_cols, dropna=False).agg({
        "Incidence": "first",
        "MortalityRate": "first",
    }).reset_index()

    # For 75-85+ mapped group: average the values from 75-89 and 90-95+
    # Actually they're already mapped to same group, so groupby will merge them
    # But we need to re-aggregate in case both 75-89 and 90-95+ exist
    final = merged.groupby(group_cols, dropna=False).agg({
        "Incidence": "mean",
        "MortalityRate": "mean",
    }).reset_index()

    print(f"  Historical: {len(final)} rows, {final['CountryCode'].nunique()} countries, "
          f"{final['Registry'].nunique()} registries, "
          f"{int(final['Year'].min())}-{int(final['Year'].max())}")
    return final


def load_2024_estimates():
    """Load 2024 ECIS estimates from incidence + mortality CSVs."""
    def load_ecis(filename, indicator):
        path = os.path.join(RAW, filename)
        if not os.path.exists(path):
            return pd.DataFrame()
        df = pd.read_csv(path, sep=";")
        df = df[df[f"{indicator} - Male"].notna()].copy()
        df = df[df["Country"].str.strip() != "EU-27"]
        df["Country"] = df["Country"].str.strip()
        male = df[["Country", f"{indicator} - Male"]].rename(
            columns={f"{indicator} - Male": indicator}
        )
        male["Gender"] = "Male"
        female = df[["Country", f"{indicator} - Female"]].rename(
            columns={f"{indicator} - Female": indicator}
        )
        female["Gender"] = "Female"
        return pd.concat([male, female], ignore_index=True)

    all_rows = []
    for ag in AGE_GROUPS_2024:
        label = AGE_LABELS_2024[ag]
        inc = load_ecis(f"ecis_incidence_{ag}.csv", "Incidence")
        mort = load_ecis(f"ecis_mortality_{ag}.csv", "Mortality")
        if inc.empty and mort.empty:
            continue
        merged = inc.merge(mort, on=["Country", "Gender"], how="outer")
        merged.rename(columns={"Mortality": "MortalityRate"}, inplace=True)
        merged["AgeGroup"] = label
        merged["Year"] = 2024
        all_rows.append(merged)

    if not all_rows:
        print("  [WARN] No 2024 estimate CSVs found.")
        return pd.DataFrame()

    df = pd.concat(all_rows, ignore_index=True)
    df["CountryCode"] = df["Country"].map(COUNTRY_CODES_2024).fillna(
        df["Country"].str[:2].str.upper()
    )
    df["Registry"] = df["Country"]  # national level = registry is country

    df = df[["Country", "CountryCode", "Registry", "Gender", "AgeGroup",
             "Incidence", "MortalityRate", "Year"]]
    df = df.dropna(subset=["Incidence", "MortalityRate"], how="all")

    print(f"  2024 Estimates: {len(df)} rows, {df['CountryCode'].nunique()} countries")
    return df


def ecis_pipeline():
    print("Loading ECIS data...")
    hist = load_historical()
    est_2024 = load_2024_estimates()

    # Combine
    dfs = [d for d in [hist, est_2024] if not d.empty]
    if not dfs:
        print("  ERROR: No data loaded!")
        return

    final = pd.concat(dfs, ignore_index=True)

    # Add disease info
    final["DiseaseName"] = "Malignant neoplasm of lung"
    final["DiseaseCode"] = "C0242379"

    # Reorder
    final = final[["DiseaseName", "DiseaseCode", "Country", "CountryCode",
                    "Registry", "Gender", "AgeGroup", "Incidence",
                    "MortalityRate", "Year"]]
    final = final.sort_values(["Year", "Country", "Registry", "Gender", "AgeGroup"])
    final = final.reset_index(drop=True)

    print(f"\n  Combined: {len(final)} rows")
    print(f"  Countries: {final['CountryCode'].nunique()}")
    print(f"  Registries: {final['Registry'].nunique()}")
    print(f"  Years: {int(final['Year'].min())}-{int(final['Year'].max())}")
    print(f"  Age groups: {sorted(final['AgeGroup'].unique())}")

    out_path = os.path.join(PROCESSED, "ecis_final.csv")
    final.to_csv(out_path, index=False)
    print(f"  Saved: {out_path}")


if __name__ == "__main__":
    ecis_pipeline()