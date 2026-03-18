import pandas as pd
import os

BASE = os.path.join(os.path.dirname(__file__), "..", "..")
RAW = os.path.join(BASE, "data", "raw")
PROCESSED = os.path.join(BASE, "data", "processed")
os.makedirs(PROCESSED, exist_ok=True)

AGE_GROUPS = ['0_14', '15_29', '30_44', '45_59', '60_74', '75_85']
AGE_LABELS = {'0_14':'0-14', '15_29':'15-29', '30_44':'30-44',
              '45_59':'45-59', '60_74':'60-74', '75_85':'75-85+'}

# Country name → ISO2 code (EU-27)
COUNTRY_CODES = {
    "Austria": "AT", "Belgium": "BE", "Bulgaria": "BG", "Croatia": "HR",
    "Cyprus": "CY", "Czechia": "CZ", "Denmark": "DK", "Estonia": "EE",
    "Finland": "FI", "France": "FR", "Germany": "DE", "Greece": "GR",
    "Hungary": "HU", "Ireland": "IE", "Italy": "IT", "Latvia": "LV",
    "Lithuania": "LT", "Luxembourg": "LU", "Malta": "MT", "Netherlands": "NL",
    "Poland": "PL", "Portugal": "PT", "Romania": "RO", "Slovakia": "SK",
    "Slovenia": "SI", "Spain": "ES", "Sweden": "SE",
}


def load_ecis(filename, indicator):
    df = pd.read_csv(os.path.join(RAW, filename), sep=';')
    df = df[df[f'{indicator} - Male'].notna()].copy()
    df = df[df['Country'].str.strip() != 'EU-27']
    df['Country'] = df['Country'].str.strip()

    male = df[['Country', f'{indicator} - Male']].rename(columns={f'{indicator} - Male': indicator})
    male['Gender'] = 'Male'
    female = df[['Country', f'{indicator} - Female']].rename(columns={f'{indicator} - Female': indicator})
    female['Gender'] = 'Female'
    return pd.concat([male, female], ignore_index=True)


print("Loading ECIS data...")
all_rows = []
for ag in AGE_GROUPS:
    label = AGE_LABELS[ag]
    inc = load_ecis(f'ecis_incidence_{ag}.csv', 'Incidence')
    mort = load_ecis(f'ecis_mortality_{ag}.csv', 'Mortality')
    merged = inc.merge(mort, on=['Country', 'Gender'], how='outer')
    merged.rename(columns={'Mortality': 'MortalityRate'}, inplace=True)
    merged['AgeGroup'] = label
    merged['Year'] = 2024
    all_rows.append(merged)
    print(f"  {label}: {len(merged)} rows")

final = pd.concat(all_rows, ignore_index=True)

# Add disease (always the same for ECIS lung cancer data)
final['DiseaseName'] = 'Malignant neoplasm of lung'
final['DiseaseCode'] = 'C0242379'

# Add ISO2 country code
final['CountryCode'] = final['Country'].map(COUNTRY_CODES).fillna(final['Country'].str[:2].str.upper())

# Reorder columns: DiseaseName first
final = final[['DiseaseName', 'DiseaseCode', 'Country', 'CountryCode', 'Gender', 'AgeGroup', 'Incidence', 'MortalityRate', 'Year']]

before = len(final)
final = final.dropna()
final = final.sort_values(['Country', 'Gender', 'AgeGroup']).reset_index(drop=True)
print(f"\n  Incomplete rows dropped: {before - len(final)}")
print(f"  {len(final)} rows | {final['Country'].nunique()} countries | {final['AgeGroup'].nunique()} age groups")

out_path = os.path.join(PROCESSED, "ecis_final.csv")
final.to_csv(out_path, index=False)
print(f"  Saved: {out_path}")