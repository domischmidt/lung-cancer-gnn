"""
Collect Parquet file URLs from the EEA Air Quality Download API.
Covers all 10 pollutants: PM2.5, PM10, NO2, O3, BaP, C6H6, As, Cd, Pb, Ni.
Output: data/raw/eea_urls.csv + data/raw/eea_urls_additional.csv
"""
import requests
import os
import time

BASE = os.path.join(os.path.dirname(__file__), "..", "..")
RAW = os.path.join(BASE, "data", "raw")

API_URL = "https://eeadmz1-downloads-api-appservice.azurewebsites.net/ParquetFile/urls"

EU_COUNTRIES = [
    "AT", "BE", "BG", "CY", "CZ", "DE", "DK", "EE", "ES", "FI",
    "FR", "GR", "HR", "HU", "IE", "IT", "LT", "LU", "LV", "MT",
    "NL", "PL", "PT", "RO", "SE", "SI", "SK",
    "AL", "BA", "CH", "GB", "IS", "LI", "ME", "MK", "NO", "RS", "TR", "XK"
]

# Primary pollutants (EU Air Quality Directive dashboards)
PRIMARY = ["PM2.5", "PM10", "NO2", "O3"]

# Additional pollutants: BaP + heavy metals measured within PM10 filters
# "As in PM10" = arsenic content in PM10 particles, NOT PM10 itself
ADDITIONAL = ["BaP", "C6H6", "As in PM10", "Cd in PM10", "Pb in PM10", "Ni in PM10"]


def collect_urls(pollutants, output_filename):
    all_urls = []

    for poll in pollutants:
        print(f"\n=== {poll} ===")
        for country in EU_COUNTRIES:
            request_body = {
                "countries": [country],
                "cities": [],
                "pollutants": [poll],
                "dataset": 2,
                "source": "API"
            }
            try:
                response = requests.post(API_URL, json=request_body, timeout=60)
                if response.status_code == 200:
                    lines = response.text.strip().split("\n")
                    urls = [l.strip() for l in lines[1:] if l.strip().startswith("http")]
                    if urls:
                        print(f"  {country}: {len(urls)} files")
                        for u in urls:
                            all_urls.append(f"{poll},{country},{u}")
                else:
                    print(f"  {country}: HTTP {response.status_code}")
            except Exception as e:
                print(f"  {country}: ERROR {e}")
            time.sleep(0.3)

    out_path = os.path.join(RAW, output_filename)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("pollutant,country,url\n")
        for row in all_urls:
            f.write(row + "\n")
    print(f"\nTotal URLs: {len(all_urls)} → {out_path}")


if __name__ == "__main__":
    collect_urls(PRIMARY, "eea_urls.csv")
    collect_urls(ADDITIONAL, "eea_urls_additional.csv")