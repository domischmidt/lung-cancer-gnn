"""
Download individual parquet files from EEA and compute annual means.
Reads URLs from data/raw/eea_urls.csv + data/raw/eea_urls_additional.csv.
Output: data/interim/eea_all_results.csv (single combined file)

Extracts ALL available years from each parquet file (typically 2013-2024).
Resume-safe: tracks progress in data/interim/eea_download_progress.txt.
"""
import requests
import pandas as pd
import io
import os
import time

BASE = os.path.join(os.path.dirname(__file__), "..", "..")
RAW = os.path.join(BASE, "data", "raw")
INTERIM = os.path.join(BASE, "data", "interim")
os.makedirs(INTERIM, exist_ok=True)

RESULTS_FILE = os.path.join(INTERIM, "eea_all_results.csv")
PROGRESS_FILE = os.path.join(INTERIM, "eea_download_progress.txt")

# Minimum year to include (EEA verified data starts ~2013)
MIN_YEAR = 2013

COLUMNS = [
    "pollutant", "country", "sampling_point", "station_name", "municipality",
    "latitude", "longitude", "station_area", "annual_mean",
    "n_observations", "unit", "agg_type", "year"
]

# ── Load metadata for municipality lookup ────────────────────────────────────
META_FILE = os.path.join(RAW, "eea_metadata.csv")
print("Loading metadata...")
meta = pd.read_csv(META_FILE, low_memory=False)
meta["Sampling Point Id"] = meta["Sampling Point Id"].astype(str).str.strip()
meta_lookup = (
    meta[meta["Municipality"].notna()]
    .groupby("Sampling Point Id")
    .agg({
        "Municipality": "first",
        "Air Quality Station Name": "first",
        "Latitude": "first",
        "Longitude": "first",
        "Air Quality Station Area": "first",
    })
)
print(f"  {len(meta_lookup)} sampling points in metadata")

# ── Load ALL URLs (primary + additional) ─────────────────────────────────────
url_files = [
    os.path.join(RAW, "eea_urls.csv"),
    os.path.join(RAW, "eea_urls_additional.csv"),
]

url_frames = []
for f in url_files:
    if os.path.exists(f):
        df = pd.read_csv(f)
        url_frames.append(df)
        print(f"  {os.path.basename(f)}: {len(df)} URLs")

urls = pd.concat(url_frames, ignore_index=True)
print(f"  Total: {len(urls)} URLs to process")
print(f"  Extracting all years >= {MIN_YEAR}")

# ── Bootstrap: create results file if needed ─────────────────────────────────
if not os.path.exists(RESULTS_FILE):
    pd.DataFrame(columns=COLUMNS).to_csv(RESULTS_FILE, index=False)
    print("  Created fresh results file")

# ── Resume support ───────────────────────────────────────────────────────────
done = set()
if os.path.exists(PROGRESS_FILE):
    with open(PROGRESS_FILE, "r") as f:
        done = set(line.strip() for line in f if line.strip())
    print(f"  Resuming: {len(done)} URLs already processed")

remaining = urls[~urls["url"].isin(done)]
print(f"  Remaining: {len(remaining)} URLs\n")

if len(remaining) == 0:
    print("Nothing to download – all URLs already processed.")
    df_results = pd.read_csv(RESULTS_FILE, low_memory=False)
    print(f"\n=== Total: {len(df_results)} records ===")
    if "year" in df_results.columns:
        print(df_results.groupby(["year", "pollutant"]).agg(
            stations=("sampling_point", "count"),
            countries=("country", "nunique"),
        ).to_string())
    exit(0)

# ── Download loop ────────────────────────────────────────────────────────────
errors = 0
new_results = 0

for idx, row in remaining.iterrows():
    url = row["url"]
    poll = row["pollutant"]
    country = row["country"]

    try:
        r = requests.get(url, timeout=60)
        if r.status_code != 200:
            errors += 1
            done.add(url)
            continue

        df = pd.read_parquet(io.BytesIO(r.content))

        # Sampling point ID
        sp_raw = df["Samplingpoint"].iloc[0]
        sp_id = sp_raw.split("/", 1)[1] if "/" in str(sp_raw) else str(sp_raw)

        # Parse dates + filter validity
        df["Start"] = pd.to_datetime(df["Start"], errors="coerce")
        if "Validity" in df.columns:
            df = df[df["Validity"] == 1]
        df["Value"] = pd.to_numeric(df["Value"], errors="coerce")
        df = df.dropna(subset=["Value", "Start"])
        df["year"] = df["Start"].dt.year
        df = df[df["year"] >= MIN_YEAR]

        if len(df) == 0:
            done.add(url)
            continue

        # Metadata lookup
        station_name = municipality = lat = lon = area = ""
        if sp_id in meta_lookup.index:
            m = meta_lookup.loc[sp_id]
            municipality = str(m.get("Municipality", "")).replace(",", ";")
            station_name = str(m.get("Air Quality Station Name", "")).replace(",", ";")
            lat = m.get("Latitude", "")
            lon = m.get("Longitude", "")
            area = m.get("Air Quality Station Area", "")

        # Compute annual mean for EACH year in the data
        for target_year, df_year in df.groupby("year"):
            annual_mean = df_year["Value"].mean()
            n_obs = len(df_year)
            unit = df_year["Unit"].iloc[0] if "Unit" in df_year.columns else ""
            agg = df_year["AggType"].iloc[0] if "AggType" in df_year.columns else ""

            with open(RESULTS_FILE, "a", encoding="utf-8") as f:
                f.write(f"{poll},{country},{sp_id},{station_name},{municipality},"
                        f"{lat},{lon},{area},{annual_mean:.4f},{n_obs},{unit},{agg},"
                        f"{int(target_year)}\n")
            new_results += 1

    except Exception as e:
        errors += 1

    done.add(url)

    # Checkpoint every 200 files
    if len(done) % 200 == 0:
        with open(PROGRESS_FILE, "w") as f:
            f.write("\n".join(done))
        n_done = len(urls) - len(urls[~urls["url"].isin(done)])
        print(f"  [{n_done}/{len(urls)}] new={new_results} errors={errors}")

    time.sleep(0.1)

# ── Final save ───────────────────────────────────────────────────────────────
with open(PROGRESS_FILE, "w") as f:
    f.write("\n".join(done))

print(f"\nDone: {new_results} new results, {errors} errors")
print(f"Results: {RESULTS_FILE}")

# Summary
df_results = pd.read_csv(RESULTS_FILE, low_memory=False)
print(f"\n=== Total: {len(df_results)} records ===")
if "year" in df_results.columns:
    for y in sorted(df_results["year"].unique()):
        n = len(df_results[df_results["year"] == y])
        print(f"  {int(y)}: {n} records")
    print()
    print(df_results.groupby(["year", "pollutant"]).agg(
        stations=("sampling_point", "count"),
        countries=("country", "nunique"),
    ).to_string())