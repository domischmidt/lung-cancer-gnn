# Graph Neural Networks for Knowledge Discovery in Lung Cancer Data

Master's Thesis – M.Sc. International Information Systems (FAU Erlangen-Nürnberg)

**Author:** Domenic Schmidt  
**Supervisor:** Prof. Antonio Honrubia (UPM)  
**Co-supervisor:** Paloma Tejera Nevado (UPM)

## Overview

This repository contains the data pipelines for extending the LUCIA Knowledge Graph with environmental and epidemiological data from four sources (EEA, OECD, ECIS, CDC), plus the groundwork for link prediction using Graph Neural Networks.

## Repository Structure

```
lung-cancer-gnn/
├── data/
│   ├── raw/
│   │   ├── eea_urls.csv              # EEA parquet file URLs
│   │   ├── eea_urls_additional.csv   # EEA additional pollutant URLs
│   │   ├── eea_metadata.csv          # EEA station metadata
│   │   ├── eurostat_lau.xlsx         # Eurostat LAU population (Jan 2024)
│   │   ├── ecis_historical/          # ECIS historical CSVs (32 countries)
│   │   │   ├── AT.csv
│   │   │   ├── BE.csv
│   │   │   └── ...                   # 32 files total
│   │   ├── ecis_incidence_*.csv      # ECIS 2024 incidence estimates (6 age groups)
│   │   ├── ecis_mortality_*.csv      # ECIS 2024 mortality estimates (6 age groups)
│   │   ├── oecd_exposure.csv         # OECD air pollution exposure
│   │   ├── oecd_population.csv       # OECD Regional Demography TL2 (1990–2023)
│   │   └── CDC_Final.csv             # CDC lung cancer statistics (US + Puerto Rico)
│   ├── interim/                      # Intermediate EEA processing files
│   └── processed/                    # Final CSVs and TTL files
│       ├── eea_final.csv
│       ├── ecis_final.csv
│       ├── oecd_exposure_final.csv
│       ├── graph_EEA.ttl             # ~96 MB
│       ├── graph_ECIS.ttl
│       ├── graph_OECD.ttl
│       ├── graph_CDC.ttl
│       └── graph_shared.ttl
├── src/
│   ├── shared/
│   │   ├── ttl_utils.py              # URI builders, constants, existing entities
│   │   └── generate_shared_ttl.py    # CalendarYear + Chemical shared entities
│   ├── eea/
│   │   ├── 01_collect_urls.py        # Collect parquet URLs from EEA API
│   │   ├── 02_download_parquets.py   # Download parquets + compute annual means
│   │   ├── eea_pipeline.py           # Population matching + city normalization
│   │   └── eea_to_ttl.py             # CSV → TTL conversion
│   ├── ecis/
│   │   ├── ecis_pipeline.py          # Combine historical (1953–2023) + 2024 estimates
│   │   └── ecis_to_ttl.py            # CSV → TTL with registry-level support
│   ├── oecd/
│   │   ├── oecd_exposure_pipeline.py # Clean exposure data + population merge
│   │   └── oecd_exposure_to_ttl.py   # CSV → TTL conversion
│   └── cdc/
│       └── cdc_to_ttl.py             # Fix CDC: year-separated VitalStatistics
├── readme.md
└── requirements.txt
```

## Data Sources

### EEA (European Environment Agency)
- **Content:** Air quality measurements at city/municipality level
- **Pollutants:** PM2.5, PM10, NO2, O3, BaP, C6H6, As/Cd/Ni/Pb in PM10
- **Coverage:** 30 European countries, 2013–2024, 3,144 cities
- **Population:** Eurostat LAU (January 2024), 2,791 cities matched (88.8%)
- **Location type:** `sio:SIO_000415` (Geopolitical Region)
- **URIs:** `country/gpr/<CC>_<city>`, population: `country/gpr/population/<city>_2024`
- **Output:** 3,144 Geopolitical Region + 2,791 Population + 117,609 CLA instances

### OECD (Exposure to Air Pollution)
- **Content:** Regional population exposure to air pollution (TL2 level)
- **Pollutants:** PM2.5 (1990–2023), PM10 and NO2 (2021–2023)
- **Coverage:** 40 countries, 444 TL2 regions
- **Population:** OECD Regional Demography TL2 (2023), 439 regions matched (98.9%)
- **Location type:** `sio:SIO_000414` (Geographic Region)
- **URIs:** `country/gr/<CC>_<region>`, population: `country/gr/population/<region>_2023`
- **Output:** 444 Geographic Region + 439 Population + 14,001 CLA instances

### ECIS (European Cancer Information System)
- **Content:** Lung cancer incidence and mortality rates (ICD C33-C34)
- **Historical:** 1953–2023, 32 countries, 128 registries (national + regional), Male + Female
- **2024 estimates:** 27 EU countries (national level)
- **Note:** Historical data labelled "Lung", 2024 estimates labelled "Trachea, bronchus and lung" – both refer to ICD C33-C34
- **Age groups:** 0-14, 15-29, 30-44, 45-59, 60-74, 75-85+ (historical 75-89/90-95+ mapped to 75-85+)
- **Registries:** 106 regional registries modelled as Geographic Region (`SIO_000414`) → Country
- **Output:** 106 Registry entities + 32,304 VitalStatistics (9,228 national + 23,076 regional)

### CDC (Centers for Disease Control and Prevention)
- **Content:** Lung cancer mortality counts (US + Puerto Rico)
- **Coverage:** 1999–2021, 15 age groups, 2 genders, 2 ethnicities
- **Fix applied:** Year-separated VitalStatistics instances (was: all years merged)
- **Output:** 1,184 VitalStatistics + 54 People entities

### Shared Entities (graph_shared.ttl)
- **CalendarYear:** 2023, 2024 (1990–2022 already exist)
- **Chemicals:** 5 PM10 variants (PM2.5, BaP, C6H6, NO2, O3 already exist)
- **Countries:** KR (Korea) in graph_OECD.ttl, XK (Kosovo) in graph_EEA.ttl

## Ontology Compliance

All TTL files follow the LUCIA ontology:
- One instance per city/region (no year in location URI)
- Population as separate entity (`sio:SIO_001061`) with `sio:SIO_000300` (value) and `sio:SIO_000679` (CalendarYear)
- Population linked from region via `sio:SIO_000216` (has measurement value)
- Disease → `sio:SIO_000300` → VitalStatistics
- Existing entities (Countries, CalendarYears, Chemicals, Sources) referenced, not redefined
- VitalStatistics URIs include year for uniqueness

## Pipeline Workflow

```bash
# EEA
python src/eea/01_collect_urls.py
python src/eea/02_download_parquets.py
python src/eea/eea_pipeline.py
python src/eea/eea_to_ttl.py

# ECIS
python src/ecis/ecis_pipeline.py
python src/ecis/ecis_to_ttl.py

# OECD
python src/oecd/oecd_exposure_pipeline.py
python src/oecd/oecd_exposure_to_ttl.py

# CDC
python src/cdc/cdc_to_ttl.py

# Shared
python src/shared/generate_shared_ttl.py
```

## Repositories

- **GitHub:** [github.com/domischmidt/lung-cancer-gnn](https://github.com/domischmidt/lung-cancer-gnn)
- **GitLab UPM:** medal.ctb.upm.es/internal/gitlab/lucia/domenic_schmidt