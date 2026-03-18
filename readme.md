# Graph Neural Networks for Knowledge Discovery in Lung Cancer Data

Master's Thesis – M.Sc. International Information Systems (FAU Erlangen-Nürnberg)

**Author:** Domenic Schmidt  
**Supervisor:** Prof. Antonio Guillen Honrubia (UPM)  
**Co-supervisor:** Paloma Tejera Nevado (UPM)

## Overview

This repository contains the data pipelines for extending the LUCIA Knowledge Graph with environmental and epidemiological data from three sources (EEA, ECIS, OECD) plus corrections to the existing CDC data.

## Repository Structure

```
lung-cancer-gnn/
├── data/
│   ├── raw/                          # Source CSVs (downloads)
│   │   ├── eea_urls.csv              # EEA parquet file URLs
│   │   ├── eea_urls_additional.csv   # EEA additional pollutant URLs
│   │   ├── eea_metadata.csv          # EEA station metadata
│   │   ├── eurostat_lau.xlsx         # Eurostat LAU population (Jan 2024)
│   │   ├── ecis_incidence_*.csv      # ECIS 2024 incidence (6 age groups)
│   │   ├── ecis_mortality_*.csv      # ECIS 2024 mortality (6 age groups)
│   │   ├── ecis_final_2022.csv       # ECIS 2022 data (from drive.upm.es)
│   │   ├── oecd_exposure.csv         # OECD air pollution exposure
│   │   ├── oecd_population.csv       # OECD Regional Demography TL2 (1990-2023)
│   │   └── CDC_Final.csv             # CDC lung cancer incidence (US + Puerto Rico)
│   ├── interim/                      # Intermediate EEA processing files
│   └── processed/                    # Final CSVs and TTL files
│       ├── eea_final.csv
│       ├── ecis_final.csv
│       ├── oecd_exposure_final.csv
│       ├── graph_EEA.ttl             # >100MB, only on GitLab
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
│   │   ├── ecis_pipeline.py          # Merge incidence + mortality CSVs (2024)
│   │   └── ecis_to_ttl.py            # CSV → TTL (combines 2022 + 2024)
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
- **Content:** Air quality measurements at city/station level
- **Pollutants:** PM2.5, PM10, NO2, O3, BaP, C6H6, As in PM10, Cd in PM10, Pb in PM10, Ni in PM10
- **Coverage:** 30 European countries, 2013–2024
- **Population:** Eurostat LAU (reference date: January 2024, 89.7% match)
- **Location type:** `sio:SIO_000415` (Geopolitical Region)
- **Output:** 117,609 ChemicalLocationAssociation instances, 27,809 Population entities

### ECIS (European Cancer Information System)
- **Content:** Lung cancer incidence and mortality rates
- **Coverage:** 27–28 EU countries, 2022 + 2024
- **Granularity:** Country-level, per gender and age group (15-year intervals)
- **Output:** 580 VitalStatistics instances (256 × 2022 + 324 × 2024)

### OECD (Organisation for Economic Co-operation and Development)
- **Content:** Air pollution exposure at regional level
- **Pollutants:** PM2.5, PM10, NO2
- **Coverage:** 40 countries, 440 TL2 regions, 1990–2023
- **Population:** OECD Regional Demography TL2 (year-matched, 97% coverage)
- **Location type:** `sio:SIO_000414` (Geographic Region)
- **Output:** 14,001 ChemicalLocationAssociation instances, 11,067 Population entities

### CDC (Centers for Disease Control and Prevention)
- **Content:** Lung cancer incidence (US + Puerto Rico)
- **Coverage:** 1999–2021, by age group, gender, ethnicity
- **Fix applied:** Year-separated VitalStatistics instances (was: all years merged in one)
- **Output:** 1,184 VitalStatistics instances

### Shared Entities
- **CalendarYear:** 2023, 2024 (years 1990–2022 already exist in ontology)
- **Chemicals:** 5 PM10 variants (PM2.5, BaP, C6H6, NO2, O3, NOx already exist)

## Ontology Compliance

All TTL files follow the LUCIA ontology (Environmental_Ontology.pdf):
- Population modeled as separate entity (`sio:SIO_001061`) with `sio:SIO_000300` (has value), linked via `sio:SIO_000216` (has measurement value)
- Disease → `sio:SIO_000300` → VitalStatistics (not the other way around)
- Existing entities (Countries, CalendarYears, Chemicals, Sources) are referenced, not redefined
- VitalStatistics URIs include the year to prevent RDF triple merging across time periods

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

- **GitHub:** [github.com/domischmidt/lung-cancer-gnn](https://github.com/domischmidt/lung-cancer-gnn) (source code + small files)
- **GitLab UPM:** medal.ctb.upm.es (deliverables including large TTL files)

## Supervisor

Prof. Antonio Honrubia – Universidad Politécnica de Madrid