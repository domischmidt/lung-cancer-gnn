# Graph Neural Networks for Knowledge Discovery in Lung Cancer Data

Master's thesis project applying Relational Graph Convolutional Networks (R-GCN) to the LUNG-CABO knowledge graph to discover new biological and environmental risk factors for lung cancer.

## Knowledge Graph Extension

This repository contains data acquisition, cleaning, and TTL conversion pipelines for three environmental/epidemiological data sources that extend the existing LUNG-CABO knowledge graph.

### Data Sources

| Source | Description | Coverage | Years |
|--------|-------------|----------|-------|
| **EEA** | European Environment Agency – Air quality measurements at city level | 30 countries, 3169 cities, 10 pollutants | 2013–2024 |
| **OECD** | OECD Regional Air Pollution Exposure (TL2 regions) | 40 countries, 440 regions, 3 pollutants | 2021–2023 |
| **ECIS** | European Cancer Information System – Lung cancer incidence and mortality | EU-27, 6 age groups, 2 genders | 2024 |

### Pollutants

- **Primary:** PM2.5, PM10, NO2, O3
- **Heavy metals in PM10:** Arsenic (As), Cadmium (Cd), Nickel (Ni), Lead (Pb)
- **Other:** Benzo[a]pyrene (BaP), Benzene (C6H6)

## Project Structure

```
src/
├── eea/
│   ├── 01_collect_urls.py          # Collect parquet file URLs from EEA API
│   ├── 02_download_parquets.py     # Download + compute annual means (all years)
│   ├── eea_pipeline.py             # Merge with Eurostat LAU population data
│   └── eea_to_ttl.py               # Convert to RDF/Turtle
├── oecd/
│   ├── oecd_exposure_pipeline.py   # Clean OECD regional exposure data
│   └── oecd_exposure_to_ttl.py     # Convert to RDF/Turtle
├── ecis/
│   ├── ecis_pipeline.py            # Clean ECIS cancer statistics
│   └── ecis_to_ttl.py              # Convert to RDF/Turtle
└── shared/
    ├── ttl_utils.py                # Shared URI builders, UMLS IDs, existing entities
    └── generate_shared_ttl.py      # Generate shared CalendarYear + Chemical entities

data/
├── raw/            # Original downloads (CSV, XLSX, Parquet)
├── interim/        # Intermediate results (EEA annual means)
└── processed/      # Final CSVs + TTL files for KG ingestion
```

## Pipeline Workflow

```
Raw data (API / manual download)
        │
        ▼
  *_pipeline.py  →  Cleaning, matching, aggregation
        │
        ▼
  *_final.csv    →  Standardized CSV
        │
        ▼
  *_to_ttl.py    →  RDF/Turtle conversion
        │
        ▼
  graph_*.ttl    →  Load into Virtuoso triplestore
```

## Ontology Alignment

TTL files follow the LUCIA ontology conventions:
- **Existing entities** (Countries, CalendarYears, Chemicals, Sources) are only referenced, not redefined
- **New shared entities** (CalendarYear 2023/2024, PM10 chemical variants) are defined in `graph_shared.ttl`
- **City URIs** include year to support year-dependent population values
- **Chemical IDs** use UMLS identifiers (e.g. C5890534 for PM2.5, C1720884_10 for PM10)

## Output TTL Files

| File | Instances | Description |
|------|-----------|-------------|
| `graph_shared.ttl` | 7 | CalendarYear 2023+2024, 5 PM10 chemicals |
| `graph_EEA.ttl` | 117,795 CLA | City-level air quality across Europe |
| `graph_OECD.ttl` | 3,996 CLA | Regional air pollution exposure (OECD) |
| `graph_ECIS.ttl` | 324 VitalStats | Lung cancer incidence and mortality (EU-27) |

## Setup

```bash
python -m venv .venv
source .venv/bin/activate   # Linux/Mac
.venv\Scripts\activate      # Windows
pip install -r requirements.txt
```

## Supervisors

- **Prof. Antonio Honrubia** – Universidad Politécnica de Madrid (UPM)
- **Paloma Tejera Nevado** – UPM (co-supervisor)

## Author

Domenic Schmidt – M.Sc. International Information Systems, FAU Erlangen-Nürnberg