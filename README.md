# Knowledge Graph Curation, Extension and GNN-based Knowledge Discovery in Lung Cancer Data

Master's thesis applying Relational Graph Convolutional Networks (R-GCN) to the LUCIA Knowledge Graph for predicting biological and environmental associations with lung cancer subtypes.

**Author:** Domenic Schmidt
**Programme:** M.Sc. International Information Systems, FAU Erlangen-Nurnberg
**Exchange:** Universidad Politecnica de Madrid (UPM), Erasmus 2025/26
**Supervisors:** Antonio Jesus Diaz Honrubia (UPM), Paloma Tejera Nevado (UPM)

## Key Results

Multi-seed evaluation across 5 random seeds on Gene-Disease link prediction:

| Model | MRR | Hits@1 | Hits@3 | Hits@10 |
|-------|-----|--------|--------|---------|
| R-GCN | 0.360 +/- 0.055 | 0.167 +/- 0.054 | 0.397 +/- 0.107 | 0.859 +/- 0.137 |
| TransE | 0.291 +/- 0.046 | 0.175 +/- 0.048 | 0.298 +/- 0.065 | 0.527 +/- 0.071 |
| Dot Product | 0.002 +/- 0.002 | 0.000 +/- 0.000 | 0.000 +/- 0.000 | 0.012 +/- 0.010 |

R-GCN ranks the correct lung cancer subtype in the top 10 in 86% of cases (out of 43 candidate diseases). The ablation study confirms that adding environmental data improves Gene-Disease MRR by 45% compared to a biological-only graph. Score calibration against 1,000 random false triples per relation yields AUROC 0.933 for Gene-Disease and >0.98 on most other relations.

## Knowledge Graph

The LUCIA KG combines biological and environmental data via shared Disease entities:

| Layer | Nodes | Edges | Sources |
|-------|-------|-------|---------|
| Biological | 18,759 | 80,014 | DisGeNET, COSMIC, DISEASES, WikiPathways, Mitelman, MarkerDB |
| Environmental | 172,183 | 538,897 | EEA, ECIS, OECD, CDC |
| Collapsed (training) | 22,553 | 177,712 | After n-ary reification removal |

13 node types, 20 edge types. Disease nodes serve as the structural bridge between the two layers via 35 subtype_of edges connecting specific lung cancer subtypes to the parent term (Malignant neoplasm of lung, UMLS C0242379).

## Repository Structure

```
domenic_schmidt/
├── data_curation/                     # Part 1: KG extension pipelines
│   ├── scripts/
│   │   ├── shared/                    #   Shared TTL utilities + entity defs
│   │   │   ├── ttl_utils.py
│   │   │   └── generate_shared_ttl.py
│   │   ├── eea/                       #   EEA air quality (10 pollutants, 30 countries)
│   │   │   ├── eea_pipeline.py
│   │   │   └── eea_to_ttl.py
│   │   ├── ecis/                      #   ECIS cancer incidence/mortality
│   │   │   ├── ecis_pipeline.py
│   │   │   └── ecis_to_ttl.py
│   │   ├── oecd/                      #   OECD pollution exposure per TL2 region
│   │   │   ├── oecd_exposure_pipeline.py
│   │   │   └── oecd_exposure_to_ttl.py
│   │   ├── cdc/                       #   CDC US lung cancer incidence
│   │   │   └── cdc_to_ttl.py
│   │   └── a_priori_analysis.py       #   Spearman correlation analysis
│   ├── outputs/
│   │   ├── csv/                       #   Final processed CSVs
│   │   └── ttl/                       #   Generated TTL graphs
│   └── figures/
│
├── gnn/                               # Part 2: GNN link prediction pipeline
│   ├── scripts/
│   │   ├── 01_parse_kg.py             #   TTL parsing to unified triples
│   │   ├── 02_build_graph.py          #   Triples to PyG HeteroData
│   │   ├── 03_train_baselines.py      #   TransE + DistMult baselines
│   │   ├── 04_hyperparam_search.py    #   Optuna hyperparameter optimization
│   │   ├── 05_train_rgcn.py           #   R-GCN training
│   │   ├── 06_evaluate.py             #   MRR, Hits@k evaluation
│   │   ├── 07_predict_novel_links.py  #   Novel link prediction
│   │   ├── 07_environmental_predictions.py  # Environmental association scoring
│   │   ├── 08_chemical_disease_analysis.py  # Chemical-Disease analysis
│   │   ├── 09_full_evaluation.py      #   Full evaluation suite
│   │   ├── 09_full_evaluation_extra_seeds.py  # Multi-seed robustness
│   │   ├── 10_normalize_scores.py     #   Score calibration + AUROC
│   │   ├── run_stability_seed.sh      #   Shell script for stability runs
│   │   ├── fix_figures.py             #   Figure label corrections
│   │   └── visualize_kg_schema.py     #   KG schema diagram generator
│   ├── outputs/
│   └── figures/
│
├── requirements.txt
└── README.md
```

## Data Curation Pipeline

Each source follows a two-step pattern: `*_pipeline.py` (raw data to clean CSV) and `*_to_ttl.py` (CSV to RDF/TTL conforming to the LUCIA ontology).

Sources and entity counts:
- EEA: 117,680 ChemicalLocationAssociation entities (2013-2024, 10 pollutants, 30 countries)
- ECIS: 324 VitalStatistics (2024 incidence + mortality, country-level)
- OECD: 14,001 ChemicalLocationAssociation entities (1990-2023, PM2.5/PM10/NO2, 440 TL2 regions)
- CDC: US and Puerto Rico lung cancer incidence
- Shared: CalendarYear and Chemical entities used across sources

## GNN Pipeline

```bash
python gnn/scripts/01_parse_kg.py
python gnn/scripts/02_build_graph.py
python gnn/scripts/03_train_baselines.py
python gnn/scripts/04_hyperparam_search.py
python gnn/scripts/05_train_rgcn.py
python gnn/scripts/06_evaluate.py
python gnn/scripts/07_predict_novel_links.py
python gnn/scripts/08_chemical_disease_analysis.py
python gnn/scripts/09_full_evaluation.py
python gnn/scripts/10_normalize_scores.py
```

For multi-seed stability analysis:
```bash
bash gnn/scripts/run_stability_seed.sh 42
bash gnn/scripts/run_stability_seed.sh 123
# ... etc.
python gnn/scripts/09_full_evaluation_extra_seeds.py
```

## Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Training runs on CPU but is significantly faster on GPU. R-GCN training used an RTX 3090 (approximately 25 minutes per run, approximately 4 hours for full multi-seed and ablation experiments).

## Related Repositories

- GitHub (development): https://github.com/domischmidt/lung-cancer-gnn
- SPARQL endpoint: http://138.4.130.153:8890/sparql
