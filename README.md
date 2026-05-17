# Knowledge Graph Curation, Extension and GNN-based Knowledge Discovery in Lung Cancer Data

Master's thesis applying Relational Graph Convolutional Networks (R-GCN) to the LUCIA Knowledge Graph for predicting biological and environmental associations with lung cancer subtypes.

**Author:** Domenic Schmidt
**Programme:** M.Sc. International Information Systems, FAU Erlangen-Nurnberg
**Exchange:** Universidad Politecnica de Madrid (UPM), Erasmus 2025/26
**Supervisors:** Antonio Jesus Diaz Honrubia (UPM), Paloma Tejera Nevado (UPM)

## Key Findings

### Environmental Predictions

The R-GCN learned meaningful Chemical-Disease associations from the extended Knowledge Graph. Ranking pollutants by mean cosine similarity to lung cancer subtypes:

| Pollutant | Mean Similarity | Strongest Association |
|-----------|----------------|-----------------------|
| PM10 Nickel | 0.432 | Lung Neoplasms (0.685) |
| PM2.5 | 0.429 | Giant cell carcinoma (0.601) |
| PM10 Cadmium | 0.411 | Lung Neoplasms (0.776) |
| Benzene | 0.405 | Giant cell carcinoma (0.537) |
| PM10 Arsenic | 0.357 | Lung Neoplasms (0.709) |
| PM10 Lead | 0.344 | Bronchioloalveolar Adenocarcinoma (0.602) |
| Benzo[a]pyrene | 0.313 | Carcinoma in situ (0.443) |
| NO2 | 0.294 | Squamous cell carcinoma (0.455) |
| PM10 | 0.292 | Squamous cell carcinoma (0.456) |
| Ozone | 0.192 | Squamous cell carcinoma (0.372) |

Heavy metal components in PM10 (Cadmium, Arsenic, Nickel) show the strongest disease-specific associations, particularly with Lung Neoplasms and Bronchioloalveolar Adenocarcinoma. The temporal trend analysis shows a gradual increase in mean predicted Disease-VitalStatistics scores from 0.587 (1990) to 0.671 (2023), with Lithuania, Latvia, and Liechtenstein consistently scoring highest among countries.

### GNN Link Prediction

Multi-seed evaluation (5 seeds) on Gene-Disease link prediction:

| Model | MRR | Hits@1 | Hits@3 | Hits@10 |
|-------|-----|--------|--------|---------|
| R-GCN | 0.360 +/- 0.055 | 0.167 +/- 0.054 | 0.397 +/- 0.107 | 0.859 +/- 0.137 |
| TransE | 0.291 +/- 0.046 | 0.175 +/- 0.048 | 0.298 +/- 0.065 | 0.527 +/- 0.071 |

R-GCN ranks the correct lung cancer subtype in the top 10 in 86% of cases (out of 43 candidate diseases). The ablation study confirms that adding environmental data improves Gene-Disease MRR by 45% (0.353 to 0.513) compared to a biological-only graph. Score calibration yields AUROC 0.933 for Gene-Disease and >0.98 on most other relations.

## Knowledge Graph

The LUCIA KG combines biological and environmental data via shared Disease entities:

| Layer | Nodes | Edges | Sources |
|-------|-------|-------|---------|
| Biological | 18,759 | 80,014 | DisGeNET, COSMIC, DISEASES, WikiPathways, Mitelman, MarkerDB |
| Environmental | 172,183 | 538,897 | EEA, ECIS, OECD, CDC |
| Collapsed (training) | 22,553 | 177,712 | After n-ary reification removal |

13 node types, 20 edge types. Disease nodes bridge the two layers via 35 subtype_of edges connecting specific lung cancer subtypes to the parent term (Malignant neoplasm of lung, UMLS C0242379).

## Repository Structure

```
domenic_schmidt/
├── data_curation/                         # Part 1: KG extension
│   ├── scripts/
│   │   ├── shared/                        #   TTL utilities + entity definitions
│   │   ├── eea/                           #   EEA air quality (10 pollutants, 30 countries)
│   │   ├── ecis/                          #   ECIS cancer incidence/mortality
│   │   ├── oecd/                          #   OECD pollution exposure (TL2 regions)
│   │   ├── cdc/                           #   CDC US lung cancer incidence
│   │   └── a_priori_analysis.py           #   Spearman correlation analysis
│   ├── outputs/
│   │   ├── csv/                           #   Final processed CSVs
│   │   └── ttl/                           #   Generated RDF/TTL graphs
│   └── figures/
│
├── gnn/                                   # Part 2: GNN link prediction
│   ├── scripts/
│   │   ├── 01_parse_kg.py                 #   TTL parsing to unified triples
│   │   ├── 02_build_graph.py              #   Triples to PyG HeteroData
│   │   ├── 03_train_baselines.py          #   TransE + DistMult baselines
│   │   ├── 04_hyperparam_search.py        #   Optuna hyperparameter optimization
│   │   ├── 05_train_rgcn.py               #   R-GCN training
│   │   ├── 06_evaluate.py                 #   MRR, Hits@k evaluation
│   │   ├── 07_predict_novel_links.py      #   Novel link prediction
│   │   ├── 07_environmental_predictions.py #  Environmental association scoring
│   │   ├── 08_chemical_disease_analysis.py #  Chemical-Disease embedding analysis
│   │   ├── 09_full_evaluation.py          #   Full evaluation suite
│   │   ├── 09_full_evaluation_extra_seeds.py # Multi-seed robustness
│   │   ├── 10_normalize_scores.py         #   Score calibration + AUROC
│   │   ├── run_stability_seed.sh          #   Stability run shell script
│   │   ├── fix_figures.py                 #   Figure label corrections
│   │   └── visualize_kg_schema.py         #   KG schema diagram generator
│   ├── outputs/
│   └── figures/
│
├── requirements.txt
└── README.md
```

## Data Curation Pipeline

Each source follows a two-step pattern: `*_pipeline.py` (raw to CSV) and `*_to_ttl.py` (CSV to RDF/TTL).

Entity counts per source:
- EEA: 117,680 ChemicalLocationAssociation entities (2013-2024, 10 pollutants, 30 countries)
- ECIS: 324 VitalStatistics (2024, country-level incidence + mortality)
- OECD: 14,001 ChemicalLocationAssociation entities (1990-2023, PM2.5/PM10/NO2, 440 TL2 regions)
- CDC: US and Puerto Rico lung cancer incidence
- Shared: CalendarYear and Chemical entities across sources

## GNN Pipeline

```bash
python gnn/scripts/01_parse_kg.py
python gnn/scripts/02_build_graph.py
python gnn/scripts/03_train_baselines.py
python gnn/scripts/04_hyperparam_search.py
python gnn/scripts/05_train_rgcn.py
python gnn/scripts/06_evaluate.py
python gnn/scripts/07_predict_novel_links.py
python gnn/scripts/07_environmental_predictions.py
python gnn/scripts/08_chemical_disease_analysis.py
python gnn/scripts/09_full_evaluation.py
python gnn/scripts/10_normalize_scores.py
```

Multi-seed stability analysis:
```bash
bash gnn/scripts/run_stability_seed.sh 42
bash gnn/scripts/run_stability_seed.sh 123
python gnn/scripts/09_full_evaluation_extra_seeds.py
```

## Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Training runs on CPU but is significantly faster on GPU. R-GCN training used an NVIDIA RTX 3090 (approximately 25 min/run, approximately 4h for full multi-seed + ablation experiments).

## Related

- Development repo: https://github.com/domischmidt/lung-cancer-gnn
- SPARQL endpoint: http://138.4.130.153:8890/sparql
