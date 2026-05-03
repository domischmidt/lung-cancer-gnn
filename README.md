# Graph Neural Networks for Knowledge Discovery in Lung Cancer Data

Master's thesis project applying Relational Graph Convolutional Networks (R-GCN) to the LUCIA Knowledge Graph for predicting biological and environmental associations with lung cancer subtypes.

## Key Results

Multi-seed evaluation across 5 random seeds on Gene-Disease link prediction:

| Model | MRR | Hits@1 | Hits@3 | Hits@10 |
|-------|-----|--------|--------|---------|
| **R-GCN** | **0.360 ± 0.055** | **0.167 ± 0.054** | **0.397 ± 0.107** | **0.859 ± 0.137** |
| TransE | 0.291 ± 0.046 | 0.175 ± 0.048 | 0.298 ± 0.065 | 0.527 ± 0.071 |
| Dot Product | 0.002 ± 0.002 | 0.000 ± 0.000 | 0.000 ± 0.000 | 0.012 ± 0.010 |

R-GCN ranks the correct lung cancer subtype in the top 10 in 86% of cases (out of 43 candidate diseases). The ablation study confirms that adding environmental data improves Gene-Disease MRR by 45% (0.353 → 0.513) compared to a biological-only graph.

Score calibration against 1,000 random false triples per relation yields AUROC 0.933 for Gene-Disease and >0.98 on most other relations.

## Documentation

- **Thesis:** [docs/thesis/](docs/thesis/)
- **Presentation slides:** [docs/presentation/Results_Summary_for_Paloma_Slides.pdf](docs/presentation/Results_Summary_for_Paloma_Slides.pdf)
- **Detailed results report:** [docs/presentation/Results_Summary_for_Paloma.pdf](docs/presentation/Results_Summary_for_Paloma.pdf)
- **KG schema analysis:** [docs/analysis/kg_schema_analysis.md](docs/analysis/kg_schema_analysis.md)

## Knowledge Graph

The LUCIA KG combines biological and environmental data via shared Disease entities:

| Layer | Nodes | Edges | Sources |
|-------|-------|-------|---------|
| Biological | 18,759 | 80,014 | DisGeNET, COSMIC, DISEASES, WikiPathways, Mitelman, MarkerDB |
| Environmental | 172,183 | 538,897 | EEA, ECIS, OECD, CDC |
| **Collapsed graph (used for training)** | **22,553** | **177,712** | After n-ary reification removal |

13 node types, 20 edge types. Disease nodes serve as the structural bridge between the two layers via 35 explicit subtype_of edges connecting specific lung cancer subtypes to the parent term (Malignant neoplasm of lung, UMLS C0242379).

## Project Structure

\`\`\`
lung-cancer-gnn/
├── env_data/                          # Environmental KG extension pipelines
│   ├── src/
│   │   ├── eea/                       #   EEA air quality
│   │   ├── ecis/                      #   ECIS cancer incidence/mortality
│   │   ├── oecd/                      #   OECD pollution exposure per TL2 region
│   │   ├── cdc/                       #   CDC US/PR lung cancer incidence
│   │   ├── shared/                    #   Shared TTL utilities + entity defs
│   │   └── analysis/                  #   A-priori statistical analysis
│   └── data/                          #   raw, interim, processed
│
├── bio_data/                          # Biological SPARQL exports
│   └── data/                          #   7 TTL files
│
├── gnn/                               # GNN pipeline
│   ├── src/                           #   01-09 numerated scripts
│   └── data/                          #   interim, processed
│
├── docs/
│   ├── thesis/
│   ├── presentation/
│   └── analysis/
│
├── requirements.txt
└── README.md
\`\`\`

## Pipeline

\`\`\`bash
python gnn/src/01_parse_kg.py
python gnn/src/02_build_graph.py
python gnn/src/03_train_baselines.py
python gnn/src/04_train_rgcn.py
python gnn/src/05_evaluate.py
python gnn/src/06_predict_novel_links.py
python gnn/src/07_chemical_disease_analysis.py
python gnn/src/08_full_evaluation.py
python gnn/src/09_normalize_scores.py
\`\`\`

## Setup

\`\`\`bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
\`\`\`

The pipeline runs on CPU but training is significantly faster on GPU. R-GCN training in this work used an RTX 3090 (~25 minutes per run, ~4 hours for full multi-seed and ablation experiments).

## About

Master's thesis written at Universidad Politécnica de Madrid (UPM) during an Erasmus exchange from Friedrich-Alexander-Universität Erlangen-Nürnberg (FAU).

**Title:** Knowledge Graph Curation, Extension and Graph Neural Network-based Knowledge Discovery in Lung Cancer Data

**Supervisors:** Antonio Jesús Díaz Honrubia (UPM), Paloma Tejera Nevado (UPM)
