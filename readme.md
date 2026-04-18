# Graph Neural Networks for Knowledge Discovery in Lung Cancer Data

Master's thesis project applying Relational Graph Convolutional Networks (R-GCN) to the Lung-CABO knowledge graph for discovering new biological and environmental risk factors for lung cancer.

## Key Results

| Model | Gene-Disease MRR | Gene-Disease Hits@10 |
|-------|-----------------|---------------------|
| TransE | 0.227 +/- 0.050 | 0.540 +/- 0.116 |
| DistMult | 0.046 +/- 0.009 | 0.178 +/- 0.055 |
| **R-GCN** | **0.470 +/- 0.068** | **0.890 +/- 0.059** |

R-GCN outperforms shallow embedding baselines on Gene-Disease association prediction by leveraging pathway neighborhood and environmental context through message passing. The ablation study confirms that the combined biological + environmental graph (MRR 0.495) outperforms the biological-only graph (MRR 0.451).

## Project Structure

```
lung-cancer-gnn/
├── env_data/                          # Environmental KG extension pipelines
│   ├── src/
│   │   ├── eea/                       #   EEA air quality (PM2.5, O3, NO2, ...)
│   │   ├── ecis/                      #   ECIS cancer mortality/incidence
│   │   ├── oecd/                      #   OECD pollution exposure per TL2 region
│   │   ├── cdc/                       #   CDC US lung cancer statistics
│   │   └── shared/                    #   Shared TTL utilities + entity defs
│   └── data/
│       ├── raw/                       #   Source CSVs
│       └── processed/                 #   Final CSVs + TTL files (5 graphs)
│
├── bio_data/                          # Biological SPARQL exports from Virtuoso
│   └── data/                          #   7 TTL files (GDA, VDA, Pathways, ...)
│
├── gnn/                               # GNN pipeline
│   ├── src/
│   │   ├── 01_parse_kg.py             #   Parse all TTLs -> nodes.csv, edges.csv
│   │   ├── 02_build_graph.py          #   Collapse n-ary relations -> PyG HeteroData
│   │   ├── 03_train_baselines.py      #   TransE + DistMult baselines
│   │   ├── 04_train_rgcn.py           #   R-GCN with DistMult decoder
│   │   ├── 05_evaluate.py             #   Final comparison tables + figures
│   │   ├── 06_predict_novel_links.py  #   Novel link predictions (all 15 edge types)
│   │   ├── 07_chemical_disease_analysis.py  # Cross-domain: pollution vs cancer
│   │   └── 08_full_evaluation.py      #   Multi-seed, ablation, significance test
│   └── data/
│       ├── interim/                   #   Parsed node/edge lists + figures
│       └── processed/                 #   PyG graph, model weights, predictions
│
├── docs/
│   └── kg_schema_analysis.md          # KG topology documentation
└── readme.md
```

## Knowledge Graph

The Lung-CABO KG combines biological and environmental data:

| Layer | Nodes | Edges | Sources |
|-------|-------|-------|---------|
| Biological | 18,759 | 80,014 | DisGeNET, COSMIC, DISEASES, WikiPathways, Mitelman, MarkerDB |
| Environmental | 172,183 | 538,897 | EEA, ECIS, OECD, CDC |
| **Collapsed** | **22,481** | **248,773** | After n-ary reification removal |

12 node types, 15 edge types. Disease nodes bridge both layers.

## Pipeline

Run scripts sequentially from the repo root:

```bash
python gnn/src/01_parse_kg.py        # ~30s, parse TTLs
python gnn/src/02_build_graph.py     # ~10s, build PyG graph
python gnn/src/03_train_baselines.py # ~20min, TransE + DistMult
python gnn/src/04_train_rgcn.py      # ~25min, R-GCN
python gnn/src/05_evaluate.py        # ~5s, comparison figures
python gnn/src/06_predict_novel_links.py  # ~15min, novel predictions
python gnn/src/07_chemical_disease_analysis.py  # ~2min, cross-domain
python gnn/src/08_full_evaluation.py # ~4h, multi-seed + ablation
```

## Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install rdflib matplotlib torch torch-geometric
pip install torch-scatter torch-sparse
```

## About

Master's thesis at FAU Erlangen-Nurnberg (International Information Systems) and UPM Madrid (Master Universitario en Innovacion Digital).

Thesis: "Knowledge Graph Curation, Extension and Graph Neural Network-based Knowledge Discovery in Lung Cancer Data"
