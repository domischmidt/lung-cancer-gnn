# Knowledge Graph Curation, Extension and GNN-based Knowledge Discovery in Lung Cancer Data

Master's thesis applying Relational Graph Convolutional Networks (R-GCN) to the LUCIA Knowledge Graph for predicting biological and environmental associations with lung cancer subtypes.

**Author:** Domenic Schmidt
**Programme:** M.Sc. International Information Systems, FAU Erlangen-Nurnberg
**Exchange:** Universidad Politecnica de Madrid (UPM), Erasmus 2025/26
**Supervisors:** Antonio Jesus Diaz Honrubia (UPM), Paloma Tejera Nevado (UPM)

## Key Findings

### Environmental Predictions

The R-GCN learned a structured Chemical-Disease embedding space. Pollutants ranked by mean cosine similarity to lung cancer subtypes:

| Pollutant      | Mean Similarity | Strongest Association                     |
| -------------- | --------------- | ----------------------------------------- |
| PM10 Nickel    | 0.432           | Lung Neoplasms (0.685)                    |
| PM2.5          | 0.429           | Giant cell carcinoma (0.601)              |
| PM10 Cadmium   | 0.411           | Lung Neoplasms (0.776)                    |
| Benzene        | 0.405           | Giant cell carcinoma (0.537)              |
| PM10 Arsenic   | 0.357           | Lung Neoplasms (0.709)                    |
| PM10 Lead      | 0.345           | Bronchioloalveolar Adenocarcinoma (0.602) |
| Benzo[a]pyrene | 0.313           | Carcinoma in situ (0.443)                 |
| NO2            | 0.294           | Squamous cell carcinoma (0.455)           |
| PM10           | 0.292           | Squamous cell carcinoma (0.456)           |
| Ozone          | 0.192           | Squamous cell carcinoma (0.372)           |

Heavy metal components in PM10 (cadmium, arsenic, nickel) sit closest to Lung Neoplasms, while gaseous pollutants (NO2, PM10, ozone) cluster around squamous cell carcinoma, consistent with epidemiological evidence linking traffic-related air pollution to squamous histology. The differentiation between pollutant classes emerged from the graph structure alone. The similarity reflects the structural position of entities in the embedding space rather than a direct causal mechanism, so the rankings are best read as hypothesis-generating candidates for biological follow-up. The consistent top ranking of known carcinogenic heavy metals provides face validity for the learned representations.

The model produced 6,661 novel Chemical-Region predictions (cosine similarity above 0.3), identifying regions where a pollutant is predicted but not yet measured. Benzo[a]pyrene dominates with 2,184 novel regions, concentrated in small and medium-sized towns in Poland, Czech Republic, Sweden and Finland, many in areas associated with residential solid-fuel combustion or legacy industry. A temporal analysis scores Disease-VitalStatistics associations per year and country; the Baltic states (Lithuania, Latvia, Estonia) consistently receive the highest predicted scores across all years. These scores also correlate with registry coverage (years with more records score higher), so the temporal variation should not be read as an epidemiological trend.

### GNN Link Prediction

Multi-seed evaluation (5 seeds: 42, 123, 456, 789, 1337). Overall metrics across all relation types:

| Model       | MRR   | Hits@1 | Hits@3 | Hits@10 |
| ----------- | ----- | ------ | ------ | ------- |
| R-GCN       | 0.301 | 0.186  | 0.351  | 0.547   |
| TransE      | 0.263 | 0.154  | 0.277  | 0.556   |
| Dot Product | 0.227 | 0.133  | 0.246  | 0.445   |

R-GCN is the strongest model overall. On the key relation, Gene-Disease association (which of 43 candidate lung cancer subtypes a gene-disease association links to), R-GCN reaches a mean MRR of 0.357 (TransE 0.284, Dot Product 0.192). Per-seed variance is high (std around 0.09) because each random split yields only 10 to 17 Gene-Disease test triples.

### Ablation: does environmental data help gene-disease prediction?

R-GCN trained on three graph variants, Gene-Disease MRR (5 seeds):

| Configuration | Gene-Disease MRR | Description                                    |
| ------------- | ---------------- | ---------------------------------------------- |
| Bio-only      | 0.355 +/- 0.036  | 13 biological edge types only                  |
| Combined      | 0.322 +/- 0.019  | All 24 edge types (biological + environmental) |
| Env-only      | 0.124 +/- 0.068  | Environmental edge types + subtype_of bridge   |

Within the 2-layer R-GCN, adding environmental data does not improve gene-disease prediction; Bio-only is marginally higher than Combined (within one standard deviation), and Env-only is far weaker. This is a structural effect: the shortest Chemical-to-Disease path spans 4 hops and exceeds the 2-hop receptive field that the hyperparameter search identified as optimal (deeper models oversmooth). The environmental predictions above operate within the environmental subgraph and are not affected by this bottleneck.

## Knowledge Graph

The LUCIA KG combines biological and environmental data via shared Disease entities. Subgraph sizes after parsing:

| Layer           | Nodes   | Edges   | Sources                                            |
| --------------- | ------- | ------- | -------------------------------------------------- |
| Biological      | 18,759  | 80,014  | DisGeNET, COSMIC, WikiPathways, Mitelman, MarkerDB |
| Environmental   | 172,183 | 538,897 | EEA, ECIS, OECD, CDC                               |
| Merged (parsed) | 190,941 | 618,946 | After unifying shared Disease entities             |

The final training graph preserves n-ary reifications as first-class nodes (Chemical-Location Associations, Vital Statistics, Gene-Disease Associations, Variant-Disease Associations carry their literal values as node features). After creating GDA/VDA nodes and dropping Source and Population nodes, the training graph has 206,151 nodes and 631,659 edges across 18 node types and 24 edge types.

Disease nodes bridge the two layers via 35 `subtype_of` edges connecting specific lung cancer subtypes to the parent term (Malignant neoplasm of lung, UMLS C0242379).

## Data Curation

Each environmental source follows a two-step pattern: `*_pipeline.py` (raw to CSV) and `*_to_ttl.py` (CSV to RDF/TTL). Entity counts per source:

- **EEA:** 117,609 ChemicalLocationAssociation, 3,143 GeoPoliticalRegion, 2,791 Population (2013-2024, 10 pollutants, 30 countries)
- **ECIS:** 32,304 VitalStatistics (9,228 national-level + 23,076 registry-level), 35 countries, 106 registries, 1953-2024
- **OECD:** 14,001 ChemicalLocationAssociation, 444 GeographicRegion (1990-2023 PM2.5, 2021-2023 PM10/NO2, TL2 regions)
- **CDC:** 1,184 VitalStatistics, United States and Puerto Rico, 1999-2021
- **Shared:** newly defined CalendarYear, Chemical and Country entities used across sources

Total: 165,098 new entity instances, approximately 131 MB of validated RDF.

## Repository Structure

```
domenic_schmidt/
├── data_curation/                         # Part 1: KG curation & extension
│   ├── scripts/
│   │   ├── shared/                        #   ttl_utils.py, generate_shared_ttl.py
│   │   ├── eea/                           #   01_collect_urls, 02_download_parquets, eea_pipeline, eea_to_ttl
│   │   ├── ecis/                          #   ecis_pipeline.py, ecis_to_ttl.py
│   │   ├── oecd/                          #   oecd_exposure_pipeline.py, oecd_exposure_to_ttl.py
│   │   ├── cdc/                           #   cdc_to_ttl.py
│   │   └── a_priori_analysis.py           #   exposure-mortality correlation analysis
│   ├── outputs/
│   │   ├── csv/                           #   final processed CSVs
│   │   └── ttl/                           #   generated RDF/TTL graphs (graph_*.ttl)
│   └── figures/
│
├── gnn/                                   # Part 2: GNN link prediction
│   ├── scripts/                           #   01_parse_kg ... 10_normalize_scores, run_stability_seed.sh, visualize_kg_schema.py
│   ├── outputs/                           #   result JSONs, predictions, stability runs, logs, node_id_maps.json, hetero_graph.pt
│   └── figures/
│
├── requirements.txt
└── README.md
```

Note: `graph_EEA.ttl` is large (approximately 96 MB). It is pushed to this repository but excluded from the public GitHub mirror via `.gitignore`.

## GNN Pipeline

```
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

Multi-seed stability:

```
bash gnn/scripts/run_stability_seed.sh 42
bash gnn/scripts/run_stability_seed.sh 123
python gnn/scripts/09_full_evaluation_extra_seeds.py
```

Best R-GCN configuration (Optuna, 20 trials, TPE sampler): 2 layers, 6 basis matrices, dropout 0.15, learning rate 0.0003, 5 negatives per positive, batch size 4096, 200 epochs.

## Setup

```
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Training runs on CPU but is faster on GPU. R-GCN training used an NVIDIA RTX 3090 (approximately 25 min per run, approximately 4 h for the full multi-seed and ablation experiments).

## Related

- Public GitHub mirror: https://github.com/domischmidt/lung-cancer-gnn
- SPARQL endpoint: http://138.4.130.153:8890/sparql

## License

See LICENSE.