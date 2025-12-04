# Graph Neural Networks for Knowledge Discovery in Lung Cancer Data

This repository contains the code for my master's thesis on applying
Relational Graph Convolutional Networks (R-GCN) to the Lung-CABO
knowledge graph to discover new biological and environmental risk factors
for lung cancer.

## Project structure

- `data/raw/`: SPARQL exports from Lung-CABO (CSV files)
- `data/interim/`: cleaned and mapped node/edge lists
- `data/processed/`: final PyTorch Geometric graph objects
- `src/sparql/`: scripts to query the SPARQL endpoint and export data
- `src/data/`: graph construction utilities
- `src/models/`: R-GCN implementation and training scripts
- `notebooks/`: exploratory analysis and prototyping

## Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt