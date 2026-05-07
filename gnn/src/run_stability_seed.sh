#!/usr/bin/env bash
set -euo pipefail

SEED="${1:?Usage: bash gnn/src/run_stability_seed.sh <seed>}"

cd "$(dirname "$0")/../.."

OUT_DIR="gnn/data/processed/stability/seed_${SEED}"
mkdir -p "$OUT_DIR"

echo "======================================================================"
echo "Stability run for seed ${SEED}"
echo "Output: ${OUT_DIR}"
echo "======================================================================"

# Patch SEED in 05_train_rgcn.py for this run
python - <<PY
from pathlib import Path
import re

p = Path("gnn/src/05_train_rgcn.py")
s = p.read_text()
s = re.sub(r"^SEED = \\d+", "SEED = ${SEED}", s, flags=re.MULTILINE)
p.write_text(s)
print("Set 05_train_rgcn.py SEED = ${SEED}")
PY

python gnn/src/05_train_rgcn.py 2>&1 | tee "${OUT_DIR}/05_train_rgcn_seed_${SEED}.log"

python gnn/src/07_predict_novel_links.py 2>&1 | tee "${OUT_DIR}/07_predict_novel_links_seed_${SEED}.log"

python gnn/src/10_normalize_scores.py 2>&1 | tee "${OUT_DIR}/10_normalize_scores_seed_${SEED}.log"

# Copy final outputs into seed folder
cp gnn/data/processed/rgcn_weights.pt "${OUT_DIR}/rgcn_weights.pt"
cp gnn/data/processed/rgcn_results.json "${OUT_DIR}/rgcn_results.json"

cp gnn/data/processed/novel_predictions_all.json "${OUT_DIR}/novel_predictions_all.json"
cp gnn/data/processed/novel_predictions_validated.json "${OUT_DIR}/novel_predictions_validated.json"
cp gnn/data/processed/score_distributions.json "${OUT_DIR}/score_distributions.json"

cp gnn/data/processed/novel_*.csv "${OUT_DIR}/" 2>/dev/null || true

echo "======================================================================"
echo "DONE stability seed ${SEED}"
echo "======================================================================"
