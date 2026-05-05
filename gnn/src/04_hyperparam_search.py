"""
04_hyperparam_search.py - Hyperparameter search for R-GCN via Optuna.

Uses Bayesian optimization (TPE sampler) to find the best R-GCN configuration.
Each trial trains for a reduced number of epochs on the training split and
evaluates on the validation split. The best configuration is saved to
best_config.json for use by 05_train_rgcn.py.

Search space:
  - hidden_dim: [64, 128, 256]
  - num_layers: [2, 3, 4, 5]
  - num_bases:  [2, 4, 6, 8]
  - dropout:    [0.1, 0.2, 0.3, 0.4]
  - lr:         [0.0005, 0.001, 0.005]
  - neg_ratio:  [5, 10, 20]
  - batch_size: [2048, 4096, 8192]

Objective: maximize GDA -> Disease MRR on the validation set.
Falls back to overall MRR if no GDA triples in the val split.

Usage:  python gnn/src/04_hyperparam_search.py [--n_trials 50] [--search_epochs 100]
Input:  gnn/data/processed/hetero_graph.pt
Output: gnn/data/processed/best_config.json
        gnn/data/processed/hyperparam_search_results.json
        gnn/data/interim/figs/hyperparam_*.png
"""

import json
import time
import random
import argparse
from pathlib import Path
from collections import defaultdict

import torch
import torch.nn as nn
import torch.nn.functional as F
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

from torch_geometric.nn import RGCNConv

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
PROCESSED_DIR = REPO_ROOT / "gnn" / "data" / "processed"
FIG_DIR = REPO_ROOT / "gnn" / "data" / "interim" / "figs"

DEVICE = torch.device("mps" if torch.backends.mps.is_available() else "cuda" if torch.cuda.is_available() else "cpu")
SEED = 42

VAL_RATIO = 0.1
TEST_RATIO = 0.1
N_EVAL_SAMPLES = 300  # fewer samples per trial for speed

GDA_KEY = "GeneDiseaseAssociation__associated_with__Disease"


# =========================================================================
# Model
# =========================================================================

class RGCNWithFeatures(nn.Module):
    def __init__(self, n_nodes, n_relations, hidden_dim, num_layers, num_bases, dropout, node_features=None):
        super().__init__()
        self.node_emb = nn.Embedding(n_nodes, hidden_dim)
        nn.init.xavier_uniform_(self.node_emb.weight)
        self.feat_projections = nn.ModuleDict()
        self.node_feature_info = node_features or {}
        for nt, info in self.node_feature_info.items():
            feat_dim = info["feat"].shape[1]
            self.feat_projections[nt] = nn.Linear(feat_dim, hidden_dim)
        self.convs = nn.ModuleList()
        for _ in range(num_layers):
            self.convs.append(RGCNConv(hidden_dim, hidden_dim, n_relations, num_bases=num_bases))
        self.rel_emb = nn.Embedding(n_relations, hidden_dim)
        nn.init.xavier_uniform_(self.rel_emb.weight)
        self.dropout = dropout

    def get_initial_embeddings(self):
        x = self.node_emb.weight.clone()
        for nt, info in self.node_feature_info.items():
            offset = info["offset"]
            n = info["n"]
            feat = info["feat"].to(x.device)
            projected = self.feat_projections[nt](feat)
            x[offset:offset + n] = x[offset:offset + n] + projected
        return x

    def encode(self, edge_index, edge_type):
        x = self.get_initial_embeddings()
        for conv in self.convs:
            x = conv(x, edge_index, edge_type)
            x = F.relu(x)
            x = F.dropout(x, p=self.dropout, training=self.training)
        return x

    def decode(self, z, h_idx, r_idx, t_idx):
        return (z[h_idx] * self.rel_emb(r_idx) * z[t_idx]).sum(dim=-1)


# =========================================================================
# Data utilities
# =========================================================================

def set_seed(seed):
    random.seed(seed)
    np.random.seed(seed)
    torch.manual_seed(seed)


def load_graph():
    data = torch.load(PROCESSED_DIR / "hetero_graph.pt", weights_only=False)

    node_offsets = {}
    type_ranges = {}
    offset = 0
    node_features = {}
    for nt in data.node_types:
        node_offsets[nt] = offset
        n = data[nt].num_nodes
        type_ranges[nt] = (offset, offset + n)
        if hasattr(data[nt], 'x') and data[nt].x is not None:
            node_features[nt] = {"offset": offset, "n": n, "feat": data[nt].x}
        offset += n
    total_nodes = offset

    all_triples = []
    rel_to_id = {}
    edge_type_names = []
    rel_type_info = {}

    for et in data.edge_types:
        src_type, rel, dst_type = et
        rel_key = f"{src_type}__{rel}__{dst_type}"
        if rel_key not in rel_to_id:
            rel_to_id[rel_key] = len(rel_to_id)
            edge_type_names.append(rel_key)
            rel_type_info[rel_to_id[rel_key]] = (src_type, dst_type)
        rid = rel_to_id[rel_key]
        ei = data[et].edge_index
        src_off = node_offsets[src_type]
        dst_off = node_offsets[dst_type]
        for i in range(ei.size(1)):
            all_triples.append((ei[0, i].item() + src_off, rid, ei[1, i].item() + dst_off))

    triples = torch.tensor(all_triples, dtype=torch.long)
    return triples, total_nodes, rel_to_id, edge_type_names, rel_type_info, type_ranges, node_features


def split_triples(triples, seed):
    set_seed(seed)
    n = triples.size(0)
    perm = torch.randperm(n)
    n_test = int(n * TEST_RATIO)
    n_val = int(n * VAL_RATIO)
    return triples[perm[n_test + n_val:]], triples[perm[n_test:n_test + n_val]], triples[perm[:n_test]]


def build_filter_set(triples):
    return {tuple(triples[i].tolist()) for i in range(triples.size(0))}


def build_type_range_tensors(rel_type_info, type_ranges):
    n_rels = len(rel_type_info)
    rel_ranges = torch.zeros(n_rels, 4, dtype=torch.long)
    for rid, (src_type, dst_type) in rel_type_info.items():
        s_lo, s_hi = type_ranges[src_type]
        d_lo, d_hi = type_ranges[dst_type]
        rel_ranges[rid] = torch.tensor([s_lo, s_hi, d_lo, d_hi])
    return rel_ranges


def generate_negatives_type_aware(batch, rel_ranges, neg_ratio):
    neg = batch.repeat(neg_ratio, 1)
    n_neg = neg.size(0)
    mask = torch.randint(0, 2, (n_neg,), dtype=torch.bool)
    rels = neg[:, 1]
    ranges = rel_ranges[rels]
    head_lo = ranges[:, 0]
    head_range = (ranges[:, 1] - ranges[:, 0]).clamp(min=1)
    tail_lo = ranges[:, 2]
    tail_range = (ranges[:, 3] - ranges[:, 2]).clamp(min=1)
    rand_heads = head_lo + (torch.rand(n_neg) * head_range.float()).long()
    rand_tails = tail_lo + (torch.rand(n_neg) * tail_range.float()).long()
    neg[mask, 0] = rand_heads[mask]
    neg[~mask, 2] = rand_tails[~mask]
    return neg


# =========================================================================
# Training and evaluation
# =========================================================================

def train_and_evaluate(config, triples, train, val, filter_set, n_entities, n_relations,
                       edge_type_names, rel_type_info, type_ranges, node_features, rel_ranges,
                       search_epochs):
    set_seed(SEED)

    model = RGCNWithFeatures(
        n_entities, n_relations, config["hidden_dim"],
        config["num_layers"], config["num_bases"], config["dropout"],
        node_features
    )
    model.to(DEVICE)
    optimizer = torch.optim.Adam(model.parameters(), lr=config["lr"])

    ei = triples[:, [0, 2]].t().to(DEVICE)
    et = triples[:, 1].to(DEVICE)

    # Training
    for epoch in range(1, search_epochs + 1):
        model.train()
        z = model.encode(ei, et)
        perm = torch.randperm(train.size(0))

        for i in range(0, train.size(0), config["batch_size"]):
            batch = train[perm[i:i + config["batch_size"]]]
            neg = generate_negatives_type_aware(batch, rel_ranges, config["neg_ratio"])

            pos_score = model.decode(z, batch[:, 0].to(DEVICE), batch[:, 1].to(DEVICE), batch[:, 2].to(DEVICE))
            neg_score = model.decode(z, neg[:, 0].to(DEVICE), neg[:, 1].to(DEVICE), neg[:, 2].to(DEVICE))

            pos_loss = -torch.log(torch.sigmoid(pos_score) + 1e-9).mean()
            neg_loss = -torch.log(1 - torch.sigmoid(neg_score) + 1e-9).mean()
            loss = (pos_loss + neg_loss) / 2

            optimizer.zero_grad()
            loss.backward()
            torch.nn.utils.clip_grad_norm_(model.parameters(), 1.0)
            optimizer.step()

            z = model.encode(ei, et)

    # Evaluation on validation set
    model.eval()
    with torch.no_grad():
        z = model.encode(ei, et)

    if val.size(0) > N_EVAL_SAMPLES:
        idx = torch.randperm(val.size(0))[:N_EVAL_SAMPLES]
        val_sample = val[idx]
    else:
        val_sample = val

    per_rel = defaultdict(list)
    all_ranks = []

    for i in range(val_sample.size(0)):
        h, r, t = val_sample[i].tolist()
        dst_type = rel_type_info[r][1]
        dst_lo, dst_hi = type_ranges[dst_type]
        n_cand = dst_hi - dst_lo

        all_ents = torch.arange(dst_lo, dst_hi, device=DEVICE)
        h_rep = torch.full((n_cand,), h, dtype=torch.long, device=DEVICE)
        r_rep = torch.full((n_cand,), r, dtype=torch.long, device=DEVICE)
        scores = model.decode(z, h_rep, r_rep, all_ents)

        for j, eid in enumerate(range(dst_lo, dst_hi)):
            if eid != t and (h, r, eid) in filter_set:
                scores[j] = -1e9

        t_local = t - dst_lo
        if 0 <= t_local < n_cand:
            rank = (scores >= scores[t_local]).sum().item()
            rank = max(rank, 1)
        else:
            rank = n_cand

        all_ranks.append(rank)
        rel_name = edge_type_names[r] if r < len(edge_type_names) else f"rel_{r}"
        per_rel[rel_name].append(rank)

    def metrics(ranks):
        r = np.array(ranks, dtype=np.float64)
        return {"mrr": float(np.mean(1.0 / r)), "hits@1": float(np.mean(r <= 1)),
                "hits@3": float(np.mean(r <= 3)), "hits@10": float(np.mean(r <= 10))}

    overall = metrics(all_ranks)
    gda_ranks = per_rel.get(GDA_KEY, [])
    gda = metrics(gda_ranks) if len(gda_ranks) >= 3 else None

    del model
    if torch.cuda.is_available():
        torch.cuda.empty_cache()

    return overall, gda


# =========================================================================
# Figures
# =========================================================================

def fig_optimization_history(study, fig_dir):
    fig_dir.mkdir(parents=True, exist_ok=True)

    trials = [t for t in study.trials if t.state.name == "COMPLETE"]
    if not trials:
        return

    values = [t.value for t in trials]
    best_so_far = [max(values[:i+1]) for i in range(len(values))]

    fig, ax = plt.subplots(figsize=(10, 5))
    ax.scatter(range(len(values)), values, alpha=0.5, s=30, c="#4c72b0", label="Trial MRR")
    ax.plot(range(len(best_so_far)), best_so_far, color="#c44e52", linewidth=2, label="Best so far")
    ax.set_xlabel("Trial", fontsize=11)
    ax.set_ylabel("GDA -> Disease MRR (validation)", fontsize=11)
    ax.set_title("Hyperparameter search: optimization history", fontsize=13, fontweight="bold")
    ax.legend()
    ax.grid(True, alpha=0.2)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    plt.tight_layout()
    fig.savefig(fig_dir / "hyperparam_optimization_history.png", dpi=200, bbox_inches="tight")
    plt.close(fig)
    print(f"  -> figs/hyperparam_optimization_history.png")


def fig_param_importance(study, fig_dir):
    fig_dir.mkdir(parents=True, exist_ok=True)

    try:
        from optuna.importance import get_param_importances
        importances = get_param_importances(study)
    except Exception:
        print("  [SKIP] Could not compute parameter importances")
        return

    params = list(importances.keys())
    values = list(importances.values())

    fig, ax = plt.subplots(figsize=(8, max(3, len(params) * 0.4)))
    ax.barh(params[::-1], values[::-1], color="#4c72b0", alpha=0.85, edgecolor="white")
    ax.set_xlabel("Importance")
    ax.set_title("Hyperparameter importance (fANOVA)", fontsize=12, fontweight="bold")
    for i, v in enumerate(values[::-1]):
        ax.text(v + max(values) * 0.02, i, f"{v:.3f}", va="center", fontsize=9)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    plt.tight_layout()
    fig.savefig(fig_dir / "hyperparam_importance.png", dpi=200, bbox_inches="tight")
    plt.close(fig)
    print(f"  -> figs/hyperparam_importance.png")


# =========================================================================
# Main
# =========================================================================

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--n_trials", type=int, default=50, help="Number of Optuna trials")
    parser.add_argument("--search_epochs", type=int, default=100, help="Epochs per trial (reduced for speed)")
    args = parser.parse_args()

    print("=" * 70)
    print(f"04_hyperparam_search.py ({args.n_trials} trials, {args.search_epochs} epochs each)")
    print("=" * 70)
    print(f"Device: {DEVICE}")
    print(f"Objective: maximize GDA -> Disease MRR on validation set")

    print("\n[1/4] Loading graph ...")
    (triples, n_entities, rel_to_id, edge_type_names,
     rel_type_info, type_ranges, node_features) = load_graph()
    n_relations = len(rel_to_id)
    print(f"  {n_entities:,} nodes, {n_relations} relations, {triples.size(0):,} triples")

    print("\n[2/4] Splitting data ...")
    train, val, test = split_triples(triples, SEED)
    filter_set = build_filter_set(triples)
    rel_ranges = build_type_range_tensors(rel_type_info, type_ranges)
    print(f"  Train: {train.size(0):,}, Val: {val.size(0):,}, Test: {test.size(0):,}")

    print(f"\n[3/4] Running Optuna search ({args.n_trials} trials) ...")
    try:
        import optuna
        optuna.logging.set_verbosity(optuna.logging.WARNING)
    except ImportError:
        print("  ERROR: optuna not installed. Run: pip install optuna")
        print("  Saving default config instead.")
        default_config = {
            "hidden_dim": 128, "num_layers": 4, "num_bases": 6,
            "dropout": 0.2, "lr": 0.001, "neg_ratio": 10,
            "batch_size": 4096, "epochs": 200,
        }
        with open(PROCESSED_DIR / "best_config.json", "w") as f:
            json.dump(default_config, f, indent=2)
        print(f"  -> best_config.json (defaults)")
        return

    all_trial_results = []

    def objective(trial):
        config = {
            "hidden_dim": trial.suggest_categorical("hidden_dim", [64, 128, 256]),
            "num_layers": trial.suggest_int("num_layers", 2, 5),
            "num_bases": trial.suggest_categorical("num_bases", [2, 4, 6, 8]),
            "dropout": trial.suggest_float("dropout", 0.05, 0.4, step=0.05),
            "lr": trial.suggest_float("lr", 1e-4, 1e-2, log=True),
            "neg_ratio": trial.suggest_categorical("neg_ratio", [5, 10, 20]),
            "batch_size": trial.suggest_categorical("batch_size", [2048, 4096, 8192]),
        }

        t0 = time.time()
        overall, gda = train_and_evaluate(
            config, triples, train, val, filter_set,
            n_entities, n_relations, edge_type_names,
            rel_type_info, type_ranges, node_features, rel_ranges,
            args.search_epochs
        )
        dt = time.time() - t0

        # Primary objective: GDA MRR, fallback to overall MRR
        score = gda["mrr"] if gda else overall["mrr"]

        trial_result = {
            "trial": trial.number,
            "config": config,
            "overall_mrr": overall["mrr"],
            "gda_mrr": gda["mrr"] if gda else None,
            "gda_hits10": gda["hits@10"] if gda else None,
            "objective": score,
            "time_seconds": round(dt, 1),
        }
        all_trial_results.append(trial_result)

        gda_str = f"{gda['mrr']:.4f}" if gda else "N/A"
        print(f"    Trial {trial.number:>3d}  "
              f"layers={config['num_layers']} bases={config['num_bases']} "
              f"hidden={config['hidden_dim']} dropout={config['dropout']:.2f} "
              f"lr={config['lr']:.5f}  "
              f"MRR={overall['mrr']:.4f}  "
              f"GDA={gda_str:>7}  "
              f"({dt:.0f}s)")

        return score

    study = optuna.create_study(
        direction="maximize",
        sampler=optuna.samplers.TPESampler(seed=SEED),
        pruner=optuna.pruners.NopPruner(),
    )
    study.optimize(objective, n_trials=args.n_trials, show_progress_bar=False)

    # Best config
    best = study.best_trial
    best_config = best.params
    best_config["epochs"] = 200  # full training epochs for final run

    print(f"\n  Best trial: {best.number}")
    print(f"  Best GDA MRR: {best.value:.4f}")
    print(f"  Best config:")
    for k, v in best_config.items():
        print(f"    {k}: {v}")

    # Save
    print(f"\n[4/4] Saving results ...")
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    with open(PROCESSED_DIR / "best_config.json", "w") as f:
        json.dump(best_config, f, indent=2)
    print(f"  -> best_config.json")

    with open(PROCESSED_DIR / "hyperparam_search_results.json", "w") as f:
        json.dump({
            "best_trial": best.number,
            "best_value": best.value,
            "best_config": best_config,
            "n_trials": args.n_trials,
            "search_epochs": args.search_epochs,
            "all_trials": all_trial_results,
        }, f, indent=2)
    print(f"  -> hyperparam_search_results.json")

    # Figures
    FIG_DIR.mkdir(parents=True, exist_ok=True)
    fig_optimization_history(study, FIG_DIR)
    fig_param_importance(study, FIG_DIR)

    print(f"\n{'=' * 70}")
    print(f"Hyperparameter search complete.")
    print(f"Best config saved to best_config.json")
    print(f"Run 05_train_rgcn.py to train with the best configuration.")
    print(f"{'=' * 70}")


if __name__ == "__main__":
    main()