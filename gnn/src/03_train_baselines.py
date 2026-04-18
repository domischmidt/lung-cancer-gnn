"""
03_train_baselines.py - TransE and DistMult link prediction baselines.

Trains on the full heterogeneous graph, evaluates per edge type.
Uses filtered ranking protocol with MRR, Hits@1, Hits@3, Hits@10.

Usage:  python gnn/src/03_train_baselines.py
Input:  gnn/data/processed/hetero_graph.pt
Output: gnn/data/processed/baseline_results.json
        gnn/data/interim/figs/baseline_metrics.png
"""

import json
import time
import random
from pathlib import Path
from collections import defaultdict

import torch
import torch.nn as nn
import torch.optim as optim
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
PROCESSED_DIR = REPO_ROOT / "gnn" / "data" / "processed"
FIG_DIR = REPO_ROOT / "gnn" / "data" / "interim" / "figs"

DEVICE = torch.device("mps" if torch.backends.mps.is_available() else "cuda" if torch.cuda.is_available() else "cpu")
SEED = 42

EMBEDDING_DIM = 128
EPOCHS = 200
BATCH_SIZE = 4096
LR = 0.001
MARGIN = 1.0
NEG_RATIO = 10
EVAL_EVERY = 20
VAL_RATIO = 0.1
TEST_RATIO = 0.1


def set_seed(seed):
    random.seed(seed)
    np.random.seed(seed)
    torch.manual_seed(seed)


def load_graph():
    data = torch.load(PROCESSED_DIR / "hetero_graph.pt", weights_only=False)

    node_offsets = {}
    offset = 0
    for nt in data.node_types:
        node_offsets[nt] = offset
        offset += data[nt].num_nodes
    total_nodes = offset

    all_triples = []
    rel_to_id = {}
    edge_type_names = []

    for et in data.edge_types:
        src_type, rel, dst_type = et
        rel_key = f"{src_type}__{rel}__{dst_type}"
        if rel_key not in rel_to_id:
            rel_to_id[rel_key] = len(rel_to_id)
            edge_type_names.append(rel_key)
        rid = rel_to_id[rel_key]

        ei = data[et].edge_index
        src_offset = node_offsets[src_type]
        dst_offset = node_offsets[dst_type]

        for i in range(ei.size(1)):
            h = ei[0, i].item() + src_offset
            t = ei[1, i].item() + dst_offset
            all_triples.append((h, rid, t))

    triples = torch.tensor(all_triples, dtype=torch.long)
    print(f"Graph: {total_nodes:,} nodes, {len(rel_to_id)} relations, {triples.size(0):,} triples")
    print(f"Device: {DEVICE}")

    return triples, total_nodes, rel_to_id, edge_type_names, node_offsets


def split_triples(triples):
    n = triples.size(0)
    perm = torch.randperm(n)
    n_test = int(n * TEST_RATIO)
    n_val = int(n * VAL_RATIO)

    test = triples[perm[:n_test]]
    val = triples[perm[n_test:n_test + n_val]]
    train = triples[perm[n_test + n_val:]]

    print(f"Split: {train.size(0):,} train, {val.size(0):,} val, {test.size(0):,} test")
    return train, val, test


def build_filter_set(triples):
    s = set()
    for i in range(triples.size(0)):
        h, r, t = triples[i].tolist()
        s.add((h, r, t))
    return s


class TransE(nn.Module):
    def __init__(self, n_entities, n_relations, dim):
        super().__init__()
        self.ent_emb = nn.Embedding(n_entities, dim)
        self.rel_emb = nn.Embedding(n_relations, dim)
        nn.init.xavier_uniform_(self.ent_emb.weight)
        nn.init.xavier_uniform_(self.rel_emb.weight)

    def score(self, h, r, t):
        return -torch.norm(self.ent_emb(h) + self.rel_emb(r) - self.ent_emb(t), p=2, dim=-1)

    def forward(self, pos_h, pos_r, pos_t, neg_h, neg_r, neg_t):
        pos_score = self.score(pos_h, pos_r, pos_t)
        neg_score = self.score(neg_h, neg_r, neg_t).view(-1, NEG_RATIO)
        return torch.relu(MARGIN - pos_score.unsqueeze(1) + neg_score).mean()

class DistMult(nn.Module):
    def __init__(self, n_entities, n_relations, dim):
        super().__init__()
        self.ent_emb = nn.Embedding(n_entities, dim)
        self.rel_emb = nn.Embedding(n_relations, dim)
        nn.init.xavier_uniform_(self.ent_emb.weight)
        nn.init.xavier_uniform_(self.rel_emb.weight)

    def score(self, h, r, t):
        return (self.ent_emb(h) * self.rel_emb(r) * self.ent_emb(t)).sum(dim=-1)

    def forward(self, pos_h, pos_r, pos_t, neg_h, neg_r, neg_t):
        pos_score = self.score(pos_h, pos_r, pos_t)
        neg_score = self.score(neg_h, neg_r, neg_t)
        pos_loss = -torch.log(torch.sigmoid(pos_score) + 1e-9).mean()
        neg_loss = -torch.log(1 - torch.sigmoid(neg_score) + 1e-9).mean()
        return (pos_loss + neg_loss) / 2


def generate_negatives(batch, n_entities):
    neg = batch.repeat(NEG_RATIO, 1)
    mask = torch.randint(0, 2, (neg.size(0),), dtype=torch.bool)
    rand_ents = torch.randint(0, n_entities, (neg.size(0),))
    neg[mask, 0] = rand_ents[mask]
    neg[~mask, 2] = rand_ents[~mask]
    return neg


def train_model(model, train_triples, n_entities, epochs):
    optimizer = optim.Adam(model.parameters(), lr=LR)
    model.to(DEVICE)
    model.train()
    losses = []

    for epoch in range(1, epochs + 1):
        perm = torch.randperm(train_triples.size(0))
        epoch_loss = 0.0
        n_batches = 0

        for i in range(0, train_triples.size(0), BATCH_SIZE):
            batch = train_triples[perm[i:i + BATCH_SIZE]]
            neg = generate_negatives(batch, n_entities)

            pos_h = batch[:, 0].to(DEVICE)
            pos_r = batch[:, 1].to(DEVICE)
            pos_t = batch[:, 2].to(DEVICE)
            neg_h = neg[:, 0].to(DEVICE)
            neg_r = neg[:, 1].to(DEVICE)
            neg_t = neg[:, 2].to(DEVICE)

            loss = model(pos_h, pos_r, pos_t, neg_h, neg_r, neg_t)
            optimizer.zero_grad()
            loss.backward()
            optimizer.step()

            epoch_loss += loss.item()
            n_batches += 1

        avg_loss = epoch_loss / max(n_batches, 1)
        losses.append(avg_loss)
        if epoch % EVAL_EVERY == 0 or epoch == 1:
            print(f"    Epoch {epoch:>3d}/{epochs}  loss={avg_loss:.4f}")

    return losses


@torch.no_grad()
def evaluate(model, test_triples, filter_set, n_entities, edge_type_names, n_sample=500):
    model.eval()
    model.to(DEVICE)

    if test_triples.size(0) > n_sample:
        idx = torch.randperm(test_triples.size(0))[:n_sample]
        test_triples = test_triples[idx]

    per_rel = defaultdict(lambda: {"ranks": []})
    all_ranks = []

    for i in range(test_triples.size(0)):
        h, r, t = test_triples[i].tolist()

        # tail prediction
        all_ents = torch.arange(n_entities, device=DEVICE)
        h_rep = torch.full((n_entities,), h, dtype=torch.long, device=DEVICE)
        r_rep = torch.full((n_entities,), r, dtype=torch.long, device=DEVICE)
        scores = model.score(h_rep, r_rep, all_ents)

        # filtered: set scores of known true triples (except target) to -inf
        for eid in range(n_entities):
            if eid != t and (h, r, eid) in filter_set:
                scores[eid] = -1e9

        rank = (scores >= scores[t]).sum().item()
        rank = max(rank, 1)
        all_ranks.append(rank)

        rel_name = edge_type_names[r] if r < len(edge_type_names) else f"rel_{r}"
        per_rel[rel_name]["ranks"].append(rank)

    def calc_metrics(ranks):
        ranks = np.array(ranks, dtype=np.float64)
        return {
            "mrr": float(np.mean(1.0 / ranks)),
            "hits@1": float(np.mean(ranks <= 1)),
            "hits@3": float(np.mean(ranks <= 3)),
            "hits@10": float(np.mean(ranks <= 10)),
            "n_triples": len(ranks),
        }

    results = {"overall": calc_metrics(all_ranks)}
    for rel_name, data in per_rel.items():
        if len(data["ranks"]) >= 3:
            results[rel_name] = calc_metrics(data["ranks"])

    return results


def fig_training_curves(all_losses, fig_dir):
    fig_dir.mkdir(parents=True, exist_ok=True)
    fig, ax = plt.subplots(figsize=(8, 4))
    for name, losses in all_losses.items():
        ax.plot(range(1, len(losses) + 1), losses, label=name, linewidth=1.5)
    ax.set_xlabel("Epoch")
    ax.set_ylabel("Loss")
    ax.set_title("Baseline Training Curves")
    ax.legend()
    ax.grid(True, alpha=0.3)
    plt.tight_layout()
    fig.savefig(fig_dir / "baseline_training_curves.png", dpi=200, bbox_inches="tight")
    plt.close(fig)
    print(f"  -> figs/baseline_training_curves.png")


def fig_metrics_comparison(all_results, fig_dir):
    fig_dir.mkdir(parents=True, exist_ok=True)
    models = list(all_results.keys())
    metrics = ["mrr", "hits@1", "hits@3", "hits@10"]
    x = np.arange(len(metrics))
    width = 0.35

    fig, ax = plt.subplots(figsize=(8, 4))
    for i, model_name in enumerate(models):
        vals = [all_results[model_name]["overall"].get(m, 0) for m in metrics]
        offset = (i - (len(models) - 1) / 2) * width
        bars = ax.bar(x + offset, vals, width, label=model_name, alpha=0.85)
        for bar, v in zip(bars, vals):
            ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.01,
                    f"{v:.3f}", ha="center", va="bottom", fontsize=8)

    ax.set_ylabel("Score")
    ax.set_title("Link Prediction: Baseline Comparison (Overall)")
    ax.set_xticks(x)
    ax.set_xticklabels([m.upper() for m in metrics])
    ax.legend()
    ax.set_ylim(0, 1.0)
    ax.grid(True, alpha=0.3, axis="y")
    plt.tight_layout()
    fig.savefig(fig_dir / "baseline_metrics.png", dpi=200, bbox_inches="tight")
    plt.close(fig)
    print(f"  -> figs/baseline_metrics.png")


def fig_per_relation(all_results, fig_dir):
    fig_dir.mkdir(parents=True, exist_ok=True)

    all_rels = set()
    for model_results in all_results.values():
        all_rels.update(k for k in model_results if k != "overall")
    rels = sorted(all_rels)
    if not rels:
        return

    models = list(all_results.keys())
    fig, axes = plt.subplots(1, len(models), figsize=(7 * len(models), max(4, len(rels) * 0.35)), sharey=True)
    if len(models) == 1:
        axes = [axes]

    for ax, model_name in zip(axes, models):
        mrrs = [all_results[model_name].get(r, {}).get("mrr", 0) for r in rels]
        colors = ["#4c72b0" if "__associated_with__" in r or "__in_pathway__" in r or "__variant_of__" in r
                   or "__linked_to__" in r or "__marker_for__" in r or "__has_rearrangement__" in r
                   or "__has_fusion__" in r or "__part_of_pathway__" in r or "__located_in_gene__" in r
                   else "#2ca02c" for r in rels]
        short_names = [r.replace("__", " > ").replace("_", " ") for r in rels]
        ax.barh(short_names, mrrs, color=colors, edgecolor="white", linewidth=0.5)
        ax.set_xlabel("MRR")
        ax.set_title(f"{model_name}: MRR per Edge Type")
        ax.set_xlim(0, 1)
        for i, v in enumerate(mrrs):
            ax.text(v + 0.01, i, f"{v:.3f}", va="center", fontsize=7)

    plt.tight_layout()
    fig.savefig(fig_dir / "baseline_per_relation.png", dpi=200, bbox_inches="tight")
    plt.close(fig)
    print(f"  -> figs/baseline_per_relation.png")


def main():
    print("=" * 70)
    print("03_train_baselines.py")
    print("=" * 70)
    set_seed(SEED)

    print("\n[1/6] Loading graph ...")
    triples, n_entities, rel_to_id, edge_type_names, node_offsets = load_graph()

    print("\n[2/6] Splitting triples ...")
    train, val, test = split_triples(triples)
    filter_set = build_filter_set(triples)

    all_losses = {}
    all_results = {}

    for model_name, ModelClass in [("TransE", TransE), ("DistMult", DistMult)]:
        print(f"\n[{'3' if model_name == 'TransE' else '4'}/6] Training {model_name} ...")
        model = ModelClass(n_entities, len(rel_to_id), EMBEDDING_DIM)
        t0 = time.time()
        losses = train_model(model, train, n_entities, EPOCHS)
        dt = time.time() - t0
        print(f"    Done in {dt:.1f}s")
        all_losses[model_name] = losses

        print(f"  Evaluating {model_name} ...")
        results = evaluate(model, test, filter_set, n_entities, edge_type_names)
        all_results[model_name] = results

        m = results["overall"]
        print(f"    Overall: MRR={m['mrr']:.4f}  H@1={m['hits@1']:.4f}  H@3={m['hits@3']:.4f}  H@10={m['hits@10']:.4f}")

        torch.save(model.state_dict(), PROCESSED_DIR / f"{model_name.lower()}_weights.pt")

    print("\n[5/6] Saving results ...")
    with open(PROCESSED_DIR / "baseline_results.json", "w") as f:
        json.dump(all_results, f, indent=2)
    print(f"  -> baseline_results.json")

    print("\n[6/6] Generating thesis figures ...")
    fig_training_curves(all_losses, FIG_DIR)
    fig_metrics_comparison(all_results, FIG_DIR)
    fig_per_relation(all_results, FIG_DIR)

    print("\n" + "=" * 70)
    print("Done.")
    for name, res in all_results.items():
        m = res["overall"]
        print(f"  {name:10s}  MRR={m['mrr']:.4f}  H@1={m['hits@1']:.4f}  H@3={m['hits@3']:.4f}  H@10={m['hits@10']:.4f}")
    print("=" * 70)


if __name__ == "__main__":
    main()
