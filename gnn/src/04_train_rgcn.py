"""
04_train_rgcn.py - R-GCN link prediction on the Lung-CABO heterogeneous KG.

Uses message passing over the full heterogeneous graph structure to learn
node embeddings, then scores candidate links with DistMult decoder.
Evaluates with filtered MRR and Hits@k, per edge type.

Usage:  python gnn/src/04_train_rgcn.py
Input:  gnn/data/processed/hetero_graph.pt
Output: gnn/data/processed/rgcn_results.json
        gnn/data/processed/rgcn_weights.pt
        gnn/data/interim/figs/rgcn_*.png
"""

import json
import time
import random
from pathlib import Path
from collections import defaultdict

import torch
import torch.nn as nn
import torch.nn.functional as F
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches

from torch_geometric.nn import RGCNConv
from torch_geometric.data import HeteroData

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
PROCESSED_DIR = REPO_ROOT / "gnn" / "data" / "processed"
FIG_DIR = REPO_ROOT / "gnn" / "data" / "interim" / "figs"

DEVICE = torch.device("mps" if torch.backends.mps.is_available() else "cuda" if torch.cuda.is_available() else "cpu")
SEED = 42

HIDDEN_DIM = 128
NUM_LAYERS = 2
NUM_BASES = 4
DROPOUT = 0.2
EPOCHS = 200
BATCH_SIZE = 4096
LR = 0.001
NEG_RATIO = 10
EVAL_EVERY = 20
VAL_RATIO = 0.1
TEST_RATIO = 0.1


def set_seed(seed):
    random.seed(seed)
    np.random.seed(seed)
    torch.manual_seed(seed)


def load_and_flatten():
    data = torch.load(PROCESSED_DIR / "hetero_graph.pt", weights_only=False)

    node_offsets = {}
    offset = 0
    for nt in data.node_types:
        node_offsets[nt] = offset
        offset += data[nt].num_nodes
    total_nodes = offset

    all_src, all_dst, all_rel = [], [], []
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
        all_src.append(ei[0] + node_offsets[src_type])
        all_dst.append(ei[1] + node_offsets[dst_type])
        all_rel.append(torch.full((ei.size(1),), rid, dtype=torch.long))

    edge_index = torch.stack([torch.cat(all_src), torch.cat(all_dst)])
    edge_type = torch.cat(all_rel)
    n_relations = len(rel_to_id)

    print(f"Graph: {total_nodes:,} nodes, {n_relations} relations, {edge_index.size(1):,} edges")
    print(f"Device: {DEVICE}")

    return edge_index, edge_type, total_nodes, n_relations, rel_to_id, edge_type_names, node_offsets


def build_triples(edge_index, edge_type):
    h = edge_index[0]
    t = edge_index[1]
    r = edge_type
    return torch.stack([h, r, t], dim=1)


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


class RGCN(nn.Module):
    def __init__(self, n_nodes, n_relations, hidden_dim, num_layers, num_bases, dropout):
        super().__init__()
        self.node_emb = nn.Embedding(n_nodes, hidden_dim)
        nn.init.xavier_uniform_(self.node_emb.weight)

        self.convs = nn.ModuleList()
        for _ in range(num_layers):
            self.convs.append(RGCNConv(hidden_dim, hidden_dim, n_relations, num_bases=num_bases))

        self.rel_emb = nn.Embedding(n_relations, hidden_dim)
        nn.init.xavier_uniform_(self.rel_emb.weight)

        self.dropout = dropout

    def encode(self, edge_index, edge_type):
        x = self.node_emb.weight
        for conv in self.convs:
            x = conv(x, edge_index, edge_type)
            x = F.relu(x)
            x = F.dropout(x, p=self.dropout, training=self.training)
        return x

    def decode(self, z, h_idx, r_idx, t_idx):
        h_emb = z[h_idx]
        t_emb = z[t_idx]
        r_emb = self.rel_emb(r_idx)
        return (h_emb * r_emb * t_emb).sum(dim=-1)


def generate_negatives(batch, n_entities):
    neg = batch.repeat(NEG_RATIO, 1)
    mask = torch.randint(0, 2, (neg.size(0),), dtype=torch.bool)
    rand_ents = torch.randint(0, n_entities, (neg.size(0),))
    neg[mask, 0] = rand_ents[mask]
    neg[~mask, 2] = rand_ents[~mask]
    return neg


def train_epoch(model, train_triples, train_edge_index, train_edge_type, n_entities):
    model.train()
    z = model.encode(train_edge_index.to(DEVICE), train_edge_type.to(DEVICE))

    perm = torch.randperm(train_triples.size(0))
    total_loss = 0.0
    n_batches = 0

    for i in range(0, train_triples.size(0), BATCH_SIZE):
        batch = train_triples[perm[i:i + BATCH_SIZE]]
        neg = generate_negatives(batch, n_entities)

        pos_score = model.decode(z, batch[:, 0].to(DEVICE), batch[:, 1].to(DEVICE), batch[:, 2].to(DEVICE))
        neg_score = model.decode(z, neg[:, 0].to(DEVICE), neg[:, 1].to(DEVICE), neg[:, 2].to(DEVICE))

        pos_loss = -torch.log(torch.sigmoid(pos_score) + 1e-9).mean()
        neg_loss = -torch.log(1 - torch.sigmoid(neg_score) + 1e-9).mean()
        loss = (pos_loss + neg_loss) / 2

        total_loss += loss.item()
        n_batches += 1

    return total_loss / max(n_batches, 1), z


def train_model(model, optimizer, train_triples, all_triples_tensor, n_entities, epochs):
    train_ei = all_triples_tensor[:, [0, 2]].t()
    train_et = all_triples_tensor[:, 1]

    losses = []
    for epoch in range(1, epochs + 1):
        avg_loss, z = train_epoch(model, train_triples, train_ei, train_et, n_entities)

        optimizer.zero_grad()

        # re-encode and compute loss with gradient
        model.train()
        z = model.encode(train_ei.to(DEVICE), train_et.to(DEVICE))

        perm = torch.randperm(train_triples.size(0))
        batch = train_triples[perm[:BATCH_SIZE]]
        neg = generate_negatives(batch, n_entities)

        pos_score = model.decode(z, batch[:, 0].to(DEVICE), batch[:, 1].to(DEVICE), batch[:, 2].to(DEVICE))
        neg_score = model.decode(z, neg[:, 0].to(DEVICE), neg[:, 1].to(DEVICE), neg[:, 2].to(DEVICE))

        pos_loss = -torch.log(torch.sigmoid(pos_score) + 1e-9).mean()
        neg_loss = -torch.log(1 - torch.sigmoid(neg_score) + 1e-9).mean()
        loss = (pos_loss + neg_loss) / 2

        loss.backward()
        torch.nn.utils.clip_grad_norm_(model.parameters(), 1.0)
        optimizer.step()

        losses.append(loss.item())
        if epoch % EVAL_EVERY == 0 or epoch == 1:
            print(f"    Epoch {epoch:>3d}/{epochs}  loss={loss.item():.4f}")

    return losses


@torch.no_grad()
def evaluate(model, test_triples, all_triples_tensor, filter_set, n_entities, edge_type_names, n_sample=500):
    model.eval()

    train_ei = all_triples_tensor[:, [0, 2]].t()
    train_et = all_triples_tensor[:, 1]
    z = model.encode(train_ei.to(DEVICE), train_et.to(DEVICE))

    if test_triples.size(0) > n_sample:
        idx = torch.randperm(test_triples.size(0))[:n_sample]
        test_triples = test_triples[idx]

    per_rel = defaultdict(lambda: {"ranks": []})
    all_ranks = []

    for i in range(test_triples.size(0)):
        h, r, t = test_triples[i].tolist()

        all_ents = torch.arange(n_entities, device=DEVICE)
        h_rep = torch.full((n_entities,), h, dtype=torch.long, device=DEVICE)
        r_rep = torch.full((n_entities,), r, dtype=torch.long, device=DEVICE)
        scores = model.decode(z, h_rep, r_rep, all_ents)

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
    for rel_name, d in per_rel.items():
        if len(d["ranks"]) >= 3:
            results[rel_name] = calc_metrics(d["ranks"])

    return results


def fig_training_curve(losses, fig_dir):
    fig_dir.mkdir(parents=True, exist_ok=True)
    fig, ax = plt.subplots(figsize=(8, 4))
    ax.plot(range(1, len(losses) + 1), losses, color="#c44e52", linewidth=1.5, label="R-GCN")
    ax.set_xlabel("Epoch")
    ax.set_ylabel("Loss")
    ax.set_title("R-GCN Training Curve")
    ax.legend()
    ax.grid(True, alpha=0.3)
    plt.tight_layout()
    fig.savefig(fig_dir / "rgcn_training_curve.png", dpi=200, bbox_inches="tight")
    plt.close(fig)
    print(f"  -> figs/rgcn_training_curve.png")


def fig_comparison(rgcn_results, fig_dir):
    fig_dir.mkdir(parents=True, exist_ok=True)

    baseline_path = PROCESSED_DIR / "baseline_results.json"
    if not baseline_path.exists():
        return

    with open(baseline_path) as f:
        baselines = json.load(f)

    all_results = dict(baselines)
    all_results["R-GCN"] = rgcn_results

    models = list(all_results.keys())
    metrics = ["mrr", "hits@1", "hits@3", "hits@10"]
    colors = {"TransE": "#4c72b0", "DistMult": "#55a868", "R-GCN": "#c44e52"}
    x = np.arange(len(metrics))
    width = 0.25

    fig, ax = plt.subplots(figsize=(10, 5))
    for i, model_name in enumerate(models):
        vals = [all_results[model_name]["overall"].get(m, 0) for m in metrics]
        offset = (i - (len(models) - 1) / 2) * width
        bars = ax.bar(x + offset, vals, width, label=model_name, color=colors.get(model_name, "#888"), alpha=0.85)
        for bar, v in zip(bars, vals):
            ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.01,
                    f"{v:.3f}", ha="center", va="bottom", fontsize=8)

    ax.set_ylabel("Score")
    ax.set_title("Link Prediction: TransE vs DistMult vs R-GCN (Overall)")
    ax.set_xticks(x)
    ax.set_xticklabels([m.upper() for m in metrics])
    ax.legend()
    ax.set_ylim(0, 1.05)
    ax.grid(True, alpha=0.3, axis="y")
    plt.tight_layout()
    fig.savefig(fig_dir / "rgcn_vs_baselines.png", dpi=200, bbox_inches="tight")
    plt.close(fig)
    print(f"  -> figs/rgcn_vs_baselines.png")

    # per-relation comparison for Gene__associated_with__Disease
    gda_key = "Gene__associated_with__Disease"
    if all(gda_key in all_results[m] for m in models):
        fig, ax = plt.subplots(figsize=(8, 4))
        model_names = models
        for metric in metrics:
            vals = [all_results[m][gda_key].get(metric, 0) for m in model_names]
            ax.plot(model_names, vals, marker="o", label=metric.upper(), linewidth=2)
        ax.set_ylabel("Score")
        ax.set_title("Gene-Disease Association Prediction (Key Relation)")
        ax.legend()
        ax.set_ylim(0, 1.0)
        ax.grid(True, alpha=0.3)
        plt.tight_layout()
        fig.savefig(fig_dir / "rgcn_gda_comparison.png", dpi=200, bbox_inches="tight")
        plt.close(fig)
        print(f"  -> figs/rgcn_gda_comparison.png")

    # per-relation MRR heatmap
    all_rels = set()
    for m in models:
        all_rels.update(k for k in all_results[m] if k != "overall")
    rels = sorted(all_rels)

    if rels:
        fig, ax = plt.subplots(figsize=(10, max(4, len(rels) * 0.4)))
        data_matrix = []
        for r in rels:
            row = [all_results[m].get(r, {}).get("mrr", 0) for m in models]
            data_matrix.append(row)
        data_matrix = np.array(data_matrix)

        im = ax.imshow(data_matrix, cmap="RdYlGn", aspect="auto", vmin=0, vmax=1)
        ax.set_xticks(range(len(models)))
        ax.set_xticklabels(models)
        short_rels = [r.replace("__", "\n") for r in rels]
        ax.set_yticks(range(len(rels)))
        ax.set_yticklabels(short_rels, fontsize=7)
        for i in range(len(rels)):
            for j in range(len(models)):
                v = data_matrix[i, j]
                ax.text(j, i, f"{v:.2f}", ha="center", va="center", fontsize=7,
                        color="black" if v > 0.5 else "white")
        plt.colorbar(im, ax=ax, label="MRR")
        ax.set_title("MRR per Edge Type and Model")
        plt.tight_layout()
        fig.savefig(fig_dir / "rgcn_mrr_heatmap.png", dpi=200, bbox_inches="tight")
        plt.close(fig)
        print(f"  -> figs/rgcn_mrr_heatmap.png")


def main():
    print("=" * 70)
    print("04_train_rgcn.py")
    print("=" * 70)
    set_seed(SEED)

    print("\n[1/5] Loading graph ...")
    edge_index, edge_type, n_entities, n_relations, rel_to_id, edge_type_names, node_offsets = load_and_flatten()

    print("\n[2/5] Splitting triples ...")
    all_triples = build_triples(edge_index, edge_type)
    train, val, test = split_triples(all_triples)
    filter_set = build_filter_set(all_triples)

    print("\n[3/5] Training R-GCN ...")
    model = RGCN(n_entities, n_relations, HIDDEN_DIM, NUM_LAYERS, NUM_BASES, DROPOUT)
    model.to(DEVICE)
    optimizer = torch.optim.Adam(model.parameters(), lr=LR)

    t0 = time.time()
    losses = train_model(model, optimizer, train, all_triples, n_entities, EPOCHS)
    dt = time.time() - t0
    print(f"    Done in {dt:.1f}s")

    print("\n[4/5] Evaluating R-GCN ...")
    results = evaluate(model, test, all_triples, filter_set, n_entities, edge_type_names)
    m = results["overall"]
    print(f"    Overall: MRR={m['mrr']:.4f}  H@1={m['hits@1']:.4f}  H@3={m['hits@3']:.4f}  H@10={m['hits@10']:.4f}")

    for rel in sorted(results.keys()):
        if rel == "overall":
            continue
        rm = results[rel]
        print(f"    {rel}: MRR={rm['mrr']:.4f}  H@1={rm['hits@1']:.4f}  H@10={rm['hits@10']:.4f}  (n={rm['n_triples']})")

    torch.save(model.state_dict(), PROCESSED_DIR / "rgcn_weights.pt")
    with open(PROCESSED_DIR / "rgcn_results.json", "w") as f:
        json.dump(results, f, indent=2)
    print(f"  -> rgcn_results.json, rgcn_weights.pt")

    print("\n[5/5] Generating thesis figures ...")
    fig_training_curve(losses, FIG_DIR)
    fig_comparison(results, FIG_DIR)

    print("\n" + "=" * 70)
    print(f"R-GCN: MRR={m['mrr']:.4f}  H@1={m['hits@1']:.4f}  H@3={m['hits@3']:.4f}  H@10={m['hits@10']:.4f}")
    print("=" * 70)


if __name__ == "__main__":
    main()
