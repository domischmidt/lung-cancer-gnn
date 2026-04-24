"""
08_full_evaluation.py - Multi-seed evaluation, ablation study, significance tests.

1. Multi-seed: 5 seeds x 3 models (TransE, DotProduct, R-GCN), reports mean +/- std
2. Ablation: R-GCN on Bio-only vs Env-only vs Combined graph
3. Significance: Chemical-Disease embedding similarity vs random pairs (permutation test)

Usage:  python gnn/src/08_full_evaluation.py
Output: gnn/data/processed/full_evaluation_results.json
        gnn/data/interim/figs/eval_*.png
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

from torch_geometric.nn import RGCNConv

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
PROCESSED_DIR = REPO_ROOT / "gnn" / "data" / "processed"
INTERIM_DIR = REPO_ROOT / "gnn" / "data" / "interim"
FIG_DIR = INTERIM_DIR / "figs"

DEVICE = torch.device("mps" if torch.backends.mps.is_available() else "cuda" if torch.cuda.is_available() else "cpu")

HIDDEN_DIM = 128
NUM_LAYERS = 2
NUM_BASES = 4
DROPOUT = 0.2
EPOCHS = 200
BATCH_SIZE = 4096
LR = 0.001
MARGIN = 1.0
NEG_RATIO = 10
VAL_RATIO = 0.1
TEST_RATIO = 0.1

SEEDS = [42, 123, 456, 789, 1337]
N_EVAL_SAMPLES = 500
N_PERMUTATIONS = 1000

BIO_RELATIONS = {
    "Gene__associated_with__Disease", "Gene__in_pathway__Pathway",
    "Disease__has_fusion__GeneFusion", "Disease__has_rearrangement__ChromoRearr",
    "Variant__variant_of__Disease", "Variant__located_in_gene__Gene",
    "GeneProduct__part_of_pathway__Pathway", "Biomarker__marker_for__Disease",
    "Pathway__linked_to__Disease",
}

CHEMICAL_NAMES = {
    "C0030106": "Ozone", "C5890534": "PM2.5", "C0005036": "Benzene",
    "C0005052": "BaP", "C0028160": "NO2", "C1720884_10": "PM10",
    "C1720884_10_As": "PM10(As)", "C1720884_10_Cd": "PM10(Cd)",
    "C1720884_10_Ni": "PM10(Ni)", "C1720884_10_Pb": "PM10(Pb)",
}


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


class DotProduct(nn.Module):
    def __init__(self, n_entities, n_relations, dim):
        super().__init__()
        self.ent_emb = nn.Embedding(n_entities, dim)
        nn.init.xavier_uniform_(self.ent_emb.weight)

    def score(self, h, r, t):
        return (self.ent_emb(h) * self.ent_emb(t)).sum(dim=-1)

    def forward(self, pos_h, pos_r, pos_t, neg_h, neg_r, neg_t):
        pos_score = self.score(pos_h, pos_r, pos_t)
        neg_score = self.score(neg_h, neg_r, neg_t)
        pos_loss = -torch.log(torch.sigmoid(pos_score) + 1e-9).mean()
        neg_loss = -torch.log(1 - torch.sigmoid(neg_score) + 1e-9).mean()
        return (pos_loss + neg_loss) / 2


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

    def score(self, h, r, t):
        return (h * self.rel_emb(r) * t).sum(dim=-1)

    def encode(self, edge_index, edge_type):
        x = self.get_initial_embeddings()
        for conv in self.convs:
            x = conv(x, edge_index, edge_type)
            x = F.relu(x)
            x = F.dropout(x, p=self.dropout, training=self.training)
        return x


def set_seed(seed):
    random.seed(seed)
    np.random.seed(seed)
    torch.manual_seed(seed)


def load_graph():
    data = torch.load(PROCESSED_DIR / "hetero_graph.pt", weights_only=False)
    node_offsets = {}
    offset = 0
    node_features = {}
    for nt in data.node_types:
        node_offsets[nt] = offset
        n = data[nt].num_nodes
        if hasattr(data[nt], 'x') and data[nt].x is not None:
            node_features[nt] = {"offset": offset, "n": n, "feat": data[nt].x}
        offset += n
    total_nodes = offset

    all_triples = []
    rel_to_id = {}
    edge_type_names = []
    edge_type_meta = {}

    for et in data.edge_types:
        src_type, rel, dst_type = et
        rel_key = f"{src_type}__{rel}__{dst_type}"
        if rel_key not in rel_to_id:
            rel_to_id[rel_key] = len(rel_to_id)
            edge_type_names.append(rel_key)
            edge_type_meta[rel_key] = {"src_type": src_type, "dst_type": dst_type}
        rid = rel_to_id[rel_key]
        ei = data[et].edge_index
        src_off = node_offsets[src_type]
        dst_off = node_offsets[dst_type]
        for i in range(ei.size(1)):
            all_triples.append((ei[0, i].item() + src_off, rid, ei[1, i].item() + dst_off))

    triples = torch.tensor(all_triples, dtype=torch.long)
    return triples, total_nodes, rel_to_id, edge_type_names, edge_type_meta, node_offsets, node_features, data


def split_triples(triples, seed):
    set_seed(seed)
    n = triples.size(0)
    perm = torch.randperm(n)
    n_test = int(n * TEST_RATIO)
    n_val = int(n * VAL_RATIO)
    test = triples[perm[:n_test]]
    val = triples[perm[n_test:n_test + n_val]]
    train = triples[perm[n_test + n_val:]]
    return train, val, test


def build_filter_set(triples):
    s = set()
    for i in range(triples.size(0)):
        s.add(tuple(triples[i].tolist()))
    return s


def generate_negatives(batch, n_entities):
    neg = batch.repeat(NEG_RATIO, 1)
    mask = torch.randint(0, 2, (neg.size(0),), dtype=torch.bool)
    rand_ents = torch.randint(0, n_entities, (neg.size(0),))
    neg[mask, 0] = rand_ents[mask]
    neg[~mask, 2] = rand_ents[~mask]
    return neg


def train_shallow(model, train_triples, n_entities):
    optimizer = torch.optim.Adam(model.parameters(), lr=LR)
    model.to(DEVICE)
    model.train()
    for epoch in range(1, EPOCHS + 1):
        perm = torch.randperm(train_triples.size(0))
        for i in range(0, train_triples.size(0), BATCH_SIZE):
            batch = train_triples[perm[i:i + BATCH_SIZE]]
            neg = generate_negatives(batch, n_entities)
            loss = model(batch[:, 0].to(DEVICE), batch[:, 1].to(DEVICE), batch[:, 2].to(DEVICE),
                         neg[:, 0].to(DEVICE), neg[:, 1].to(DEVICE), neg[:, 2].to(DEVICE))
            optimizer.zero_grad()
            loss.backward()
            optimizer.step()


def train_rgcn(model, train_triples, all_triples, n_entities):
    optimizer = torch.optim.Adam(model.parameters(), lr=LR)
    model.to(DEVICE)
    train_ei = all_triples[:, [0, 2]].t()
    train_et = all_triples[:, 1]

    for epoch in range(1, EPOCHS + 1):
        model.train()
        z = model.encode(train_ei.to(DEVICE), train_et.to(DEVICE))
        perm = torch.randperm(train_triples.size(0))
        batch = train_triples[perm[:BATCH_SIZE]]
        neg = generate_negatives(batch, n_entities)

        pos_score = model.score(z[batch[:, 0].to(DEVICE)], batch[:, 1].to(DEVICE), z[batch[:, 2].to(DEVICE)])
        neg_score = model.score(z[neg[:, 0].to(DEVICE)], neg[:, 1].to(DEVICE), z[neg[:, 2].to(DEVICE)])

        pos_loss = -torch.log(torch.sigmoid(pos_score) + 1e-9).mean()
        neg_loss = -torch.log(1 - torch.sigmoid(neg_score) + 1e-9).mean()
        loss = (pos_loss + neg_loss) / 2

        optimizer.zero_grad()
        loss.backward()
        torch.nn.utils.clip_grad_norm_(model.parameters(), 1.0)
        optimizer.step()


@torch.no_grad()
def evaluate_model(model, test_triples, filter_set, n_entities, edge_type_names, is_rgcn=False, all_triples=None):
    model.eval()
    model.to(DEVICE)

    if is_rgcn:
        train_ei = all_triples[:, [0, 2]].t()
        train_et = all_triples[:, 1]
        z = model.encode(train_ei.to(DEVICE), train_et.to(DEVICE))

    if test_triples.size(0) > N_EVAL_SAMPLES:
        idx = torch.randperm(test_triples.size(0))[:N_EVAL_SAMPLES]
        test_triples = test_triples[idx]

    per_rel = defaultdict(list)
    all_ranks = []

    for i in range(test_triples.size(0)):
        h, r, t = test_triples[i].tolist()
        all_ents = torch.arange(n_entities, device=DEVICE)
        h_rep = torch.full((n_entities,), h, dtype=torch.long, device=DEVICE)
        r_rep = torch.full((n_entities,), r, dtype=torch.long, device=DEVICE)

        if is_rgcn:
            scores = model.score(z[h_rep], r_rep, z[all_ents])
        else:
            scores = model.score(h_rep, r_rep, all_ents)

        for eid in range(n_entities):
            if eid != t and (h, r, eid) in filter_set:
                scores[eid] = -1e9

        rank = (scores >= scores[t]).sum().item()
        rank = max(rank, 1)
        all_ranks.append(rank)
        rel_name = edge_type_names[r] if r < len(edge_type_names) else f"rel_{r}"
        per_rel[rel_name].append(rank)

    def metrics(ranks):
        r = np.array(ranks, dtype=np.float64)
        return {"mrr": float(np.mean(1.0 / r)), "hits@1": float(np.mean(r <= 1)),
                "hits@3": float(np.mean(r <= 3)), "hits@10": float(np.mean(r <= 10)), "n": len(r)}

    results = {"overall": metrics(all_ranks)}
    for rel, ranks in per_rel.items():
        if len(ranks) >= 3:
            results[rel] = metrics(ranks)
    return results


def filter_triples_by_relations(triples, rel_to_id, keep_rels):
    keep_ids = {rel_to_id[r] for r in keep_rels if r in rel_to_id}
    mask = torch.tensor([triples[i, 1].item() in keep_ids for i in range(triples.size(0))])
    return triples[mask]


# =========================================================================
# 1. Multi-seed evaluation
# =========================================================================

def run_multi_seed(triples, n_entities, rel_to_id, edge_type_names, node_features):
    print("\n" + "=" * 70)
    print("PART 1: Multi-Seed Evaluation")
    print("=" * 70)

    all_seed_results = {m: [] for m in ["TransE", "DotProduct", "R-GCN"]}
    filter_set = build_filter_set(triples)

    for seed in SEEDS:
        print(f"\n  Seed {seed}")
        set_seed(seed)
        train, val, test = split_triples(triples, seed)

        for model_name, ModelClass, is_rgcn in [
            ("TransE", TransE, False),
            ("DotProduct", DotProduct, False),
            ("R-GCN", RGCNWithFeatures, True),
        ]:
            print(f"    {model_name} ...", end=" ", flush=True)
            t0 = time.time()
            set_seed(seed)

            if is_rgcn:
                model = ModelClass(n_entities, len(rel_to_id), HIDDEN_DIM, NUM_LAYERS, NUM_BASES, DROPOUT, node_features)
                train_rgcn(model, train, triples, n_entities)
                results = evaluate_model(model, test, filter_set, n_entities, edge_type_names, True, triples)
            else:
                model = ModelClass(n_entities, len(rel_to_id), HIDDEN_DIM)
                train_shallow(model, train, n_entities)
                results = evaluate_model(model, test, filter_set, n_entities, edge_type_names, False)

            dt = time.time() - t0
            m = results["overall"]
            print(f"MRR={m['mrr']:.4f}  H@10={m['hits@10']:.4f}  ({dt:.0f}s)")
            all_seed_results[model_name].append(results)

            del model
            torch.cuda.empty_cache() if torch.cuda.is_available() else None

    return all_seed_results


def aggregate_multi_seed(all_seed_results):
    aggregated = {}
    for model_name, seed_results in all_seed_results.items():
        all_rels = set()
        for sr in seed_results:
            all_rels.update(sr.keys())

        model_agg = {}
        for rel in all_rels:
            vals = {m: [] for m in ["mrr", "hits@1", "hits@3", "hits@10"]}
            for sr in seed_results:
                if rel in sr:
                    for m in vals:
                        vals[m].append(sr[rel][m])
            model_agg[rel] = {
                m: {"mean": float(np.mean(v)), "std": float(np.std(v)), "values": v}
                for m, v in vals.items() if v
            }
        aggregated[model_name] = model_agg
    return aggregated


# =========================================================================
# 2. Ablation study
# =========================================================================

def run_ablation(triples, n_entities, rel_to_id, edge_type_names, node_features):
    print("\n" + "=" * 70)
    print("PART 2: Ablation Study (R-GCN)")
    print("=" * 70)

    bio_rels = {r for r in rel_to_id if r in BIO_RELATIONS}
    env_rels = {r for r in rel_to_id if r not in BIO_RELATIONS}

    configs = {
        "Combined": None,
        "Bio-only": bio_rels,
        "Env-only": env_rels,
    }

    filter_set = build_filter_set(triples)
    ablation_results = {}

    for config_name, keep_rels in configs.items():
        print(f"\n  {config_name}:")
        seed_results = []

        for seed in SEEDS[:3]:
            set_seed(seed)
            train, val, test = split_triples(triples, seed)

            if keep_rels is not None:
                train_filtered = filter_triples_by_relations(train, rel_to_id, keep_rels)
                all_filtered = filter_triples_by_relations(triples, rel_to_id, keep_rels)
            else:
                train_filtered = train
                all_filtered = triples

            if train_filtered.size(0) == 0:
                print(f"    Seed {seed}: no training triples, skipping")
                continue

            print(f"    Seed {seed} ({train_filtered.size(0):,} train triples) ...", end=" ", flush=True)
            t0 = time.time()

            n_rels_used = len(keep_rels) if keep_rels else len(rel_to_id)
            model = RGCNWithFeatures(n_entities, len(rel_to_id), HIDDEN_DIM, NUM_LAYERS, NUM_BASES, DROPOUT, node_features)
            train_rgcn(model, train_filtered, all_filtered, n_entities)

            gda_key = "Gene__associated_with__Disease"
            if gda_key in rel_to_id:
                gda_test = filter_triples_by_relations(test, rel_to_id, {gda_key})
                if gda_test.size(0) > 0:
                    results = evaluate_model(model, gda_test, filter_set, n_entities, edge_type_names, True, all_filtered)
                else:
                    results = evaluate_model(model, test, filter_set, n_entities, edge_type_names, True, all_filtered)
            else:
                results = evaluate_model(model, test, filter_set, n_entities, edge_type_names, True, all_filtered)

            dt = time.time() - t0
            m = results.get("Gene__associated_with__Disease", results.get("overall", {}))
            print(f"GDA MRR={m.get('mrr', 0):.4f}  H@10={m.get('hits@10', 0):.4f}  ({dt:.0f}s)")
            seed_results.append(results)

            del model
            torch.cuda.empty_cache() if torch.cuda.is_available() else None

        ablation_results[config_name] = seed_results

    return ablation_results


def aggregate_ablation(ablation_results):
    aggregated = {}
    gda_key = "Gene__associated_with__Disease"

    for config, seed_results in ablation_results.items():
        gda_metrics = {m: [] for m in ["mrr", "hits@1", "hits@3", "hits@10"]}
        overall_metrics = {m: [] for m in ["mrr", "hits@1", "hits@3", "hits@10"]}

        for sr in seed_results:
            if gda_key in sr:
                for m in gda_metrics:
                    gda_metrics[m].append(sr[gda_key][m])
            if "overall" in sr:
                for m in overall_metrics:
                    overall_metrics[m].append(sr["overall"][m])

        aggregated[config] = {
            "gda": {m: {"mean": float(np.mean(v)), "std": float(np.std(v))} for m, v in gda_metrics.items() if v},
            "overall": {m: {"mean": float(np.mean(v)), "std": float(np.std(v))} for m, v in overall_metrics.items() if v},
        }
    return aggregated


# =========================================================================
# 3. Significance test
# =========================================================================

def run_significance_test(triples, n_entities, rel_to_id, edge_type_names, node_offsets, node_id_maps, node_features):
    print("\n" + "=" * 70)
    print("PART 3: Embedding Similarity Significance Test")
    print("=" * 70)

    set_seed(42)
    train, val, test = split_triples(triples, 42)

    model = RGCNWithFeatures(n_entities, len(rel_to_id), HIDDEN_DIM, NUM_LAYERS, NUM_BASES, DROPOUT, node_features)
    model.load_state_dict(torch.load(PROCESSED_DIR / "rgcn_weights.pt", map_location=DEVICE, weights_only=True))
    model.to(DEVICE)
    model.eval()

    train_ei = triples[:, [0, 2]].t()
    train_et = triples[:, 1]
    with torch.no_grad():
        z = model.encode(train_ei.to(DEVICE), train_et.to(DEVICE)).cpu()

    chem_offset = node_offsets.get("Chemical", 0)
    disease_offset = node_offsets.get("Disease", 0)
    chem_map = node_id_maps.get("Chemical", {})
    disease_map = node_id_maps.get("Disease", {})

    chem_indices = [info["idx"] + chem_offset for info in chem_map.values()]
    disease_indices = [info["idx"] + disease_offset for info in disease_map.values()]

    if not chem_indices or not disease_indices:
        print("  No Chemical or Disease nodes found")
        return {}

    observed_sims = []
    for ci in chem_indices:
        for di in disease_indices:
            sim = F.cosine_similarity(z[ci].unsqueeze(0), z[di].unsqueeze(0)).item()
            observed_sims.append(sim)
    observed_mean = np.mean(observed_sims)

    print(f"  Observed mean Chemical-Disease similarity: {observed_mean:.4f}")
    print(f"  Running {N_PERMUTATIONS} permutations ...")

    all_indices = list(range(n_entities))
    perm_means = []
    for p in range(N_PERMUTATIONS):
        random_a = random.sample(all_indices, len(chem_indices))
        random_b = random.sample(all_indices, len(disease_indices))
        sims = []
        for ai in random_a:
            for bi in random_b:
                sim = F.cosine_similarity(z[ai].unsqueeze(0), z[bi].unsqueeze(0)).item()
                sims.append(sim)
        perm_means.append(np.mean(sims))

    perm_means = np.array(perm_means)
    p_value = float(np.mean(perm_means >= observed_mean))

    print(f"  Random pair mean similarity: {np.mean(perm_means):.4f} +/- {np.std(perm_means):.4f}")
    print(f"  p-value: {p_value:.4f}")
    print(f"  Significant (p < 0.05): {'Yes' if p_value < 0.05 else 'No'}")

    return {
        "observed_mean": float(observed_mean),
        "random_mean": float(np.mean(perm_means)),
        "random_std": float(np.std(perm_means)),
        "p_value": p_value,
        "n_permutations": N_PERMUTATIONS,
        "n_chemical": len(chem_indices),
        "n_disease": len(disease_indices),
        "permutation_distribution": perm_means.tolist(),
    }


# =========================================================================
# Figures
# =========================================================================

def fig_multi_seed(aggregated, fig_dir):
    fig_dir.mkdir(parents=True, exist_ok=True)
    models = list(aggregated.keys())
    metrics = ["mrr", "hits@1", "hits@3", "hits@10"]
    colors = {"TransE": "#4c72b0", "DotProduct": "#55a868", "R-GCN": "#c44e52"}
    x = np.arange(len(metrics))
    width = 0.22

    # Overall
    fig, ax = plt.subplots(figsize=(10, 5))
    for i, m in enumerate(models):
        means = [aggregated[m].get("overall", {}).get(metric, {}).get("mean", 0) for metric in metrics]
        stds = [aggregated[m].get("overall", {}).get(metric, {}).get("std", 0) for metric in metrics]
        offset = (i - (len(models) - 1) / 2) * width
        bars = ax.bar(x + offset, means, width, yerr=stds, capsize=3,
                      label=m, color=colors.get(m, "#888"), alpha=0.85, edgecolor="white")
        for bar, mean, std in zip(bars, means, stds):
            ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + std + 0.01,
                    f"{mean:.3f}", ha="center", va="bottom", fontsize=7.5)
    ax.set_ylabel("Score")
    ax.set_title("Overall Link Prediction (5 Seeds, mean +/- std)", fontsize=12, fontweight="bold")
    ax.set_xticks(x)
    ax.set_xticklabels([m.upper() for m in metrics])
    ax.legend()
    ax.set_ylim(0, 1.1)
    ax.grid(True, alpha=0.2, axis="y")
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    plt.tight_layout()
    fig.savefig(fig_dir / "eval_multi_seed_overall.png", dpi=300, bbox_inches="tight")
    plt.close(fig)
    print(f"  -> figs/eval_multi_seed_overall.png")

    # Gene-Disease
    gda = "Gene__associated_with__Disease"
    fig, ax = plt.subplots(figsize=(10, 5))
    for i, m in enumerate(models):
        means = [aggregated[m].get(gda, {}).get(metric, {}).get("mean", 0) for metric in metrics]
        stds = [aggregated[m].get(gda, {}).get(metric, {}).get("std", 0) for metric in metrics]
        offset = (i - (len(models) - 1) / 2) * width
        bars = ax.bar(x + offset, means, width, yerr=stds, capsize=3,
                      label=m, color=colors.get(m, "#888"), alpha=0.85, edgecolor="white")
        for bar, mean, std in zip(bars, means, stds):
            if mean > 0:
                ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + std + 0.01,
                        f"{mean:.3f}", ha="center", va="bottom", fontsize=7.5)
    ax.set_ylabel("Score")
    ax.set_title("Gene-Disease Link Prediction (5 Seeds, mean +/- std)", fontsize=12, fontweight="bold")
    ax.set_xticks(x)
    ax.set_xticklabels([m.upper() for m in metrics])
    ax.legend()
    ax.set_ylim(0, 1.1)
    ax.grid(True, alpha=0.2, axis="y")
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    plt.tight_layout()
    fig.savefig(fig_dir / "eval_multi_seed_gda.png", dpi=300, bbox_inches="tight")
    plt.close(fig)
    print(f"  -> figs/eval_multi_seed_gda.png")


def fig_ablation(ablation_agg, fig_dir):
    fig_dir.mkdir(parents=True, exist_ok=True)
    configs = list(ablation_agg.keys())
    metrics = ["mrr", "hits@1", "hits@3", "hits@10"]
    colors_list = ["#4c72b0", "#c44e52", "#55a868"]
    x = np.arange(len(metrics))
    width = 0.22

    fig, ax = plt.subplots(figsize=(10, 5))
    for i, config in enumerate(configs):
        gda = ablation_agg[config].get("gda", {})
        means = [gda.get(m, {}).get("mean", 0) for m in metrics]
        stds = [gda.get(m, {}).get("std", 0) for m in metrics]
        offset = (i - (len(configs) - 1) / 2) * width
        bars = ax.bar(x + offset, means, width, yerr=stds, capsize=3,
                      label=config, color=colors_list[i % len(colors_list)], alpha=0.85, edgecolor="white")
        for bar, mean, std in zip(bars, means, stds):
            if mean > 0:
                ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + std + 0.01,
                        f"{mean:.3f}", ha="center", va="bottom", fontsize=7.5)

    ax.set_ylabel("Score")
    ax.set_title("Ablation Study: Gene-Disease Prediction by Graph Composition\n(R-GCN, 3 Seeds, mean +/- std)",
                 fontsize=12, fontweight="bold")
    ax.set_xticks(x)
    ax.set_xticklabels([m.upper() for m in metrics])
    ax.legend()
    ax.set_ylim(0, 1.1)
    ax.grid(True, alpha=0.2, axis="y")
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    plt.tight_layout()
    fig.savefig(fig_dir / "eval_ablation_gda.png", dpi=300, bbox_inches="tight")
    plt.close(fig)
    print(f"  -> figs/eval_ablation_gda.png")


def fig_significance(sig_results, fig_dir):
    fig_dir.mkdir(parents=True, exist_ok=True)
    if not sig_results or "permutation_distribution" not in sig_results:
        return

    perm_dist = np.array(sig_results["permutation_distribution"])
    observed = sig_results["observed_mean"]
    p_val = sig_results["p_value"]

    fig, ax = plt.subplots(figsize=(8, 5))
    ax.hist(perm_dist, bins=50, color="#4c72b0", alpha=0.7, edgecolor="white", linewidth=0.5, label="Random pair similarities")
    ax.axvline(x=observed, color="#c44e52", linewidth=2.5, linestyle="-", label=f"Observed Chemical-Disease (mean={observed:.4f})")
    ax.set_xlabel("Mean Cosine Similarity")
    ax.set_ylabel("Frequency")
    ax.set_title(f"Permutation Test: Chemical-Disease Embedding Similarity\n(p = {p_val:.4f}, n = {sig_results['n_permutations']} permutations)",
                 fontsize=12, fontweight="bold")
    ax.legend(fontsize=9)
    ax.grid(True, alpha=0.2, axis="y")
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    plt.tight_layout()
    fig.savefig(fig_dir / "eval_significance_test.png", dpi=300, bbox_inches="tight")
    plt.close(fig)
    print(f"  -> figs/eval_significance_test.png")


# =========================================================================
# Main
# =========================================================================

def main():
    print("=" * 70)
    print("08_full_evaluation.py")
    print("=" * 70)
    print(f"Device: {DEVICE}")
    print(f"Seeds: {SEEDS}")

    print("\nLoading graph ...")
    triples, n_entities, rel_to_id, edge_type_names, edge_type_meta, node_offsets, node_features, data = load_graph()
    print(f"  {n_entities:,} nodes, {len(rel_to_id)} relations, {triples.size(0):,} triples")
    feat_str = ", ".join(f"{nt}({info['feat'].shape[1]}d)" for nt, info in node_features.items())
    print(f"  Node features: {feat_str}")

    with open(PROCESSED_DIR / "node_id_maps.json") as f:
        node_id_maps = json.load(f)

    # Part 1
    multi_seed_results = run_multi_seed(triples, n_entities, rel_to_id, edge_type_names, node_features)
    multi_seed_agg = aggregate_multi_seed(multi_seed_results)

    # Part 2
    ablation_results = run_ablation(triples, n_entities, rel_to_id, edge_type_names, node_features)
    ablation_agg = aggregate_ablation(ablation_results)

    # Part 3
    sig_results = run_significance_test(triples, n_entities, rel_to_id, edge_type_names, node_offsets, node_id_maps, node_features)

    # Save
    print("\nSaving results ...")
    full_results = {
        "multi_seed": {
            model: {
                rel: {m: {"mean": v["mean"], "std": v["std"]} for m, v in metrics.items()}
                for rel, metrics in rels.items()
            }
            for model, rels in multi_seed_agg.items()
        },
        "ablation": ablation_agg,
        "significance_test": {k: v for k, v in sig_results.items() if k != "permutation_distribution"},
    }
    with open(PROCESSED_DIR / "full_evaluation_results.json", "w") as f:
        json.dump(full_results, f, indent=2)
    print(f"  -> full_evaluation_results.json")

    # Figures
    print("\nGenerating thesis figures ...")
    fig_multi_seed(multi_seed_agg, FIG_DIR)
    fig_ablation(ablation_agg, FIG_DIR)
    fig_significance(sig_results, FIG_DIR)

    # Summary
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)

    print("\nMulti-Seed Overall (mean +/- std):")
    for model in ["TransE", "DotProduct", "R-GCN"]:
        o = multi_seed_agg[model].get("overall", {})
        mrr = o.get("mrr", {})
        h10 = o.get("hits@10", {})
        print(f"  {model:10s}  MRR={mrr.get('mean',0):.4f}+/-{mrr.get('std',0):.4f}  "
              f"H@10={h10.get('mean',0):.4f}+/-{h10.get('std',0):.4f}")

    gda = "Gene__associated_with__Disease"
    print(f"\nMulti-Seed Gene-Disease (mean +/- std):")
    for model in ["TransE", "DotProduct", "R-GCN"]:
        o = multi_seed_agg[model].get(gda, {})
        mrr = o.get("mrr", {})
        h10 = o.get("hits@10", {})
        print(f"  {model:10s}  MRR={mrr.get('mean',0):.4f}+/-{mrr.get('std',0):.4f}  "
              f"H@10={h10.get('mean',0):.4f}+/-{h10.get('std',0):.4f}")

    print(f"\nAblation (Gene-Disease MRR, R-GCN):")
    for config in ["Combined", "Bio-only", "Env-only"]:
        g = ablation_agg.get(config, {}).get("gda", {}).get("mrr", {})
        print(f"  {config:12s}  MRR={g.get('mean',0):.4f}+/-{g.get('std',0):.4f}")

    print(f"\nSignificance Test:")
    print(f"  Observed Chemical-Disease similarity: {sig_results.get('observed_mean', 0):.4f}")
    print(f"  Random pairs: {sig_results.get('random_mean', 0):.4f}+/-{sig_results.get('random_std', 0):.4f}")
    print(f"  p-value: {sig_results.get('p_value', 1):.4f}")
    print("=" * 70)


if __name__ == "__main__":
    main()
