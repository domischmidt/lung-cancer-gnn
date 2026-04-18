"""
06_predict_novel_links.py - Predict novel links for ALL edge types in the KG.

Scores all possible (head, relation, tail) combinations for every relation
type, filters out known triples, and ranks novel predictions by confidence.

Usage:  python gnn/src/06_predict_novel_links.py
Input:  gnn/data/processed/{hetero_graph.pt, rgcn_weights.pt, node_id_maps.json}
Output: gnn/data/processed/novel_predictions_all.json
        gnn/data/processed/novel_*.csv (one per relation)
        gnn/data/interim/figs/novel_*.png
"""

import json
import csv
import time
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

HIDDEN_DIM = 128
NUM_LAYERS = 2
NUM_BASES = 4
DROPOUT = 0.2
TOP_K = 50
MAX_CANDIDATES = 500_000


class RGCNWithFeatures(nn.Module):
    def __init__(self, n_nodes, n_relations, hidden_dim, num_layers, num_bases, dropout, node_features):
        super().__init__()
        self.node_emb = nn.Embedding(n_nodes, hidden_dim)
        nn.init.xavier_uniform_(self.node_emb.weight)
        self.feat_projections = nn.ModuleDict()
        for nt, info in node_features.items():
            feat_dim = info["feat"].shape[1]
            self.feat_projections[nt] = nn.Linear(feat_dim, hidden_dim)
        self.node_feature_info = node_features
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


def load_graph_and_model():
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

    node_counts = {}
    for nt in data.node_types:
        node_counts[nt] = data[nt].num_nodes

    all_src, all_dst, all_rel = [], [], []
    rel_to_id = {}
    edge_type_names = []
    edge_type_meta = {}

    for et in data.edge_types:
        src_type, rel, dst_type = et
        rel_key = f"{src_type}__{rel}__{dst_type}"
        if rel_key not in rel_to_id:
            rel_to_id[rel_key] = len(rel_to_id)
            edge_type_names.append(rel_key)
            edge_type_meta[rel_key] = {"src_type": src_type, "rel": rel, "dst_type": dst_type}
        rid = rel_to_id[rel_key]
        ei = data[et].edge_index
        all_src.append(ei[0] + node_offsets[src_type])
        all_dst.append(ei[1] + node_offsets[dst_type])
        all_rel.append(torch.full((ei.size(1),), rid, dtype=torch.long))

    edge_index = torch.stack([torch.cat(all_src), torch.cat(all_dst)])
    edge_type = torch.cat(all_rel)

    known_triples = set()
    for i in range(edge_index.size(1)):
        known_triples.add((edge_index[0, i].item(), edge_type[i].item(), edge_index[1, i].item()))

    model = RGCNWithFeatures(total_nodes, len(rel_to_id), HIDDEN_DIM, NUM_LAYERS, NUM_BASES, DROPOUT, node_features)
    model.load_state_dict(torch.load(PROCESSED_DIR / "rgcn_weights.pt", map_location=DEVICE, weights_only=True))
    model.to(DEVICE)
    model.eval()

    with open(PROCESSED_DIR / "node_id_maps.json") as f:
        node_id_maps = json.load(f)

    print(f"Graph: {total_nodes:,} nodes, {len(rel_to_id)} relations, {edge_index.size(1):,} edges")
    print(f"Known triples: {len(known_triples):,}")

    return (model, edge_index, edge_type, total_nodes, rel_to_id,
            edge_type_names, edge_type_meta, node_offsets, node_counts,
            known_triples, node_id_maps)


def build_label_maps(node_id_maps):
    idx_to_label = {}
    for ntype, entries in node_id_maps.items():
        for uri, info in entries.items():
            idx_to_label[(ntype, info["idx"])] = {
                "uri": uri,
                "label": info.get("label", ""),
                "type": ntype,
            }
    return idx_to_label


def get_neighbor_context(node_global_idx, edge_index, edge_type, edge_type_names, idx_to_label, node_offsets, max_neighbors=3):
    mask = edge_index[0] == node_global_idx
    neighbors = []
    for i in mask.nonzero(as_tuple=True)[0][:max_neighbors]:
        t = edge_index[1, i].item()
        r = edge_type[i].item()
        rel_name = edge_type_names[r] if r < len(edge_type_names) else f"rel_{r}"
        neighbor_info = None
        for ntype, noffset in node_offsets.items():
            local_idx = t - noffset
            if 0 <= local_idx and (ntype, local_idx) in idx_to_label:
                neighbor_info = idx_to_label[(ntype, local_idx)]
                break
        if neighbor_info:
            neighbors.append({
                "relation": rel_name.split("__")[1] if "__" in rel_name else rel_name,
                "node": neighbor_info.get("label", "") or neighbor_info.get("uri", ""),
                "type": neighbor_info.get("type", ""),
            })
    return neighbors


@torch.no_grad()
def predict_novel_links_for_relation(model, z, rel_id, src_type, dst_type,
                                      node_offsets, node_counts, known_triples, top_k=TOP_K):
    src_offset = node_offsets[src_type]
    dst_offset = node_offsets[dst_type]
    src_count = node_counts[src_type]
    dst_count = node_counts[dst_type]
    total_candidates = src_count * dst_count

    if total_candidates > MAX_CANDIDATES:
        sample_src = min(src_count, int(MAX_CANDIDATES / dst_count))
        src_indices = torch.randperm(src_count)[:sample_src].tolist()
    else:
        src_indices = list(range(src_count))
        sample_src = src_count

    all_predictions = []
    r_tensor = torch.full((dst_count,), rel_id, dtype=torch.long, device=DEVICE)

    for src_local in src_indices:
        src_global = src_local + src_offset
        h_tensor = torch.full((dst_count,), src_global, dtype=torch.long, device=DEVICE)
        t_tensor = torch.arange(dst_offset, dst_offset + dst_count, device=DEVICE)
        scores = model.decode(z, h_tensor, r_tensor, t_tensor)

        for dst_local in range(dst_count):
            dst_global = dst_local + dst_offset
            if (src_global, rel_id, dst_global) in known_triples:
                continue
            all_predictions.append((src_local, dst_local, scores[dst_local].item()))

    all_predictions.sort(key=lambda x: -x[2])
    return all_predictions[:top_k], sample_src, total_candidates


def format_predictions(predictions, src_type, dst_type, idx_to_label, edge_index, edge_type,
                       edge_type_names, node_offsets):
    formatted = []
    for src_local, dst_local, score in predictions:
        src_info = idx_to_label.get((src_type, src_local), {})
        dst_info = idx_to_label.get((dst_type, dst_local), {})
        src_global = src_local + node_offsets[src_type]
        context = get_neighbor_context(src_global, edge_index, edge_type, edge_type_names, idx_to_label, node_offsets)
        formatted.append({
            "src_label": src_info.get("label", src_info.get("uri", f"{src_type}_{src_local}")),
            "src_uri": src_info.get("uri", ""),
            "dst_label": dst_info.get("label", dst_info.get("uri", f"{dst_type}_{dst_local}")),
            "dst_uri": dst_info.get("uri", ""),
            "confidence": round(score, 4),
            "src_context": context,
        })
    return formatted


def write_predictions_csv(predictions, path, src_type, dst_type):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["rank", f"{src_type}_label", f"{src_type}_uri", f"{dst_type}_label", f"{dst_type}_uri", "confidence", "context"])
        for i, p in enumerate(predictions, 1):
            ctx = "; ".join(f"{c['relation']} -> {c['node']}" for c in p["src_context"][:3])
            w.writerow([i, p["src_label"], p["src_uri"], p["dst_label"], p["dst_uri"], p["confidence"], ctx])


def fig_all_predictions_summary(all_novel, fig_dir):
    fig_dir.mkdir(parents=True, exist_ok=True)
    relations = sorted(all_novel.keys(), key=lambda k: -all_novel[k]["top_confidence"])
    rel_labels = [k.replace("__", "\n") for k in relations]
    top_confs = [all_novel[k]["top_confidence"] for k in relations]

    fig, ax = plt.subplots(figsize=(10, max(5, len(relations) * 0.45)))
    colors = ["#c44e52" if c > 3 else "#4c72b0" if c > 1 else "#55a868" for c in top_confs]
    ax.barh(rel_labels[::-1], top_confs[::-1], color=colors[::-1], alpha=0.85, edgecolor="white", linewidth=0.5)
    ax.set_xlabel("Top-1 Confidence Score")
    ax.set_title("Highest Confidence Novel Prediction per Relation Type", fontsize=12, fontweight="bold")
    for i, v in enumerate(top_confs[::-1]):
        ax.text(v + max(top_confs) * 0.01, i, f"{v:.2f}", va="center", fontsize=7)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    plt.tight_layout()
    fig.savefig(fig_dir / "novel_all_relations_summary.png", dpi=300, bbox_inches="tight")
    plt.close(fig)
    print(f"  -> figs/novel_all_relations_summary.png")


def fig_top_predictions_grid(all_formatted, fig_dir):
    fig_dir.mkdir(parents=True, exist_ok=True)
    interesting = [k for k, v in all_formatted.items() if v and v[0]["confidence"] > 0.5]
    interesting.sort(key=lambda k: -all_formatted[k][0]["confidence"])
    interesting = interesting[:6]
    if not interesting:
        return

    cols = min(3, len(interesting))
    rows = (len(interesting) + cols - 1) // cols
    fig, axes = plt.subplots(rows, cols, figsize=(7 * cols, 4 * rows))
    axes = np.array(axes).flatten() if hasattr(np.array(axes), 'flatten') else [axes]

    for ax, rel_key in zip(axes, interesting):
        preds = all_formatted[rel_key][:10]
        labels = [f"{p['src_label'][:18]} -> {p['dst_label'][:18]}" for p in preds]
        scores = [p["confidence"] for p in preds]
        ax.barh(range(len(scores)), scores, color="#c44e52", alpha=0.8, edgecolor="white", linewidth=0.5)
        ax.set_yticks(range(len(labels)))
        ax.set_yticklabels(labels, fontsize=6)
        ax.set_xlabel("Confidence")
        ax.set_title(rel_key.replace("__", " > "), fontsize=9, fontweight="bold")
        ax.invert_yaxis()
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)

    for ax in axes[len(interesting):]:
        ax.set_visible(False)

    plt.suptitle("Top 10 Novel Predictions per Relation Type", fontsize=13, fontweight="bold")
    plt.tight_layout()
    fig.savefig(fig_dir / "novel_top10_per_type.png", dpi=300, bbox_inches="tight")
    plt.close(fig)
    print(f"  -> figs/novel_top10_per_type.png")


def main():
    print("=" * 70)
    print("06_predict_novel_links.py (all relations)")
    print("=" * 70)

    print("\n[1/4] Loading graph and trained R-GCN ...")
    (model, edge_index, edge_type, total_nodes, rel_to_id,
     edge_type_names, edge_type_meta, node_offsets, node_counts,
     known_triples, node_id_maps) = load_graph_and_model()

    idx_to_label = build_label_maps(node_id_maps)

    print("\n[2/4] Computing embeddings ...")
    with torch.no_grad():
        z = model.encode(edge_index.to(DEVICE), edge_type.to(DEVICE))
    print(f"  Embeddings shape: {z.shape}")

    print("\n[3/4] Predicting novel links for all relations ...")
    all_novel = {}
    all_formatted = {}

    for rel_key in edge_type_names:
        meta = edge_type_meta[rel_key]
        src_type, dst_type = meta["src_type"], meta["dst_type"]
        rel_id = rel_to_id[rel_key]
        display = rel_key.replace("__", " > ")
        print(f"\n  {display}")

        t0 = time.time()
        top_preds, n_scored, total_cand = predict_novel_links_for_relation(
            model, z, rel_id, src_type, dst_type,
            node_offsets, node_counts, known_triples, top_k=TOP_K
        )
        dt = time.time() - t0

        formatted = format_predictions(top_preds, src_type, dst_type, idx_to_label,
                                       edge_index, edge_type, edge_type_names, node_offsets)
        all_formatted[rel_key] = formatted

        top_conf = formatted[0]["confidence"] if formatted else 0
        all_novel[rel_key] = {
            "n_predictions": len(formatted),
            "top_confidence": top_conf,
            "total_candidates": total_cand,
            "time_seconds": round(dt, 1),
        }

        csv_path = PROCESSED_DIR / f"novel_{rel_key.lower()}.csv"
        write_predictions_csv(formatted, csv_path, src_type, dst_type)
        print(f"    {len(formatted)} preds (top={top_conf:.4f}, {dt:.1f}s) -> {csv_path.name}")

        for i, p in enumerate(formatted[:3], 1):
            ctx_str = ", ".join(f"{c['relation']}->{c['node'][:15]}" for c in p["src_context"][:2])
            print(f"      {i}. {p['src_label'][:30]} -> {p['dst_label'][:30]}  ({p['confidence']:.4f})  [{ctx_str}]")

    print("\n[4/4] Saving and generating figures ...")
    with open(PROCESSED_DIR / "novel_predictions_all.json", "w") as f:
        json.dump({"summary": all_novel, "predictions": {k: v[:20] for k, v in all_formatted.items()}}, f, indent=2)
    print(f"  -> novel_predictions_all.json")

    fig_all_predictions_summary(all_novel, FIG_DIR)
    fig_top_predictions_grid(all_formatted, FIG_DIR)

    print("\n" + "=" * 70)
    print("Done.")
    for rel_key in edge_type_names:
        info = all_novel[rel_key]
        print(f"  {rel_key:55s} {info['n_predictions']:>3d} preds  top={info['top_confidence']:>7.4f}")
    print("=" * 70)


if __name__ == "__main__":
    main()
