"""
06_predict_novel_links.py - Predict novel links for DISCOVERY edge types.

Only predicts on relations where novel discoveries are meaningful:
  - GDA -> Disease (Gene-Disease associations, mapped back to Gene-Disease pairs)
  - CLA -> GeoPoliticalRegion / GeographicRegion (new exposure measurements)
  - VitalStats -> GeographicRegion / Country (new cancer stats)
  - Disease -> GeneFusion / ChromoRearr (new structural variants)
  - VDA -> Disease (Variant-Disease associations)
  - Gene -> in_pathway -> Pathway (new pathway memberships)

Excludes definitional/structural relations (subtype_of, part_of Country,
has_time_boundary, etc.) that cannot produce meaningful discoveries.

Applies:
  - Type-aware scoring (only correct tail types)
  - Sigmoid normalization (scores are probabilities, not raw logits)
  - Tail diversity filter (max N predictions per unique tail entity)
  - Gene-Disease mapping (GDA predictions resolved to Gene-Disease pairs)

Usage:  python gnn/src/06_predict_novel_links.py
Input:  gnn/data/processed/{hetero_graph.pt, rgcn_weights.pt, node_id_maps.json}
Output: gnn/data/processed/novel_predictions_all.json
        gnn/data/processed/novel_*.csv
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
NUM_LAYERS = 4
NUM_BASES = 6
DROPOUT = 0.2
TOP_K = 50
MAX_PER_TAIL = 3       # diversity: max predictions per unique tail entity
MAX_CANDIDATES = 500_000

# Relations where novel predictions are meaningful
DISCOVERY_RELATIONS = {
    "GeneDiseaseAssociation__associated_with__Disease",
    "VariantDiseaseAssociation__variant_of__Disease",
    "Gene__in_pathway__Pathway",
    "Disease__has_fusion__GeneFusion",
    "Disease__has_rearrangement__ChromoRearr",
    "Biomarker__marker_for__Disease",
    "ChemicalLocationAssociation__refers_to__GeoPoliticalRegion",
    "ChemicalLocationAssociation__refers_to__GeographicRegion",
    "Disease__detected_finding__VitalStatistics",
    "VitalStatistics__part_of__GeographicRegion",
    "VitalStatistics__part_of__Country",
}

RELATION_DISPLAY = {
    "GeneDiseaseAssociation__associated_with__Disease": "GDA -> Disease",
    "VariantDiseaseAssociation__variant_of__Disease": "VDA -> Disease",
    "Gene__in_pathway__Pathway": "Gene -> Pathway",
    "Disease__has_fusion__GeneFusion": "Disease -> GeneFusion",
    "Disease__has_rearrangement__ChromoRearr": "Disease -> ChromoRearr",
    "Biomarker__marker_for__Disease": "Biomarker -> Disease",
    "ChemicalLocationAssociation__refers_to__GeoPoliticalRegion": "CLA -> City",
    "ChemicalLocationAssociation__refers_to__GeographicRegion": "CLA -> Region",
    "Disease__detected_finding__VitalStatistics": "Disease -> VitalStats",
    "VitalStatistics__part_of__GeographicRegion": "VitalStats -> Region",
    "VitalStatistics__part_of__Country": "VitalStats -> Country",
}


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


# =========================================================================
# Graph and model loading
# =========================================================================

def load_graph_and_model():
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

    node_counts = {nt: data[nt].num_nodes for nt in data.node_types}

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

    # Known triples for filtering
    known_triples = set()
    for i in range(edge_index.size(1)):
        known_triples.add((edge_index[0, i].item(), edge_type[i].item(), edge_index[1, i].item()))

    # Load model
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
            type_ranges, known_triples, node_id_maps)


# =========================================================================
# Label and context helpers
# =========================================================================

def build_label_maps(node_id_maps, node_offsets):
    """Build global_idx -> label map."""
    idx_to_label = {}
    for ntype, entries in node_id_maps.items():
        offset = node_offsets.get(ntype, 0)
        for uri, info in entries.items():
            global_idx = info["idx"] + offset
            idx_to_label[global_idx] = {
                "uri": uri,
                "label": info.get("label", ""),
                "type": ntype,
            }
    return idx_to_label


def build_gda_to_gene_map(edge_index, edge_type, rel_to_id, edge_type_names, idx_to_label, node_offsets):
    """Map each GDA node to its source Gene (via Gene -> has_association -> GDA edges)."""
    gda_to_gene = {}
    gene_assoc_key = "Gene__has_association__GeneDiseaseAssociation"
    if gene_assoc_key not in rel_to_id:
        return gda_to_gene

    rid = rel_to_id[gene_assoc_key]
    mask = edge_type == rid
    src_indices = edge_index[0, mask].tolist()
    dst_indices = edge_index[1, mask].tolist()

    for gene_global, gda_global in zip(src_indices, dst_indices):
        gene_info = idx_to_label.get(gene_global, {})
        gda_to_gene[gda_global] = {
            "gene_label": gene_info.get("label", ""),
            "gene_uri": gene_info.get("uri", ""),
            "gene_global_idx": gene_global,
        }

    return gda_to_gene


# =========================================================================
# Novel link prediction
# =========================================================================

@torch.no_grad()
def predict_novel_links_for_relation(model, z, rel_id, src_type, dst_type,
                                      node_offsets, node_counts, type_ranges,
                                      known_triples, top_k=TOP_K):
    src_offset = node_offsets[src_type]
    dst_lo, dst_hi = type_ranges[dst_type]
    src_count = node_counts[src_type]
    dst_count = dst_hi - dst_lo
    total_candidates = src_count * dst_count

    if total_candidates > MAX_CANDIDATES:
        sample_src = min(src_count, int(MAX_CANDIDATES / max(dst_count, 1)))
        src_indices = torch.randperm(src_count)[:sample_src].tolist()
    else:
        src_indices = list(range(src_count))

    all_predictions = []
    r_tensor = torch.full((dst_count,), rel_id, dtype=torch.long, device=DEVICE)
    t_tensor = torch.arange(dst_lo, dst_hi, device=DEVICE)

    for src_local in src_indices:
        src_global = src_local + src_offset
        h_tensor = torch.full((dst_count,), src_global, dtype=torch.long, device=DEVICE)
        scores = model.decode(z, h_tensor, r_tensor, t_tensor)
        # Sigmoid normalization
        probs = torch.sigmoid(scores)

        for j in range(dst_count):
            dst_global = dst_lo + j
            if (src_global, rel_id, dst_global) in known_triples:
                continue
            all_predictions.append((src_local, dst_global, probs[j].item()))

    all_predictions.sort(key=lambda x: -x[2])

    # Apply tail diversity filter: max MAX_PER_TAIL predictions per unique tail
    tail_counts = defaultdict(int)
    filtered = []
    for src_local, dst_global, prob in all_predictions:
        if tail_counts[dst_global] >= MAX_PER_TAIL:
            continue
        tail_counts[dst_global] += 1
        filtered.append((src_local, dst_global, prob))
        if len(filtered) >= top_k:
            break

    return filtered, len(src_indices), total_candidates


def format_predictions(predictions, src_type, dst_type, idx_to_label,
                       node_offsets, gda_to_gene=None):
    formatted = []
    src_offset = node_offsets[src_type]

    for src_local, dst_global, prob in predictions:
        src_global = src_local + src_offset
        src_info = idx_to_label.get(src_global, {})
        dst_info = idx_to_label.get(dst_global, {})

        entry = {
            "src_label": src_info.get("label", f"{src_type}_{src_local}"),
            "src_uri": src_info.get("uri", ""),
            "dst_label": dst_info.get("label", f"{dst_type}_{dst_global}"),
            "dst_uri": dst_info.get("uri", ""),
            "confidence": round(prob, 4),
        }

        # For GDA -> Disease: resolve back to Gene
        if gda_to_gene is not None and src_global in gda_to_gene:
            gene_info = gda_to_gene[src_global]
            entry["gene_label"] = gene_info["gene_label"]
            entry["gene_uri"] = gene_info["gene_uri"]
            entry["display"] = f"{gene_info['gene_label']} -> {entry['dst_label']}"
        else:
            entry["display"] = f"{entry['src_label']} -> {entry['dst_label']}"

        formatted.append(entry)

    return formatted


def write_predictions_csv(predictions, path, src_type, dst_type, has_gene=False):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if has_gene:
            w.writerow(["rank", "gene_label", "gene_uri", "disease_label", "disease_uri", "confidence"])
            for i, p in enumerate(predictions, 1):
                w.writerow([i, p.get("gene_label", p["src_label"]), p.get("gene_uri", p["src_uri"]),
                            p["dst_label"], p["dst_uri"], p["confidence"]])
        else:
            w.writerow(["rank", f"{src_type}_label", f"{src_type}_uri",
                         f"{dst_type}_label", f"{dst_type}_uri", "confidence"])
            for i, p in enumerate(predictions, 1):
                w.writerow([i, p["src_label"], p["src_uri"],
                            p["dst_label"], p["dst_uri"], p["confidence"]])


# =========================================================================
# Figures
# =========================================================================

def fig_summary(all_novel, fig_dir):
    fig_dir.mkdir(parents=True, exist_ok=True)

    relations = sorted(all_novel.keys(), key=lambda k: -all_novel[k]["top_confidence"])
    rel_labels = [RELATION_DISPLAY.get(k, k.replace("__", " > ")) for k in relations]
    top_confs = [all_novel[k]["top_confidence"] for k in relations]

    fig, ax = plt.subplots(figsize=(10, max(4, len(relations) * 0.5)))
    colors = ["#c44e52" if c > 0.9 else "#4c72b0" if c > 0.7 else "#55a868" for c in top_confs]
    ax.barh(rel_labels[::-1], top_confs[::-1], color=colors[::-1], alpha=0.85, edgecolor="white", linewidth=0.5)
    ax.set_xlabel("Top-1 confidence (sigmoid)")
    ax.set_title("Highest confidence novel prediction per relation", fontsize=12, fontweight="bold")
    ax.set_xlim(0, 1.05)
    for i, v in enumerate(top_confs[::-1]):
        ax.text(v + 0.01, i, f"{v:.3f}", va="center", fontsize=8)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    plt.tight_layout()
    fig.savefig(fig_dir / "novel_summary.png", dpi=300, bbox_inches="tight")
    plt.close(fig)
    print(f"  -> figs/novel_summary.png")


def fig_gene_disease_predictions(predictions, fig_dir):
    """Dedicated figure for Gene-Disease novel predictions (resolved from GDA)."""
    fig_dir.mkdir(parents=True, exist_ok=True)
    if not predictions:
        return

    top = predictions[:20]
    labels = [p["display"][:45] for p in top]
    confs = [p["confidence"] for p in top]

    fig, ax = plt.subplots(figsize=(10, max(4, len(top) * 0.35)))
    ax.barh(range(len(top)), confs, color="#c44e52", alpha=0.85, edgecolor="white", linewidth=0.5)
    ax.set_yticks(range(len(top)))
    ax.set_yticklabels(labels, fontsize=8)
    ax.set_xlabel("Confidence (sigmoid)")
    ax.set_title("Top 20 novel Gene-Disease predictions", fontsize=12, fontweight="bold")
    ax.set_xlim(0, 1.05)
    ax.invert_yaxis()
    for i, v in enumerate(confs):
        ax.text(v + 0.01, i, f"{v:.3f}", va="center", fontsize=7)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    plt.tight_layout()
    fig.savefig(fig_dir / "novel_gene_disease.png", dpi=300, bbox_inches="tight")
    plt.close(fig)
    print(f"  -> figs/novel_gene_disease.png")


def fig_top_per_type(all_formatted, fig_dir):
    fig_dir.mkdir(parents=True, exist_ok=True)

    # Select relations with actual predictions
    available = [k for k, v in all_formatted.items() if v and v[0]["confidence"] > 0.1]
    available.sort(key=lambda k: -all_formatted[k][0]["confidence"])
    available = available[:6]
    if not available:
        return

    cols = min(3, len(available))
    rows = (len(available) + cols - 1) // cols
    fig, axes = plt.subplots(rows, cols, figsize=(7 * cols, 4 * rows))
    axes = np.array(axes).flatten() if hasattr(np.array(axes), 'flatten') else [axes]

    for ax, rel_key in zip(axes, available):
        preds = all_formatted[rel_key][:10]
        labels = [p["display"][:35] for p in preds]
        scores = [p["confidence"] for p in preds]
        ax.barh(range(len(scores)), scores, color="#4c72b0", alpha=0.8, edgecolor="white", linewidth=0.5)
        ax.set_yticks(range(len(labels)))
        ax.set_yticklabels(labels, fontsize=6)
        ax.set_xlabel("Confidence")
        ax.set_xlim(0, 1.05)
        display = RELATION_DISPLAY.get(rel_key, rel_key.replace("__", " > "))
        ax.set_title(display, fontsize=9, fontweight="bold")
        ax.invert_yaxis()
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)

    for ax in axes[len(available):]:
        ax.set_visible(False)

    plt.suptitle("Top 10 novel predictions per relation (sigmoid normalized, tail-diverse)",
                 fontsize=12, fontweight="bold")
    plt.tight_layout()
    fig.savefig(fig_dir / "novel_top10_per_type.png", dpi=300, bbox_inches="tight")
    plt.close(fig)
    print(f"  -> figs/novel_top10_per_type.png")


# =========================================================================
# Main
# =========================================================================

def main():
    print("=" * 70)
    print("06_predict_novel_links.py (discovery relations only)")
    print("=" * 70)

    print("\n[1/5] Loading graph and trained R-GCN ...")
    (model, edge_index, edge_type, total_nodes, rel_to_id,
     edge_type_names, edge_type_meta, node_offsets, node_counts,
     type_ranges, known_triples, node_id_maps) = load_graph_and_model()

    idx_to_label = build_label_maps(node_id_maps, node_offsets)
    gda_to_gene = build_gda_to_gene_map(edge_index, edge_type, rel_to_id,
                                         edge_type_names, idx_to_label, node_offsets)
    print(f"  GDA -> Gene mappings: {len(gda_to_gene):,}")

    print("\n[2/5] Computing embeddings ...")
    with torch.no_grad():
        z = model.encode(edge_index.to(DEVICE), edge_type.to(DEVICE))
    print(f"  Embeddings shape: {z.shape}")

    # Filter to discovery relations only
    discovery_rels = [r for r in edge_type_names if r in DISCOVERY_RELATIONS]
    skipped_rels = [r for r in edge_type_names if r not in DISCOVERY_RELATIONS]

    print(f"\n  Discovery relations ({len(discovery_rels)}):")
    for r in discovery_rels:
        display = RELATION_DISPLAY.get(r, r)
        print(f"    {display}")
    print(f"  Skipped relations ({len(skipped_rels)}):")
    for r in skipped_rels:
        print(f"    {r.replace('__', ' > ')}")

    print("\n[3/5] Predicting novel links ...")
    all_novel = {}
    all_formatted = {}

    for rel_key in discovery_rels:
        meta = edge_type_meta[rel_key]
        src_type, dst_type = meta["src_type"], meta["dst_type"]
        rel_id = rel_to_id[rel_key]
        display = RELATION_DISPLAY.get(rel_key, rel_key.replace("__", " > "))

        print(f"\n  {display}")

        # Use GDA->Gene mapping for Gene-Disease predictions
        is_gda = rel_key == "GeneDiseaseAssociation__associated_with__Disease"

        t0 = time.time()
        top_preds, n_scored, total_cand = predict_novel_links_for_relation(
            model, z, rel_id, src_type, dst_type,
            node_offsets, node_counts, type_ranges,
            known_triples, top_k=TOP_K,
        )
        dt = time.time() - t0

        formatted = format_predictions(
            top_preds, src_type, dst_type, idx_to_label, node_offsets,
            gda_to_gene=gda_to_gene if is_gda else None,
        )
        all_formatted[rel_key] = formatted

        top_conf = formatted[0]["confidence"] if formatted else 0
        all_novel[rel_key] = {
            "n_predictions": len(formatted),
            "top_confidence": top_conf,
            "total_candidates": total_cand,
            "time_seconds": round(dt, 1),
        }

        csv_path = PROCESSED_DIR / f"novel_{rel_key.lower()}.csv"
        write_predictions_csv(formatted, csv_path, src_type, dst_type, has_gene=is_gda)
        print(f"    {len(formatted)} preds (top={top_conf:.4f}, {dt:.1f}s) -> {csv_path.name}")

        for i, p in enumerate(formatted[:5], 1):
            print(f"      {i}. {p['display'][:50]:50s}  conf={p['confidence']:.4f}")

    print("\n[4/5] Saving results ...")
    with open(PROCESSED_DIR / "novel_predictions_all.json", "w") as f:
        json.dump({
            "summary": all_novel,
            "predictions": {k: v[:20] for k, v in all_formatted.items()},
            "config": {
                "top_k": TOP_K,
                "max_per_tail": MAX_PER_TAIL,
                "score_normalization": "sigmoid",
                "discovery_relations": list(DISCOVERY_RELATIONS),
            },
        }, f, indent=2)
    print(f"  -> novel_predictions_all.json")

    print("\n[5/5] Generating figures ...")
    fig_summary(all_novel, FIG_DIR)
    fig_top_per_type(all_formatted, FIG_DIR)

    # Dedicated Gene-Disease figure (resolved from GDA)
    gda_key = "GeneDiseaseAssociation__associated_with__Disease"
    if gda_key in all_formatted:
        fig_gene_disease_predictions(all_formatted[gda_key], FIG_DIR)

    print("\n" + "=" * 70)
    print("Done. Novel predictions summary:")
    for rel_key in discovery_rels:
        if rel_key in all_novel:
            info = all_novel[rel_key]
            display = RELATION_DISPLAY.get(rel_key, rel_key)
            print(f"  {display:35s} {info['n_predictions']:>3d} preds  "
                  f"top={info['top_confidence']:.4f}")
    print("=" * 70)


if __name__ == "__main__":
    main()