"""
09_normalize_scores.py - Validate novel predictions against random false triples.

For each novel prediction, we compare its R-GCN score against:
  1. The distribution of scores of KNOWN TRUE triples of the same relation type
     (does the model rate this prediction higher than what it learned as true?)
  2. The distribution of scores of RANDOM FALSE triples of the same relation type
     (does the model rate this prediction higher than what it considers false?)

The gap between (1) and (2) tells us whether the model can distinguish true from
false. The position of a novel prediction within these distributions tells us
how confident the model is.

Usage:  python gnn/src/09_normalize_scores.py
Input:  gnn/data/processed/hetero_graph.pt
        gnn/data/processed/rgcn_weights.pt
        gnn/data/processed/novel_predictions_all.json
Output: gnn/data/processed/novel_predictions_validated.json
        gnn/data/processed/score_distributions.json
        gnn/data/interim/figs/score_validation.png
"""

import json
import torch
import torch.nn as nn
import torch.nn.functional as F
import numpy as np
from pathlib import Path
from collections import defaultdict
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

from torch_geometric.nn import RGCNConv

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
PROCESSED_DIR = REPO_ROOT / "gnn" / "data" / "processed"
FIG_DIR = REPO_ROOT / "gnn" / "data" / "interim" / "figs"

DEVICE = torch.device("mps" if torch.backends.mps.is_available() else "cuda" if torch.cuda.is_available() else "cpu")
SEED = 42

HIDDEN_DIM = 128
NUM_LAYERS = 2
NUM_BASES = 4
DROPOUT = 0.2
N_RANDOM_FALSE = 1000  # number of random false triples to score per relation


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


def main():
    print("=" * 70)
    print("09_normalize_scores.py")
    print("=" * 70)

    np.random.seed(SEED)
    torch.manual_seed(SEED)

    print("\n[1/6] Loading graph and model ...")
    data = torch.load(PROCESSED_DIR / "hetero_graph.pt", weights_only=False)

    # Build node-type ranges (for type-aware random sampling)
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

    # Build flat triples and remember type info per relation
    all_src, all_dst, all_rel = [], [], []
    rel_to_id = {}
    edge_type_names = []
    rel_types = {}  # rel_key -> (src_type, dst_type)

    for et in data.edge_types:
        src_type, rel, dst_type = et
        rel_key = f"{src_type}__{rel}__{dst_type}"
        if rel_key not in rel_to_id:
            rel_to_id[rel_key] = len(rel_to_id)
            edge_type_names.append(rel_key)
            rel_types[rel_key] = (src_type, dst_type)
        rid = rel_to_id[rel_key]
        ei = data[et].edge_index
        all_src.append(ei[0] + node_offsets[src_type])
        all_dst.append(ei[1] + node_offsets[dst_type])
        all_rel.append(torch.full((ei.size(1),), rid, dtype=torch.long))

    edge_index = torch.stack([torch.cat(all_src), torch.cat(all_dst)])
    edge_type = torch.cat(all_rel)
    n_relations = len(rel_to_id)

    print(f"  Graph: {total_nodes:,} nodes, {n_relations} relations, {edge_index.size(1):,} edges")

    # Build set of known triples for fast lookup
    known_triples = set()
    for i in range(edge_index.size(1)):
        known_triples.add((edge_index[0, i].item(), edge_type[i].item(), edge_index[1, i].item()))
    print(f"  Known triples set: {len(known_triples):,}")

    # Load model
    model = RGCNWithFeatures(total_nodes, n_relations, HIDDEN_DIM, NUM_LAYERS, NUM_BASES, DROPOUT, node_features)
    model.load_state_dict(torch.load(PROCESSED_DIR / "rgcn_weights.pt", map_location=DEVICE))
    model.to(DEVICE)
    model.eval()
    print("  Model loaded")

    # === Compute embeddings ===
    print("\n[2/6] Computing embeddings ...")
    with torch.no_grad():
        z = model.encode(edge_index.to(DEVICE), edge_type.to(DEVICE))
    print(f"  Embeddings shape: {z.shape}")

    # === Score known true triples per relation ===
    print("\n[3/6] Scoring known TRUE triples per relation ...")
    known_scores_per_rel = {}
    with torch.no_grad():
        for r_idx in range(n_relations):
            mask = edge_type == r_idx
            if mask.sum() == 0:
                continue
            h = edge_index[0, mask].to(DEVICE)
            t = edge_index[1, mask].to(DEVICE)
            r = torch.full((h.size(0),), r_idx, dtype=torch.long, device=DEVICE)
            scores = model.decode(z, h, r, t).cpu().numpy()
            known_scores_per_rel[edge_type_names[r_idx]] = scores
            print(f"  {edge_type_names[r_idx]}: {len(scores)} TRUE, "
                  f"median={np.median(scores):.3f}, max={scores.max():.3f}")

    # === Score random FALSE triples per relation (type-aware) ===
    print(f"\n[4/6] Scoring {N_RANDOM_FALSE} random FALSE triples per relation ...")
    random_scores_per_rel = {}
    with torch.no_grad():
        for rel_key, (src_type, dst_type) in rel_types.items():
            r_idx = rel_to_id[rel_key]
            src_lo, src_hi = type_ranges[src_type]
            dst_lo, dst_hi = type_ranges[dst_type]

            # Generate N_RANDOM_FALSE random (h, r, t) where h is of src_type, t of dst_type
            # and (h, r, t) is NOT in known_triples
            random_h = []
            random_t = []
            attempts = 0
            max_attempts = N_RANDOM_FALSE * 10
            while len(random_h) < N_RANDOM_FALSE and attempts < max_attempts:
                h_cand = np.random.randint(src_lo, src_hi)
                t_cand = np.random.randint(dst_lo, dst_hi)
                if (h_cand, r_idx, t_cand) not in known_triples:
                    random_h.append(h_cand)
                    random_t.append(t_cand)
                attempts += 1

            if len(random_h) < 10:
                print(f"  {rel_key}: only {len(random_h)} false triples generable, skipping")
                continue

            h = torch.tensor(random_h, dtype=torch.long, device=DEVICE)
            t = torch.tensor(random_t, dtype=torch.long, device=DEVICE)
            r = torch.full((len(random_h),), r_idx, dtype=torch.long, device=DEVICE)
            scores = model.decode(z, h, r, t).cpu().numpy()
            random_scores_per_rel[rel_key] = scores
            print(f"  {rel_key}: {len(scores)} FALSE, "
                  f"median={np.median(scores):.3f}, max={scores.max():.3f}")

    # === Compute distribution stats per relation ===
    print("\n[5/6] Computing distribution statistics ...")
    dist_stats = {}
    for rel in known_scores_per_rel:
        true_arr = known_scores_per_rel[rel]
        false_arr = random_scores_per_rel.get(rel, np.array([]))
        dist_stats[rel] = {
            "true": {
                "n": len(true_arr),
                "min": float(true_arr.min()),
                "median": float(np.median(true_arr)),
                "p75": float(np.percentile(true_arr, 75)),
                "p90": float(np.percentile(true_arr, 90)),
                "p95": float(np.percentile(true_arr, 95)),
                "p99": float(np.percentile(true_arr, 99)),
                "max": float(true_arr.max()),
                "mean": float(true_arr.mean()),
                "std": float(true_arr.std()),
            },
            "false": {
                "n": len(false_arr),
                "min": float(false_arr.min()) if len(false_arr) else None,
                "median": float(np.median(false_arr)) if len(false_arr) else None,
                "p75": float(np.percentile(false_arr, 75)) if len(false_arr) else None,
                "p90": float(np.percentile(false_arr, 90)) if len(false_arr) else None,
                "p95": float(np.percentile(false_arr, 95)) if len(false_arr) else None,
                "p99": float(np.percentile(false_arr, 99)) if len(false_arr) else None,
                "max": float(false_arr.max()) if len(false_arr) else None,
                "mean": float(false_arr.mean()) if len(false_arr) else None,
                "std": float(false_arr.std()) if len(false_arr) else None,
            },
            "separation": {
                "true_median_minus_false_median": float(np.median(true_arr) - np.median(false_arr)) if len(false_arr) else None,
                # AUROC-like: probability that a random TRUE scores higher than a random FALSE
                "auroc": float((true_arr.mean() > false_arr.mean()) if len(false_arr) else None),
            },
        }
        # Compute AUROC properly
        if len(false_arr) > 0:
            n_better = 0
            n_total = 0
            # For efficiency on large arrays, use a sample
            t_sample = true_arr if len(true_arr) <= 1000 else np.random.choice(true_arr, 1000, replace=False)
            f_sample = false_arr if len(false_arr) <= 1000 else np.random.choice(false_arr, 1000, replace=False)
            for ts in t_sample:
                n_better += (ts > f_sample).sum()
                n_total += len(f_sample)
            dist_stats[rel]["separation"]["auroc"] = float(n_better / n_total) if n_total else None

    with open(PROCESSED_DIR / "score_distributions.json", "w") as f:
        json.dump(dist_stats, f, indent=2)
    print(f"  -> score_distributions.json")

    # === Apply validation to novel predictions ===
    print("\n[6/6] Validating novel predictions against true vs false distributions ...")
    with open(PROCESSED_DIR / "novel_predictions_all.json") as f:
        novel = json.load(f)

    # The novel JSON may have summary or predictions wrapper keys; handle both
    novel_per_rel = novel.get("predictions", novel)

    validated = {}
    for rel_key, preds in novel_per_rel.items():
        if rel_key not in known_scores_per_rel or rel_key not in random_scores_per_rel:
            continue
        if not isinstance(preds, list):
            continue

        true_arr = known_scores_per_rel[rel_key]
        false_arr = random_scores_per_rel[rel_key]
        true_median = float(np.median(true_arr))
        false_median = float(np.median(false_arr))
        false_p95 = float(np.percentile(false_arr, 95))

        validated[rel_key] = []
        for pred in preds:
            score = pred.get("score", pred.get("confidence", 0))
            # How does the score compare?
            pct_true = float((true_arr < score).sum() / len(true_arr) * 100)
            pct_false = float((false_arr < score).sum() / len(false_arr) * 100)
            confidence_label = (
                "very strong" if pct_false >= 99 and pct_true >= 75 else
                "strong" if pct_false >= 95 and pct_true >= 50 else
                "moderate" if pct_false >= 80 and pct_true >= 25 else
                "weak"
            )
            validated[rel_key].append({
                **pred,
                "raw_score": score,
                "pct_above_true": round(pct_true, 1),
                "pct_above_false": round(pct_false, 1),
                "above_false_p95": score > false_p95,
                "above_true_median": score > true_median,
                "confidence": confidence_label,
            })

    with open(PROCESSED_DIR / "novel_predictions_validated.json", "w") as f:
        json.dump(validated, f, indent=2)
    print(f"  -> novel_predictions_validated.json")

    # === Generate validation figure ===
    print("\n  Generating validation figure ...")
    FIG_DIR.mkdir(parents=True, exist_ok=True)
    key_rels = [
        "Gene__associated_with__Disease",
        "Variant__variant_of__Disease",
        "Biomarker__marker_for__Disease",
        "Disease__subtype_of__Disease",
    ]
    available = [r for r in key_rels if r in known_scores_per_rel and r in random_scores_per_rel]

    n_plots = len(available)
    if n_plots > 0:
        ncols = 2
        nrows = (n_plots + 1) // 2
        fig, axes = plt.subplots(nrows, ncols, figsize=(13, 4 * nrows))
        if n_plots == 1:
            axes = np.array([[axes]])
        elif nrows == 1:
            axes = np.array([axes])
        axes_flat = axes.flatten()

        for ax, rel in zip(axes_flat, available):
            true_arr = known_scores_per_rel[rel]
            false_arr = random_scores_per_rel[rel]
            ax.hist(false_arr, bins=40, alpha=0.55, color="#c44e52", label=f"Random false (n={len(false_arr)})", density=True)
            ax.hist(true_arr, bins=40, alpha=0.55, color="#4c72b0", label=f"Known true (n={len(true_arr)})", density=True)

            # Mark top 5 novel predictions
            if rel in validated:
                top_scores = sorted([p["raw_score"] for p in validated[rel]], reverse=True)[:5]
                for s in top_scores:
                    ax.axvline(s, color="#2ca02c", linestyle="--", linewidth=1.5, alpha=0.85)
                if top_scores:
                    ax.axvline(top_scores[0], color="#2ca02c", linewidth=2, label="Top 5 novel predictions")

            ax.set_title(rel.replace("__", " > "), fontsize=10)
            ax.set_xlabel("DistMult Score")
            ax.set_ylabel("Density")
            ax.legend(fontsize=8)
            ax.grid(True, alpha=0.3)

        # Hide any unused subplots
        for j in range(len(available), len(axes_flat)):
            axes_flat[j].axis("off")

        plt.tight_layout()
        plt.savefig(FIG_DIR / "score_validation.png", dpi=200, bbox_inches="tight")
        plt.close()
        print(f"  -> figs/score_validation.png")

    # === Print summary ===
    print("\n" + "=" * 70)
    print("SUMMARY: True vs False score separation per relation")
    print("=" * 70)
    print(f"{'Relation':<60} {'TRUE med':>10} {'FALSE med':>10} {'AUROC':>8}")
    print("-" * 95)
    for rel, stats in dist_stats.items():
        if stats["false"]["median"] is None:
            continue
        true_med = stats["true"]["median"]
        false_med = stats["false"]["median"]
        auroc = stats["separation"]["auroc"]
        print(f"{rel[:58]:<60} {true_med:>10.3f} {false_med:>10.3f} {auroc:>8.3f}")

    print("\nTop novel predictions per key relation (with confidence):")
    for rel in available:
        if rel not in validated:
            continue
        print(f"\n  {rel}:")
        for i, p in enumerate(validated[rel][:5], 1):
            head = str(p.get("head_label", p.get("head", "?")))[:35]
            tail = str(p.get("tail_label", p.get("tail", "?")))[:35]
            print(f"    {i}. {head:<35} -> {tail:<35} "
                  f"score={p['raw_score']:.3f}  "
                  f"vs_false={p['pct_above_false']:>5.1f}%  "
                  f"vs_true={p['pct_above_true']:>5.1f}%  "
                  f"[{p['confidence']}]")

    print("\nDone.")


if __name__ == "__main__":
    main()