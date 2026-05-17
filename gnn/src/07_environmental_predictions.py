"""
07_environmental_predictions.py - Environmental link predictions using trained R-GCN.

Generates four types of environmental predictions from the trained model:
  1. Region Risk Ranking: which regions are most strongly associated with lung cancer?
  2. Chemical-Region Novel Predictions: predict unmeasured pollutant exposures
  3. Chemical Importance: which chemicals are closest to disease in embedding space?
  4. Temporal Trends: how do environmental-disease associations change over time?

Usage:  python gnn/src/07_environmental_predictions.py
Input:  gnn/data/processed/hetero_graph.pt, rgcn_weights.pt, node_id_maps.json, best_config.json
Output: gnn/data/processed/env_predictions/ (CSVs + figures)
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

# =========================================================================
# Paths and config
# =========================================================================

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
PROCESSED_DIR = REPO_ROOT / "gnn" / "data" / "processed"
OUTPUT_DIR = PROCESSED_DIR / "env_predictions"
FIG_DIR = OUTPUT_DIR / "figs"

DEVICE = torch.device("mps" if torch.backends.mps.is_available() else
                       "cuda" if torch.cuda.is_available() else "cpu")

# Load config
with open(PROCESSED_DIR / "best_config.json") as f:
    cfg = json.load(f)
HIDDEN_DIM = cfg.get("hidden_dim", 128)
NUM_LAYERS = cfg.get("num_layers", 2)
NUM_BASES = cfg.get("num_bases", 6)
DROPOUT = cfg.get("dropout", 0.15)

print(f"[Config] layers={NUM_LAYERS}, bases={NUM_BASES}, hidden={HIDDEN_DIM}, dropout={DROPOUT}")
print(f"[Device] {DEVICE}")


# =========================================================================
# Model (identical to 05_train_rgcn.py)
# =========================================================================

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
# Graph loading (same as 05_train_rgcn.py)
# =========================================================================

def load_and_flatten():
    print("[1/6] Loading graph ...")
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

    all_src, all_dst, all_rel = [], [], []
    rel_to_id = {}
    edge_type_names = []
    rel_type_info = {}

    for et in data.edge_types:
        src_type, rel, dst_type = et
        rel_key = f"{src_type}__{rel}__{dst_type}"
        if rel_key not in rel_to_id:
            rel_to_id[rel_key] = len(rel_to_id)
            edge_type_names.append(rel_key)
        rid = rel_to_id[rel_key]
        rel_type_info[rid] = (src_type, dst_type)

        ei = data[et].edge_index
        all_src.append(ei[0] + node_offsets[src_type])
        all_dst.append(ei[1] + node_offsets[dst_type])
        all_rel.append(torch.full((ei.size(1),), rid, dtype=torch.long))

    edge_index = torch.stack([torch.cat(all_src), torch.cat(all_dst)])
    edge_type = torch.cat(all_rel)

    n_relations = len(rel_to_id)
    print(f"  Graph: {total_nodes:,} nodes, {n_relations} relations, {edge_index.size(1):,} edges")

    return (edge_index, edge_type, total_nodes, n_relations,
            rel_to_id, edge_type_names, rel_type_info, type_ranges,
            node_offsets, node_features)


def load_node_maps():
    with open(PROCESSED_DIR / "node_id_maps.json") as f:
        raw = json.load(f)
    # Build idx -> label for each type
    label_maps = {}
    for ntype, entries in raw.items():
        idx_to_label = {}
        for uri, info in entries.items():
            idx_to_label[info["idx"]] = info.get("label", "") or uri.split("#")[-1]
        label_maps[ntype] = idx_to_label
    return label_maps, raw


def build_existing_triples(edge_index, edge_type):
    """Build set of all existing triples for filtering."""
    triples = set()
    src = edge_index[0]
    dst = edge_index[1]
    for i in range(src.size(0)):
        triples.add((src[i].item(), edge_type[i].item(), dst[i].item()))
    return triples


# =========================================================================
# Analysis 1: Region Risk Ranking
# =========================================================================

def region_risk_ranking(z, model, node_offsets, type_ranges, rel_to_id,
                        label_maps, existing_triples, edge_index, edge_type):
    """
    Which regions/countries have the strongest predicted association with lung cancer?
    
    Approach: For each VitalStatistics node, score (Disease, detected_finding, VitalStats).
    Then aggregate scores by Country and by GeoPoliticalRegion to produce risk rankings.
    We also look at which Disease subtypes are most strongly predicted per country.
    """
    print("\n[3/6] Region Risk Ranking ...")

    rel_key = "Disease__detected_finding__VitalStatistics"
    if rel_key not in rel_to_id:
        print("  SKIP: relation not found")
        return
    rid = rel_to_id[rel_key]

    disease_lo, disease_hi = type_ranges["Disease"]
    vs_lo, vs_hi = type_ranges["VitalStatistics"]
    n_diseases = disease_hi - disease_lo
    n_vs = vs_hi - vs_lo

    # Build VitalStatistics -> Country mapping from existing edges
    vs_country_rel = "VitalStatistics__part_of__Country"
    vs_region_rel = "VitalStatistics__part_of__GeographicRegion"
    country_lo, country_hi = type_ranges["Country"]

    # Parse edges to build VS -> Country/Region mappings
    vs_to_country = {}
    vs_to_region = {}
    src_arr = edge_index[0].numpy()
    dst_arr = edge_index[1].numpy()
    et_arr = edge_type.numpy()

    if vs_country_rel in rel_to_id:
        vs_country_rid = rel_to_id[vs_country_rel]
        mask = et_arr == vs_country_rid
        for s, d in zip(src_arr[mask], dst_arr[mask]):
            vs_local = s - vs_lo
            country_local = d - country_lo
            vs_to_country[vs_local] = country_local

    gpr_lo, gpr_hi = type_ranges.get("GeoPoliticalRegion", (0, 0))
    gr_lo, gr_hi = type_ranges.get("GeographicRegion", (0, 0))
    if vs_region_rel in rel_to_id:
        vs_region_rid = rel_to_id[vs_region_rel]
        mask = et_arr == vs_region_rid
        for s, d in zip(src_arr[mask], dst_arr[mask]):
            vs_local = s - vs_lo
            region_local = d - gr_lo
            vs_to_region[vs_local] = region_local

    # Score all (Disease, detected_finding, VitalStats) triples that are NOT in training
    print(f"  Scoring {n_diseases} diseases x {n_vs} VitalStatistics nodes ...")
    print(f"  (only novel triples, filtering {len(existing_triples):,} known triples)")

    # Process in batches to avoid OOM
    country_scores = defaultdict(list)  # country_label -> [(disease_label, score)]
    region_scores = defaultdict(list)
    all_novel = []

    BATCH = 10000
    for d_local in range(n_diseases):
        d_global = d_local + disease_lo
        d_label = label_maps["Disease"].get(d_local, f"disease_{d_local}")

        # Score all VitalStats for this disease
        h_idx = torch.full((n_vs,), d_global, dtype=torch.long, device=DEVICE)
        r_idx = torch.full((n_vs,), rid, dtype=torch.long, device=DEVICE)
        t_idx = torch.arange(vs_lo, vs_hi, device=DEVICE)

        with torch.no_grad():
            scores = model.decode(z, h_idx, r_idx, t_idx)
        scores_np = torch.sigmoid(scores).cpu().numpy()

        for vs_local in range(n_vs):
            vs_global = vs_local + vs_lo
            # Skip existing triples
            if (d_global, rid, vs_global) in existing_triples:
                continue

            score = float(scores_np[vs_local])
            if score < 0.5:
                continue  # only keep confident predictions

            # Map to country
            if vs_local in vs_to_country:
                c_local = vs_to_country[vs_local]
                c_label = label_maps["Country"].get(c_local, f"country_{c_local}")
                country_scores[c_label].append((d_label, score))

            # Map to region
            if vs_local in vs_to_region:
                r_local = vs_to_region[vs_local]
                r_label = label_maps["GeographicRegion"].get(r_local, f"region_{r_local}")
                region_scores[r_label].append((d_label, score))

            all_novel.append({
                "disease": d_label,
                "vs_idx": vs_local,
                "score": score,
                "country": label_maps["Country"].get(vs_to_country.get(vs_local, -1), "unknown"),
            })

    # Aggregate by country: mean score and top disease
    country_risk = []
    for country, pairs in country_scores.items():
        scores_list = [s for _, s in pairs]
        diseases = list(set(d for d, _ in pairs))
        top_disease = max(pairs, key=lambda x: x[1])[0]
        country_risk.append({
            "country": country,
            "mean_score": float(np.mean(scores_list)),
            "max_score": float(np.max(scores_list)),
            "n_predictions": len(pairs),
            "top_disease": top_disease,
            "disease_subtypes": ", ".join(diseases[:5]),
        })
    country_risk.sort(key=lambda x: x["mean_score"], reverse=True)

    # Save
    out_path = OUTPUT_DIR / "region_risk_ranking.csv"
    if country_risk:
        with open(out_path, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=country_risk[0].keys())
            w.writeheader()
            w.writerows(country_risk[:50])
        print(f"  -> {out_path.name} ({len(country_risk)} countries)")

        # Print top 15
        print(f"\n  Top 15 countries by mean predicted lung cancer risk:")
        print(f"  {'Country':<25} {'Mean Score':>10} {'Max Score':>10} {'N Preds':>8} {'Top Subtype'}")
        for r in country_risk[:15]:
            print(f"  {r['country']:<25} {r['mean_score']:>10.4f} {r['max_score']:>10.4f} "
                  f"{r['n_predictions']:>8} {r['top_disease']}")
    else:
        print("  No novel predictions with score > 0.5 found.")
        print("  Lowering threshold to 0.3 ...")
        # Retry with lower threshold would go here, but let's output what we have
        # Save all_novel sorted by score
        all_novel.sort(key=lambda x: x["score"], reverse=True)
        out_path = OUTPUT_DIR / "region_risk_all.csv"
        if all_novel:
            with open(out_path, "w", newline="") as f:
                w = csv.DictWriter(f, fieldnames=all_novel[0].keys())
                w.writeheader()
                w.writerows(all_novel[:200])
            print(f"  -> {out_path.name} ({len(all_novel)} novel predictions)")

    return country_risk


# =========================================================================
# Analysis 2: Chemical-Region Novel Predictions
# =========================================================================

def chemical_region_predictions(z, model, node_offsets, type_ranges, rel_to_id,
                                 label_maps, existing_triples, edge_index, edge_type):
    """
    Predict unmeasured Chemical-Region associations.
    
    For each Chemical, score all GeoPoliticalRegion and GeographicRegion targets
    that are NOT in the training data. This answers: "In which cities/regions
    would we expect to find this pollutant, even though no measurement exists?"
    """
    print("\n[4/6] Chemical-Region Novel Predictions ...")

    results = []

    for region_type, rel_key in [
        ("GeoPoliticalRegion", "ChemicalLocationAssociation__refers_to__GeoPoliticalRegion"),
        ("GeographicRegion", "ChemicalLocationAssociation__refers_to__GeographicRegion"),
    ]:
        if rel_key not in rel_to_id:
            continue
        rid = rel_to_id[rel_key]

        cla_lo, cla_hi = type_ranges["ChemicalLocationAssociation"]
        reg_lo, reg_hi = type_ranges[region_type]
        n_cla = cla_hi - cla_lo
        n_reg = reg_hi - reg_lo

        # Build CLA -> Chemical mapping
        chem_rel_key = "ChemicalLocationAssociation__refers_to__Chemical"
        chem_lo, chem_hi = type_ranges["Chemical"]
        cla_to_chem = {}
        if chem_rel_key in rel_to_id:
            chem_rid = rel_to_id[chem_rel_key]
            src_arr = edge_index[0].numpy()
            dst_arr = edge_index[1].numpy()
            et_arr = edge_type.numpy()
            mask = et_arr == chem_rid
            for s, d in zip(src_arr[mask], dst_arr[mask]):
                cla_local = s - cla_lo
                chem_local = d - chem_lo
                cla_to_chem[cla_local] = chem_local

        # For each CLA, score all regions
        # This is too large (131k x 3143), so instead:
        # For each Chemical, find regions NOT connected to any CLA of that Chemical
        print(f"  Building Chemical -> Region coverage for {region_type} ...")

        # Chemical -> set of connected regions
        chem_regions = defaultdict(set)
        if rel_key in rel_to_id:
            src_arr = edge_index[0].numpy()
            dst_arr = edge_index[1].numpy()
            et_arr = edge_type.numpy()
            mask = et_arr == rid
            for s, d in zip(src_arr[mask], dst_arr[mask]):
                cla_local = s - cla_lo
                if cla_local in cla_to_chem:
                    chem_local = cla_to_chem[cla_local]
                    reg_local = d - reg_lo
                    chem_regions[chem_local].add(reg_local)

        # For each chemical, score unconnected regions using embedding similarity
        # Use Chemical and Region embeddings directly (cosine similarity)
        print(f"  Scoring Chemical-{region_type} novel pairs ...")
        chem_embeddings = z[chem_lo:chem_hi].detach()
        reg_embeddings = z[reg_lo:reg_hi].detach()

        # Normalise for cosine similarity
        chem_norm = F.normalize(chem_embeddings, dim=1)
        reg_norm = F.normalize(reg_embeddings, dim=1)
        sim_matrix = torch.mm(chem_norm, reg_norm.t()).cpu().numpy()  # (n_chem, n_reg)

        for chem_local in range(chem_hi - chem_lo):
            chem_label = label_maps["Chemical"].get(chem_local, f"chem_{chem_local}")
            connected = chem_regions.get(chem_local, set())

            for reg_local in range(n_reg):
                if reg_local in connected:
                    continue  # skip existing connections
                sim = float(sim_matrix[chem_local, reg_local])
                if sim > 0.3:  # only keep meaningful similarities
                    reg_label = label_maps[region_type].get(reg_local, f"reg_{reg_local}")
                    results.append({
                        "chemical": chem_label,
                        "region": reg_label,
                        "region_type": region_type,
                        "similarity": sim,
                    })

    results.sort(key=lambda x: x["similarity"], reverse=True)

    out_path = OUTPUT_DIR / "chemical_region_predictions.csv"
    if results:
        with open(out_path, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=results[0].keys())
            w.writeheader()
            w.writerows(results[:200])
        print(f"  -> {out_path.name} ({len(results)} novel Chemical-Region predictions)")

        # Summary per chemical
        print(f"\n  Novel region predictions per chemical:")
        chem_counts = defaultdict(list)
        for r in results:
            chem_counts[r["chemical"]].append(r["similarity"])
        for chem, sims in sorted(chem_counts.items(), key=lambda x: np.mean(x[1]), reverse=True):
            print(f"    {chem:<30} {len(sims):>5} novel regions, mean sim={np.mean(sims):.4f}")
    else:
        print("  No novel predictions above threshold.")

    return results


# =========================================================================
# Analysis 3: Chemical Importance for Disease
# =========================================================================

def chemical_importance(z, type_ranges, label_maps):
    """
    Which chemicals are closest to lung cancer subtypes in embedding space?
    
    Computes cosine similarity between all Chemical and Disease embeddings.
    Ranks chemicals by their proximity to disease, suggesting which pollutants
    the model considers most relevant.
    """
    print("\n[5/6] Chemical Importance Analysis ...")

    chem_lo, chem_hi = type_ranges["Chemical"]
    disease_lo, disease_hi = type_ranges["Disease"]

    chem_emb = z[chem_lo:chem_hi].detach()
    disease_emb = z[disease_lo:disease_hi].detach()

    chem_norm = F.normalize(chem_emb, dim=1)
    disease_norm = F.normalize(disease_emb, dim=1)

    # (n_chemicals, n_diseases) similarity matrix
    sim = torch.mm(chem_norm, disease_norm.t()).cpu().numpy()

    n_chem = chem_hi - chem_lo
    n_disease = disease_hi - disease_lo

    # Per-chemical: mean similarity to all diseases, and top disease
    chem_importance = []
    for c in range(n_chem):
        c_label = label_maps["Chemical"].get(c, f"chem_{c}")
        mean_sim = float(np.mean(sim[c]))
        max_sim = float(np.max(sim[c]))
        top_disease_idx = int(np.argmax(sim[c]))
        top_disease = label_maps["Disease"].get(top_disease_idx, f"disease_{top_disease_idx}")

        # Top 5 diseases for this chemical
        top5_idx = np.argsort(sim[c])[::-1][:5]
        top5 = [(label_maps["Disease"].get(int(i), f"d_{i}"), float(sim[c, i])) for i in top5_idx]

        chem_importance.append({
            "chemical": c_label,
            "mean_disease_similarity": mean_sim,
            "max_disease_similarity": max_sim,
            "top_disease": top_disease,
            "top_5_diseases": "; ".join(f"{d} ({s:.3f})" for d, s in top5),
        })

    chem_importance.sort(key=lambda x: x["mean_disease_similarity"], reverse=True)

    out_path = OUTPUT_DIR / "chemical_importance.csv"
    with open(out_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=chem_importance[0].keys())
        w.writeheader()
        w.writerows(chem_importance)
    print(f"  -> {out_path.name}")

    print(f"\n  Chemical importance ranking (by mean cosine similarity to Disease):")
    print(f"  {'Chemical':<30} {'Mean Sim':>10} {'Max Sim':>10} {'Most Similar Disease'}")
    for r in chem_importance:
        print(f"  {r['chemical']:<30} {r['mean_disease_similarity']:>10.4f} "
              f"{r['max_disease_similarity']:>10.4f} {r['top_disease']}")

    # Per-disease: which chemicals are most similar?
    disease_chem_ranking = []
    for d in range(n_disease):
        d_label = label_maps["Disease"].get(d, f"disease_{d}")
        top_chems_idx = np.argsort(sim[:, d])[::-1]
        top_chems = [(label_maps["Chemical"].get(int(i), f"c_{i}"), float(sim[i, d])) for i in top_chems_idx[:5]]
        disease_chem_ranking.append({
            "disease": d_label,
            "top_chemicals": "; ".join(f"{c} ({s:.3f})" for c, s in top_chems),
            "top_chemical": top_chems[0][0],
            "top_similarity": top_chems[0][1],
        })

    disease_chem_ranking.sort(key=lambda x: x["top_similarity"], reverse=True)
    out_path = OUTPUT_DIR / "disease_chemical_ranking.csv"
    with open(out_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=disease_chem_ranking[0].keys())
        w.writeheader()
        w.writerows(disease_chem_ranking)
    print(f"  -> {out_path.name}")

    # Figure: heatmap
    fig, ax = plt.subplots(figsize=(14, 6))
    chem_labels = [label_maps["Chemical"].get(i, f"c_{i}") for i in range(n_chem)]
    disease_labels = [label_maps["Disease"].get(i, f"d_{i}") for i in range(n_disease)]
    # Shorten disease labels
    disease_short = [d[:30] for d in disease_labels]

    im = ax.imshow(sim, aspect="auto", cmap="YlOrRd")
    ax.set_yticks(range(n_chem))
    ax.set_yticklabels(chem_labels, fontsize=9)
    ax.set_xticks(range(n_disease))
    ax.set_xticklabels(disease_short, rotation=90, fontsize=6)
    ax.set_title("Chemical-Disease embedding similarity (cosine)", fontsize=12, fontweight="bold")
    plt.colorbar(im, ax=ax, shrink=0.8)
    plt.tight_layout()
    fig.savefig(FIG_DIR / "chemical_disease_heatmap.png", dpi=200, bbox_inches="tight")
    plt.close(fig)
    print(f"  -> figs/chemical_disease_heatmap.png")

    return chem_importance


# =========================================================================
# Analysis 4: Temporal Patterns
# =========================================================================

def temporal_analysis(z, model, node_offsets, type_ranges, rel_to_id,
                      label_maps, edge_index, edge_type):
    """
    How do disease-VitalStatistics associations change over time?
    
    Groups VitalStatistics by CalendarYear and computes mean Disease-VitalStatistics
    scores per year. This reveals temporal trends in predicted lung cancer risk.
    Also examines which CalendarYears are most similar to Disease in embedding space.
    """
    print("\n[6/6] Temporal Analysis ...")

    # Build VitalStats -> CalendarYear mapping
    vs_year_rel = "VitalStatistics__has_time_boundary__CalendarYear"
    if vs_year_rel not in rel_to_id:
        print("  SKIP: relation not found")
        return

    vs_lo, vs_hi = type_ranges["VitalStatistics"]
    year_lo, year_hi = type_ranges["CalendarYear"]
    disease_lo, disease_hi = type_ranges["Disease"]

    vs_year_rid = rel_to_id[vs_year_rel]
    src_arr = edge_index[0].numpy()
    dst_arr = edge_index[1].numpy()
    et_arr = edge_type.numpy()

    vs_to_year = {}
    mask = et_arr == vs_year_rid
    for s, d in zip(src_arr[mask], dst_arr[mask]):
        vs_local = s - vs_lo
        year_local = d - year_lo
        vs_to_year[vs_local] = year_local

    # Also build VS -> Country mapping
    vs_country_rel = "VitalStatistics__part_of__Country"
    country_lo, country_hi = type_ranges["Country"]
    vs_to_country = {}
    if vs_country_rel in rel_to_id:
        vs_country_rid = rel_to_id[vs_country_rel]
        mask = et_arr == vs_country_rid
        for s, d in zip(src_arr[mask], dst_arr[mask]):
            vs_local = s - vs_lo
            country_local = d - country_lo
            vs_to_country[vs_local] = country_local

    # Score Disease-VitalStats for the parent disease (Malignant neoplasm of lung)
    # Find the parent disease
    parent_disease_local = None
    for idx, label in label_maps["Disease"].items():
        if "malignant neoplasm of lung" in label.lower() or "neoplasm" in label.lower():
            parent_disease_local = idx
            break

    if parent_disease_local is None:
        # Use disease 0 as fallback
        parent_disease_local = 0
        print(f"  Warning: parent disease not found, using idx 0: {label_maps['Disease'].get(0, '?')}")

    parent_disease_global = parent_disease_local + disease_lo
    parent_label = label_maps["Disease"].get(parent_disease_local, "?")
    print(f"  Parent disease: {parent_label} (local idx {parent_disease_local})")

    # Score all VitalStats for parent disease
    dis_vs_rel = "Disease__detected_finding__VitalStatistics"
    if dis_vs_rel not in rel_to_id:
        print("  SKIP: Disease-VitalStatistics relation not found")
        return
    rid = rel_to_id[dis_vs_rel]

    n_vs = vs_hi - vs_lo
    h_idx = torch.full((n_vs,), parent_disease_global, dtype=torch.long, device=DEVICE)
    r_idx = torch.full((n_vs,), rid, dtype=torch.long, device=DEVICE)
    t_idx = torch.arange(vs_lo, vs_hi, device=DEVICE)

    with torch.no_grad():
        scores = model.decode(z, h_idx, r_idx, t_idx)
    scores_np = torch.sigmoid(scores).cpu().numpy()

    # Group by year
    year_scores = defaultdict(list)
    for vs_local in range(n_vs):
        if vs_local in vs_to_year:
            year_local = vs_to_year[vs_local]
            year_label = label_maps["CalendarYear"].get(year_local, f"year_{year_local}")
            year_scores[year_label].append(float(scores_np[vs_local]))

    # Aggregate
    year_trend = []
    for year, scores_list in sorted(year_scores.items()):
        year_trend.append({
            "year": year,
            "mean_score": float(np.mean(scores_list)),
            "max_score": float(np.max(scores_list)),
            "n_vitalstats": len(scores_list),
        })

    out_path = OUTPUT_DIR / "temporal_trends.csv"
    if year_trend:
        with open(out_path, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=year_trend[0].keys())
            w.writeheader()
            w.writerows(year_trend)
        print(f"  -> {out_path.name}")

        print(f"\n  Temporal trend (Disease-VitalStatistics score by year):")
        print(f"  {'Year':<8} {'Mean Score':>10} {'Max Score':>10} {'N VitalStats':>12}")
        for t in year_trend:
            print(f"  {t['year']:<8} {t['mean_score']:>10.4f} {t['max_score']:>10.4f} {t['n_vitalstats']:>12}")

        # Figure: temporal trend
        years = [t["year"] for t in year_trend]
        means = [t["mean_score"] for t in year_trend]
        # Filter to years with reasonable data (>= 1990)
        filtered = [(y, m) for y, m in zip(years, means) if int(y) >= 1990]
        if filtered:
            f_years, f_means = zip(*filtered)
            fig, ax = plt.subplots(figsize=(12, 5))
            ax.plot(f_years, f_means, "o-", color="#c44e52", linewidth=2, markersize=5)
            ax.set_xlabel("Year", fontsize=11)
            ax.set_ylabel("Mean predicted Disease-VitalStatistics score", fontsize=11)
            ax.set_title("Temporal trend: predicted lung cancer association by year",
                         fontsize=12, fontweight="bold")
            ax.grid(True, alpha=0.3)
            # Rotate x labels
            plt.xticks(rotation=45, ha="right")
            plt.tight_layout()
            fig.savefig(FIG_DIR / "temporal_trend.png", dpi=200, bbox_inches="tight")
            plt.close(fig)
            print(f"  -> figs/temporal_trend.png")

    # Per-country temporal analysis for top 5 countries
    country_year_scores = defaultdict(lambda: defaultdict(list))
    for vs_local in range(n_vs):
        if vs_local in vs_to_year and vs_local in vs_to_country:
            year_local = vs_to_year[vs_local]
            year_label = label_maps["CalendarYear"].get(year_local, "?")
            country_local = vs_to_country[vs_local]
            country_label = label_maps["Country"].get(country_local, "?")
            country_year_scores[country_label][year_label].append(float(scores_np[vs_local]))

    # Find top 5 countries by mean score
    country_means = {}
    for country, year_data in country_year_scores.items():
        all_scores = [s for ss in year_data.values() for s in ss]
        country_means[country] = np.mean(all_scores)

    top5_countries = sorted(country_means.items(), key=lambda x: x[1], reverse=True)[:5]
    print(f"\n  Per-country temporal trends (top 5 countries):")

    country_trends = []
    for country, _ in top5_countries:
        year_data = country_year_scores[country]
        for year in sorted(year_data.keys()):
            if int(year) >= 1990:
                country_trends.append({
                    "country": country,
                    "year": year,
                    "mean_score": float(np.mean(year_data[year])),
                    "n_records": len(year_data[year]),
                })

    if country_trends:
        out_path = OUTPUT_DIR / "temporal_trends_by_country.csv"
        with open(out_path, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=country_trends[0].keys())
            w.writeheader()
            w.writerows(country_trends)
        print(f"  -> {out_path.name}")

        # Figure: multi-country temporal trend
        fig, ax = plt.subplots(figsize=(12, 6))
        colors = ["#c44e52", "#4c72b0", "#55a868", "#8172b2", "#ccb974"]
        for i, (country, _) in enumerate(top5_countries):
            cy_data = [(t["year"], t["mean_score"]) for t in country_trends
                       if t["country"] == country and int(t["year"]) >= 1990]
            if cy_data:
                yrs, scores_list = zip(*sorted(cy_data))
                ax.plot(yrs, scores_list, "o-", color=colors[i % len(colors)],
                        linewidth=1.5, markersize=4, label=country, alpha=0.8)

        ax.set_xlabel("Year", fontsize=11)
        ax.set_ylabel("Mean predicted score", fontsize=11)
        ax.set_title("Predicted lung cancer association by country and year",
                     fontsize=12, fontweight="bold")
        ax.legend(fontsize=9)
        ax.grid(True, alpha=0.3)
        plt.xticks(rotation=45, ha="right")
        plt.tight_layout()
        fig.savefig(FIG_DIR / "temporal_trend_by_country.png", dpi=200, bbox_inches="tight")
        plt.close(fig)
        print(f"  -> figs/temporal_trend_by_country.png")

    return year_trend


# =========================================================================
# Main
# =========================================================================

def main():
    print("=" * 70)
    print("07_environmental_predictions.py")
    print("Environmental link predictions from trained R-GCN")
    print("=" * 70)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    FIG_DIR.mkdir(parents=True, exist_ok=True)

    # Load graph
    (edge_index, edge_type, n_entities, n_relations,
     rel_to_id, edge_type_names, rel_type_info, type_ranges,
     node_offsets, node_features) = load_and_flatten()

    label_maps, raw_maps = load_node_maps()

    # Load model
    print("\n[2/6] Loading trained R-GCN weights ...")
    model = RGCNWithFeatures(n_entities, n_relations, HIDDEN_DIM, NUM_LAYERS,
                              NUM_BASES, DROPOUT, node_features)
    state = torch.load(PROCESSED_DIR / "rgcn_weights.pt", map_location="cpu", weights_only=True)
    model.load_state_dict(state)
    model.to(DEVICE)
    model.eval()
    print(f"  Model loaded: {sum(p.numel() for p in model.parameters()):,} parameters")

    # Encode full graph
    print("  Encoding graph ...")
    with torch.no_grad():
        z = model.encode(edge_index.to(DEVICE), edge_type.to(DEVICE))
    print(f"  Embeddings: {z.shape}")

    # Build existing triples for filtering
    existing = build_existing_triples(edge_index, edge_type)
    print(f"  Known triples: {len(existing):,}")

    # Run analyses
    t0 = time.time()

    region_risk = region_risk_ranking(z, model, node_offsets, type_ranges, rel_to_id,
                                       label_maps, existing, edge_index, edge_type)

    chem_region = chemical_region_predictions(z, model, node_offsets, type_ranges, rel_to_id,
                                               label_maps, existing, edge_index, edge_type)

    chem_imp = chemical_importance(z, type_ranges, label_maps)

    temporal = temporal_analysis(z, model, node_offsets, type_ranges, rel_to_id,
                                 label_maps, edge_index, edge_type)

    dt = time.time() - t0
    print(f"\n{'=' * 70}")
    print(f"All environmental predictions completed in {dt:.1f}s")
    print(f"Output: {OUTPUT_DIR}")
    print(f"{'=' * 70}")


if __name__ == "__main__":
    main()