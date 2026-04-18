"""
02_build_graph.py - Build PyG HeteroData from parsed KG node/edge lists.

Collapses n-ary reification patterns (CLA, VitalStatistics) into direct
typed edges and constructs a PyTorch Geometric HeteroData object.

Usage:  python gnn/src/02_build_graph.py
Input:  gnn/data/interim/{nodes.csv, edges.csv}
Output: gnn/data/processed/hetero_graph.pt
        gnn/data/processed/node_id_maps.json
        gnn/data/interim/figs/collapsed_schema.png
"""

import json
import csv
import time
from pathlib import Path
from collections import defaultdict, Counter

import torch
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
INTERIM_DIR = REPO_ROOT / "gnn" / "data" / "interim"
PROCESSED_DIR = REPO_ROOT / "gnn" / "data" / "processed"
FIG_DIR = INTERIM_DIR / "figs"

REIFICATION_TYPES = {"ChemicalLocationAssociation", "VitalStatistics", "Population"}

BIO_NODE_TYPES = {"Disease", "Gene", "Variant", "Pathway", "GeneProduct", "Biomarker", "ChromoRearr", "GeneFusion"}


def load_interim():
    nodes = {}
    with open(INTERIM_DIR / "nodes.csv", "r", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            nodes[row["node_id"]] = {"type": row["node_type"], "label": row["label"]}

    edges = []
    with open(INTERIM_DIR / "edges.csv", "r", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            edges.append({
                "src": row["src_id"], "src_type": row["src_type"],
                "rel": row["relation"],
                "dst": row["dst_id"], "dst_type": row["dst_type"],
                "attrs": json.loads(row["attrs_json"]) if row["attrs_json"] != "{}" else {},
            })

    print(f"Loaded {len(nodes):,} nodes, {len(edges):,} edges")
    return nodes, edges


def collapse_cla(nodes, edges):
    """
    CLA is a reification of (Chemical, Region, Year, value).
    Each CLA has edges to Chemical, Region, CalendarYear.
    Collapse into: (Chemical, exposure_in, Region) with year as edge attr.
    """
    cla_chemical = {}
    cla_region = {}
    cla_year = {}

    for e in edges:
        if e["src_type"] != "ChemicalLocationAssociation":
            continue
        cla_id = e["src"]
        if e["dst_type"] == "Chemical":
            cla_chemical[cla_id] = e["dst"]
        elif e["dst_type"] in ("GeoPoliticalRegion", "GeographicRegion"):
            cla_region[cla_id] = (e["dst"], e["dst_type"])
        elif e["dst_type"] == "CalendarYear":
            cla_year[cla_id] = e["dst"]

    collapsed = []
    cla_ids = set(cla_chemical.keys()) & set(cla_region.keys())
    for cla_id in cla_ids:
        chem = cla_chemical[cla_id]
        region_id, region_type = cla_region[cla_id]
        year = cla_year.get(cla_id, "")
        collapsed.append({
            "src": chem, "src_type": "Chemical",
            "rel": "exposure_in",
            "dst": region_id, "dst_type": region_type,
            "attrs": {"year": year} if year else {},
        })

    print(f"  CLA: {len(cla_ids):,} reification nodes -> {len(collapsed):,} (Chemical, exposure_in, Region) edges")
    return collapsed


def collapse_vitalstats(nodes, edges):
    """
    VitalStatistics is a reification of (Disease -> Region, Year, People, rates).
    Collapse into: (Disease, cancer_stats_in, Region) with year + demographics.
    """
    vs_region = {}
    vs_year = {}
    vs_people = {}

    disease_to_vs = {}
    for e in edges:
        if e["dst_type"] == "VitalStatistics" and e["src_type"] == "Disease":
            disease_to_vs[e["dst"]] = e["src"]

    for e in edges:
        if e["src_type"] != "VitalStatistics":
            continue
        vs_id = e["src"]
        if e["dst_type"] in ("GeographicRegion", "Country"):
            vs_region[vs_id] = (e["dst"], e["dst_type"])
        elif e["dst_type"] == "CalendarYear":
            vs_year[vs_id] = e["dst"]
        elif e["dst_type"] == "People":
            vs_people[vs_id] = e["dst"]

    collapsed = []
    vs_ids = set(vs_region.keys())
    for vs_id in vs_ids:
        disease = disease_to_vs.get(vs_id)
        if not disease:
            continue
        region_id, region_type = vs_region[vs_id]
        year = vs_year.get(vs_id, "")
        people = vs_people.get(vs_id, "")
        collapsed.append({
            "src": disease, "src_type": "Disease",
            "rel": "cancer_stats_in",
            "dst": region_id, "dst_type": region_type,
            "attrs": {k: v for k, v in [("year", year), ("people", people)] if v},
        })

    print(f"  VitalStats: {len(vs_ids):,} reification nodes -> {len(collapsed):,} (Disease, cancer_stats_in, Region) edges")
    return collapsed


def build_collapsed_graph(nodes, edges):
    print("\nCollapsing n-ary relations ...")

    cla_edges = collapse_cla(nodes, edges)
    vs_edges = collapse_vitalstats(nodes, edges)

    direct_edges = [
        e for e in edges
        if e["src_type"] not in REIFICATION_TYPES
        and e["dst_type"] not in REIFICATION_TYPES
    ]

    all_edges = direct_edges + cla_edges + vs_edges

    kept_node_ids = set()
    for e in all_edges:
        kept_node_ids.add(e["src"])
        kept_node_ids.add(e["dst"])

    kept_nodes = {nid: info for nid, info in nodes.items() if nid in kept_node_ids and info["type"] not in REIFICATION_TYPES}

    print(f"\nCollapsed graph: {len(kept_nodes):,} nodes, {len(all_edges):,} edges")
    return kept_nodes, all_edges


def build_pyg_heterodata(nodes, edges):
    print("\nBuilding PyG HeteroData ...")

    node_type_to_ids = defaultdict(dict)
    for nid, info in nodes.items():
        nt = info["type"]
        if nid not in node_type_to_ids[nt]:
            node_type_to_ids[nt][nid] = len(node_type_to_ids[nt])

    from torch_geometric.data import HeteroData
    data = HeteroData()

    for nt, id_map in node_type_to_ids.items():
        data[nt].num_nodes = len(id_map)
        data[nt].node_ids = list(id_map.keys())

    edge_index_dict = defaultdict(lambda: ([], []))
    edge_attr_dict = defaultdict(list)

    skipped = 0
    for e in edges:
        st, rel, dt = e["src_type"], e["rel"], e["dst_type"]
        src_map = node_type_to_ids.get(st, {})
        dst_map = node_type_to_ids.get(dt, {})
        src_idx = src_map.get(e["src"])
        dst_idx = dst_map.get(e["dst"])
        if src_idx is None or dst_idx is None:
            skipped += 1
            continue
        key = (st, rel, dt)
        edge_index_dict[key][0].append(src_idx)
        edge_index_dict[key][1].append(dst_idx)
        edge_attr_dict[key].append(e["attrs"])

    for key, (src_list, dst_list) in edge_index_dict.items():
        data[key].edge_index = torch.tensor([src_list, dst_list], dtype=torch.long)

    if skipped > 0:
        print(f"  Skipped {skipped:,} edges (unmapped nodes)")

    print(f"\n  HeteroData summary:")
    print(f"    Node types: {len(node_type_to_ids)}")
    total_nodes = sum(len(m) for m in node_type_to_ids.values())
    total_edges = sum(data[key].edge_index.size(1) for key in edge_index_dict)
    print(f"    Total nodes: {total_nodes:,}")
    print(f"    Total edges: {total_edges:,}")
    print(f"    Edge types: {len(edge_index_dict)}")
    for key in sorted(edge_index_dict.keys(), key=lambda k: -edge_index_dict[k][0].__len__()):
        n = data[key].edge_index.size(1)
        print(f"      {key}: {n:,}")

    return data, node_type_to_ids


def save_outputs(data, node_type_to_ids, nodes):
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    torch.save(data, PROCESSED_DIR / "hetero_graph.pt")
    print(f"\n  Saved -> {PROCESSED_DIR / 'hetero_graph.pt'}")

    id_maps = {}
    for nt, id_map in node_type_to_ids.items():
        id_maps[nt] = {nid: {"idx": idx, "label": nodes.get(nid, {}).get("label", "")} for nid, idx in id_map.items()}

    with open(PROCESSED_DIR / "node_id_maps.json", "w") as f:
        json.dump(id_maps, f, indent=1)
    print(f"  Saved -> {PROCESSED_DIR / 'node_id_maps.json'}")


def fig_collapsed_schema(nodes, edges, fig_dir):
    fig_dir.mkdir(parents=True, exist_ok=True)

    node_counts = Counter(info["type"] for info in nodes.values())
    edge_counts = Counter((e["src_type"], e["rel"], e["dst_type"]) for e in edges)

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, max(6, len(node_counts) * 0.4)))

    types = [k for k, _ in node_counts.most_common()]
    counts = [node_counts[t] for t in types]
    colors = ["#1f77b4" if t in BIO_NODE_TYPES else "#2ca02c" for t in types]
    bars = ax1.barh(types[::-1], counts[::-1], color=colors[::-1], edgecolor="white", linewidth=0.5)
    ax1.set_xlabel("Number of Nodes")
    ax1.set_title("Node Types (after collapsing)")
    ax1.xaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"{int(x):,}"))
    for bar, cnt in zip(bars, counts[::-1]):
        ax1.text(bar.get_width() + max(counts) * 0.01, bar.get_y() + bar.get_height() / 2, f"{cnt:,}", va="center", fontsize=7)
    ax1.legend(handles=[
        mpatches.Patch(color="#1f77b4", label="Biological"),
        mpatches.Patch(color="#2ca02c", label="Environmental"),
    ], loc="lower right", fontsize=8)

    labels = [f"({s}, {r}, {d})" for (s, r, d), _ in edge_counts.most_common()]
    ecounts = [c for _, c in edge_counts.most_common()]
    ax2.barh(labels[::-1], ecounts[::-1], color="#4c72b0", edgecolor="white", linewidth=0.5)
    ax2.set_xlabel("Number of Edges")
    ax2.set_title("Edge Types (after collapsing)")
    ax2.xaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"{int(x):,}"))
    for i, cnt in enumerate(ecounts[::-1]):
        ax2.text(cnt + max(ecounts) * 0.01, i, f"{cnt:,}", va="center", fontsize=6)

    fig.suptitle("Lung-CABO KG: Collapsed Heterogeneous Graph Schema", fontsize=13, fontweight="bold")
    plt.tight_layout()
    fig.savefig(fig_dir / "collapsed_schema.png", dpi=200, bbox_inches="tight")
    plt.close(fig)
    print(f"  -> figs/collapsed_schema.png")


def main():
    print("=" * 70)
    print("02_build_graph.py")
    print("=" * 70)

    t0 = time.time()

    print("\n[1/4] Loading interim data ...")
    nodes, edges = load_interim()

    print("\n[2/4] Collapsing reification patterns ...")
    collapsed_nodes, collapsed_edges = build_collapsed_graph(nodes, edges)

    print("\n[3/4] Building PyG HeteroData ...")
    data, id_maps = build_pyg_heterodata(collapsed_nodes, collapsed_edges)
    save_outputs(data, id_maps, collapsed_nodes)

    print("\n[4/4] Generating thesis figures ...")
    fig_collapsed_schema(collapsed_nodes, collapsed_edges, FIG_DIR)

    dt = time.time() - t0
    print(f"\n{'=' * 70}")
    print(f"Done in {dt:.1f}s. Graph saved to {PROCESSED_DIR / 'hetero_graph.pt'}")
    print(f"{'=' * 70}")


if __name__ == "__main__":
    main()
