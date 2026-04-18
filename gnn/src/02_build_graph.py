"""
02_build_graph.py - Build PyG HeteroData with node features from parsed KG.

Instead of discarding exposure values and mortality rates during collapsing,
this version:
  - Aggregates exposure values per region as node features
  - Aggregates mortality/incidence per region as node features  
  - Keeps CalendarYear nodes with temporal edges
  - Preserves exposure values as edge weights on Chemical->Region edges

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

def normalize_features(feat_matrix):
    mean = feat_matrix.mean(dim=0, keepdim=True)
    std = feat_matrix.std(dim=0, keepdim=True).clamp(min=1e-6)
    return (feat_matrix - mean) / std

import numpy as np
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

CHEMICAL_FEATURE_ORDER = [
    "C5890534", "C0028160", "C0030106", "C1720884_10", "C0005036",
    "C0005052", "C1720884_10_As", "C1720884_10_Cd", "C1720884_10_Ni", "C1720884_10_Pb",
]


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


def extract_cla_data(edges):
    cla_chemical = {}
    cla_region = {}
    cla_year = {}
    cla_value = {}

    literal_edges = defaultdict(dict)

    with open(INTERIM_DIR / "edges.csv", "r", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            pass

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

    with open(INTERIM_DIR / "edges.csv", "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row["src_type"] == "ChemicalLocationAssociation":
                attrs = json.loads(row["attrs_json"]) if row["attrs_json"] != "{}" else {}
                if attrs:
                    cla_id = row["src_id"]
                    if "value" in attrs:
                        cla_value[cla_id] = attrs["value"]

    return cla_chemical, cla_region, cla_year, cla_value


def extract_vitalstats_data(nodes, edges):
    vs_region = {}
    vs_year = {}
    vs_people = {}
    disease_to_vs = {}

    for e in edges:
        if e["dst_type"] == "VitalStatistics" and e["src_type"] == "Disease":
            disease_to_vs[e["dst"]] = e["src"]
        if e["src_type"] != "VitalStatistics":
            continue
        vs_id = e["src"]
        if e["dst_type"] in ("GeographicRegion", "Country"):
            vs_region[vs_id] = (e["dst"], e["dst_type"])
        elif e["dst_type"] == "CalendarYear":
            vs_year[vs_id] = e["dst"]
        elif e["dst_type"] == "People":
            vs_people[vs_id] = e["dst"]

    return vs_region, vs_year, vs_people, disease_to_vs


def read_literal_values_from_ttls():
    import rdflib

    env_dir = REPO_ROOT / "env_data" / "data" / "processed"
    cla_values = {}
    vs_incidence = {}
    vs_mortality = {}

    for ttl_name in ["graph_EEA.ttl", "graph_OECD.ttl"]:
        ttl_path = env_dir / ttl_name
        if not ttl_path.exists():
            continue
        print(f"  Reading values from {ttl_name} ...")
        g = rdflib.Graph()
        g.parse(str(ttl_path), format="turtle")
        for s, p, o in g:
            s_str, p_str = str(s), str(p)
            if "#cla/" in s_str and "source" not in s_str:
                if "value" in p_str and isinstance(o, rdflib.Literal):
                    v = float(o.toPython())
                    if v > -9000:
                        cla_values[s_str] = v

    for ttl_name in ["graph_ECIS.ttl", "graph_CDC.ttl"]:
        ttl_path = env_dir / ttl_name
        if not ttl_path.exists():
            continue
        print(f"  Reading values from {ttl_name} ...")
        g = rdflib.Graph()
        g.parse(str(ttl_path), format="turtle")
        for s, p, o in g:
            s_str, p_str = str(s), str(p)
            if "#vitalstatistics/" in s_str and isinstance(o, rdflib.Literal):
                if "incidence" in p_str:
                    vs_incidence[s_str] = float(o.toPython())
                elif "mortalityrate" in p_str:
                    vs_mortality[s_str] = float(o.toPython())

    print(f"  CLA values: {len(cla_values):,}")
    print(f"  VS incidence: {len(vs_incidence):,}, mortality: {len(vs_mortality):,}")
    return cla_values, vs_incidence, vs_mortality


def build_region_features(cla_chemical, cla_region, cla_values, vs_region, vs_incidence, vs_mortality):
    region_chem_vals = defaultdict(lambda: defaultdict(list))
    for cla_id, (region_uri, _) in cla_region.items():
        chem = cla_chemical.get(cla_id)
        val = cla_values.get(cla_id)
        if chem and val is not None:
            chem_id = chem.split("/")[-1]
            region_chem_vals[region_uri][chem_id].append(val)

    region_health = defaultdict(lambda: {"incidence": [], "mortality": []})
    for vs_id, (region_uri, _) in vs_region.items():
        if vs_id in vs_incidence:
            region_health[region_uri]["incidence"].append(vs_incidence[vs_id])
        if vs_id in vs_mortality:
            region_health[region_uri]["mortality"].append(vs_mortality[vs_id])

    region_features = {}
    for region_uri in set(list(region_chem_vals.keys()) + list(region_health.keys())):
        feat = []
        for chem_id in CHEMICAL_FEATURE_ORDER:
            vals = region_chem_vals.get(region_uri, {}).get(chem_id, [])
            feat.append(np.mean(vals) if vals else 0.0)
        health = region_health.get(region_uri, {"incidence": [], "mortality": []})
        feat.append(np.mean(health["incidence"]) if health["incidence"] else 0.0)
        feat.append(np.mean(health["mortality"]) if health["mortality"] else 0.0)
        region_features[region_uri] = feat

    return region_features


def build_chemical_features(cla_chemical, cla_region, cla_values):
    chem_vals = defaultdict(list)
    chem_regions = defaultdict(set)
    for cla_id, chem_uri in cla_chemical.items():
        val = cla_values.get(cla_id)
        if val is not None:
            chem_vals[chem_uri].append(val)
            region = cla_region.get(cla_id)
            if region:
                chem_regions[chem_uri].add(region[0])

    chem_features = {}
    for chem_uri in chem_vals:
        vals = chem_vals[chem_uri]
        chem_features[chem_uri] = [np.mean(vals), len(chem_regions[chem_uri]), np.max(vals)]
    return chem_features


def build_collapsed_edges(nodes, edges, cla_chemical, cla_region, cla_year, cla_values,
                          vs_region, vs_year, disease_to_vs):
    direct_edges = [
        e for e in edges
        if e["src_type"] not in REIFICATION_TYPES and e["dst_type"] not in REIFICATION_TYPES
    ]

    # Chemical -> Region edges with mean exposure value
    chem_region_vals = defaultdict(list)
    for cla_id in set(cla_chemical.keys()) & set(cla_region.keys()):
        chem = cla_chemical[cla_id]
        region_uri, region_type = cla_region[cla_id]
        val = cla_values.get(cla_id, 0.0)
        chem_region_vals[(chem, region_uri, region_type)].append(val)

    exposure_edges = []
    for (chem, region_uri, region_type), vals in chem_region_vals.items():
        mean_val = np.mean([v for v in vals if v > -9000]) if vals else 0.0
        exposure_edges.append({
            "src": chem, "src_type": "Chemical", "rel": "exposure_in",
            "dst": region_uri, "dst_type": region_type,
            "attrs": {"mean_value": round(float(mean_val), 4)},
        })

    # Disease -> Region edges from VitalStats
    cancer_edges = []
    for vs_id, (region_uri, region_type) in vs_region.items():
        disease = disease_to_vs.get(vs_id)
        if not disease:
            continue
        cancer_edges.append({
            "src": disease, "src_type": "Disease", "rel": "cancer_stats_in",
            "dst": region_uri, "dst_type": region_type, "attrs": {},
        })

    # Region -> CalendarYear edges (from CLA years)
    region_years_cla = defaultdict(set)
    for cla_id, year_uri in cla_year.items():
        if cla_id in cla_region:
            region_uri, region_type = cla_region[cla_id]
            region_years_cla[(region_uri, region_type)].add(year_uri)

    region_years_vs = defaultdict(set)
    for vs_id, year_uri in vs_year.items():
        if vs_id in vs_region:
            region_uri, region_type = vs_region[vs_id]
            region_years_vs[(region_uri, region_type)].add(year_uri)

    temporal_edges = []
    for (region_uri, region_type), years in region_years_cla.items():
        for year_uri in years:
            temporal_edges.append({
                "src": region_uri, "src_type": region_type, "rel": "measured_in",
                "dst": year_uri, "dst_type": "CalendarYear", "attrs": {},
            })
    for (region_uri, region_type), years in region_years_vs.items():
        for year_uri in years:
            temporal_edges.append({
                "src": region_uri, "src_type": region_type, "rel": "cancer_data_in",
                "dst": year_uri, "dst_type": "CalendarYear", "attrs": {},
            })

    # Deduplicate temporal edges
    seen_temporal = set()
    deduped_temporal = []
    for e in temporal_edges:
        key = (e["src"], e["rel"], e["dst"])
        if key not in seen_temporal:
            seen_temporal.add(key)
            deduped_temporal.append(e)

    all_edges = direct_edges + exposure_edges + cancer_edges + deduped_temporal

    print(f"  Direct edges: {len(direct_edges):,}")
    print(f"  Exposure edges (Chemical->Region): {len(exposure_edges):,}")
    print(f"  Cancer stats edges (Disease->Region): {len(cancer_edges):,}")
    print(f"  Temporal edges (Region->CalendarYear): {len(deduped_temporal):,}")

    kept_ids = set()
    for e in all_edges:
        kept_ids.add(e["src"])
        kept_ids.add(e["dst"])
    kept_nodes = {nid: info for nid, info in nodes.items() if nid in kept_ids and info["type"] not in REIFICATION_TYPES}

    return kept_nodes, all_edges


def build_pyg_heterodata(nodes, edges, region_features, chemical_features):
    from torch_geometric.data import HeteroData

    node_type_to_ids = defaultdict(dict)
    for nid, info in nodes.items():
        nt = info["type"]
        if nid not in node_type_to_ids[nt]:
            node_type_to_ids[nt][nid] = len(node_type_to_ids[nt])

    data = HeteroData()

    for nt, id_map in node_type_to_ids.items():
        data[nt].num_nodes = len(id_map)
        data[nt].node_ids = list(id_map.keys())

        if nt in ("GeoPoliticalRegion", "GeographicRegion"):
            feat_dim = len(CHEMICAL_FEATURE_ORDER) + 2  # chemicals + incidence + mortality
            feat_matrix = torch.zeros(len(id_map), feat_dim)
            for nid, idx in id_map.items():
                if nid in region_features:
                    feat_matrix[idx] = torch.tensor(region_features[nid], dtype=torch.float)
            data[nt].x = normalize_features(feat_matrix)
            print(f"  {nt}: {len(id_map)} nodes, features={feat_dim}-dim")

        elif nt == "Chemical":
            feat_matrix = torch.zeros(len(id_map), 3)
            for nid, idx in id_map.items():
                if nid in chemical_features:
                    feat_matrix[idx] = torch.tensor(chemical_features[nid], dtype=torch.float)
            data[nt].x = normalize_features(feat_matrix)
            print(f"  {nt}: {len(id_map)} nodes, features=3-dim")

        elif nt == "CalendarYear":
            feat_matrix = torch.zeros(len(id_map), 1)
            for nid, idx in id_map.items():
                year_str = nid.split("/")[-1]
                try:
                    year = int(year_str)
                    feat_matrix[idx] = (year - 1950) / 80.0
                except ValueError:
                    pass
            data[nt].x = normalize_features(feat_matrix)
            print(f"  {nt}: {len(id_map)} nodes, features=1-dim")
        else:
            print(f"  {nt}: {len(id_map)} nodes, no features (learnable)")

    edge_index_dict = defaultdict(lambda: ([], []))
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

    for key, (src_list, dst_list) in edge_index_dict.items():
        data[key].edge_index = torch.tensor([src_list, dst_list], dtype=torch.long)

    if skipped > 0:
        print(f"  Skipped {skipped:,} edges (unmapped nodes)")

    total_nodes = sum(len(m) for m in node_type_to_ids.values())
    total_edges = sum(data[key].edge_index.size(1) for key in edge_index_dict)
    print(f"\n  Total: {total_nodes:,} nodes, {total_edges:,} edges, "
          f"{len(node_type_to_ids)} node types, {len(edge_index_dict)} edge types")

    for key in sorted(edge_index_dict.keys(), key=lambda k: -len(edge_index_dict[k][0])):
        n = data[key].edge_index.size(1)
        print(f"    {key}: {n:,}")

    return data, node_type_to_ids


def save_outputs(data, node_type_to_ids, nodes):
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    torch.save(data, PROCESSED_DIR / "hetero_graph.pt")
    print(f"\n  -> {PROCESSED_DIR / 'hetero_graph.pt'}")

    id_maps = {}
    for nt, id_map in node_type_to_ids.items():
        id_maps[nt] = {nid: {"idx": idx, "label": nodes.get(nid, {}).get("label", "")} for nid, idx in id_map.items()}
    with open(PROCESSED_DIR / "node_id_maps.json", "w") as f:
        json.dump(id_maps, f, indent=1)
    print(f"  -> {PROCESSED_DIR / 'node_id_maps.json'}")


def fig_collapsed_schema(nodes, edges, fig_dir):
    fig_dir.mkdir(parents=True, exist_ok=True)
    node_counts = Counter(info["type"] for info in nodes.values())
    edge_counts = Counter((e["src_type"], e["rel"], e["dst_type"]) for e in edges)

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, max(6, len(node_counts) * 0.4)))
    types = [k for k, _ in node_counts.most_common()]
    counts = [node_counts[t] for t in types]
    colors = ["#1f77b4" if t in BIO_NODE_TYPES else "#2ca02c" for t in types]
    ax1.barh(types[::-1], counts[::-1], color=colors[::-1], edgecolor="white", linewidth=0.5)
    ax1.set_xlabel("Nodes")
    ax1.set_title("Node Types (with features)", fontsize=11, fontweight="bold")
    ax1.xaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"{int(x):,}"))
    for i, cnt in enumerate(counts[::-1]):
        ax1.text(cnt + max(counts) * 0.01, i, f"{cnt:,}", va="center", fontsize=7)
    ax1.legend(handles=[mpatches.Patch(color="#1f77b4", label="Biological"),
                        mpatches.Patch(color="#2ca02c", label="Environmental")], loc="lower right", fontsize=8)

    labels = [f"({s}, {r}, {d})" for (s, r, d), _ in edge_counts.most_common()]
    ecounts = [c for _, c in edge_counts.most_common()]
    ax2.barh(labels[::-1], ecounts[::-1], color="#4c72b0", edgecolor="white", linewidth=0.5)
    ax2.set_xlabel("Edges")
    ax2.set_title("Edge Types", fontsize=11, fontweight="bold")
    ax2.xaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"{int(x):,}"))
    for i, cnt in enumerate(ecounts[::-1]):
        ax2.text(cnt + max(ecounts) * 0.01, i, f"{cnt:,}", va="center", fontsize=6)

    fig.suptitle("Lung-CABO KG: Heterogeneous Graph with Node Features", fontsize=13, fontweight="bold")
    plt.tight_layout()
    fig.savefig(fig_dir / "collapsed_schema.png", dpi=200, bbox_inches="tight")
    plt.close(fig)
    print(f"  -> figs/collapsed_schema.png")


def main():
    print("=" * 70)
    print("02_build_graph.py (with node features)")
    print("=" * 70)

    t0 = time.time()

    print("\n[1/6] Loading interim data ...")
    nodes, edges = load_interim()

    print("\n[2/6] Reading literal values from TTLs ...")
    cla_values, vs_incidence, vs_mortality = read_literal_values_from_ttls()

    print("\n[3/6] Extracting reification data ...")
    cla_chemical, cla_region, cla_year, _ = extract_cla_data(edges)
    vs_region, vs_year, vs_people, disease_to_vs = extract_vitalstats_data(nodes, edges)
    print(f"  CLAs: {len(cla_chemical):,} with chemical, {len(cla_region):,} with region, "
          f"{len(cla_year):,} with year, {len(cla_values):,} with value")
    print(f"  VitalStats: {len(vs_region):,} with region, {len(vs_year):,} with year")

    print("\n[4/6] Building node features ...")
    region_features = build_region_features(cla_chemical, cla_region, cla_values, vs_region, vs_incidence, vs_mortality)
    chemical_features = build_chemical_features(cla_chemical, cla_region, cla_values)
    print(f"  Regions with features: {len(region_features):,}")
    print(f"  Chemicals with features: {len(chemical_features):,}")

    print("\n[5/6] Building collapsed edges ...")
    collapsed_nodes, collapsed_edges = build_collapsed_edges(
        nodes, edges, cla_chemical, cla_region, cla_year, cla_values,
        vs_region, vs_year, disease_to_vs
    )
    print(f"  Collapsed graph: {len(collapsed_nodes):,} nodes, {len(collapsed_edges):,} edges")

    print("\n[6/6] Building PyG HeteroData ...")
    data, id_maps = build_pyg_heterodata(collapsed_nodes, collapsed_edges, region_features, chemical_features)
    save_outputs(data, id_maps, collapsed_nodes)

    print("\n  Generating figure ...")
    fig_collapsed_schema(collapsed_nodes, collapsed_edges, FIG_DIR)

    dt = time.time() - t0
    print(f"\n{'=' * 70}")
    print(f"Done in {dt:.1f}s")
    print(f"{'=' * 70}")


if __name__ == "__main__":
    main()
