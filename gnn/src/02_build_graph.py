"""
02_build_graph.py - Build PyG HeteroData preserving the full KG structure.

This script keeps ALL n-ary reification nodes as first-class graph nodes,
consistent with the Lung-CABO ontology. Each reification carries its literal
values as node features:

  Environmental reifications:
    - ChemicalLocationAssociation: concentration value (1-dim)
    - VitalStatistics: incidence + mortality (2-dim)

  Biological reifications (NEW):
    - GeneDiseaseAssociation: GDA score from DisGeNET (1-dim)
    - VariantDiseaseAssociation: DSI + DPI scores (2-dim)

  Other featured nodes:
    - People: age + gender (2-dim, no ethnicity)
    - Variant: chromosome + consequence type + position (3-dim)
    - ChromoRearr: rearrangement type (1-dim)
    - GeoPoliticalRegion / GeographicRegion: population (1-dim)
    - CalendarYear: normalized year (1-dim)

  Dropped nodes:
    - Population (folded into Region features)
    - Source (administrative only)

Usage:  python gnn/src/02_build_graph.py
Input:  gnn/data/interim/{nodes.csv, edges.csv}
        env_data/data/processed/{graph_EEA.ttl, graph_OECD.ttl, graph_ECIS.ttl, graph_CDC.ttl}
        bio_data/data/{gene_disease_assoc.ttl, variant_disease.ttl, ...}
Output: gnn/data/processed/hetero_graph.pt
        gnn/data/processed/node_id_maps.json
        gnn/data/interim/figs/graph_schema.png
"""

import json
import csv
import re
import time
from pathlib import Path
from collections import defaultdict, Counter

import torch
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
INTERIM_DIR = REPO_ROOT / "gnn" / "data" / "interim"
PROCESSED_DIR = REPO_ROOT / "gnn" / "data" / "processed"
FIG_DIR = INTERIM_DIR / "figs"
ENV_DATA = REPO_ROOT / "env_data" / "data" / "processed"
BIO_DATA = REPO_ROOT / "bio_data" / "data"

# Node types to EXCLUDE entirely
EXCLUDED_TYPES = {"Source"}
# Node types to DROP (fold info into other nodes)
DROP_TYPES = {"Population"}
# Relations to exclude
EXCLUDED_RELATIONS = {"has_source", "has_attribute"}

BIO_NODE_TYPES = {
    "Disease", "Gene", "Variant", "Pathway", "GeneProduct",
    "Biomarker", "ChromoRearr", "GeneFusion",
    "GeneDiseaseAssociation", "VariantDiseaseAssociation",
}

# Consequence type encoding for Variant features
CONSEQUENCE_MAP = {
    "missense_variant": 0,
    "intron_variant": 1,
    "3_prime_UTR_variant": 2,
    "synonymous_variant": 3,
    "non_coding_transcript_exon_variant": 4,
    "5_prime_UTR_variant": 5,
    "frameshift_variant": 6,
    "stop_gained": 7,
    "splice_region_variant": 8,
    "inframe_deletion": 9,
    "inframe_insertion": 10,
}
N_CONSEQUENCE_TYPES = len(CONSEQUENCE_MAP) + 1  # +1 for "other"

# ChromoRearr type encoding
REARR_TYPE_MAP = {
    "Translocations": 0,
    "Inverstions": 1,  # typo in source data
    "Inversions": 1,
    "Derivative chromosomes": 2,
    "Deletions": 3,
}
N_REARR_TYPES = 4


def normalize_features(feat_matrix):
    """Z-score normalization per feature dimension."""
    mean = feat_matrix.mean(dim=0, keepdim=True)
    std = feat_matrix.std(dim=0, keepdim=True).clamp(min=1e-6)
    return (feat_matrix - mean) / std


# =========================================================================
# Step 1: Load interim data
# =========================================================================

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

    print(f"  Loaded {len(nodes):,} nodes, {len(edges):,} edges from interim")
    return nodes, edges


# =========================================================================
# Step 2: Read literal values from env TTLs
# =========================================================================

def read_env_literal_values():
    """Read concentration, incidence, mortality, population from TTL files."""
    import rdflib

    cla_values = {}
    vs_incidence = {}
    vs_mortality = {}
    population_values = {}

    for ttl_name in ["graph_EEA.ttl", "graph_OECD.ttl"]:
        ttl_path = ENV_DATA / ttl_name
        if not ttl_path.exists():
            print(f"    [WARN] {ttl_name} not found")
            continue
        print(f"    {ttl_name} ...")
        g = rdflib.Graph()
        g.parse(str(ttl_path), format="turtle")
        for s, p, o in g:
            s_str, p_str = str(s), str(p)
            if "#cla/" in s_str and "source" not in s_str:
                if "value" in p_str and isinstance(o, rdflib.Literal):
                    v = float(o.toPython())
                    if v > -9000:
                        cla_values[s_str] = v
            if "/population/" in s_str and isinstance(o, rdflib.Literal):
                if "value" in p_str or "SIO_000300" in p_str:
                    try:
                        population_values[s_str] = float(o.toPython())
                    except (ValueError, TypeError):
                        pass

    for ttl_name in ["graph_ECIS.ttl", "graph_CDC.ttl"]:
        ttl_path = ENV_DATA / ttl_name
        if not ttl_path.exists():
            print(f"    [WARN] {ttl_name} not found")
            continue
        print(f"    {ttl_name} ...")
        g = rdflib.Graph()
        g.parse(str(ttl_path), format="turtle")
        for s, p, o in g:
            s_str, p_str = str(s), str(p)
            if "#vitalstatistics/" in s_str and isinstance(o, rdflib.Literal):
                try:
                    val = float(o.toPython())
                except (ValueError, TypeError):
                    continue
                if "incidence" in p_str:
                    vs_incidence[s_str] = val
                elif "mortalityrate" in p_str:
                    vs_mortality[s_str] = val

    print(f"    CLA values: {len(cla_values):,}")
    print(f"    VS incidence: {len(vs_incidence):,}, mortality: {len(vs_mortality):,}")
    print(f"    Population: {len(population_values):,}")
    return cla_values, vs_incidence, vs_mortality, population_values


# =========================================================================
# Step 3: Read bio reification data (GDA scores, VDA scores, Variant features)
# =========================================================================

def read_bio_features():
    """Parse SPARQL result TTLs for GDA scores, VDA scores, and Variant metadata."""
    import rdflib
    from rdflib.namespace import RDF

    RES_NS = rdflib.Namespace("http://www.w3.org/2005/sparql-results#")

    def parse_resultset(filepath):
        g = rdflib.Graph()
        g.parse(str(filepath), format="turtle")
        rows = []
        for solution in g.objects(predicate=RES_NS.solution):
            row = {}
            for binding in g.objects(solution, RES_NS.binding):
                var = str(list(g.objects(binding, RES_NS.variable))[0])
                val_list = list(g.objects(binding, RES_NS.value))
                if val_list:
                    v = val_list[0]
                    row[var] = v.toPython() if isinstance(v, rdflib.Literal) else str(v)
            rows.append(row)
        return rows

    # --- GDA scores ---
    gda_scores = {}  # (gene_uri, disease_code) -> score
    gda_path = BIO_DATA / "gene_disease_assoc.ttl"
    if gda_path.exists():
        print(f"    gene_disease_assoc.ttl ...")
        rows = parse_resultset(gda_path)
        for r in rows:
            gene_id = r.get("GeneId", "")
            disease_cui = r.get("DiseaseCui", "")
            score = r.get("GdaScore")
            if gene_id and disease_cui and score is not None:
                # Use full gene_id as-is (e.g. "ncbigene:29799") to match 01's make_uri("gene", g_id)
                gda_scores[(gene_id, disease_cui)] = float(score)
        print(f"      {len(gda_scores):,} GDA scores")

    # --- VDA scores + Variant metadata ---
    vda_scores = {}  # (variant_id, disease_code) -> {dsi, dpi, gene_num}
    variant_meta = {}  # variant_uri -> [chrom, consequence, position]
    vda_path = BIO_DATA / "variant_disease.ttl"
    if vda_path.exists():
        print(f"    variant_disease.ttl ...")
        rows = parse_resultset(vda_path)
        for r in rows:
            variant_id = r.get("VariantId", "")
            disease_cui = r.get("DiseaseCui", "")
            gene_id = r.get("GeneId", "")
            dsi = r.get("DiseaseSpecificity")
            dpi = r.get("DiseasePleiotropy")
            if variant_id and disease_cui:
                # Keep full gene_id (e.g. "ncbigene:1956") to match 01's make_uri("gene", g_id)
                vda_scores[(variant_id, disease_cui)] = {
                    "dsi": float(dsi) if dsi is not None else 0.0,
                    "dpi": float(dpi) if dpi is not None else 0.0,
                    "gene_id": gene_id,
                }
            if variant_id:
                v_uri = f"lucia:variant/{variant_id}"
                if v_uri not in variant_meta:
                    chrom = r.get("Chromosome", "")
                    consequence = r.get("Consequence", "")
                    start_pos = r.get("ChromosomeStartPosition", "")
                    chrom_val = 0.0
                    if chrom:
                        if str(chrom).isdigit():
                            chrom_val = int(chrom) / 24.0
                        elif str(chrom).upper() == "X":
                            chrom_val = 23.0 / 24.0
                        elif str(chrom).upper() == "Y":
                            chrom_val = 24.0 / 24.0
                    cons_val = CONSEQUENCE_MAP.get(str(consequence), len(CONSEQUENCE_MAP)) / N_CONSEQUENCE_TYPES
                    pos_val = 0.0
                    if start_pos:
                        try:
                            pos_val = float(start_pos) / 250_000_000.0
                        except (ValueError, TypeError):
                            pass
                    variant_meta[v_uri] = [chrom_val, cons_val, pos_val]
        print(f"      {len(vda_scores):,} VDA scores, {len(variant_meta):,} variant metadata")

    # --- ChromoRearr type ---
    rearr_types = {}
    rearr_path = BIO_DATA / "disease_and_chromo_arr.ttl"
    if rearr_path.exists():
        print(f"    disease_and_chromo_arr.ttl ...")
        rows = parse_resultset(rearr_path)
        for r in rows:
            name = r.get("ChromosomalRearrengementName", "")
            rtype = r.get("ChromosomalRearrengementType", "")
            if name:
                uri = f"lucia:chromo_rearr/{name}"
                rearr_types[uri] = REARR_TYPE_MAP.get(rtype, 0) / N_REARR_TYPES
        print(f"      {len(rearr_types):,} rearrangement types")

    return gda_scores, vda_scores, variant_meta, rearr_types


# =========================================================================
# Step 4: Extract People features and Region population
# =========================================================================

def extract_people_features(nodes):
    people_features = {}
    for uri, info in nodes.items():
        if info["type"] != "People":
            continue
        parts = uri.split("/")[-1].split("_") if "/" in uri else []
        age_str, gender_str = "", ""
        for p in parts:
            p_lower = p.lower()
            if p_lower in ("male", "female"):
                gender_str = p_lower
            elif "-" in p or "+" in p or p.replace("-", "").isdigit():
                age_str = p
        age_val = 0.5
        if age_str:
            # Strip trailing + first (e.g. "85+" -> "85", "75-85+" -> "75-85")
            clean = age_str.rstrip("+")
            if "-" in clean:
                try:
                    lo, hi = clean.split("-", 1)
                    age_val = (float(lo) + float(hi)) / 2.0 / 100.0
                except ValueError:
                    pass
            else:
                try:
                    age_val = float(clean) / 100.0
                except ValueError:
                    pass
        gender_val = 0.0 if gender_str == "male" else 1.0 if gender_str == "female" else 0.5
        people_features[uri] = [age_val, gender_val]
    return people_features


def build_region_population(nodes, edges, population_values):
    region_pop = {}
    for e in edges:
        if e["rel"] == "has_measurement_value" and e["dst_type"] == "Population":
            if e["dst"] in population_values:
                region_pop[e["src"]] = population_values[e["dst"]]
        if e["src_type"] == "Population" and e["dst_type"] in ("GeoPoliticalRegion", "GeographicRegion"):
            if e["src"] in population_values:
                region_pop[e["dst"]] = population_values[e["src"]]
    return region_pop


# =========================================================================
# Step 5: Create bio reification nodes and replace direct edges
# =========================================================================

def create_bio_reification_nodes(nodes, edges, gda_scores, vda_scores):
    """Replace direct Gene-Disease and Variant-Disease edges with reification nodes."""
    new_nodes = dict(nodes)
    new_edges = []
    gda_counter = 0
    vda_counter = 0

    for e in edges:
        # Replace Gene -> associated_with -> Disease
        if e["rel"] == "associated_with" and e["src_type"] == "Gene" and e["dst_type"] == "Disease":
            gene_uri = e["src"]
            disease_uri = e["dst"]
            disease_code = disease_uri.split("/")[-1]
            gene_num = gene_uri.split("/")[-1]

            score = gda_scores.get((gene_num, disease_code), 0.0)

            gda_uri = f"lucia:gda/gda_{gda_counter}"
            gda_counter += 1
            new_nodes[gda_uri] = {
                "type": "GeneDiseaseAssociation",
                "label": f"GDA_{gene_num}_{disease_code}",
                "gda_score": score,
            }
            new_edges.append({"src": gene_uri, "src_type": "Gene", "rel": "has_association",
                              "dst": gda_uri, "dst_type": "GeneDiseaseAssociation", "attrs": {}})
            new_edges.append({"src": gda_uri, "src_type": "GeneDiseaseAssociation", "rel": "associated_with",
                              "dst": disease_uri, "dst_type": "Disease", "attrs": {}})

        # Replace Variant -> variant_of -> Disease
        elif e["rel"] == "variant_of" and e["src_type"] == "Variant" and e["dst_type"] == "Disease":
            variant_uri = e["src"]
            disease_uri = e["dst"]
            disease_code = disease_uri.split("/")[-1]
            variant_id = variant_uri.split("/")[-1]

            vda_info = vda_scores.get((variant_id, disease_code), {"dsi": 0.0, "dpi": 0.0, "gene_id": ""})

            vda_uri = f"lucia:vda/vda_{vda_counter}"
            vda_counter += 1
            new_nodes[vda_uri] = {
                "type": "VariantDiseaseAssociation",
                "label": f"VDA_{variant_id}_{disease_code}",
                "dsi": vda_info["dsi"],
                "dpi": vda_info["dpi"],
            }
            new_edges.append({"src": variant_uri, "src_type": "Variant", "rel": "has_variant_association",
                              "dst": vda_uri, "dst_type": "VariantDiseaseAssociation", "attrs": {}})
            new_edges.append({"src": vda_uri, "src_type": "VariantDiseaseAssociation", "rel": "variant_of",
                              "dst": disease_uri, "dst_type": "Disease", "attrs": {}})
            # VDA -> Gene (which gene this variant belongs to)
            gene_id = vda_info["gene_id"]
            if gene_id:
                gene_uri = f"lucia:gene/{gene_id}"
                if gene_uri in new_nodes:
                    new_edges.append({"src": vda_uri, "src_type": "VariantDiseaseAssociation",
                                      "rel": "located_in_gene",
                                      "dst": gene_uri, "dst_type": "Gene", "attrs": {}})
        else:
            new_edges.append(e)

    print(f"  Created {gda_counter:,} GDA reification nodes")
    print(f"  Created {vda_counter:,} VDA reification nodes")
    return new_nodes, new_edges


# =========================================================================
# Step 6: Filter graph
# =========================================================================

def filter_graph(nodes, edges):
    remove_types = EXCLUDED_TYPES | DROP_TYPES
    kept_nodes = {uri: info for uri, info in nodes.items() if info["type"] not in remove_types}
    kept_edges = []
    for e in edges:
        if e["src_type"] in remove_types or e["dst_type"] in remove_types:
            continue
        if e["rel"] in EXCLUDED_RELATIONS:
            continue
        if e["src"] in kept_nodes and e["dst"] in kept_nodes:
            kept_edges.append(e)
    print(f"  Filtered: {len(kept_nodes):,} nodes, {len(kept_edges):,} edges")
    return kept_nodes, kept_edges


# =========================================================================
# Step 7: Build PyG HeteroData
# =========================================================================

def build_pyg_heterodata(nodes, edges, cla_values, vs_incidence, vs_mortality,
                         people_features, region_population, variant_meta, rearr_types):
    from torch_geometric.data import HeteroData

    node_type_to_ids = defaultdict(dict)
    for uri, info in nodes.items():
        nt = info["type"]
        if uri not in node_type_to_ids[nt]:
            node_type_to_ids[nt][uri] = len(node_type_to_ids[nt])

    data = HeteroData()

    for nt, id_map in node_type_to_ids.items():
        n_nodes = len(id_map)
        data[nt].num_nodes = n_nodes

        if nt == "ChemicalLocationAssociation":
            feat = torch.zeros(n_nodes, 1)
            found = 0
            for uri, idx in id_map.items():
                if uri in cla_values:
                    feat[idx, 0] = cla_values[uri]
                    found += 1
            data[nt].x = normalize_features(feat)
            print(f"  {nt}: {n_nodes:,} nodes, 1d (concentration) [{found:,} with values]")

        elif nt == "VitalStatistics":
            feat = torch.zeros(n_nodes, 2)
            fi, fm = 0, 0
            for uri, idx in id_map.items():
                if uri in vs_incidence:
                    feat[idx, 0] = vs_incidence[uri]
                    fi += 1
                if uri in vs_mortality:
                    feat[idx, 1] = vs_mortality[uri]
                    fm += 1
            data[nt].x = normalize_features(feat)
            print(f"  {nt}: {n_nodes:,} nodes, 2d (inc+mort) [{fi:,} inc, {fm:,} mort]")

        elif nt == "GeneDiseaseAssociation":
            feat = torch.zeros(n_nodes, 1)
            found = 0
            for uri, idx in id_map.items():
                score = nodes[uri].get("gda_score", 0.0)
                if score > 0:
                    feat[idx, 0] = score
                    found += 1
            data[nt].x = normalize_features(feat)
            print(f"  {nt}: {n_nodes:,} nodes, 1d (GDA score) [{found:,} with scores]")

        elif nt == "VariantDiseaseAssociation":
            feat = torch.zeros(n_nodes, 2)
            found = 0
            for uri, idx in id_map.items():
                dsi = nodes[uri].get("dsi", 0.0)
                dpi = nodes[uri].get("dpi", 0.0)
                feat[idx, 0] = dsi
                feat[idx, 1] = dpi
                if dsi > 0 or dpi > 0:
                    found += 1
            data[nt].x = normalize_features(feat)
            print(f"  {nt}: {n_nodes:,} nodes, 2d (DSI+DPI) [{found:,} with scores]")

        elif nt == "People":
            feat = torch.zeros(n_nodes, 2)
            found = 0
            for uri, idx in id_map.items():
                if uri in people_features:
                    feat[idx] = torch.tensor(people_features[uri])
                    found += 1
            data[nt].x = normalize_features(feat)
            print(f"  {nt}: {n_nodes:,} nodes, 2d (age+gender) [{found:,} parsed]")

        elif nt == "Variant":
            feat = torch.zeros(n_nodes, 3)
            found = 0
            for uri, idx in id_map.items():
                if uri in variant_meta:
                    feat[idx] = torch.tensor(variant_meta[uri])
                    found += 1
            data[nt].x = normalize_features(feat)
            print(f"  {nt}: {n_nodes:,} nodes, 3d (chrom+cons+pos) [{found:,} with meta]")

        elif nt == "ChromoRearr":
            feat = torch.zeros(n_nodes, 1)
            found = 0
            for uri, idx in id_map.items():
                if uri in rearr_types:
                    feat[idx, 0] = rearr_types[uri]
                    found += 1
            data[nt].x = normalize_features(feat)
            print(f"  {nt}: {n_nodes:,} nodes, 1d (rearr type) [{found:,} with type]")

        elif nt in ("GeoPoliticalRegion", "GeographicRegion"):
            feat = torch.zeros(n_nodes, 1)
            found = 0
            for uri, idx in id_map.items():
                if uri in region_population:
                    feat[idx, 0] = region_population[uri]
                    found += 1
            data[nt].x = normalize_features(feat)
            print(f"  {nt}: {n_nodes:,} nodes, 1d (population) [{found:,} with pop]")

        elif nt == "CalendarYear":
            feat = torch.zeros(n_nodes, 1)
            for uri, idx in id_map.items():
                year_str = uri.split("/")[-1]
                try:
                    feat[idx, 0] = (int(year_str) - 1950) / 80.0
                except ValueError:
                    pass
            data[nt].x = normalize_features(feat)
            print(f"  {nt}: {n_nodes:,} nodes, 1d (year)")

        else:
            print(f"  {nt}: {n_nodes:,} nodes, no features (learnable)")

    # --- Edges ---
    print(f"\n  Building edges ({len(edges):,} total) ...")
    edge_index_dict = defaultdict(lambda: ([], []))
    skipped = 0
    for i, e in enumerate(edges):
        if i > 0 and i % 100_000 == 0:
            print(f"    processed {i:,} / {len(edges):,} edges ...")
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

    print(f"  Converting to tensors ...")
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


# =========================================================================
# Step 8: Save
# =========================================================================

def save_outputs(data, node_type_to_ids, nodes):
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    torch.save(data, PROCESSED_DIR / "hetero_graph.pt")
    print(f"\n  -> {PROCESSED_DIR / 'hetero_graph.pt'}")

    id_maps = {}
    for nt, id_map in node_type_to_ids.items():
        id_maps[nt] = {
            uri: {"idx": idx, "label": nodes.get(uri, {}).get("label", "")}
            for uri, idx in id_map.items()
        }
    with open(PROCESSED_DIR / "node_id_maps.json", "w") as f:
        json.dump(id_maps, f, indent=1)
    print(f"  -> {PROCESSED_DIR / 'node_id_maps.json'}")


# =========================================================================
# Step 9: Figure
# =========================================================================

def fig_graph_schema(nodes, edges, fig_dir):
    fig_dir.mkdir(parents=True, exist_ok=True)
    node_counts = Counter(info["type"] for info in nodes.values())
    edge_counts = Counter((e["src_type"], e["rel"], e["dst_type"]) for e in edges)

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(18, max(8, len(edge_counts) * 0.35)))

    types = [k for k, _ in node_counts.most_common()]
    counts = [node_counts[t] for t in types]
    colors = ["#1f77b4" if t in BIO_NODE_TYPES else "#2ca02c" for t in types]
    ax1.barh(types[::-1], counts[::-1], color=colors[::-1], edgecolor="white", linewidth=0.5)
    ax1.set_xlabel("Nodes")
    ax1.set_title("Node types (full KG structure)", fontsize=11, fontweight="bold")
    ax1.xaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"{int(x):,}"))
    for i, cnt in enumerate(counts[::-1]):
        ax1.text(cnt + max(counts) * 0.01, i, f"{cnt:,}", va="center", fontsize=7)
    ax1.legend(handles=[
        mpatches.Patch(color="#1f77b4", label="Biological"),
        mpatches.Patch(color="#2ca02c", label="Environmental"),
    ], loc="lower right", fontsize=8)

    labels = [f"({s}, {r}, {d})" for (s, r, d), _ in edge_counts.most_common()]
    ecounts = [c for _, c in edge_counts.most_common()]
    ax2.barh(labels[::-1], ecounts[::-1], color="#4c72b0", edgecolor="white", linewidth=0.5)
    ax2.set_xlabel("Edges")
    ax2.set_title("Edge types", fontsize=11, fontweight="bold")
    ax2.xaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"{int(x):,}"))
    for i, cnt in enumerate(ecounts[::-1]):
        ax2.text(cnt + max(ecounts) * 0.01, i, f"{cnt:,}", va="center", fontsize=6)

    fig.suptitle("Lung-CABO KG: full heterogeneous graph structure", fontsize=13, fontweight="bold")
    plt.tight_layout()
    fig.savefig(fig_dir / "graph_schema.png", dpi=200, bbox_inches="tight")
    plt.close(fig)
    print(f"  -> figs/graph_schema.png")


# =========================================================================
# Main
# =========================================================================

def main():
    print("=" * 70)
    print("02_build_graph.py (full structure, bio + env reifications)")
    print("=" * 70)
    t0 = time.time()

    print("\n[1/8] Loading interim data ...")
    nodes, edges = load_interim()

    print("\n[2/8] Reading environmental literal values ...")
    cla_values, vs_incidence, vs_mortality, population_values = read_env_literal_values()

    print("\n[3/8] Reading biological features (GDA, VDA, Variant, ChromoRearr) ...")
    gda_scores, vda_scores, variant_meta, rearr_types = read_bio_features()

    print("\n[4/8] Extracting People features ...")
    people_features = extract_people_features(nodes)
    print(f"  People with parsed features: {len(people_features):,}")

    print("\n[5/8] Building Region population features ...")
    region_population = build_region_population(nodes, edges, population_values)
    print(f"  Regions with population: {len(region_population):,}")

    print("\n[6/8] Creating bio reification nodes (GDA, VDA) ...")
    nodes, edges = create_bio_reification_nodes(nodes, edges, gda_scores, vda_scores)

    print("\n[7/8] Filtering graph ...")
    kept_nodes, kept_edges = filter_graph(nodes, edges)

    print("\n[8/8] Building PyG HeteroData ...")
    data, id_maps = build_pyg_heterodata(
        kept_nodes, kept_edges,
        cla_values, vs_incidence, vs_mortality,
        people_features, region_population,
        variant_meta, rearr_types,
    )
    save_outputs(data, id_maps, kept_nodes)

    print("\n  Generating figure ...")
    fig_graph_schema(kept_nodes, kept_edges, FIG_DIR)

    dt = time.time() - t0
    print(f"\n{'=' * 70}")
    print(f"Done in {dt:.1f}s")
    print(f"{'=' * 70}")


if __name__ == "__main__":
    main()