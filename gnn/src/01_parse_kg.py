"""
01_parse_kg.py - Parse Lung-CABO KG into unified node/edge lists with schema visualizations.

Usage:  python gnn/src/01_parse_kg.py
Output: gnn/data/interim/{nodes.csv, edges.csv, schema_summary.json, figs/*.png}
"""

import json
import csv
import time
from pathlib import Path
from collections import Counter
from decimal import Decimal

import rdflib
from rdflib.namespace import RDF, RDFS
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
ENV_PROCESSED = REPO_ROOT / "env_data" / "data" / "processed"
BIO_DATA = REPO_ROOT / "bio_data" / "data"
OUTPUT_DIR = REPO_ROOT / "gnn" / "data" / "interim"
FIG_DIR = OUTPUT_DIR / "figs"

ENV_TTL_FILES = [
    ENV_PROCESSED / "graph_shared.ttl",
    ENV_PROCESSED / "graph_CDC.ttl",
    ENV_PROCESSED / "graph_ECIS.ttl",
    ENV_PROCESSED / "graph_OECD.ttl",
    ENV_PROCESSED / "graph_EEA.ttl",
]

BIO_TTL_FILES = {
    "gene_disease_assoc": BIO_DATA / "gene_disease_assoc.ttl",
    "variant_disease": BIO_DATA / "variant_disease.ttl",
    "disease_gene_pathway": BIO_DATA / "disease_gene_pathway.ttl",
    "pathway_disease": BIO_DATA / "pathway_disease.ttl",
    "biomarker_disease": BIO_DATA / "biomarker_disease.ttl",
    "disease_chromo_arr": BIO_DATA / "disease_and_chromo_arr.ttl",
    "disease_gene_fusions": BIO_DATA / "disease_and_gene_fusions.ttl",
}

RDF_TYPE_MAP = {
    "C7057": "Disease", "C16612": "Gene", "SO_0001060": "Variant",
    "C48807": "Chemical", "C25464": "Country", "SIO_000415": "GeoPoliticalRegion",
    "SIO_000414": "GeographicRegion", "277267003": "CalendarYear",
    "C95553": "People", "C17258": "VitalStatistics",
    "ChemicalLocationAssociation": "ChemicalLocationAssociation",
    "SIO_001061": "Population",
}

URI_TYPE_PATTERNS = {
    "#calendaryear/": "CalendarYear",
    "#country/gr/population/": "Population",
    "#country/gpr/population/": "Population",
    "#country/gr/": "GeographicRegion",
    "#country/gpr/": "GeoPoliticalRegion",
    "#chemical/": "Chemical",
    "#disease/": "Disease",
    "#people/": "People",
    "#cla/": "ChemicalLocationAssociation",
    "#vitalstatistics/": "VitalStatistics",
    "#cla/source/": "Source",
    "#cda/source/": "Source",
    "#gda/source/": "Source",
    "#vda/source/": "Source",
    "#bda/source/": "Source",
    "#source/": "Source",
}

CHEMICAL_LABELS = {
    "C0030106": "Ozone (O3)",
    "C5890534": "PM2.5",
    "C0005036": "Benzene",
    "C0005052": "Benzo[a]pyrene (BaP)",
    "C0028160": "NO2",
    "C0028167": "NOx",
    "C1720884_10": "PM10",
    "C1720884_10_As": "PM10 (Arsenic)",
    "C1720884_10_Cd": "PM10 (Cadmium)",
    "C1720884_10_Ni": "PM10 (Nickel)",
    "C1720884_10_Pb": "PM10 (Lead)",
}

COUNTRY_LABELS = {
    "AL": "Albania", "AT": "Austria", "AU": "Australia", "BA": "Bosnia and Herzegovina",
    "BE": "Belgium", "BG": "Bulgaria", "CA": "Canada", "CH": "Switzerland",
    "CL": "Chile", "CO": "Colombia", "CR": "Costa Rica", "CY": "Cyprus",
    "CZ": "Czechia", "DE": "Germany", "DK": "Denmark", "EE": "Estonia",
    "ES": "Spain", "FI": "Finland", "FR": "France", "GB": "United Kingdom",
    "GR": "Greece", "HR": "Croatia", "HU": "Hungary", "IE": "Ireland",
    "IS": "Iceland", "IT": "Italy", "JP": "Japan", "KR": "South Korea",
    "LT": "Lithuania", "LU": "Luxembourg", "LV": "Latvia", "ME": "Montenegro",
    "MK": "North Macedonia", "MT": "Malta", "MX": "Mexico", "NL": "Netherlands",
    "NO": "Norway", "NZ": "New Zealand", "PL": "Poland", "PT": "Portugal",
    "RO": "Romania", "RS": "Serbia", "SE": "Sweden", "SI": "Slovenia",
    "SK": "Slovakia", "TR": "Turkey", "UA": "Ukraine", "US": "United States",
    "XK": "Kosovo",
}

PRED_MAP = {
    "SIO_000628": "refers_to", "SIO_000679": "has_time_boundary",
    "SIO_000061": "part_of", "SIO_000253": "has_source",
    "SIO_000008": "has_attribute", "SIO_000216": "has_measurement_value",
    "SIO_000229": "has_output", "SIO_000300": "detected_finding",
    "SIO_000028": "is_part_of",
}

RES_NS = rdflib.Namespace("http://www.w3.org/2005/sparql-results#")

LITERAL_PREDS = {
    "https://w3id.org/LUCIA/sem-lucia#value", "https://w3id.org/LUCIA/sem-lucia#category",
    "https://w3id.org/LUCIA/sem-lucia#incidence", "https://w3id.org/LUCIA/sem-lucia#mortalityrate",
    "https://w3id.org/LUCIA/sem-lucia#age", "https://w3id.org/LUCIA/sem-lucia#gender",
    "https://w3id.org/LUCIA/sem-lucia#ethnicity",
}

SKIP_PREDS = {str(RDF.type), str(RDFS.label), "http://purl.org/dc/terms/identifier"}

BIO_NODE_TYPES = {"Disease", "Gene", "Variant", "Pathway", "GeneProduct", "Biomarker", "ChromoRearr", "GeneFusion"}

EXCLUDED_TYPES = {"Source"}


def infer_type_from_uri(uri):
    for pattern, ntype in sorted(URI_TYPE_PATTERNS.items(), key=lambda x: -len(x[0])):
        if pattern in uri:
            return ntype
    if "#country/" in uri and "/gr/" not in uri and "/gpr/" not in uri and "/population/" not in uri:
        return "Country"
    return None


def infer_label_from_uri(uri, ntype):
    if ntype == "Chemical":
        chem_id = uri.split("/")[-1]
        return CHEMICAL_LABELS.get(chem_id, chem_id)
    if ntype == "Country":
        code = uri.split("/")[-1]
        return COUNTRY_LABELS.get(code, code)
    if ntype == "CalendarYear":
        return uri.split("/")[-1]
    return ""


def parse_sparql_resultset(filepath):
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


def make_uri(prefix, identifier):
    return f"lucia:{prefix}/{identifier}"


def parse_env_ttls(ttl_files):
    g = rdflib.Graph()
    for f in ttl_files:
        if not f.exists():
            print(f"  [WARN] Not found: {f}")
            continue
        t0 = time.time()
        g.parse(str(f), format="turtle")
        print(f"  {f.name:30s} -> {len(g):>10,} cumul. triples  ({time.time()-t0:.1f}s)")
    return g


def extract_env_nodes_and_edges(g):
    nodes = {}
    edges = []

    for s, p, o in g.triples((None, RDF.type, None)):
        uri = str(s)
        frag = str(o).split("#")[-1].split("/")[-1]
        readable = RDF_TYPE_MAP.get(frag, frag)
        if readable not in EXCLUDED_TYPES and uri not in nodes:
            nodes[uri] = {"id": uri, "type": readable, "label": ""}

    for s, p, o in g.triples((None, RDFS.label, None)):
        uri = str(s)
        if uri in nodes:
            nodes[uri]["label"] = str(o)

    inferred_count = 0
    for s, p, o in g:
        p_str = str(p)
        if p_str in SKIP_PREDS or p_str in LITERAL_PREDS or isinstance(o, rdflib.Literal):
            continue

        s_uri, o_uri = str(s), str(o)

        for uri in (s_uri, o_uri):
            if uri not in nodes:
                inferred = infer_type_from_uri(uri)
                if inferred and inferred not in EXCLUDED_TYPES:
                    label = infer_label_from_uri(uri, inferred)
                    nodes[uri] = {"id": uri, "type": inferred, "label": label}
                    inferred_count += 1

        if s_uri in nodes and o_uri in nodes:
            if nodes[s_uri]["type"] in EXCLUDED_TYPES or nodes[o_uri]["type"] in EXCLUDED_TYPES:
                continue
            frag = p_str.split("#")[-1].split("/")[-1]
            rel = PRED_MAP.get(frag, frag)
            edges.append({
                "src": s_uri, "src_type": nodes[s_uri]["type"], "rel": rel,
                "dst": o_uri, "dst_type": nodes[o_uri]["type"], "attrs": {},
            })

    # backfill labels for typed nodes that got labels from inference
    for uri, info in nodes.items():
        if not info["label"]:
            label = infer_label_from_uri(uri, info["type"])
            if label:
                info["label"] = label

    print(f"  Inferred types for {inferred_count:,} previously untyped nodes")
    return nodes, edges


def extract_bio_nodes_and_edges(bio_files):
    nodes = {}
    edges = []

    def ensure(uri, ntype, label=""):
        if uri not in nodes:
            nodes[uri] = {"id": uri, "type": ntype, "label": label}

    f = bio_files.get("gene_disease_assoc")
    if f and f.exists():
        print(f"  {f.name} ...")
        rows = parse_sparql_resultset(f)
        seen = set()
        for r in rows:
            d, g_id = r.get("DiseaseCui", ""), r.get("GeneId", "")
            if not d or not g_id: continue
            d_uri, g_uri = make_uri("disease", d), make_uri("gene", g_id)
            ensure(d_uri, "Disease", r.get("DiseaseName", ""))
            ensure(g_uri, "Gene", r.get("GeneSymbol", ""))
            pair = (g_uri, d_uri)
            if pair not in seen:
                seen.add(pair)
                attrs = {}
                if r.get("GdaScore") is not None: attrs["gda_score"] = float(r["GdaScore"])
                edges.append({"src": g_uri, "src_type": "Gene", "rel": "associated_with", "dst": d_uri, "dst_type": "Disease", "attrs": attrs})
        print(f"    {len(seen):,} unique Gene-Disease edges")

    f = bio_files.get("variant_disease")
    if f and f.exists():
        print(f"  {f.name} ...")
        rows = parse_sparql_resultset(f)
        seen = set()
        for r in rows:
            d, v_id, g_id = r.get("DiseaseCui", ""), r.get("VariantId", ""), r.get("GeneId", "")
            if not d or not v_id: continue
            d_uri, v_uri = make_uri("disease", d), make_uri("variant", v_id)
            ensure(d_uri, "Disease", r.get("DiseaseName", ""))
            ensure(v_uri, "Variant", v_id)
            pair = (v_uri, d_uri)
            if pair not in seen:
                seen.add(pair)
                attrs = {}
                if r.get("DiseaseSpecificity") is not None: attrs["dsi"] = float(r["DiseaseSpecificity"])
                if r.get("DiseasePleiotropy") is not None: attrs["dpi"] = float(r["DiseasePleiotropy"])
                if r.get("Consequence"): attrs["consequence"] = r["Consequence"]
                edges.append({"src": v_uri, "src_type": "Variant", "rel": "variant_of", "dst": d_uri, "dst_type": "Disease", "attrs": attrs})
            if g_id:
                g_uri = make_uri("gene", g_id)
                ensure(g_uri, "Gene", r.get("GeneSymbol", ""))
                vg = (v_uri, g_uri)
                if vg not in seen:
                    seen.add(vg)
                    edges.append({"src": v_uri, "src_type": "Variant", "rel": "located_in_gene", "dst": g_uri, "dst_type": "Gene", "attrs": {}})
        print(f"    {len(seen):,} unique VDA + Variant-Gene edges")

    f = bio_files.get("disease_gene_pathway")
    if f and f.exists():
        print(f"  {f.name} ...")
        rows = parse_sparql_resultset(f)
        seen = set()
        for r in rows:
            g_id, pw = r.get("GeneId", ""), r.get("PathwayId", "")
            if not g_id or not pw: continue
            g_uri, pw_uri = make_uri("gene", g_id), make_uri("pathway", pw)
            ensure(g_uri, "Gene", r.get("GeneSymbol", ""))
            ensure(pw_uri, "Pathway", pw)
            pair = (g_uri, pw_uri)
            if pair not in seen:
                seen.add(pair)
                edges.append({"src": g_uri, "src_type": "Gene", "rel": "in_pathway", "dst": pw_uri, "dst_type": "Pathway", "attrs": {}})
        print(f"    {len(seen):,} unique Gene-Pathway edges")

    f = bio_files.get("pathway_disease")
    if f and f.exists():
        print(f"  {f.name} ...")
        rows = parse_sparql_resultset(f)
        pw_seen, gp_seen = set(), set()
        for r in rows:
            pw, d = r.get("PathwayId", ""), r.get("DiseaseCui", "")
            if pw and d:
                pw_uri, d_uri = make_uri("pathway", pw), make_uri("disease", d)
                ensure(pw_uri, "Pathway", r.get("PathwayName", ""))
                ensure(d_uri, "Disease", r.get("DiseaseName", ""))
                pair = (pw_uri, d_uri)
                if pair not in pw_seen:
                    pw_seen.add(pair)
                    edges.append({"src": pw_uri, "src_type": "Pathway", "rel": "linked_to", "dst": d_uri, "dst_type": "Disease", "attrs": {}})
            gp = r.get("GeneProductId", "")
            if gp and pw:
                gp_uri = make_uri("geneproduct", gp)
                pw_uri = make_uri("pathway", pw)
                ensure(gp_uri, "GeneProduct", r.get("GeneProductName", ""))
                pair = (gp_uri, pw_uri)
                if pair not in gp_seen:
                    gp_seen.add(pair)
                    edges.append({"src": gp_uri, "src_type": "GeneProduct", "rel": "part_of_pathway", "dst": pw_uri, "dst_type": "Pathway", "attrs": {}})
        print(f"    {len(pw_seen):,} Pathway-Disease + {len(gp_seen):,} GeneProduct-Pathway")

    f = bio_files.get("biomarker_disease")
    if f and f.exists():
        print(f"  {f.name} ...")
        rows = parse_sparql_resultset(f)
        for r in rows:
            bm, d = r.get("BiomarkerId", ""), r.get("DiseaseCui", "")
            if not bm or not d: continue
            ensure(make_uri("biomarker", bm), "Biomarker", r.get("BiomarkerName", ""))
            ensure(make_uri("disease", d), "Disease", r.get("DiseaseName", ""))
            edges.append({"src": make_uri("biomarker", bm), "src_type": "Biomarker", "rel": "marker_for", "dst": make_uri("disease", d), "dst_type": "Disease", "attrs": {}})
        print(f"    {len(rows):,} Biomarker-Disease edges")

    f = bio_files.get("disease_chromo_arr")
    if f and f.exists():
        print(f"  {f.name} ...")
        rows = parse_sparql_resultset(f)
        seen = set()
        for r in rows:
            d, cr = r.get("DiseaseCui", ""), r.get("ChromosomalRearrengementName", "")
            if not d or not cr: continue
            d_uri, cr_uri = make_uri("disease", d), make_uri("chromo_rearr", cr)
            ensure(d_uri, "Disease", r.get("DiseaseName", ""))
            ensure(cr_uri, "ChromoRearr", cr)
            pair = (d_uri, cr_uri)
            if pair not in seen:
                seen.add(pair)
                attrs = {"type": r["ChromosomalRearrengementType"]} if r.get("ChromosomalRearrengementType") else {}
                edges.append({"src": d_uri, "src_type": "Disease", "rel": "has_rearrangement", "dst": cr_uri, "dst_type": "ChromoRearr", "attrs": attrs})
        print(f"    {len(seen):,} Disease-ChromoRearr edges")

    f = bio_files.get("disease_gene_fusions")
    if f and f.exists():
        print(f"  {f.name} ...")
        rows = parse_sparql_resultset(f)
        seen = set()
        for r in rows:
            d, gf = r.get("DiseaseCui", ""), r.get("GeneFusion", "")
            if not d or not gf: continue
            d_uri, gf_uri = make_uri("disease", d), make_uri("gene_fusion", gf)
            ensure(d_uri, "Disease", r.get("DiseaseName", ""))
            ensure(gf_uri, "GeneFusion", gf)
            pair = (d_uri, gf_uri)
            if pair not in seen:
                seen.add(pair)
                edges.append({"src": d_uri, "src_type": "Disease", "rel": "has_fusion", "dst": gf_uri, "dst_type": "GeneFusion", "attrs": {}})
        print(f"    {len(seen):,} Disease-GeneFusion edges")

    return nodes, edges


# All 42 diseases to be linked as subtypes of C0242379 (Malignant neoplasm of lung).
# NOTE: Some are not strictly subtypes (marked with ?). To be reviewed by domain expert.
LUNG_CANCER_SUBTYPES = {
    "C0007120": "Bronchioloalveolar Adenocarcinoma",
    "C0007131": "Non-Small Cell Lung Carcinoma",
    "C0020507": "Hyperplasia",                            # ? general pathology
    "C0024115": "Lung cancer panel",
    "C0024121": "Lung Neoplasms",
    "C0025568": "Metaplasia",                             # ? general pathology
    "C0085261": "Proteus Syndrome",                       # ? genetic syndrome
    "C0149782": "Squamous cell carcinoma of lung",
    "C0149925": "Small cell carcinoma of lung",
    "C0149927": "Hamartoma of lung",                      # ? benign tumor
    "C0152013": "Adenocarcinoma of lung (disorder)",
    "C0205642": "Adenocarcinoma, Oxyphilic",
    "C0205697": "Carcinoma, Spindle-Cell",
    "C0278517": "Non-small cell lung cancer recurrent",
    "C0278725": "Small cell lung cancer limited stage",
    "C0278726": "Small cell lung cancer extensive stage",
    "C0278727": "Small cell lung cancer recurrent",
    "C0279557": "Adenosquamous cell lung cancer",
    "C0280089": "Carcinoid tumor of lung",
    "C0280217": "stage, non-small cell lung cancer",
    "C0334254": "Lymphoepithelial carcinoma",             # ? not lung-specific
    "C0345958": "Large cell carcinoma of lung",
    "C0345960": "Giant cell carcinoma of lung",
    "C0349649": "Pulmonary lymphangioleiomyomatosis",     # ? not cancer
    "C0684249": "Lung Carcinoma Metastatic in the Brain",
    "C0685053": "Carcinoma in situ of lung",
    "C1332137": "Lung Acinar Adenocarcinoma",
    "C1333125": "Combined Lung Small Cell Carcinoma",
    "C1334363": "large cell neuroendocrine carcinoma of lung",
    "C1334439": "adenoid cystic carcinoma of lung",
    "C1334455": "Pulmonary Sclerosing Hemangioma",        # ? benign tumor
    "C1708045": "Fetal adenocarcinoma of lung",
    "C1708778": "mucoepidermoid carcinoma of lung",
    "C1708781": "Pseudosarcomatous carcinoma of lung",
    "C1711276": "carcinosarcoma of lung",
    "C1960396": "EGFR- non-small cell lung cancer",
    "C1960925": "EGFR+ non-small cell lung cancer",
    "C4072942": "Atypical pulmonary carcinoid tumor",
    "C4324656": "Non-squamous non-small cell lung cancer",
    "C4509816": "Squamous non-small cell lung cancer",
    "C4521520": "Lung Adenocarcinoma In Situ",
    "C4522160": "Invasive Lung Mucinous Adenocarcinoma",
}

PARENT_DISEASE = "C0242379"


def add_subtype_edges(nodes, edges, disease_uri_map):
    parent_uri = None
    for uri, info in nodes.items():
        if info["type"] == "Disease" and PARENT_DISEASE in uri:
            parent_uri = uri
            break
    if not parent_uri:
        print("  [WARN] Parent disease C0242379 not found, skipping subtype edges")
        return edges

    count = 0
    for code in LUNG_CANCER_SUBTYPES:
        child_uri = f"lucia:disease/{code}"
        if child_uri in nodes or disease_uri_map.get(code):
            resolved = disease_uri_map.get(code, child_uri)
            if resolved == parent_uri:
                continue
            edges.append({
                "src": resolved, "src_type": "Disease",
                "rel": "subtype_of",
                "dst": parent_uri, "dst_type": "Disease",
                "attrs": {},
            })
            count += 1
    print(f"  Added {count} (Disease, subtype_of, Disease) edges bridging to env layer")
    return edges


def merge_nodes(env_nodes, bio_nodes):
    merged = dict(env_nodes)
    disease_uri_map = {}
    for uri, info in env_nodes.items():
        if info["type"] == "Disease":
            disease_uri_map[uri.split("/")[-1].split("#")[-1]] = uri
    for uri, info in bio_nodes.items():
        if info["type"] == "Disease" and uri.split("/")[-1] in disease_uri_map:
            continue
        merged[uri] = info
    return merged, disease_uri_map


def remap_bio_edges(bio_edges, disease_uri_map):
    remapped = []
    for e in bio_edges:
        new_e = dict(e)
        for key in ("src", "dst"):
            if new_e[f"{key}_type"] == "Disease":
                code = new_e[key].split("/")[-1]
                if code in disease_uri_map:
                    new_e[key] = disease_uri_map[code]
        remapped.append(new_e)
    return remapped


def write_outputs(nodes, edges, output_dir):
    output_dir.mkdir(parents=True, exist_ok=True)

    with open(output_dir / "nodes.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["node_id", "node_type", "label"])
        for uri, info in sorted(nodes.items()):
            w.writerow([info["id"], info["type"], info["label"]])
    print(f"  {len(nodes):,} nodes -> nodes.csv")

    with open(output_dir / "edges.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["src_id", "src_type", "relation", "dst_id", "dst_type", "attrs_json"])
        for e in edges:
            w.writerow([e["src"], e["src_type"], e["rel"], e["dst"], e["dst_type"], json.dumps(e["attrs"]) if e["attrs"] else "{}"])
    print(f"  {len(edges):,} edges -> edges.csv")

    node_type_counts = Counter(n["type"] for n in nodes.values())
    edge_type_counts = Counter((e["src_type"], e["rel"], e["dst_type"]) for e in edges)
    summary = {
        "total_nodes": len(nodes),
        "total_edges": len(edges),
        "node_types": dict(node_type_counts.most_common()),
        "edge_types": {f"({s}, {r}, {d})": c for (s, r, d), c in edge_type_counts.most_common()},
    }
    with open(output_dir / "schema_summary.json", "w") as f:
        json.dump(summary, f, indent=2)
    print(f"  schema_summary.json")
    return summary


def fig_node_type_distribution(summary, fig_dir):
    fig_dir.mkdir(parents=True, exist_ok=True)
    types = list(summary["node_types"].keys())
    counts = list(summary["node_types"].values())
    colors = ["#1f77b4" if t in BIO_NODE_TYPES else "#2ca02c" for t in types]
    fig, ax = plt.subplots(figsize=(10, max(5, len(types) * 0.4)))
    bars = ax.barh(types[::-1], counts[::-1], color=colors[::-1], edgecolor="white", linewidth=0.5)
    ax.set_xlabel("Number of Nodes")
    ax.set_title("Node Type Distribution in Lung-CABO Knowledge Graph")
    ax.xaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"{int(x):,}"))
    for bar, count in zip(bars, counts[::-1]):
        ax.text(bar.get_width() + max(counts) * 0.01, bar.get_y() + bar.get_height() / 2, f"{count:,}", va="center", fontsize=8)
    ax.legend(handles=[mpatches.Patch(color="#1f77b4", label="Biological"), mpatches.Patch(color="#2ca02c", label="Environmental")], loc="lower right")
    plt.tight_layout()
    fig.savefig(fig_dir / "node_type_distribution.png", dpi=200, bbox_inches="tight")
    plt.close(fig)
    print(f"  -> figs/node_type_distribution.png")


def fig_edge_type_distribution(summary, fig_dir):
    fig_dir.mkdir(parents=True, exist_ok=True)
    labels = list(summary["edge_types"].keys())
    counts = list(summary["edge_types"].values())
    fig, ax = plt.subplots(figsize=(12, max(5, len(labels) * 0.4)))
    ax.barh(labels[::-1], counts[::-1], color="#4c72b0", edgecolor="white", linewidth=0.5)
    ax.set_xlabel("Number of Edges")
    ax.set_title("Edge Type Distribution in Lung-CABO Knowledge Graph")
    ax.xaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"{int(x):,}"))
    for i, (_, cnt) in enumerate(zip(labels[::-1], counts[::-1])):
        ax.text(cnt + max(counts) * 0.01, i, f"{cnt:,}", va="center", fontsize=7)
    plt.tight_layout()
    fig.savefig(fig_dir / "edge_type_distribution.png", dpi=200, bbox_inches="tight")
    plt.close(fig)
    print(f"  -> figs/edge_type_distribution.png")


def fig_layer_composition(summary, fig_dir):
    fig_dir.mkdir(parents=True, exist_ok=True)
    nt = summary["node_types"]
    bio_count = sum(v for k, v in nt.items() if k in BIO_NODE_TYPES)
    env_count = sum(v for k, v in nt.items() if k not in BIO_NODE_TYPES)
    et = summary["edge_types"]
    bio_rels = {"associated_with", "variant_of", "located_in_gene", "in_pathway", "linked_to", "part_of_pathway", "marker_for", "has_rearrangement", "has_fusion"}
    bio_edges = sum(v for k, v in et.items() if any(r in k for r in bio_rels))
    env_edges = sum(v for k, v in et.items()) - bio_edges
    fig, axes = plt.subplots(1, 2, figsize=(10, 4))
    colors = ["#1f77b4", "#2ca02c"]
    total_n, total_e = bio_count + env_count, bio_edges + env_edges
    axes[0].pie([bio_count, env_count], labels=["Biological", "Environmental"], colors=colors,
                autopct=lambda p: f"{p:.1f}%\n({int(p/100*total_n):,})", startangle=90, textprops={"fontsize": 9})
    axes[0].set_title("Nodes by Layer")
    axes[1].pie([bio_edges, env_edges], labels=["Biological", "Environmental"], colors=colors,
                autopct=lambda p: f"{p:.1f}%\n({int(p/100*total_e):,})", startangle=90, textprops={"fontsize": 9})
    axes[1].set_title("Edges by Layer")
    fig.suptitle("Lung-CABO KG: Biological vs Environmental Composition", fontsize=12, fontweight="bold")
    plt.tight_layout()
    fig.savefig(fig_dir / "layer_composition.png", dpi=200, bbox_inches="tight")
    plt.close(fig)
    print(f"  -> figs/layer_composition.png")


def main():
    print("=" * 70)
    print("01_parse_kg.py")
    print("=" * 70)

    print("\n[1/5] Parsing environmental TTLs ...")
    env_graph = parse_env_ttls(ENV_TTL_FILES)
    print(f"  Total: {len(env_graph):,} triples")

    print("\n[2/5] Extracting env nodes and edges ...")
    env_nodes, env_edges = extract_env_nodes_and_edges(env_graph)
    print(f"  {len(env_nodes):,} nodes, {len(env_edges):,} edges")
    del env_graph

    print("\n[3/5] Parsing biological SPARQL exports ...")
    bio_nodes, bio_edges = extract_bio_nodes_and_edges(BIO_TTL_FILES)
    print(f"  {len(bio_nodes):,} nodes, {len(bio_edges):,} edges")

    print("\n[4/5] Merging and writing ...")
    merged, disease_map = merge_nodes(env_nodes, bio_nodes)
    for uri, info in bio_nodes.items():
        if uri not in merged:
            if info["type"] != "Disease" or uri.split("/")[-1] not in disease_map:
                merged[uri] = info
    all_edges = env_edges + remap_bio_edges(bio_edges, disease_map)
    print(f"  Bridged {len(disease_map)} Disease entities across layers")
    all_edges = add_subtype_edges(merged, all_edges, disease_map)
    summary = write_outputs(merged, all_edges, OUTPUT_DIR)

    print("\n[5/5] Generating thesis figures ...")
    fig_node_type_distribution(summary, FIG_DIR)
    fig_edge_type_distribution(summary, FIG_DIR)
    fig_layer_composition(summary, FIG_DIR)

    print("\n" + "=" * 70)
    print(f"Done. {summary['total_nodes']:,} nodes, {summary['total_edges']:,} edges, "
          f"{len(summary['node_types'])} node types, {len(summary['edge_types'])} edge types")
    print("=" * 70)


if __name__ == "__main__":
    main()
