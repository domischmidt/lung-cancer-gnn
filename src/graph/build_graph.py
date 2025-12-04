import os
import pandas as pd
import torch
from torch_geometric.data import HeteroData

RAW_DIR = "data/raw"
OUT_PATH = "data/processed/hetero_graph.pt"


def load_csv(name: str) -> pd.DataFrame:
    path = os.path.join(RAW_DIR, name)
    if not os.path.exists(path):
        print(f"[WARN] {path} not found, skipping.")
        return None
    df = pd.read_csv(path)
    print(f"[INFO] Loaded {name} -> {len(df)} rows")
    return df


def build_node_mappings(
    df_dict: dict[str, pd.DataFrame]
) -> dict[str, dict[str, int]]:
    """
    Baut für jeden Node-Typ ein Mapping: externe ID (String) -> interner Index (int)
    """
    node_keys: dict[str, set[str]] = {
        "disease": set(),
        "gene": set(),
        "variant": set(),
        "gene_fusion": set(),
        "chrom_rearr": set(),
        "pathway": set(),
        "biomarker": set(),
        "chemical": set(),
        "evidence": set(),
        "city": set(),
        "demographic_group": set(),
    }

    # disease_gene.csv
    if (df := df_dict.get("disease_gene.csv")) is not None:
        node_keys["disease"].update(df["DiseaseCui"].dropna().astype(str))
        node_keys["gene"].update(df["GeneId"].dropna().astype(str))

    # disease_gene_fusion.csv
    if (df := df_dict.get("disease_gene_fusion.csv")) is not None:
        node_keys["disease"].update(df["DiseaseCui"].dropna().astype(str))
        node_keys["gene_fusion"].update(df["GeneFusion"].dropna().astype(str))

    # disease_chromosomal_rearrangement.csv
    if (df := df_dict.get("disease_chromosomal_rearrangement.csv")) is not None:
        node_keys["disease"].update(df["DiseaseCui"].dropna().astype(str))
        node_keys["chrom_rearr"].update(
            df["ChromosomalRearrengementName"].dropna().astype(str)
        )

    # disease_variant.csv
    if (df := df_dict.get("disease_variant.csv")) is not None:
        node_keys["disease"].update(df["DiseaseCui"].dropna().astype(str))
        node_keys["gene"].update(df["GeneId"].dropna().astype(str))
        node_keys["variant"].update(df["VariantId"].dropna().astype(str))

    # pathway_disease_association.csv
    if (df := df_dict.get("pathway_disease_association.csv")) is not None:
        node_keys["disease"].update(df["DiseaseCui"].dropna().astype(str))
        node_keys["pathway"].update(df["PathwayId"].dropna().astype(str))

    # disease_gene_pathway.csv
    if (df := df_dict.get("disease_gene_pathway.csv")) is not None:
        node_keys["disease"].update(df["DiseaseCui"].dropna().astype(str))
        node_keys["gene"].update(df["GeneId"].dropna().astype(str))
        node_keys["pathway"].update(df["PathwayId"].dropna().astype(str))

    # disease_biomarker.csv
    if (df := df_dict.get("disease_biomarker.csv")) is not None:
        node_keys["disease"].update(df["DiseaseCui"].dropna().astype(str))
        node_keys["biomarker"].update(df["BiomarkerId"].dropna().astype(str))

    # chemical_evidence.csv
    if (df := df_dict.get("chemical_evidence.csv")) is not None:
        node_keys["chemical"].update(df["ChemicalId"].dropna().astype(str))
        node_keys["evidence"].update(df["EvidenceId"].dropna().astype(str))

    # chemical_location.csv
    if (df := df_dict.get("chemical_location.csv")) is not None:
        node_keys["chemical"].update(df["ChemicalId"].dropna().astype(str))
        node_keys["city"].update(df["CityId"].dropna().astype(str))

    # disease_demographics.csv
    if (df := df_dict.get("disease_demographics.csv")) is not None:
        node_keys["disease"].update(df["DiseaseCui"].dropna().astype(str))
        node_keys["demographic_group"].update(
            df["DemographicGroup"].dropna().astype(str)
        )

    # Mapping bauen
    node_maps: dict[str, dict[str, int]] = {}
    for ntype, keys in node_keys.items():
        keys = sorted(k for k in keys if k)  # filter leere Strings
        node_maps[ntype] = {k: i for i, k in enumerate(keys)}
        print(f"[INFO] Node type '{ntype}': {len(keys)} nodes")

    return node_maps


def edges_from_df(
    src_series: pd.Series,
    dst_series: pd.Series,
    src_map: dict[str, int],
    dst_map: dict[str, int],
) -> torch.Tensor:
    src_idx = []
    dst_idx = []
    for s, d in zip(src_series.astype(str), dst_series.astype(str)):
        if s not in src_map or d not in dst_map:
            continue
        src_idx.append(src_map[s])
        dst_idx.append(dst_map[d])
    if not src_idx:
        return torch.empty((2, 0), dtype=torch.long)
    return torch.tensor([src_idx, dst_idx], dtype=torch.long)


def build_hetero_graph(df_dict: dict[str, pd.DataFrame]) -> HeteroData:
    node_maps = build_node_mappings(df_dict)
    data = HeteroData()

    # Node counts
    for ntype, id_map in node_maps.items():
        num_nodes = len(id_map)
        if num_nodes == 0:
            continue
        data[ntype].num_nodes = num_nodes
        # optional: Dummy-Features (1-dim)
        data[ntype].x = torch.ones((num_nodes, 1), dtype=torch.float32)

    # disease ↔ gene
    if (df := df_dict.get("disease_gene.csv")) is not None:
        edge_index = edges_from_df(
            df["DiseaseCui"],
            df["GeneId"],
            node_maps["disease"],
            node_maps["gene"],
        )
        data["disease", "assoc_gene", "gene"].edge_index = edge_index
        print("[INFO] edges: disease–gene", edge_index.shape[1])

    # disease ↔ gene_fusion
    if (df := df_dict.get("disease_gene_fusion.csv")) is not None:
        edge_index = edges_from_df(
            df["DiseaseCui"],
            df["GeneFusion"],
            node_maps["disease"],
            node_maps["gene_fusion"],
        )
        data["disease", "assoc_gene_fusion", "gene_fusion"].edge_index = edge_index
        print("[INFO] edges: disease–gene_fusion", edge_index.shape[1])

    # disease ↔ chrom_rearr
    if (df := df_dict.get("disease_chromosomal_rearrangement.csv")) is not None:
        edge_index = edges_from_df(
            df["DiseaseCui"],
            df["ChromosomalRearrengementName"],
            node_maps["disease"],
            node_maps["chrom_rearr"],
        )
        data["disease", "assoc_chrom_rearr", "chrom_rearr"].edge_index = edge_index
        print("[INFO] edges: disease–chrom_rearr", edge_index.shape[1])

    # disease ↔ variant
    if (df := df_dict.get("disease_variant.csv")) is not None:
        edge_index = edges_from_df(
            df["DiseaseCui"],
            df["VariantId"],
            node_maps["disease"],
            node_maps["variant"],
        )
        data["disease", "assoc_variant", "variant"].edge_index = edge_index
        print("[INFO] edges: disease–variant", edge_index.shape[1])
        # Beispiel: einige Variant-Attribute als Edge-Features könnten später genutzt werden

    # disease ↔ pathway (pathway_disease_association)
    if (df := df_dict.get("pathway_disease_association.csv")) is not None:
        edge_index = edges_from_df(
            df["DiseaseCui"],
            df["PathwayId"],
            node_maps["disease"],
            node_maps["pathway"],
        )
        data["disease", "assoc_pathway", "pathway"].edge_index = edge_index
        print("[INFO] edges: disease–pathway", edge_index.shape[1])

    # gene ↔ pathway (aus disease_gene_pathway.csv)
    if (df := df_dict.get("disease_gene_pathway.csv")) is not None:
        edge_index = edges_from_df(
            df["GeneId"],
            df["PathwayId"],
            node_maps["gene"],
            node_maps["pathway"],
        )
        data["gene", "participates_in", "pathway"].edge_index = edge_index
        print("[INFO] edges: gene–pathway", edge_index.shape[1])

    # disease ↔ biomarker
    if (df := df_dict.get("disease_biomarker.csv")) is not None:
        edge_index = edges_from_df(
            df["DiseaseCui"],
            df["BiomarkerId"],
            node_maps["disease"],
            node_maps["biomarker"],
        )
        data["disease", "assoc_biomarker", "biomarker"].edge_index = edge_index
        print("[INFO] edges: disease–biomarker", edge_index.shape[1])

    # chemical ↔ evidence
    if (df := df_dict.get("chemical_evidence.csv")) is not None:
        edge_index = edges_from_df(
            df["ChemicalId"],
            df["EvidenceId"],
            node_maps["chemical"],
            node_maps["evidence"],
        )
        data["chemical", "has_evidence", "evidence"].edge_index = edge_index
        print("[INFO] edges: chemical–evidence", edge_index.shape[1])

    # chemical ↔ city
    if (df := df_dict.get("chemical_location.csv")) is not None:
        edge_index = edges_from_df(
            df["ChemicalId"],
            df["CityId"],
            node_maps["chemical"],
            node_maps["city"],
        )
        data["chemical", "measured_in", "city"].edge_index = edge_index
        print("[INFO] edges: chemical–city", edge_index.shape[1])
        # Hier könntest du später 'Value', 'Units', 'Population' als Edge-/Node-Features nutzen

    # disease ↔ demographic_group (optional)
    if (df := df_dict.get("disease_demographics.csv")) is not None:
        edge_index = edges_from_df(
            df["DiseaseCui"],
            df["DemographicGroup"],
            node_maps["disease"],
            node_maps["demographic_group"],
        )
        data["disease", "has_demographic_stats", "demographic_group"].edge_index = edge_index
        print("[INFO] edges: disease–demographic_group", edge_index.shape[1])
        # Optional: Statistiken als Features
        # Hier nur Beispiel: mittlere Werte pro DemographicGroup aggregieren wäre möglich

    return data, node_maps


def main():
    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)

    # Alle relevanten CSVs laden
    df_files = [
        "disease_gene.csv",
        "disease_gene_fusion.csv",
        "disease_chromosomal_rearrangement.csv",
        "disease_variant.csv",
        "pathway_disease_association.csv",
        "disease_gene_pathway.csv",
        "disease_biomarker.csv",
        "chemical_evidence.csv",
        "chemical_location.csv",
        "disease_demographics.csv",
    ]
    df_dict: dict[str, pd.DataFrame] = {}
    for fname in df_files:
        df = load_csv(fname)
        if df is not None:
            df_dict[fname] = df

    data, node_maps = build_hetero_graph(df_dict)

    torch.save(
        {"data": data, "node_maps": node_maps},
        OUT_PATH,
    )
    print(f"[INFO] Saved hetero graph to {OUT_PATH}")


if __name__ == "__main__":
    main()
