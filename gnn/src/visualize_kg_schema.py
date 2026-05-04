"""
visualize_kg_schema.py - Generate a presentation-ready schema diagram of the KG.

Draws the node types as colored circles (sized by log-count) and edge types
as labeled arrows. Biological nodes on the left, environmental on the right,
Disease bridge in the center. Reification nodes (CLA, VitalStats, GDA, VDA)
shown with distinct styling.

Usage:  python gnn/src/visualize_kg_schema.py
Input:  gnn/data/processed/hetero_graph.pt
Output: gnn/data/interim/figs/kg_schema_network.png
"""

import json
import math
from pathlib import Path
from collections import Counter

import torch
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.patches import FancyArrowPatch

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
PROCESSED_DIR = REPO_ROOT / "gnn" / "data" / "processed"
FIG_DIR = REPO_ROOT / "gnn" / "data" / "interim" / "figs"

# Layout: manually position node types for clarity
# x: 0 = left (bio), 0.5 = center (bridge), 1.0 = right (env)
# y: 0 = top, 1 = bottom
NODE_POSITIONS = {
    # Biological (left side)
    "Gene":           (0.08, 0.25),
    "Pathway":        (0.08, 0.45),
    "GeneProduct":    (0.08, 0.65),
    "Variant":        (0.22, 0.15),
    "Biomarker":      (0.22, 0.75),
    "GeneFusion":     (0.22, 0.45),
    "ChromoRearr":    (0.22, 0.58),
    # Bio reifications
    "GeneDiseaseAssociation":       (0.38, 0.25),
    "VariantDiseaseAssociation":    (0.38, 0.15),
    # Bridge (center)
    "Disease":        (0.50, 0.40),
    # Env reifications
    "VitalStatistics": (0.62, 0.50),
    "ChemicalLocationAssociation":  (0.62, 0.25),
    # Environmental (right side)
    "Chemical":             (0.78, 0.12),
    "GeoPoliticalRegion":   (0.78, 0.32),
    "GeographicRegion":     (0.78, 0.52),
    "Country":              (0.92, 0.42),
    "CalendarYear":         (0.92, 0.18),
    "People":               (0.78, 0.70),
}

# Color scheme by category
NODE_COLORS = {
    "Gene": "#CECBF6", "Pathway": "#CECBF6", "GeneProduct": "#CECBF6",
    "Variant": "#CECBF6", "Biomarker": "#CECBF6", "GeneFusion": "#CECBF6",
    "ChromoRearr": "#CECBF6",
    "GeneDiseaseAssociation": "#B5D4F4", "VariantDiseaseAssociation": "#B5D4F4",
    "Disease": "#F5C4B3",
    "VitalStatistics": "#9FE1CB", "ChemicalLocationAssociation": "#9FE1CB",
    "Chemical": "#C0DD97", "GeoPoliticalRegion": "#C0DD97",
    "GeographicRegion": "#C0DD97", "Country": "#C0DD97",
    "CalendarYear": "#FAC775", "People": "#F4C0D1",
}

NODE_EDGE_COLORS = {
    "Gene": "#534AB7", "Pathway": "#534AB7", "GeneProduct": "#534AB7",
    "Variant": "#534AB7", "Biomarker": "#534AB7", "GeneFusion": "#534AB7",
    "ChromoRearr": "#534AB7",
    "GeneDiseaseAssociation": "#185FA5", "VariantDiseaseAssociation": "#185FA5",
    "Disease": "#993C1D",
    "VitalStatistics": "#0F6E56", "ChemicalLocationAssociation": "#0F6E56",
    "Chemical": "#3B6D11", "GeoPoliticalRegion": "#3B6D11",
    "GeographicRegion": "#3B6D11", "Country": "#3B6D11",
    "CalendarYear": "#854F0B", "People": "#993556",
}

# Short display names
SHORT_NAMES = {
    "GeneDiseaseAssociation": "GDA",
    "VariantDiseaseAssociation": "VDA",
    "ChemicalLocationAssociation": "CLA",
    "VitalStatistics": "VitalStats",
    "GeoPoliticalRegion": "City (EEA)",
    "GeographicRegion": "Region (OECD)",
    "CalendarYear": "Year",
    "ChromoRearr": "ChromoRearr",
    "GeneProduct": "GeneProduct",
    "GeneFusion": "GeneFusion",
}

# Feature descriptions
FEATURES = {
    "ChemicalLocationAssociation": "conc. 1d",
    "VitalStatistics": "inc+mort 2d",
    "GeneDiseaseAssociation": "score 1d",
    "VariantDiseaseAssociation": "DSI+DPI 2d",
    "People": "age+gender 2d",
    "Variant": "chrom+cons+pos 3d",
    "ChromoRearr": "type 1d",
    "GeoPoliticalRegion": "pop. 1d",
    "GeographicRegion": "pop. 1d",
    "CalendarYear": "year 1d",
}


def load_graph_schema():
    data = torch.load(PROCESSED_DIR / "hetero_graph.pt", weights_only=False)

    node_counts = {}
    for nt in data.node_types:
        node_counts[nt] = data[nt].num_nodes

    edge_types = []
    edge_counts = {}
    for et in data.edge_types:
        src_type, rel, dst_type = et
        n = data[et].edge_index.size(1)
        edge_types.append((src_type, rel, dst_type, n))
        edge_counts[(src_type, rel, dst_type)] = n

    return node_counts, edge_types


def main():
    print("=" * 70)
    print("visualize_kg_schema.py")
    print("=" * 70)

    print("\nLoading graph schema ...")
    node_counts, edge_types = load_graph_schema()

    total_nodes = sum(node_counts.values())
    total_edges = sum(e[3] for e in edge_types)
    print(f"  {total_nodes:,} nodes, {total_edges:,} edges")
    print(f"  {len(node_counts)} node types, {len(edge_types)} edge types")

    # --- Draw ---
    FIG_DIR.mkdir(parents=True, exist_ok=True)
    fig, ax = plt.subplots(figsize=(20, 12))
    ax.set_xlim(-0.05, 1.05)
    ax.set_ylim(-0.05, 1.05)
    ax.set_aspect("equal")
    ax.axis("off")

    # Background regions
    ax.axvspan(-0.05, 0.32, alpha=0.03, color="#534AB7")
    ax.axvspan(0.32, 0.68, alpha=0.03, color="#993C1D")
    ax.axvspan(0.68, 1.05, alpha=0.03, color="#0F6E56")

    ax.text(0.13, 0.98, "Biological", fontsize=14, fontweight="bold", color="#534AB7",
            ha="center", va="top", alpha=0.6)
    ax.text(0.50, 0.98, "Bridge", fontsize=14, fontweight="bold", color="#993C1D",
            ha="center", va="top", alpha=0.6)
    ax.text(0.85, 0.98, "Environmental", fontsize=14, fontweight="bold", color="#0F6E56",
            ha="center", va="top", alpha=0.6)

    # Draw edges first (behind nodes)
    drawn_edges = set()
    for src_type, rel, dst_type, n in edge_types:
        if src_type not in NODE_POSITIONS or dst_type not in NODE_POSITIONS:
            continue

        edge_key = (src_type, dst_type)
        if edge_key in drawn_edges:
            continue
        drawn_edges.add(edge_key)

        sx, sy = NODE_POSITIONS[src_type]
        dx, dy = NODE_POSITIONS[dst_type]

        # Skip self-loops in drawing (subtype_of Disease->Disease)
        if src_type == dst_type:
            # Draw a small curved self-loop
            ax.annotate("", xy=(sx + 0.02, sy - 0.04), xytext=(sx - 0.02, sy - 0.04),
                        arrowprops=dict(arrowstyle="->", color="#888", lw=0.8,
                                        connectionstyle="arc3,rad=-0.5"))
            ax.text(sx, sy - 0.065, rel, fontsize=6, ha="center", color="#888", alpha=0.8)
            continue

        # Relation label
        rel_short = rel.replace("_", " ")
        if len(rel_short) > 18:
            rel_short = rel_short[:16] + ".."

        # Count formatting
        count_str = f"{n:,}" if n < 10000 else f"{n/1000:.0f}k"

        # Draw arrow
        ax.annotate("",
                    xy=(dx, dy), xytext=(sx, sy),
                    arrowprops=dict(arrowstyle="-|>", color="#888", lw=0.7,
                                    shrinkA=18, shrinkB=18, alpha=0.5))

        # Label at midpoint
        mx, my = (sx + dx) / 2, (sy + dy) / 2
        angle = math.degrees(math.atan2(dy - sy, dx - sx))
        if abs(angle) > 90:
            angle += 180

        ax.text(mx, my + 0.015, rel_short, fontsize=5.5, ha="center", va="bottom",
                color="#666", alpha=0.8, rotation=angle, rotation_mode="anchor")

    # Draw nodes
    for nt, (x, y) in NODE_POSITIONS.items():
        count = node_counts.get(nt, 0)
        if count == 0:
            continue

        # Size based on log count
        size = 300 + 150 * math.log10(max(count, 1))
        size = min(size, 2200)

        color = NODE_COLORS.get(nt, "#ddd")
        edge_color = NODE_EDGE_COLORS.get(nt, "#888")
        display = SHORT_NAMES.get(nt, nt)
        feat = FEATURES.get(nt, "")

        # Node circle
        ax.scatter(x, y, s=size, c=color, edgecolors=edge_color, linewidth=1.5, zorder=5)

        # Node label (name)
        ax.text(x, y + 0.003, display, fontsize=8, fontweight="bold", ha="center", va="center",
                color=edge_color, zorder=6)

        # Count below
        count_str = f"{count:,}"
        ax.text(x, y - 0.035, count_str, fontsize=6.5, ha="center", va="top",
                color="#666", zorder=6)

        # Feature label
        if feat:
            ax.text(x, y - 0.055, feat, fontsize=5.5, ha="center", va="top",
                    color="#999", style="italic", zorder=6)

    # Title and stats
    ax.text(0.50, 1.04, "Lung-CABO Knowledge Graph", fontsize=18, fontweight="bold",
            ha="center", va="bottom", color="#333")
    ax.text(0.50, -0.03,
            f"{total_nodes:,} nodes  |  {total_edges:,} edges  |  "
            f"{len(node_counts)} node types  |  {len(edge_types)} edge types  |  "
            f"4 R-GCN layers",
            fontsize=10, ha="center", va="top", color="#888")

    # Legend
    legend_items = [
        mpatches.Patch(facecolor="#CECBF6", edgecolor="#534AB7", label="Biological"),
        mpatches.Patch(facecolor="#B5D4F4", edgecolor="#185FA5", label="Bio reification (GDA, VDA)"),
        mpatches.Patch(facecolor="#F5C4B3", edgecolor="#993C1D", label="Bridge (Disease)"),
        mpatches.Patch(facecolor="#9FE1CB", edgecolor="#0F6E56", label="Env reification (CLA, VitalStats)"),
        mpatches.Patch(facecolor="#C0DD97", edgecolor="#3B6D11", label="Environmental"),
        mpatches.Patch(facecolor="#FAC775", edgecolor="#854F0B", label="Temporal (CalendarYear)"),
        mpatches.Patch(facecolor="#F4C0D1", edgecolor="#993556", label="Demographic (People)"),
    ]
    ax.legend(handles=legend_items, loc="lower left", fontsize=8, framealpha=0.9,
              edgecolor="#ccc", fancybox=True)

    plt.tight_layout()
    out_path = FIG_DIR / "kg_schema_network.png"
    fig.savefig(out_path, dpi=300, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    print(f"\n  -> {out_path}")
    print("Done.")


if __name__ == "__main__":
    main()
