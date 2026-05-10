"""
visualize_kg_schema.py - Presentation-ready schema diagram of the KG.

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

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
PROCESSED_DIR = REPO_ROOT / "gnn" / "data" / "processed"
FIG_DIR = REPO_ROOT / "gnn" / "data" / "interim" / "figs"

# ---- Layout positions (x, y) in [0,1] space ----
# Spread nodes more for readability
POS = {
    # Biological (left)
    "Gene":           (0.06, 0.40),
    "Pathway":        (0.06, 0.62),
    "GeneProduct":    (0.06, 0.80),
    "Variant":        (0.20, 0.22),
    "Biomarker":      (0.20, 0.80),
    "GeneFusion":     (0.20, 0.52),
    "ChromoRearr":    (0.20, 0.65),
    # Bio reifications (center-left)
    "GeneDiseaseAssociation":    (0.36, 0.40),
    "VariantDiseaseAssociation": (0.36, 0.22),
    # Bridge (center)
    "Disease":        (0.50, 0.48),
    # Env reifications (center-right)
    "VitalStatistics":             (0.64, 0.58),
    "ChemicalLocationAssociation": (0.64, 0.32),
    # Environmental (right)
    "Chemical":           (0.80, 0.18),
    "GeoPoliticalRegion": (0.80, 0.40),
    "GeographicRegion":   (0.80, 0.60),
    "Country":            (0.94, 0.50),
    "CalendarYear":       (0.94, 0.25),
    "People":             (0.80, 0.78),
}

# ---- Colors ----
FILL = {
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
STROKE = {
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

SHORT = {
    "GeneDiseaseAssociation": "GDA",
    "VariantDiseaseAssociation": "VDA",
    "ChemicalLocationAssociation": "CLA",
    "VitalStatistics": "VitalStats",
    "GeoPoliticalRegion": "City (EEA)",
    "GeographicRegion": "Region (OECD)",
    "CalendarYear": "Year",
}

FEAT = {
    "ChemicalLocationAssociation": "conc. 1d",
    "VitalStatistics": "inc + mort 2d",
    "GeneDiseaseAssociation": "score 1d",
    "VariantDiseaseAssociation": "DSI + DPI 2d",
    "People": "age + gender 2d",
    "Variant": "chrom + cons + pos 3d",
    "ChromoRearr": "type 1d",
    "GeoPoliticalRegion": "pop. 1d",
    "GeographicRegion": "pop. 1d",
    "CalendarYear": "year 1d",
}

# ---- Manual edge label offsets to avoid overlaps ----
# key: (src_type, dst_type) -> (dx, dy) offset from midpoint for label
LABEL_NUDGE = {
    ("Disease", "VitalStatistics"): (0.0, 0.02),
    ("Disease", "GeneFusion"): (0.0, 0.02),
    ("Disease", "ChromoRearr"): (0.0, -0.02),
    ("GeneDiseaseAssociation", "Disease"): (0.0, 0.02),
    ("VariantDiseaseAssociation", "Disease"): (0.0, -0.02),
    ("VariantDiseaseAssociation", "Gene"): (0.0, 0.02),
    ("VitalStatistics", "GeographicRegion"): (0.0, 0.02),
    ("VitalStatistics", "Country"): (0.02, 0.0),
    ("VitalStatistics", "CalendarYear"): (0.02, -0.02),
    ("VitalStatistics", "People"): (0.0, 0.02),
    ("ChemicalLocationAssociation", "CalendarYear"): (0.0, -0.02),
    ("ChemicalLocationAssociation", "GeoPoliticalRegion"): (0.0, 0.02),
    ("ChemicalLocationAssociation", "GeographicRegion"): (0.02, 0.0),
    ("ChemicalLocationAssociation", "Chemical"): (0.0, 0.02),
    ("GeoPoliticalRegion", "Country"): (0.0, -0.02),
    ("GeographicRegion", "Country"): (0.0, 0.02),
}


def load_graph_schema():
    data = torch.load(PROCESSED_DIR / "hetero_graph.pt", weights_only=False)
    node_counts = {}
    for nt in data.node_types:
        node_counts[nt] = data[nt].num_nodes
    edge_types = []
    for et in data.edge_types:
        src_type, rel, dst_type = et
        n = data[et].edge_index.size(1)
        edge_types.append((src_type, rel, dst_type, n))
    return node_counts, edge_types


def draw_arrow(ax, sx, sy, dx, dy, label, nudge=(0, 0), color="#888"):
    """Draw an arrow with a label at the midpoint."""
    ax.annotate("",
                xy=(dx, dy), xytext=(sx, sy),
                arrowprops=dict(arrowstyle="-|>", color=color, lw=0.8,
                                shrinkA=20, shrinkB=20, alpha=0.45))
    mx = (sx + dx) / 2 + nudge[0]
    my = (sy + dy) / 2 + nudge[1]

    # Compute angle so text reads left-to-right
    angle = math.degrees(math.atan2(dy - sy, dx - sx))
    if angle > 90:
        angle -= 180
    elif angle < -90:
        angle += 180

    ax.text(mx, my, label, fontsize=5.5, ha="center", va="center",
            color="#555", alpha=0.85, rotation=angle, rotation_mode="anchor",
            bbox=dict(boxstyle="round,pad=0.15", facecolor="white", edgecolor="none", alpha=0.7))


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

    FIG_DIR.mkdir(parents=True, exist_ok=True)
    fig, ax = plt.subplots(figsize=(22, 13))
    ax.set_xlim(-0.03, 1.03)
    ax.set_ylim(-0.03, 1.03)
    ax.set_aspect("equal")
    ax.axis("off")

    # Background regions
    ax.axvspan(-0.03, 0.30, alpha=0.025, color="#534AB7")
    ax.axvspan(0.30, 0.56, alpha=0.020, color="#993C1D")
    ax.axvspan(0.56, 1.03, alpha=0.025, color="#0F6E56")

    ax.text(0.15, 0.97, "Biological", fontsize=15, fontweight="bold", color="#534AB7",
            ha="center", va="top", alpha=0.5)
    ax.text(0.43, 0.97, "Bridge", fontsize=15, fontweight="bold", color="#993C1D",
            ha="center", va="top", alpha=0.5)
    ax.text(0.82, 0.97, "Environmental", fontsize=15, fontweight="bold", color="#0F6E56",
            ha="center", va="top", alpha=0.5)

    # ---- Draw edges ----
    drawn = set()
    for src_type, rel, dst_type, n in edge_types:
        if src_type not in POS or dst_type not in POS:
            continue

        # Self-loop (Disease -> subtype_of -> Disease)
        if src_type == dst_type:
            x, y = POS[src_type]
            # Draw loop below the node
            loop_y = y + 0.06
            ax.annotate("",
                        xy=(x + 0.025, y + 0.025), xytext=(x - 0.025, y + 0.025),
                        arrowprops=dict(arrowstyle="-|>", color="#888", lw=0.8,
                                        connectionstyle="arc3,rad=-0.8", alpha=0.5))
            ax.text(x, loop_y + 0.015, "subtype_of", fontsize=5.5, ha="center", va="bottom",
                    color="#555", alpha=0.85,
                    bbox=dict(boxstyle="round,pad=0.15", facecolor="white", edgecolor="none", alpha=0.7))
            continue

        edge_key = (src_type, rel, dst_type)
        if edge_key in drawn:
            continue
        drawn.add(edge_key)

        sx, sy = POS[src_type]
        dx, dy = POS[dst_type]
        label = rel.replace("_", " ")
        nudge = LABEL_NUDGE.get((src_type, dst_type), (0, 0.015))
        draw_arrow(ax, sx, sy, dx, dy, label, nudge)

    # ---- Draw nodes ----
    for nt, (x, y) in POS.items():
        count = node_counts.get(nt, 0)
        if count == 0:
            continue

        # Circle size based on log
        radius = 300 + 180 * math.log10(max(count, 2))
        radius = min(radius, 2500)

        fill = FILL.get(nt, "#ddd")
        stroke = STROKE.get(nt, "#888")
        name = SHORT.get(nt, nt)
        feat = FEAT.get(nt, "")

        # Draw circle
        ax.scatter(x, y, s=radius, c=fill, edgecolors=stroke, linewidth=1.8, zorder=5)

        # Name inside circle
        ax.text(x, y + 0.005, name, fontsize=8.5, fontweight="bold", ha="center", va="center",
                color=stroke, zorder=6)

        # Count below circle
        count_str = f"{count:,}"
        ax.text(x, y - 0.038, count_str, fontsize=7, ha="center", va="top",
                color="#444", zorder=6)

        # Feature annotation below count
        if feat:
            ax.text(x, y - 0.058, feat, fontsize=5.5, ha="center", va="top",
                    color="#888", style="italic", zorder=6)

    # ---- Title ----
    ax.text(0.50, 1.02, "Lung-CABO Knowledge Graph Schema", fontsize=19, fontweight="bold",
            ha="center", va="bottom", color="#222")
    ax.text(0.50, -0.015,
            f"{total_nodes:,} nodes  |  {total_edges:,} edges  |  "
            f"{len(node_counts)} node types  |  {len(edge_types)} edge types  |  "
            f"2 R-GCN layers (optimised)",
            fontsize=10, ha="center", va="top", color="#777")

    # ---- Legend ----
    legend_items = [
        mpatches.Patch(facecolor="#CECBF6", edgecolor="#534AB7", label="Biological entity"),
        mpatches.Patch(facecolor="#B5D4F4", edgecolor="#185FA5", label="Bio reification (GDA, VDA)"),
        mpatches.Patch(facecolor="#F5C4B3", edgecolor="#993C1D", label="Bridge (Disease)"),
        mpatches.Patch(facecolor="#9FE1CB", edgecolor="#0F6E56", label="Env reification (CLA, VitalStats)"),
        mpatches.Patch(facecolor="#C0DD97", edgecolor="#3B6D11", label="Environmental entity"),
        mpatches.Patch(facecolor="#FAC775", edgecolor="#854F0B", label="Temporal (CalendarYear)"),
        mpatches.Patch(facecolor="#F4C0D1", edgecolor="#993556", label="Demographic (People)"),
    ]
    ax.legend(handles=legend_items, loc="lower left", fontsize=8.5, framealpha=0.92,
              edgecolor="#bbb", fancybox=True, borderpad=1.0)

    plt.tight_layout()
    out_path = FIG_DIR / "kg_schema_network.png"
    fig.savefig(out_path, dpi=300, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    print(f"\n  -> {out_path}")
    print("Done.")


if __name__ == "__main__":
    main()