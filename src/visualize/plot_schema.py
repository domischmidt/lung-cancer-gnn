# src/visualize/plot_schema.py

import torch
import networkx as nx
import matplotlib.pyplot as plt
import os
import math


def visualize_schema(graph_path="data/processed/hetero_graph.pt", save=True):
    obj = torch.load(graph_path, weights_only=False)
    data = obj["data"]

    G = nx.DiGraph()

    # alle Node-Typen als Knoten
    for node_type in data.node_types:
        G.add_node(node_type)

    # Relationstypen als gerichtete Kanten (mit Attribut "label")
    for (src, rel, dst) in data.edge_types:
        G.add_edge(src, dst, label=rel)

    # ---------- Layout: disease in die Mitte, Rest auf Kreis ----------
    node_types = list(G.nodes())
    center = "disease" if "disease" in node_types else node_types[0]

    pos = {}
    pos[center] = (0.0, 0.0)

    others = [n for n in node_types if n != center]
    n = len(others)
    radius = 3.0

    for i, node in enumerate(others):
        angle = 2 * math.pi * i / n
        x = radius * math.cos(angle)
        y = radius * math.sin(angle)
        pos[node] = (x, y)

    plt.figure(figsize=(10, 8))

    # ---------- Nodes ----------
    node_colors = []
    node_sizes = []
    for node in node_types:
        if node == center:
            node_colors.append("#ffcc66")  # disease hervorgehoben
            node_sizes.append(3500)
        else:
            node_colors.append("#aec6cf")
            node_sizes.append(2500)

    nx.draw_networkx_nodes(G, pos, node_color=node_colors, node_size=node_sizes)
    nx.draw_networkx_labels(G, pos, font_size=12)

    # ---------- Edges ----------
    nx.draw_networkx_edges(G, pos, arrows=True, arrowstyle="-|>", arrowsize=20)

    # nur den reinen Label-String anzeigen, nicht das ganze Dict
    edge_labels = nx.get_edge_attributes(G, "label")
    nx.draw_networkx_edge_labels(
        G,
        pos,
        edge_labels=edge_labels,
        font_size=10,
        label_pos=0.55,
    )

    plt.axis("off")
    plt.title("Lung-CABO / LUCIA Knowledge Graph Schema", fontsize=14)

    if save:
        os.makedirs("assets", exist_ok=True)
        out_path = "assets/schema_graph.png"
        plt.savefig(out_path, dpi=300, bbox_inches="tight")
        print(f"Saved → {out_path}")

    plt.show()


if __name__ == "__main__":
    visualize_schema()
