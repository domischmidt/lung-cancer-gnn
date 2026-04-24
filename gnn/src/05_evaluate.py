"""
05_evaluate.py - Final evaluation summary and thesis-ready figures.

Loads results from baselines and R-GCN, produces comparison tables,
statistical summaries, and publication-quality figures.

Usage:  python gnn/src/05_evaluate.py
Input:  gnn/data/processed/{baseline_results.json, rgcn_results.json}
Output: gnn/data/interim/figs/final_*.png
        gnn/data/processed/final_results_table.csv
"""

import json
import csv
from pathlib import Path

import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.colors import LinearSegmentedColormap

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
PROCESSED_DIR = REPO_ROOT / "gnn" / "data" / "processed"
FIG_DIR = REPO_ROOT / "gnn" / "data" / "interim" / "figs"

MODEL_COLORS = {"TransE": "#4c72b0", "DotProduct": "#55a868", "R-GCN": "#c44e52"}
METRICS = ["mrr", "hits@1", "hits@3", "hits@10"]

RELATION_DISPLAY = {
    "Gene__associated_with__Disease": "Gene - Disease",
    "Gene__in_pathway__Pathway": "Gene - Pathway",
    "Chemical__exposure_in__GeoPoliticalRegion": "Chemical - City",
    "Chemical__exposure_in__GeographicRegion": "Chemical - Region",
    "Disease__cancer_stats_in__GeographicRegion": "Disease - Region (stats)",
    "Disease__cancer_stats_in__Country": "Disease - Country (stats)",
    "Disease__has_fusion__GeneFusion": "Disease - GeneFusion",
    "Disease__has_rearrangement__ChromoRearr": "Disease - ChromoRearr",
    "Variant__variant_of__Disease": "Variant - Disease",
    "Variant__located_in_gene__Gene": "Variant - Gene",
    "GeoPoliticalRegion__part_of__Country": "City - Country",
    "GeographicRegion__part_of__Country": "Region - Country",
    "GeneProduct__part_of_pathway__Pathway": "GeneProduct - Pathway",
    "Biomarker__marker_for__Disease": "Biomarker - Disease",
    "Pathway__linked_to__Disease": "Pathway - Disease",
}


def load_results():
    all_results = {}

    bp = PROCESSED_DIR / "baseline_results.json"
    if bp.exists():
        with open(bp) as f:
            all_results.update(json.load(f))

    rp = PROCESSED_DIR / "rgcn_results.json"
    if rp.exists():
        with open(rp) as f:
            all_results["R-GCN"] = json.load(f)

    print(f"Loaded results for: {', '.join(all_results.keys())}")
    return all_results


def print_summary(all_results):
    models = list(all_results.keys())

    print("\n" + "=" * 80)
    print("OVERALL RESULTS")
    print("=" * 80)
    print(f"{'Model':<12s} {'MRR':>8s} {'H@1':>8s} {'H@3':>8s} {'H@10':>8s}")
    print("-" * 48)
    for m in models:
        o = all_results[m]["overall"]
        print(f"{m:<12s} {o['mrr']:>8.4f} {o['hits@1']:>8.4f} {o['hits@3']:>8.4f} {o['hits@10']:>8.4f}")

    all_rels = set()
    for m in models:
        all_rels.update(k for k in all_results[m] if k != "overall")
    rels = sorted(all_rels)

    print("\n" + "=" * 80)
    print("PER-RELATION MRR")
    print("=" * 80)
    header = f"{'Relation':<35s}" + "".join(f"{m:>12s}" for m in models)
    print(header)
    print("-" * len(header))
    for r in rels:
        display = RELATION_DISPLAY.get(r, r)
        vals = []
        for m in models:
            v = all_results[m].get(r, {}).get("mrr", None)
            vals.append(f"{v:>12.4f}" if v is not None else f"{'---':>12s}")
        print(f"{display:<35s}" + "".join(vals))


def write_results_table(all_results):
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    models = list(all_results.keys())

    all_rels = set()
    for m in models:
        all_rels.update(k for k in all_results[m] if k != "overall")
    rels = ["overall"] + sorted(all_rels)

    path = PROCESSED_DIR / "final_results_table.csv"
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        header = ["relation"]
        for m in models:
            for metric in METRICS:
                header.append(f"{m}_{metric}")
        w.writerow(header)

        for r in rels:
            display = "Overall" if r == "overall" else RELATION_DISPLAY.get(r, r)
            row = [display]
            for m in models:
                for metric in METRICS:
                    v = all_results[m].get(r, {}).get(metric, "")
                    row.append(f"{v:.4f}" if isinstance(v, float) else "")
            w.writerow(row)

    print(f"\n  -> {path}")


def fig_overall_comparison(all_results, fig_dir):
    fig_dir.mkdir(parents=True, exist_ok=True)
    models = list(all_results.keys())
    x = np.arange(len(METRICS))
    width = 0.22

    fig, ax = plt.subplots(figsize=(9, 5))
    for i, m in enumerate(models):
        vals = [all_results[m]["overall"].get(metric, 0) for metric in METRICS]
        offset = (i - (len(models) - 1) / 2) * width
        bars = ax.bar(x + offset, vals, width, label=m, color=MODEL_COLORS.get(m, "#888"), alpha=0.88, edgecolor="white", linewidth=0.5)
        for bar, v in zip(bars, vals):
            ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.012,
                    f"{v:.3f}", ha="center", va="bottom", fontsize=8.5, fontweight="bold")

    ax.set_ylabel("Score", fontsize=11)
    ax.set_title("Overall Link Prediction Performance", fontsize=13, fontweight="bold")
    ax.set_xticks(x)
    ax.set_xticklabels([m.upper() for m in METRICS], fontsize=10)
    ax.legend(fontsize=10)
    ax.set_ylim(0, 1.08)
    ax.grid(True, alpha=0.2, axis="y")
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    plt.tight_layout()
    fig.savefig(fig_dir / "final_overall_comparison.png", dpi=300, bbox_inches="tight")
    plt.close(fig)
    print(f"  -> figs/final_overall_comparison.png")


def fig_gda_focus(all_results, fig_dir):
    fig_dir.mkdir(parents=True, exist_ok=True)
    gda = "Gene__associated_with__Disease"
    models = [m for m in all_results if gda in all_results[m]]
    if not models:
        return

    fig, axes = plt.subplots(1, 2, figsize=(12, 5))

    vals_mrr = [all_results[m][gda]["mrr"] for m in models]
    colors = [MODEL_COLORS.get(m, "#888") for m in models]
    bars = axes[0].bar(models, vals_mrr, color=colors, alpha=0.88, edgecolor="white", linewidth=0.5)
    for bar, v in zip(bars, vals_mrr):
        axes[0].text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.01,
                     f"{v:.3f}", ha="center", va="bottom", fontsize=10, fontweight="bold")
    axes[0].set_ylabel("MRR", fontsize=11)
    axes[0].set_title("Gene-Disease Association: MRR", fontsize=12, fontweight="bold")
    axes[0].set_ylim(0, max(vals_mrr) * 1.25)
    axes[0].grid(True, alpha=0.2, axis="y")
    axes[0].spines["top"].set_visible(False)
    axes[0].spines["right"].set_visible(False)

    x = np.arange(len(METRICS))
    width = 0.22
    for i, m in enumerate(models):
        vals = [all_results[m][gda].get(metric, 0) for metric in METRICS]
        offset = (i - (len(models) - 1) / 2) * width
        axes[1].bar(x + offset, vals, width, label=m, color=MODEL_COLORS.get(m, "#888"), alpha=0.88, edgecolor="white", linewidth=0.5)
    axes[1].set_xticks(x)
    axes[1].set_xticklabels([m.upper() for m in METRICS], fontsize=10)
    axes[1].set_ylabel("Score", fontsize=11)
    axes[1].set_title("Gene-Disease Association: All Metrics", fontsize=12, fontweight="bold")
    axes[1].legend(fontsize=9)
    axes[1].set_ylim(0, 1.08)
    axes[1].grid(True, alpha=0.2, axis="y")
    axes[1].spines["top"].set_visible(False)
    axes[1].spines["right"].set_visible(False)

    plt.tight_layout()
    fig.savefig(fig_dir / "final_gda_focus.png", dpi=300, bbox_inches="tight")
    plt.close(fig)
    print(f"  -> figs/final_gda_focus.png")


def fig_mrr_heatmap(all_results, fig_dir):
    fig_dir.mkdir(parents=True, exist_ok=True)
    models = list(all_results.keys())

    all_rels = set()
    for m in models:
        all_rels.update(k for k in all_results[m] if k != "overall")
    rels = sorted(all_rels, key=lambda r: -max(all_results[m].get(r, {}).get("mrr", 0) for m in models))

    if not rels:
        return

    matrix = []
    display_rels = []
    for r in rels:
        row = [all_results[m].get(r, {}).get("mrr", 0) for m in models]
        matrix.append(row)
        display_rels.append(RELATION_DISPLAY.get(r, r))
    matrix = np.array(matrix)

    cmap = LinearSegmentedColormap.from_list("custom", ["#2c1810", "#c44e52", "#f7dc6f", "#27ae60"], N=256)

    fig, ax = plt.subplots(figsize=(8, max(4, len(rels) * 0.45)))
    im = ax.imshow(matrix, cmap=cmap, aspect="auto", vmin=0, vmax=1)

    ax.set_xticks(range(len(models)))
    ax.set_xticklabels(models, fontsize=10, fontweight="bold")
    ax.set_yticks(range(len(rels)))
    ax.set_yticklabels(display_rels, fontsize=9)

    for i in range(len(rels)):
        for j in range(len(models)):
            v = matrix[i, j]
            color = "black" if v > 0.45 else "white"
            weight = "bold" if v == matrix[i].max() and v > 0 else "normal"
            ax.text(j, i, f"{v:.2f}", ha="center", va="center", fontsize=9, color=color, fontweight=weight)

    plt.colorbar(im, ax=ax, label="MRR", shrink=0.8)
    ax.set_title("MRR per Edge Type and Model", fontsize=13, fontweight="bold", pad=12)
    plt.tight_layout()
    fig.savefig(fig_dir / "final_mrr_heatmap.png", dpi=300, bbox_inches="tight")
    plt.close(fig)
    print(f"  -> figs/final_mrr_heatmap.png")


def fig_bio_vs_env(all_results, fig_dir):
    fig_dir.mkdir(parents=True, exist_ok=True)
    models = list(all_results.keys())

    bio_rels = {
        "Gene__associated_with__Disease", "Gene__in_pathway__Pathway",
        "Disease__has_fusion__GeneFusion", "Disease__has_rearrangement__ChromoRearr",
        "Variant__variant_of__Disease", "Variant__located_in_gene__Gene",
        "GeneProduct__part_of_pathway__Pathway", "Biomarker__marker_for__Disease",
        "Pathway__linked_to__Disease",
    }

    bio_mrrs = {}
    env_mrrs = {}
    for m in models:
        bio_vals = [all_results[m][r]["mrr"] for r in all_results[m] if r != "overall" and r in bio_rels]
        env_vals = [all_results[m][r]["mrr"] for r in all_results[m] if r != "overall" and r not in bio_rels]
        bio_mrrs[m] = np.mean(bio_vals) if bio_vals else 0
        env_mrrs[m] = np.mean(env_vals) if env_vals else 0

    fig, ax = plt.subplots(figsize=(8, 5))
    x = np.arange(len(models))
    width = 0.35

    bars1 = ax.bar(x - width / 2, [bio_mrrs[m] for m in models], width, label="Biological Relations",
                   color="#4c72b0", alpha=0.88, edgecolor="white")
    bars2 = ax.bar(x + width / 2, [env_mrrs[m] for m in models], width, label="Environmental Relations",
                   color="#55a868", alpha=0.88, edgecolor="white")

    for bars in [bars1, bars2]:
        for bar in bars:
            h = bar.get_height()
            ax.text(bar.get_x() + bar.get_width() / 2, h + 0.01, f"{h:.3f}",
                    ha="center", va="bottom", fontsize=9, fontweight="bold")

    ax.set_ylabel("Mean MRR", fontsize=11)
    ax.set_title("Biological vs Environmental Relations: Mean MRR by Model", fontsize=13, fontweight="bold")
    ax.set_xticks(x)
    ax.set_xticklabels(models, fontsize=10)
    ax.legend(fontsize=10)
    ax.set_ylim(0, 1.15)
    ax.grid(True, alpha=0.2, axis="y")
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    plt.tight_layout()
    fig.savefig(fig_dir / "final_bio_vs_env.png", dpi=300, bbox_inches="tight")
    plt.close(fig)
    print(f"  -> figs/final_bio_vs_env.png")


def fig_radar(all_results, fig_dir):
    fig_dir.mkdir(parents=True, exist_ok=True)
    models = list(all_results.keys())

    key_rels = [
        "Gene__associated_with__Disease",
        "Gene__in_pathway__Pathway",
        "Chemical__exposure_in__GeoPoliticalRegion",
        "Disease__cancer_stats_in__GeographicRegion",
        "Disease__has_fusion__GeneFusion",
    ]
    key_rels = [r for r in key_rels if any(r in all_results[m] for m in models)]
    if len(key_rels) < 3:
        return

    labels = [RELATION_DISPLAY.get(r, r) for r in key_rels]
    n = len(labels)
    angles = np.linspace(0, 2 * np.pi, n, endpoint=False).tolist()
    angles += angles[:1]

    fig, ax = plt.subplots(figsize=(8, 8), subplot_kw=dict(polar=True))
    ax.set_theta_offset(np.pi / 2)
    ax.set_theta_direction(-1)
    ax.set_rlabel_position(0)
    plt.yticks([0.2, 0.4, 0.6, 0.8, 1.0], ["0.2", "0.4", "0.6", "0.8", "1.0"], color="grey", size=8)
    plt.ylim(0, 1)

    for m in models:
        vals = [all_results[m].get(r, {}).get("mrr", 0) for r in key_rels]
        vals += vals[:1]
        ax.plot(angles, vals, linewidth=2, label=m, color=MODEL_COLORS.get(m, "#888"))
        ax.fill(angles, vals, alpha=0.1, color=MODEL_COLORS.get(m, "#888"))

    ax.set_xticks(angles[:-1])
    ax.set_xticklabels(labels, fontsize=9)
    ax.set_title("MRR across Key Relations", fontsize=13, fontweight="bold", pad=20)
    ax.legend(loc="upper right", bbox_to_anchor=(1.3, 1.1), fontsize=10)
    plt.tight_layout()
    fig.savefig(fig_dir / "final_radar.png", dpi=300, bbox_inches="tight")
    plt.close(fig)
    print(f"  -> figs/final_radar.png")


def main():
    print("=" * 70)
    print("05_evaluate.py")
    print("=" * 70)

    print("\n[1/3] Loading results ...")
    all_results = load_results()
    print_summary(all_results)

    print("\n[2/3] Writing results table ...")
    write_results_table(all_results)

    print("\n[3/3] Generating thesis figures ...")
    FIG_DIR.mkdir(parents=True, exist_ok=True)
    fig_overall_comparison(all_results, FIG_DIR)
    fig_gda_focus(all_results, FIG_DIR)
    fig_mrr_heatmap(all_results, FIG_DIR)
    fig_bio_vs_env(all_results, FIG_DIR)
    fig_radar(all_results, FIG_DIR)

    print("\n" + "=" * 70)
    print("All figures and tables generated.")
    print("=" * 70)


if __name__ == "__main__":
    main()
