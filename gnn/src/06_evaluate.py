import json
import csv
from pathlib import Path

import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.colors import LinearSegmentedColormap

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
PROCESSED_DIR = REPO_ROOT / "gnn" / "data" / "processed"
FIG_DIR = REPO_ROOT / "gnn" / "data" / "interim" / "figs"

MODEL_COLORS = {"TransE": "#4c72b0", "DotProduct": "#55a868", "R-GCN": "#c44e52"}
METRICS = ["mrr", "hits@1", "hits@3", "hits@10"]

RELATION_DISPLAY = {
    "Gene__has_association__GeneDiseaseAssociation": "Gene - GDA",
    "GeneDiseaseAssociation__associated_with__Disease": "GDA - Disease",
    "Variant__has_variant_association__VariantDiseaseAssociation": "Variant - VDA",
    "VariantDiseaseAssociation__variant_of__Disease": "VDA - Disease",
    "VariantDiseaseAssociation__located_in_gene__Gene": "VDA - Gene",
    "Gene__in_pathway__Pathway": "Gene - Pathway",
    "Variant__located_in_gene__Gene": "Variant - Gene",
    "Disease__has_fusion__GeneFusion": "Disease - GeneFusion",
    "Disease__has_rearrangement__ChromoRearr": "Disease - ChromoRearr",
    "GeneProduct__part_of_pathway__Pathway": "GeneProduct - Pathway",
    "Biomarker__marker_for__Disease": "Biomarker - Disease",
    "Pathway__linked_to__Disease": "Pathway - Disease",
    "Disease__subtype_of__Disease": "Disease subtype_of",
    "ChemicalLocationAssociation__refers_to__Chemical": "CLA - Chemical",
    "ChemicalLocationAssociation__refers_to__GeoPoliticalRegion": "CLA - City",
    "ChemicalLocationAssociation__refers_to__GeographicRegion": "CLA - Region",
    "ChemicalLocationAssociation__has_time_boundary__CalendarYear": "CLA - Year",
    "Disease__detected_finding__VitalStatistics": "Disease - VitalStats",
    "VitalStatistics__part_of__GeographicRegion": "VitalStats - Region",
    "VitalStatistics__part_of__Country": "VitalStats - Country",
    "VitalStatistics__has_time_boundary__CalendarYear": "VitalStats - Year",
    "VitalStatistics__has_output__People": "VitalStats - People",
    "GeoPoliticalRegion__part_of__Country": "City - Country",
    "GeographicRegion__part_of__Country": "Region - Country",
}

BIO_RELS = {
    "Gene__has_association__GeneDiseaseAssociation",
    "GeneDiseaseAssociation__associated_with__Disease",
    "Gene__in_pathway__Pathway",
    "Variant__has_variant_association__VariantDiseaseAssociation",
    "VariantDiseaseAssociation__variant_of__Disease",
    "VariantDiseaseAssociation__located_in_gene__Gene",
    "Variant__located_in_gene__Gene",
    "Disease__has_fusion__GeneFusion",
    "Disease__has_rearrangement__ChromoRearr",
    "GeneProduct__part_of_pathway__Pathway",
    "Biomarker__marker_for__Disease",
    "Pathway__linked_to__Disease",
    "Disease__subtype_of__Disease",
}

GDA_KEY = "GeneDiseaseAssociation__associated_with__Disease"


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

    print(f"\nGDA -> DISEASE (key relation)")
    print(f"{'Model':<12s} {'MRR':>8s} {'H@1':>8s} {'H@3':>8s} {'H@10':>8s} {'n':>6s}")
    print("-" * 54)
    for m in models:
        if GDA_KEY in all_results[m]:
            o = all_results[m][GDA_KEY]
            n = o.get("n_triples", o.get("n", "?"))
            print(f"{m:<12s} {o['mrr']:>8.4f} {o['hits@1']:>8.4f} {o['hits@3']:>8.4f} {o['hits@10']:>8.4f} {n:>6}")

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
        bars = ax.bar(x + offset, vals, width, label=m,
                      color=MODEL_COLORS.get(m, "#888"), alpha=0.88, edgecolor="white", linewidth=0.5)
        for bar, v in zip(bars, vals):
            ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.012,
                    f"{v:.3f}", ha="center", va="bottom", fontsize=8.5, fontweight="bold")

    ax.set_ylabel("Score", fontsize=11)
    ax.set_title("Overall link prediction (type-aware evaluation)", fontsize=13, fontweight="bold")
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
    models = [m for m in all_results if GDA_KEY in all_results[m]]
    if not models:
        print("  [SKIP] No GDA results found")
        return

    fig, axes = plt.subplots(1, 2, figsize=(12, 5))

    vals_mrr = [all_results[m][GDA_KEY]["mrr"] for m in models]
    colors = [MODEL_COLORS.get(m, "#888") for m in models]
    bars = axes[0].bar(models, vals_mrr, color=colors, alpha=0.88, edgecolor="white", linewidth=0.5)
    for bar, v in zip(bars, vals_mrr):
        axes[0].text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.01,
                     f"{v:.3f}", ha="center", va="bottom", fontsize=10, fontweight="bold")
    axes[0].set_ylabel("MRR", fontsize=11)
    axes[0].set_title("GDA -> Disease: MRR", fontsize=12, fontweight="bold")
    axes[0].set_ylim(0, max(vals_mrr) * 1.25 if vals_mrr else 1)
    axes[0].grid(True, alpha=0.2, axis="y")
    axes[0].spines["top"].set_visible(False)
    axes[0].spines["right"].set_visible(False)

    x = np.arange(len(METRICS))
    width = 0.22
    for i, m in enumerate(models):
        vals = [all_results[m][GDA_KEY].get(metric, 0) for metric in METRICS]
        offset = (i - (len(models) - 1) / 2) * width
        axes[1].bar(x + offset, vals, width, label=m,
                    color=MODEL_COLORS.get(m, "#888"), alpha=0.88, edgecolor="white", linewidth=0.5)
    axes[1].set_xticks(x)
    axes[1].set_xticklabels([m.upper() for m in METRICS], fontsize=10)
    axes[1].set_ylabel("Score", fontsize=11)
    axes[1].set_title("GDA -> Disease: all metrics", fontsize=12, fontweight="bold")
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

    fig, ax = plt.subplots(figsize=(8, max(4, len(rels) * 0.4)))
    im = ax.imshow(matrix, cmap=cmap, aspect="auto", vmin=0, vmax=1)

    ax.set_xticks(range(len(models)))
    ax.set_xticklabels(models, fontsize=10, fontweight="bold")
    ax.set_yticks(range(len(rels)))
    ax.set_yticklabels(display_rels, fontsize=8)

    for i in range(len(rels)):
        for j in range(len(models)):
            v = matrix[i, j]
            color = "black" if v > 0.45 else "white"
            weight = "bold" if v == matrix[i].max() and v > 0 else "normal"
            ax.text(j, i, f"{v:.2f}", ha="center", va="center", fontsize=8, color=color, fontweight=weight)

    plt.colorbar(im, ax=ax, label="MRR", shrink=0.8)
    ax.set_title("MRR per edge type and model", fontsize=13, fontweight="bold", pad=12)
    plt.tight_layout()
    fig.savefig(fig_dir / "final_mrr_heatmap.png", dpi=300, bbox_inches="tight")
    plt.close(fig)
    print(f"  -> figs/final_mrr_heatmap.png")


def fig_bio_vs_env(all_results, fig_dir):
    fig_dir.mkdir(parents=True, exist_ok=True)
    models = list(all_results.keys())

    bio_mrrs = {}
    env_mrrs = {}
    for m in models:
        bio_vals = [all_results[m][r]["mrr"] for r in all_results[m]
                    if r != "overall" and r in BIO_RELS]
        env_vals = [all_results[m][r]["mrr"] for r in all_results[m]
                    if r != "overall" and r not in BIO_RELS]
        bio_mrrs[m] = np.mean(bio_vals) if bio_vals else 0
        env_mrrs[m] = np.mean(env_vals) if env_vals else 0

    fig, ax = plt.subplots(figsize=(8, 5))
    x = np.arange(len(models))
    width = 0.35

    bars1 = ax.bar(x - width / 2, [bio_mrrs[m] for m in models], width,
                   label="Biological relations", color="#1f77b4", alpha=0.88, edgecolor="white")
    bars2 = ax.bar(x + width / 2, [env_mrrs[m] for m in models], width,
                   label="Environmental relations", color="#2ca02c", alpha=0.88, edgecolor="white")

    for bars in [bars1, bars2]:
        for bar in bars:
            h = bar.get_height()
            if h > 0.01:
                ax.text(bar.get_x() + bar.get_width() / 2, h + 0.01, f"{h:.3f}",
                        ha="center", va="bottom", fontsize=9, fontweight="bold")

    ax.set_ylabel("Mean MRR", fontsize=11)
    ax.set_title("Biological vs environmental relations: mean MRR", fontsize=13, fontweight="bold")
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
        "GeneDiseaseAssociation__associated_with__Disease",
        "Gene__in_pathway__Pathway",
        "ChemicalLocationAssociation__refers_to__GeoPoliticalRegion",
        "Disease__detected_finding__VitalStatistics",
        "Disease__has_fusion__GeneFusion",
        "VitalStatistics__part_of__GeographicRegion",
    ]
    key_rels = [r for r in key_rels if any(r in all_results[m] for m in models)]
    if len(key_rels) < 3:
        print("  [SKIP] Not enough key relations for radar chart")
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
    ax.set_title("MRR across key relations", fontsize=13, fontweight="bold", pad=20)
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