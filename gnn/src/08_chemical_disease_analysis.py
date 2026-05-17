import json
import csv
import time
from pathlib import Path
from collections import defaultdict

import torch
import torch.nn as nn
import torch.nn.functional as F
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

from torch_geometric.nn import RGCNConv

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
PROCESSED_DIR = REPO_ROOT / "gnn" / "data" / "processed"
INTERIM_DIR = REPO_ROOT / "gnn" / "data" / "interim"
FIG_DIR = INTERIM_DIR / "figs"
ENV_DATA = REPO_ROOT / "env_data" / "data" / "processed"

DEVICE = torch.device("mps" if torch.backends.mps.is_available() else "cuda" if torch.cuda.is_available() else "cpu")

HIDDEN_DIM = 128
NUM_LAYERS = 4
NUM_BASES = 6
DROPOUT = 0.2

CHEMICAL_NAMES = {
    "C0030106": "Ozone (O\u2083)",
    "C5890534": "PM2.5",
    "C0005036": "Benzene",
    "C0005052": "Benzo[a]pyrene",
    "C0028160": "NO\u2082",
    "C1720884_10": "PM10",
    "C1720884_10_As": "PM10 (Arsenic)",
    "C1720884_10_Cd": "PM10 (Cadmium)",
    "C1720884_10_Ni": "PM10 (Nickel)",
    "C1720884_10_Pb": "PM10 (Lead)",
}


class RGCNWithFeatures(nn.Module):
    def __init__(self, n_nodes, n_relations, hidden_dim, num_layers, num_bases, dropout, node_features):
        super().__init__()
        self.node_emb = nn.Embedding(n_nodes, hidden_dim)
        nn.init.xavier_uniform_(self.node_emb.weight)
        self.feat_projections = nn.ModuleDict()
        for nt, info in node_features.items():
            feat_dim = info["feat"].shape[1]
            self.feat_projections[nt] = nn.Linear(feat_dim, hidden_dim)
        self.node_feature_info = node_features
        self.convs = nn.ModuleList()
        for _ in range(num_layers):
            self.convs.append(RGCNConv(hidden_dim, hidden_dim, n_relations, num_bases=num_bases))
        self.rel_emb = nn.Embedding(n_relations, hidden_dim)
        nn.init.xavier_uniform_(self.rel_emb.weight)
        self.dropout = dropout

    def get_initial_embeddings(self):
        x = self.node_emb.weight.clone()
        for nt, info in self.node_feature_info.items():
            offset = info["offset"]
            n = info["n"]
            feat = info["feat"].to(x.device)
            projected = self.feat_projections[nt](feat)
            x[offset:offset + n] = x[offset:offset + n] + projected
        return x

    def encode(self, edge_index, edge_type):
        x = self.get_initial_embeddings()
        for conv in self.convs:
            x = conv(x, edge_index, edge_type)
            x = F.relu(x)
            x = F.dropout(x, p=self.dropout, training=self.training)
        return x


def load_model_and_embeddings():
    data = torch.load(PROCESSED_DIR / "hetero_graph.pt", weights_only=False)

    node_offsets = {}
    offset = 0
    node_features = {}
    for nt in data.node_types:
        node_offsets[nt] = offset
        n = data[nt].num_nodes
        if hasattr(data[nt], 'x') and data[nt].x is not None:
            node_features[nt] = {"offset": offset, "n": n, "feat": data[nt].x}
        offset += n
    total_nodes = offset

    all_src, all_dst, all_rel = [], [], []
    rel_to_id = {}
    for et in data.edge_types:
        src_type, rel, dst_type = et
        rel_key = f"{src_type}__{rel}__{dst_type}"
        if rel_key not in rel_to_id:
            rel_to_id[rel_key] = len(rel_to_id)
        rid = rel_to_id[rel_key]
        ei = data[et].edge_index
        all_src.append(ei[0] + node_offsets[src_type])
        all_dst.append(ei[1] + node_offsets[dst_type])
        all_rel.append(torch.full((ei.size(1),), rid, dtype=torch.long))

    edge_index = torch.stack([torch.cat(all_src), torch.cat(all_dst)])
    edge_type = torch.cat(all_rel)

    model = RGCNWithFeatures(total_nodes, len(rel_to_id), HIDDEN_DIM, NUM_LAYERS, NUM_BASES, DROPOUT, node_features)
    model.load_state_dict(torch.load(PROCESSED_DIR / "rgcn_weights.pt", map_location=DEVICE, weights_only=True))
    model.to(DEVICE)
    model.eval()

    with torch.no_grad():
        z = model.encode(edge_index.to(DEVICE), edge_type.to(DEVICE)).cpu()

    with open(PROCESSED_DIR / "node_id_maps.json") as f:
        node_id_maps = json.load(f)

    return z, node_offsets, node_id_maps, data

def analysis_embedding_proximity(z, node_offsets, node_id_maps):
    print("\n  Computing Chemical-Disease cosine similarities ...")

    chem_map = node_id_maps.get("Chemical", {})
    disease_map = node_id_maps.get("Disease", {})
    chem_offset = node_offsets["Chemical"]
    disease_offset = node_offsets["Disease"]

    results = []
    for chem_uri, chem_info in chem_map.items():
        chem_idx = chem_info["idx"] + chem_offset
        chem_id = chem_uri.split("/")[-1]
        chem_name = CHEMICAL_NAMES.get(chem_id, chem_info.get("label", chem_id))
        chem_emb = z[chem_idx]

        for dis_uri, dis_info in disease_map.items():
            dis_idx = dis_info["idx"] + disease_offset
            dis_name = dis_info.get("label", dis_uri.split("/")[-1])
            dis_emb = z[dis_idx]

            sim = F.cosine_similarity(chem_emb.unsqueeze(0), dis_emb.unsqueeze(0)).item()
            results.append({
                "chemical": chem_name,
                "chemical_id": chem_id,
                "disease": dis_name,
                "cosine_similarity": round(sim, 4),
            })

    results.sort(key=lambda x: -x["cosine_similarity"])
    return results

def load_exposure_data():
    exposure = defaultdict(lambda: defaultdict(list))

    eea_path = ENV_DATA / "eea_final.csv"
    if eea_path.exists():
        with open(eea_path, encoding="utf-8") as f:
            for row in csv.DictReader(f):
                country = row.get("CountryCode", "")
                chem = row.get("ChemicalID", "")
                val = row.get("Value", "")
                if country and val:
                    try:
                        exposure[country][chem].append(float(val))
                    except ValueError:
                        pass

    oecd_path = ENV_DATA / "oecd_exposure_final.csv"
    if oecd_path.exists():
        with open(oecd_path, encoding="utf-8") as f:
            for row in csv.DictReader(f):
                country = row.get("CountryCode", "")
                chem = row.get("Chemical", "PM2.5")
                val = row.get("Value", "")
                if country and val:
                    try:
                        exposure[country][chem].append(float(val))
                    except ValueError:
                        pass

    country_means = {}
    for country, chems in exposure.items():
        country_means[country] = {c: np.mean(vals) for c, vals in chems.items()}
    return country_means


def load_mortality_data():
    mortality = defaultdict(list)

    ecis_path = ENV_DATA / "ecis_final.csv"
    if ecis_path.exists():
        with open(ecis_path, encoding="utf-8") as f:
            for row in csv.DictReader(f):
                country = row.get("CountryCode", "")
                rate = row.get("MortalityRate", "")
                if country and rate:
                    try:
                        val = float(rate)
                        if val > 0:
                            mortality[country].append(val)
                    except ValueError:
                        pass

    country_means = {c: np.mean(vals) for c, vals in mortality.items() if vals}
    return country_means


def analysis_geographic_correlation(exposure_data, mortality_data):
    print("\n  Computing geographic correlations ...")

    all_chems = set()
    for chems in exposure_data.values():
        all_chems.update(chems.keys())

    correlations = {}
    for chem in sorted(all_chems):
        countries, exp_vals, mort_vals = [], [], []
        for country in exposure_data:
            if country in mortality_data and chem in exposure_data[country]:
                countries.append(country)
                exp_vals.append(exposure_data[country][chem])
                mort_vals.append(mortality_data[country])

        if len(countries) >= 5:
            exp_arr = np.array(exp_vals)
            mort_arr = np.array(mort_vals)
            corr = np.corrcoef(exp_arr, mort_arr)[0, 1]
            chem_name = CHEMICAL_NAMES.get(chem, chem)
            correlations[chem] = {
                "chemical_name": chem_name,
                "pearson_r": round(float(corr), 4),
                "n_countries": len(countries),
                "countries": countries,
                "exposure_values": [round(v, 2) for v in exp_vals],
                "mortality_values": [round(v, 2) for v in mort_vals],
            }
            print(f"    {chem_name:30s}  r={corr:+.4f}  (n={len(countries)})")

    return correlations

def analysis_risk_regions(exposure_data, mortality_data):
    print("\n  Identifying high-risk regions ...")

    pm25_key = None
    for country_chems in exposure_data.values():
        for c in country_chems:
            if "PM2.5" in str(c) or "2.5" in str(c) or "PM2" in str(c) or c == "C5890534":
                pm25_key = c
                break
        if pm25_key:
            break

    if not pm25_key:
        print("    No PM2.5 data found")
        return []

    regions = []
    for country in exposure_data:
        if country in mortality_data and pm25_key in exposure_data[country]:
            regions.append({
                "country": country,
                "pm25_exposure": exposure_data[country][pm25_key],
                "mortality_rate": mortality_data[country],
            })

    if not regions:
        return []

    exp_vals = [r["pm25_exposure"] for r in regions]
    mort_vals = [r["mortality_rate"] for r in regions]
    exp_median = np.median(exp_vals)
    mort_median = np.median(mort_vals)

    for r in regions:
        r["high_exposure"] = r["pm25_exposure"] > exp_median
        r["high_mortality"] = r["mortality_rate"] > mort_median
        if r["high_exposure"] and r["high_mortality"]:
            r["risk_category"] = "High Risk"
        elif r["high_exposure"]:
            r["risk_category"] = "High Exposure"
        elif r["high_mortality"]:
            r["risk_category"] = "High Mortality"
        else:
            r["risk_category"] = "Low Risk"

    regions.sort(key=lambda x: -(x["pm25_exposure"] + x["mortality_rate"]))
    return regions

def fig_embedding_similarity(similarities, fig_dir):
    fig_dir.mkdir(parents=True, exist_ok=True)

    chems = sorted(set(s["chemical"] for s in similarities))
    diseases = sorted(set(s["disease"] for s in similarities))

    if len(diseases) > 15:
        top_diseases = set()
        for s in similarities[:50]:
            top_diseases.add(s["disease"])
        diseases = sorted(top_diseases)

    matrix = np.zeros((len(chems), len(diseases)))
    chem_idx = {c: i for i, c in enumerate(chems)}
    dis_idx = {d: i for i, d in enumerate(diseases)}

    for s in similarities:
        ci = chem_idx.get(s["chemical"])
        di = dis_idx.get(s["disease"])
        if ci is not None and di is not None:
            matrix[ci, di] = s["cosine_similarity"]

    fig, ax = plt.subplots(figsize=(max(8, len(diseases) * 0.6), max(4, len(chems) * 0.5)))
    im = ax.imshow(matrix, cmap="RdYlBu_r", aspect="auto", vmin=-1, vmax=1)

    ax.set_xticks(range(len(diseases)))
    ax.set_xticklabels(diseases, rotation=45, ha="right", fontsize=7)
    ax.set_yticks(range(len(chems)))
    ax.set_yticklabels(chems, fontsize=8)

    for i in range(len(chems)):
        for j in range(len(diseases)):
            v = matrix[i, j]
            color = "black" if abs(v) < 0.5 else "white"
            ax.text(j, i, f"{v:.2f}", ha="center", va="center", fontsize=6, color=color)

    plt.colorbar(im, ax=ax, label="Cosine similarity", shrink=0.8)
    ax.set_title("R-GCN embedding similarity: air pollutants vs lung cancer types",
                 fontsize=11, fontweight="bold")
    plt.tight_layout()
    fig.savefig(fig_dir / "crossdomain_embedding_similarity.png", dpi=300, bbox_inches="tight")
    plt.close(fig)
    print(f"  -> figs/crossdomain_embedding_similarity.png")


def fig_geographic_correlation(correlations, fig_dir):
    fig_dir.mkdir(parents=True, exist_ok=True)
    if not correlations:
        return

    sorted_chems = sorted(correlations.keys(), key=lambda c: abs(correlations[c]["pearson_r"]), reverse=True)
    n_plots = min(3, len(sorted_chems))

    fig, axes = plt.subplots(1, n_plots, figsize=(6 * n_plots, 5))
    if n_plots == 1:
        axes = [axes]

    for ax, chem in zip(axes, sorted_chems[:n_plots]):
        data = correlations[chem]
        exp = np.array(data["exposure_values"])
        mort = np.array(data["mortality_values"])
        r = data["pearson_r"]

        ax.scatter(exp, mort, alpha=0.6, s=40, c="#c44e52", edgecolors="white", linewidth=0.5)
        for i, country in enumerate(data["countries"]):
            ax.annotate(country, (exp[i], mort[i]), fontsize=6, alpha=0.7,
                        xytext=(3, 3), textcoords="offset points")

        if len(exp) > 1:
            z = np.polyfit(exp, mort, 1)
            p = np.poly1d(z)
            x_line = np.linspace(exp.min(), exp.max(), 100)
            ax.plot(x_line, p(x_line), "--", color="#4c72b0", alpha=0.7, linewidth=1.5)

        ax.set_xlabel(f"{data['chemical_name']} exposure", fontsize=10)
        ax.set_ylabel("Lung cancer mortality rate", fontsize=10)
        ax.set_title(f"{data['chemical_name']}\nr = {r:+.3f} (n={data['n_countries']})",
                     fontsize=11, fontweight="bold")
        ax.grid(True, alpha=0.2)
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)

    plt.suptitle("Air pollutant exposure vs lung cancer mortality by country",
                 fontsize=13, fontweight="bold", y=1.02)
    plt.tight_layout()
    fig.savefig(fig_dir / "crossdomain_geographic_correlation.png", dpi=300, bbox_inches="tight")
    plt.close(fig)
    print(f"  -> figs/crossdomain_geographic_correlation.png")


def fig_correlation_summary(correlations, fig_dir):
    fig_dir.mkdir(parents=True, exist_ok=True)
    if not correlations:
        return

    names = [correlations[c]["chemical_name"]
             for c in sorted(correlations.keys(), key=lambda c: correlations[c]["pearson_r"])]
    vals = [correlations[c]["pearson_r"]
            for c in sorted(correlations.keys(), key=lambda c: correlations[c]["pearson_r"])]
    colors = ["#c44e52" if v > 0 else "#4c72b0" for v in vals]

    fig, ax = plt.subplots(figsize=(8, max(3, len(names) * 0.4)))
    bars = ax.barh(names, vals, color=colors, alpha=0.85, edgecolor="white", linewidth=0.5)
    ax.axvline(x=0, color="gray", linewidth=0.8)
    ax.set_xlabel("Pearson correlation (r)")
    ax.set_title("Correlation: pollutant exposure vs lung cancer mortality",
                 fontsize=12, fontweight="bold")

    for bar, v in zip(bars, vals):
        x_pos = bar.get_width() + 0.01 if v >= 0 else bar.get_width() - 0.01
        ha = "left" if v >= 0 else "right"
        ax.text(x_pos, bar.get_y() + bar.get_height() / 2, f"{v:+.3f}",
                va="center", ha=ha, fontsize=9, fontweight="bold")

    ax.set_xlim(min(vals) - 0.15, max(vals) + 0.15)
    ax.grid(True, alpha=0.2, axis="x")
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    plt.tight_layout()
    fig.savefig(fig_dir / "crossdomain_correlation_summary.png", dpi=300, bbox_inches="tight")
    plt.close(fig)
    print(f"  -> figs/crossdomain_correlation_summary.png")


def fig_risk_quadrant(risk_regions, fig_dir):
    fig_dir.mkdir(parents=True, exist_ok=True)
    if not risk_regions:
        return

    cat_colors = {
        "High Risk": "#c44e52", "High Exposure": "#dd8452",
        "High Mortality": "#4c72b0", "Low Risk": "#55a868",
    }

    fig, ax = plt.subplots(figsize=(10, 7))
    for r in risk_regions:
        color = cat_colors.get(r["risk_category"], "#888")
        ax.scatter(r["pm25_exposure"], r["mortality_rate"], c=color, s=60,
                   alpha=0.75, edgecolors="white", linewidth=0.5)
        ax.annotate(r["country"], (r["pm25_exposure"], r["mortality_rate"]),
                    fontsize=7, alpha=0.8, xytext=(4, 4), textcoords="offset points")

    exp_vals = [r["pm25_exposure"] for r in risk_regions]
    mort_vals = [r["mortality_rate"] for r in risk_regions]
    ax.axvline(x=np.median(exp_vals), color="gray", linestyle="--", alpha=0.4)
    ax.axhline(y=np.median(mort_vals), color="gray", linestyle="--", alpha=0.4)

    ax.text(max(exp_vals) * 0.95, max(mort_vals) * 0.95, "HIGH RISK",
            fontsize=10, color="#c44e52", ha="right", va="top", fontweight="bold", alpha=0.5)
    ax.text(min(exp_vals) * 1.05, min(mort_vals) * 1.05, "LOW RISK",
            fontsize=10, color="#55a868", ha="left", va="bottom", fontweight="bold", alpha=0.5)

    from matplotlib.lines import Line2D
    legend_elements = [
        Line2D([0], [0], marker="o", color="w", markerfacecolor=c, markersize=8, label=l)
        for l, c in cat_colors.items()
    ]
    ax.legend(handles=legend_elements, loc="upper left", fontsize=9)

    ax.set_xlabel("Mean PM2.5 exposure", fontsize=11)
    ax.set_ylabel("Mean lung cancer mortality rate", fontsize=11)
    ax.set_title("PM2.5 exposure vs lung cancer mortality: country risk quadrant",
                 fontsize=13, fontweight="bold")
    ax.grid(True, alpha=0.2)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    plt.tight_layout()
    fig.savefig(fig_dir / "crossdomain_risk_quadrant.png", dpi=300, bbox_inches="tight")
    plt.close(fig)
    print(f"  -> figs/crossdomain_risk_quadrant.png")

def main():
    print("=" * 70)
    print("07_chemical_disease_analysis.py")
    print("=" * 70)

    print("\n[1/5] Loading model and embeddings ...")
    z, node_offsets, node_id_maps, data = load_model_and_embeddings()

    print("\n[2/5] Embedding proximity analysis ...")
    similarities = analysis_embedding_proximity(z, node_offsets, node_id_maps)
    print(f"  Top 10 Chemical-Disease similarities:")
    for s in similarities[:10]:
        print(f"    {s['chemical']:30s} <-> {s['disease']:40s}  sim={s['cosine_similarity']:+.4f}")

    print("\n[3/5] Geographic correlation analysis ...")
    exposure_data = load_exposure_data()
    mortality_data = load_mortality_data()
    print(f"  Exposure data: {len(exposure_data)} countries")
    print(f"  Mortality data: {len(mortality_data)} countries")
    correlations = analysis_geographic_correlation(exposure_data, mortality_data)

    print("\n[4/5] Risk region profiling ...")
    risk_regions = analysis_risk_regions(exposure_data, mortality_data)
    high_risk = [r for r in risk_regions if r["risk_category"] == "High Risk"]
    print(f"  {len(high_risk)} high-risk countries (high exposure + high mortality):")
    for r in high_risk[:10]:
        print(f"    {r['country']:5s}  PM2.5={r['pm25_exposure']:.1f}  Mortality={r['mortality_rate']:.1f}")

    print("\n[5/5] Generating thesis figures ...")
    fig_embedding_similarity(similarities, FIG_DIR)
    if correlations:
        fig_geographic_correlation(correlations, FIG_DIR)
        fig_correlation_summary(correlations, FIG_DIR)
    if risk_regions:
        fig_risk_quadrant(risk_regions, FIG_DIR)

    analysis_results = {
        "embedding_similarities_top20": similarities[:20],
        "geographic_correlations": {
            k: {kk: vv for kk, vv in v.items() if kk != "countries"}
            for k, v in correlations.items()
        },
        "high_risk_countries": [
            {"country": r["country"], "pm25": r["pm25_exposure"], "mortality": r["mortality_rate"]}
            for r in high_risk
        ],
    }
    with open(PROCESSED_DIR / "chemical_disease_analysis.json", "w") as f:
        json.dump(analysis_results, f, indent=2)
    print(f"\n  -> chemical_disease_analysis.json")

    print("\n" + "=" * 70)
    print("Cross-domain analysis complete.")
    print("=" * 70)


if __name__ == "__main__":
    main()