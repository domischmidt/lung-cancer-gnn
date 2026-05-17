import csv
from pathlib import Path
from collections import defaultdict

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
OUTPUT_DIR = REPO_ROOT / "gnn" / "data" / "processed" / "env_predictions"
FIG_DIR = OUTPUT_DIR / "figs"

COUNTRY_LABEL_FIX = {
    "LI": "Liechtenstein",
}

def fix_label(label):
    return COUNTRY_LABEL_FIX.get(label, label)


def fix_temporal_trend():
    """Fix the overall temporal trend figure."""
    print("[1/2] Fixing temporal_trend.png ...")

    data = []
    with open(OUTPUT_DIR / "temporal_trends.csv") as f:
        reader = csv.DictReader(f)
        for row in reader:
            year = int(row["year"])
            if year >= 1990:
                data.append({
                    "year": year,
                    "mean_score": float(row["mean_score"]),
                    "max_score": float(row["max_score"]),
                    "n_vitalstats": int(row["n_vitalstats"]),
                })

    data.sort(key=lambda x: x["year"])

    years = [d["year"] for d in data]
    means = [d["mean_score"] for d in data]

    fig, ax = plt.subplots(figsize=(12, 5))
    ax.plot(years, means, "o-", color="#c44e52", linewidth=2, markersize=5)
    ax.set_xlabel("Year", fontsize=11)
    ax.set_ylabel("Mean predicted Disease-VitalStatistics score", fontsize=11)
    ax.set_title("Temporal trend: predicted lung cancer association by year",
                 fontsize=12, fontweight="bold")
    ax.grid(True, alpha=0.3)

    ax.set_xticks([y for y in years if y % 2 == 0])
    ax.set_xticklabels([str(y) for y in years if y % 2 == 0], rotation=45, ha="right")

    ax.set_ylim(0.55, 0.70)

    plt.tight_layout()
    fig.savefig(FIG_DIR / "temporal_trend.png", dpi=200, bbox_inches="tight")
    plt.close(fig)
    print(f"  -> {FIG_DIR / 'temporal_trend.png'}")


def fix_temporal_trend_by_country():
    """Fix the per-country temporal trend: LI -> Liechtenstein, sort X-axis."""
    print("[2/2] Fixing temporal_trend_by_country.png ...")

    country_year_data = defaultdict(list)
    with open(OUTPUT_DIR / "temporal_trends_by_country.csv") as f:
        reader = csv.DictReader(f)
        for row in reader:
            country = fix_label(row["country"])
            year = int(row["year"])
            mean_score = float(row["mean_score"])
            country_year_data[country].append((year, mean_score))

    for country in country_year_data:
        country_year_data[country].sort(key=lambda x: x[0])

    country_means = {}
    for country, points in country_year_data.items():
        country_means[country] = np.mean([s for _, s in points])
    top_countries = sorted(country_means.items(), key=lambda x: x[1], reverse=True)

    fig, ax = plt.subplots(figsize=(12, 6))
    colors = ["#c44e52", "#4c72b0", "#55a868", "#8172b2", "#ccb974"]

    for i, (country, _) in enumerate(top_countries):
        points = country_year_data[country]
        if points:
            yrs, scores = zip(*points)
            ax.plot(yrs, scores, "o-", color=colors[i % len(colors)],
                    linewidth=1.5, markersize=4, label=country, alpha=0.8)

    ax.set_xlabel("Year", fontsize=11)
    ax.set_ylabel("Mean predicted score", fontsize=11)
    ax.set_title("Predicted lung cancer association by country and year",
                 fontsize=12, fontweight="bold")
    ax.legend(fontsize=9, loc="lower right")
    ax.grid(True, alpha=0.3)

    all_years = sorted(set(y for pts in country_year_data.values() for y, _ in pts))
    ax.set_xticks([y for y in all_years if y % 2 == 0])
    ax.set_xticklabels([str(y) for y in all_years if y % 2 == 0], rotation=45, ha="right")

    ax.set_ylim(0.55, 0.72)

    plt.tight_layout()
    fig.savefig(FIG_DIR / "temporal_trend_by_country.png", dpi=200, bbox_inches="tight")
    plt.close(fig)
    print(f"  -> {FIG_DIR / 'temporal_trend_by_country.png'}")

    out_path = OUTPUT_DIR / "temporal_trends_by_country.csv"
    rows = []
    with open(out_path) as f:
        reader = csv.DictReader(f)
        for row in reader:
            row["country"] = fix_label(row["country"])
            rows.append(row)

    with open(out_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=rows[0].keys())
        w.writeheader()
        w.writerows(rows)
    print(f"  -> Also fixed 'LI' -> 'Liechtenstein' in {out_path.name}")


if __name__ == "__main__":
    FIG_DIR.mkdir(parents=True, exist_ok=True)
    fix_temporal_trend()
    fix_temporal_trend_by_country()
    print("\nDone. Both figures regenerated.")
