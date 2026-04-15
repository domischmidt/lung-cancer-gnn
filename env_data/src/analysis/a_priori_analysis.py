import pandas as pd
import numpy as np
from scipy import stats
import os

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "data", "processed")

print("Loading data...")
eea = pd.read_csv(os.path.join(DATA_DIR, "eea_final.csv"))
oecd = pd.read_csv(os.path.join(DATA_DIR, "oecd_exposure_final.csv"))
ecis = pd.read_csv(os.path.join(DATA_DIR, "ecis_final.csv"))

print(f"  EEA:  {len(eea):,} rows")
print(f"  OECD: {len(oecd):,} rows")
print(f"  ECIS: {len(ecis):,} rows")



print("\n" + "=" * 70)
print("STEP 1: OVERLAP ANALYSIS")
print("=" * 70)

eea_cc = set(eea["CountryCode"].unique())
oecd_cc = set(oecd["CountryCode"].unique())
ecis_cc = set(ecis["CountryCode"].unique())

eea_years = set(eea["Year"].unique())
oecd_years = set(oecd["Year"].unique())
ecis_years = set(ecis["Year"].unique())

print(f"\nCountries:")
print(f"  EEA:  {len(eea_cc)}")
print(f"  OECD: {len(oecd_cc)}")
print(f"  ECIS: {len(ecis_cc)}")
print(f"  OECD ∩ ECIS: {len(oecd_cc & ecis_cc)} → {sorted(oecd_cc & ecis_cc)}")
print(f"  EEA  ∩ ECIS: {len(eea_cc & ecis_cc)} → {sorted(eea_cc & ecis_cc)}")

print(f"\nYears:")
print(f"  EEA:  {min(eea_years)}-{max(eea_years)}")
print(f"  OECD: {min(oecd_years)}-{max(oecd_years)}")
print(f"  ECIS: {min(ecis_years)}-{max(ecis_years)}")
print(f"  OECD ∩ ECIS year overlap: {sorted(oecd_years & ecis_years)}")

ecis_national = ecis[ecis["Registry"] == ecis["Country"]]
ecis_regional = ecis[ecis["Registry"] != ecis["Country"]]
print(f"\nECIS breakdown:")
print(f"  National records: {len(ecis_national):,} ({ecis_national['CountryCode'].nunique()} countries)")
print(f"  Regional records: {len(ecis_regional):,} ({ecis_regional['Registry'].nunique()} registries)")


print("\n" + "=" * 70)
print("STEP 2: COUNTRY-LEVEL CORRELATIONS (OECD × ECIS)")
print("=" * 70)

def correlate(env_values, cancer_values, label):
    """Compute Spearman correlation between two series aligned by country."""
    common = env_values.index.intersection(cancer_values.index)
    if len(common) < 5:
        print(f"  {label}: insufficient overlap (n={len(common)})")
        return None, None, None
    sp, p = stats.spearmanr(env_values[common], cancer_values[common])
    sig = "**" if p < 0.01 else " *" if p < 0.05 else "  "
    print(f"  {label}: ρ = {sp:+.3f}  p = {p:.4f} {sig}  n = {len(common)}")
    return sp, p, len(common)


ecis_nat = ecis[ecis["Registry"] == ecis["Country"]].copy()

for period_label, year_min, year_max in [
    ("2015-2019", 2015, 2019),
    ("2020-2024", 2020, 2024),
    ("All years avg", 2000, 2024),
]:
    ecis_sub = ecis_nat[(ecis_nat["Year"] >= year_min) & (ecis_nat["Year"] <= year_max)]
    if len(ecis_sub) == 0:
        continue

    ecis_inc = ecis_sub.groupby("CountryCode")["Incidence"].mean().dropna()
    ecis_mort = ecis_sub.groupby("CountryCode")["MortalityRate"].mean().dropna()

    print(f"\n── Period: {period_label} ──")

    for chem in ["PM2.5", "PM10", "NO2"]:
        oecd_sub = oecd[(oecd["Chemical"] == chem) &
                        (oecd["Year"] >= year_min) &
                        (oecd["Year"] <= year_max)]
        if len(oecd_sub) == 0:
            continue
        env_avg = oecd_sub.groupby("CountryCode")["Value"].mean()

        correlate(env_avg, ecis_inc, f"{chem:6s} vs Incidence  ({period_label})")
        correlate(env_avg, ecis_mort, f"{chem:6s} vs Mortality  ({period_label})")


print("\n" + "=" * 70)
print("STEP 3: LAG ANALYSIS (does delayed exposure improve correlation?)")
print("=" * 70)
print("\nIdea: Cancer has a 10-20 year latency. Correlating PM2.5 from")
print("2000-2005 with cancer rates from 2015-2020 may show stronger signal.\n")

ecis_cancer_recent = ecis_nat[ecis_nat["Year"] >= 2015]
cancer_inc = ecis_cancer_recent.groupby("CountryCode")["Incidence"].mean().dropna()
cancer_mort = ecis_cancer_recent.groupby("CountryCode")["MortalityRate"].mean().dropna()

print(f"Cancer reference period: 2015+ (national averages)")
print(f"  Countries with incidence data: {len(cancer_inc)}")
print(f"  Countries with mortality data: {len(cancer_mort)}")

lags = [
    ("No lag (2015-2023)", 2015, 2023),
    ("5yr lag (2010-2014)", 2010, 2014),
    ("10yr lag (2005-2009)", 2005, 2009),
    ("15yr lag (2000-2004)", 2000, 2004),
    ("20yr lag (1995-1999)", 1995, 1999),
]

print(f"\n{'Lag period':<25s} {'ρ (Inc)':>8s} {'p (Inc)':>9s} {'ρ (Mort)':>9s} {'p (Mort)':>10s} {'n':>4s}")
print("-" * 70)

for label, y_min, y_max in lags:
    pm25 = oecd[(oecd["Chemical"] == "PM2.5") &
                (oecd["Year"] >= y_min) &
                (oecd["Year"] <= y_max)]
    if len(pm25) == 0:
        print(f"{label:<25s}  (no OECD data)")
        continue

    env_avg = pm25.groupby("CountryCode")["Value"].mean()

    common_i = env_avg.index.intersection(cancer_inc.index)
    common_m = env_avg.index.intersection(cancer_mort.index)

    if len(common_i) >= 5:
        sp_i, p_i = stats.spearmanr(env_avg[common_i], cancer_inc[common_i])
        sp_m, p_m = stats.spearmanr(env_avg[common_m], cancer_mort[common_m])
        sig_i = "**" if p_i < 0.01 else " *" if p_i < 0.05 else "  "
        sig_m = "**" if p_m < 0.01 else " *" if p_m < 0.05 else "  "
        print(f"{label:<25s} {sp_i:+.3f}{sig_i} {p_i:.4f}   {sp_m:+.3f}{sig_m}  {p_m:.4f}   {len(common_i):>3d}")
    else:
        print(f"{label:<25s}  (n={len(common_i)}, too few)")


print("\n" + "=" * 70)
print("STEP 4: REGIONAL VARIANCE (within-country patterns)")
print("=" * 70)
print("\nFor countries with both OECD TL2 regions and ECIS registries:")
print("Do regions with higher pollution also have higher cancer rates?\n")

oecd_pm25 = oecd[(oecd["Chemical"] == "PM2.5") & (oecd["Year"] >= 2015)]
oecd_regional = oecd_pm25.groupby(["CountryCode", "RegionName"])["Value"].mean().reset_index()

ecis_reg = ecis_regional.copy()
ecis_reg_avg = ecis_reg.groupby(["CountryCode", "Registry"]).agg({
    "Incidence": "mean",
    "MortalityRate": "mean"
}).dropna(subset=["Incidence"]).reset_index()

countries_with_both = (
    set(oecd_regional["CountryCode"].unique()) &
    set(ecis_reg_avg["CountryCode"].unique())
)

print(f"Countries with both OECD regions and ECIS registries: {len(countries_with_both)}")
print(f"  → {sorted(countries_with_both)}")

print(f"\n{'Country':<8s} {'OECD Regions':>12s} {'PM2.5 range':>14s} {'ECIS Registries':>16s} {'Inc range':>12s}")
print("-" * 65)

for cc in sorted(countries_with_both):
    oecd_r = oecd_regional[oecd_regional["CountryCode"] == cc]
    ecis_r = ecis_reg_avg[ecis_reg_avg["CountryCode"] == cc]

    pm_min, pm_max = oecd_r["Value"].min(), oecd_r["Value"].max()
    inc_min, inc_max = ecis_r["Incidence"].min(), ecis_r["Incidence"].max()

    print(f"{cc:<8s} {len(oecd_r):>12d} {pm_min:>6.1f}-{pm_max:.1f} {len(ecis_r):>16d} {inc_min:>5.1f}-{inc_max:.1f}")


print("\n" + "=" * 70)
print("SUMMARY & INTERPRETATION")
print("=" * 70)
print("""
What to look for in the results above:

STEP 2 (Country-level correlations):
  - ρ > 0.3 with p < 0.05 → meaningful signal exists
  - Consistent across time periods → robust pattern
  - PM2.5 strongest → aligns with medical literature

STEP 3 (Lag analysis):
  - If ρ INCREASES with lag → evidence of causal latency
    (exposure today → cancer 10-20 years later)
  - If ρ DECREASES with lag → association is contemporaneous,
    may reflect confounders (e.g. industrialization)
  - If ρ stays stable → unclear directionality

STEP 4 (Regional variance):
  - Large PM2.5 range within a country → potential for
    within-country analysis (controls for national policy)
  - If high-PM2.5 regions also have high cancer rates
    within the same country → strong evidence
  - Hard to test directly because OECD regions ≠ ECIS registries
    (different granularity and naming)

Implications for method choice:
  - Strong country-level correlations + interpretable patterns
    → Subgroup Discovery is a good fit
  - Biological bridges needed (Chemical→Gene→Disease paths)
    → GNN has advantage over SD
  - Sparse graph with few cross-domain connections
    → GNN may struggle, SD is safer
""")


if __name__ == "__main__":
    pass
