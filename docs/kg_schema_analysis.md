# Knowledge Graph Schema Analysis

## Data Sources

| Layer | Source | TTL | Triples |
|-------|--------|-----|---------|
| Env | EEA | graph_EEA.ttl | 1,208,569 |
| Env | ECIS | graph_ECIS.ttl | 247,889 |
| Env | OECD | graph_OECD.ttl | 158,878 |
| Env | CDC | graph_CDC.ttl | 8,571 |
| Env | Shared | graph_shared.ttl | 41 |
| Bio | DisGeNET/COSMIC/DISEASES | gene_disease_assoc.ttl | 21,376 rows |
| Bio | DisGeNET/COSMIC | variant_disease.ttl | 812 rows |
| Bio | WikiPathways | disease_gene_pathway.ttl | 86,960 rows |
| Bio | WikiPathways | pathway_disease.ttl | 168 rows |
| Bio | MarkerDB/Exposome | biomarker_disease.ttl | 24 rows |
| Bio | Mitelman | disease_and_chromo_arr.ttl | 2,309 rows |
| Bio | Mitelman | disease_and_gene_fusions.ttl | 4,289 rows |

Env TTLs are RDF/Turtle. Bio TTLs are SPARQL ResultSet exports from Virtuoso.

## Node Types

| Type | Count | Layer | Source |
|------|-------|-------|--------|
| Disease | ~70 | Bridge | DisGeNET, ECIS |
| Gene | ~15,816 | Bio | DisGeNET, COSMIC, DISEASES |
| Variant | ~587,344 (812 in export) | Bio | DisGeNET, COSMIC |
| Pathway | 1,495 | Bio | WikiPathways |
| GeneProduct | 133 | Bio | WikiPathways |
| Metabolite | 15 | Bio | WikiPathways |
| Biomarker | 24 | Bio | MarkerDB |
| ChromoRearr | 1,993 | Bio | Mitelman |
| GeneFusion | 4,266 | Bio | Mitelman |
| Article | ~373,001 | Bio | PubMed |
| Country | ~171 | Env | EEA, OECD, ECIS |
| GeoPoliticalRegion | 3,143 | Env | EEA |
| GeographicRegion | 518 | Env | OECD |
| Chemical | ~15 | Env | EEA, OECD |
| CalendarYear | ~35 | Env | All |
| People | 66 | Env | CDC, ECIS |
| VitalStatistics | 33,488 | Env | ECIS, CDC |
| ChemicalLocationAssoc | 131,610 | Env | EEA, OECD |
| Population | 3,224 | Env | Eurostat, OECD |

## Edge Types

| Edge | Count | Attributes |
|------|-------|------------|
| Gene -> Disease (GDA) | 17,701 unique | gda_score |
| Variant -> Disease (VDA) | 812 (export) | dsi, dpi, consequence |
| Gene -> Pathway | 86,960 | - |
| Pathway -> Disease | 2 | - |
| Biomarker -> Disease | 24 | - |
| Disease -> ChromoRearr | 2,309 | type |
| Disease -> GeneFusion | 4,289 | - |
| GeneProduct -> Pathway | 133 | - |
| CLA -> Chemical | 131,610 | - |
| CLA -> Region | 131,610 | - |
| CLA -> CalendarYear | 131,610 | - |
| VitalStats -> People | 33,488 | - |
| VitalStats -> CalendarYear | 33,488 | - |
| VitalStats -> Region | 33,488 | - |
| Disease -> VitalStats | 33,488 | - |
| Region -> Country | 3,644 | - |

## Bridge Entity

Disease (C0242379 "Malignant neoplasm of lung") connects both layers. It appears in GDAs, VDAs, Biomarker associations, and ECIS VitalStatistics.

## GNN Design Decisions

**Excluded:** Article (evidence metadata), Source (provenance), Population (becomes node feature on Region).

**N-ary collapsing:** CLA and VitalStatistics are reification nodes. They get collapsed into direct typed edges with attributes (value, year, rate).

**Variant strategy:** Core graph excludes variants (587K nodes dominate the graph). Added as optional second experiment.

**Core graph estimate:** ~12K nodes, ~280K edges, 11 node types, 11 edge types.
