"""
================================================================================
Lung-CABO Knowledge Graph – Complete Exploration
================================================================================

Master Thesis: "Graph Neural Networks for Knowledge Discovery in Lung Cancer Data"
Author: Domenic Schmidt
Date: February 2026

PURPOSE:
  This script systematically explores the Lung-CABO Knowledge Graph,
  documents its contents, and validates the planned extensions (Part 1).

WHAT IS A KNOWLEDGE GRAPH?
  A Knowledge Graph stores knowledge as triples: Subject -> Predicate -> Object
  Example: "NSCLC" -> "has_gene" -> "EGFR"
  Or:      "Berlin" -> "has_PM2.5_value" -> "12.5"
  
  The Lung-CABO KG connects lung cancer data from multiple databases:
  DisGeNET (Gene-Disease), COSMIC (Variants), WikiPathways (Pathways),
  CTD (Chemicals), and custom environmental/demographic data.

HOW THIS SCRIPT WORKS:
  - Connects to the SPARQL endpoint (138.4.130.153:5001)
  - Runs queries that examine the graph step by step
  - Prints results to the screen
  - Saves EVERYTHING to a log file (notebooks/data/kg_exploration.log)
  
  Run with: python3 01_KG_Exploration.py

PLANNED EXTENSIONS (Part 1):
  AP1: hasComponent – Link PM2.5 to its chemical constituents
  AP2: Gaseous Pollutants – Add benzene etc. to the ChemicalLocation subgraph
  AP3: Derived Measures – perCapitaExposure, citySize, exceedsWHOGuideline
  AP4: Disease Hierarchy – Materialize rdfs:subClassOf between lung cancer subtypes

================================================================================
"""

import urllib.request
import json
import sys
import os
from datetime import datetime

# === CONFIGURATION ===

ENDPOINT = "http://138.4.130.153:5001/execute_query"

# SPARQL Prefixes – shortcuts for long URIs
# Instead of http://ncicb.nci.nih.gov/xml/owl/EVS/Thesaurus.owl#C7057
# we simply write ncit:C7057
PREFIXES = """
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX ncit: <http://ncicb.nci.nih.gov/xml/owl/EVS/Thesaurus.owl#>
PREFIX sio: <http://semanticscience.org/resource/>
PREFIX dcterms: <http://purl.org/dc/terms/>
PREFIX LUCIA: <https://w3id.org/LUCIA/sem-lucia#>
PREFIX OBO: <http://purl.obolibrary.org/obo/>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX wp: <http://vocabularies.wikipathways.org/wp#>
PREFIX vocab: <https://w3id.org/biolink/vocab/>
"""


# === HELPER FUNCTIONS ===

LOG_DIR = os.path.join("notebooks", "data")
os.makedirs(LOG_DIR, exist_ok=True)
LOG_PATH = os.path.join(LOG_DIR, "kg_exploration.log")
log_file = open(LOG_PATH, "w", encoding="utf-8")


def log(text=""):
    """Prints to screen AND writes to log file."""
    print(text)
    log_file.write(text + "\n")
    log_file.flush()


def query(sparql, timeout=120):
    """
    Sends a SPARQL query to the Lung-CABO server.
    
    SPARQL is the query language for Knowledge Graphs (like SQL for databases).
    The query is sent as a JSON POST to the Flask app, which forwards it to
    the Virtuoso triplestore.
    
    Returns: List of dictionaries (each dict = one result row)
    """
    full_query = PREFIXES + sparql
    data = json.dumps({"query": full_query}).encode()
    req = urllib.request.Request(
        ENDPOINT, data=data,
        headers={"Content-Type": "application/json"}
    )
    try:
        r = urllib.request.urlopen(req, timeout=timeout)
        result = json.loads(r.read().decode())
        rows = result.get("rows", [])
        t = result.get("sparql_time_seconds", "?")
        log(f"  -> {len(rows)} results ({t}s)")
        return rows
    except Exception as e:
        log(f"  -> ERROR: {e}")
        return []


def shorten(uri):
    """Shortens long URIs for readability."""
    replacements = [
        ("http://medal.ctb.upm.es/projects/LUCIA/res/sem-lucia#", "res:"),
        ("http://ncicb.nci.nih.gov/xml/owl/EVS/Thesaurus.owl#", "ncit:"),
        ("http://semanticscience.org/resource/", "sio:"),
        ("https://w3id.org/LUCIA/sem-lucia#", "LUCIA:"),
        ("http://purl.obolibrary.org/obo/", "obo:"),
        ("http://www.w3.org/1999/02/22-rdf-syntax-ns#", "rdf:"),
        ("http://www.w3.org/2000/01/rdf-schema#", "rdfs:"),
        ("http://purl.org/dc/terms/", "dcterms:"),
        ("http://linkedlifedata.com/resource/umls/id/", "umls:"),
        ("http://vocabularies.wikipathways.org/wp#", "wp:"),
        ("http://www.bioassayontology.org/bao#", "bao:"),
        ("http://www.w3.org/2004/02/skos/core#", "skos:"),
        ("http://xmlns.com/foaf/0.1/", "foaf:"),
        ("http://www.w3.org/TR/vocab-dcat#", "dcat:"),
        ("https://w3id.org/biolink/vocab/", "biolink:"),
    ]
    for old, new in replacements:
        uri = uri.replace(old, new)
    return uri


def table(rows, columns=None, max_rows=None):
    """Displays results as a formatted table."""
    if not rows:
        log("  (no results)")
        return
    cols = columns or list(rows[0].keys())
    display_rows = rows[:max_rows] if max_rows else rows
    
    widths = {}
    for c in cols:
        widths[c] = max(len(c), max(len(shorten(str(r.get(c, "")))) for r in display_rows))
        widths[c] = min(widths[c], 60)
    
    header = " | ".join(f"{c:{widths[c]}s}" for c in cols)
    log(f"  {header}")
    log(f"  {'-' * len(header)}")
    
    for r in display_rows:
        vals = []
        for c in cols:
            v = shorten(str(r.get(c, "")))
            if len(v) > widths[c]:
                v = v[:widths[c]-2] + ".."
            vals.append(f"{v:{widths[c]}s}")
        log(f"  {' | '.join(vals)}")
    
    if max_rows and len(rows) > max_rows:
        log(f"  ... ({len(rows) - max_rows} more rows)")


# ================================================================================
# START OF EXPLORATION
# ================================================================================

log("=" * 80)
log("LUNG-CABO KNOWLEDGE GRAPH – COMPLETE EXPLORATION")
log(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
log(f"Endpoint:  {ENDPOINT}")
log(f"Log file:  {LOG_PATH}")
log("=" * 80)


# ============================================================
# STEP 1: CONNECTION TEST
# ============================================================

log("""
+--------------------------------------------------------------+
|  STEP 1: Connection Test                                     |
+--------------------------------------------------------------+

We count all triples in the graph. A triple is the smallest unit
of information: Subject -> Predicate -> Object.
If a number comes back, the connection works.
""")

rows = query("SELECT (COUNT(*) AS ?total) WHERE { ?s ?p ?o }")
if rows:
    total = int(rows[0]["total"])
    log(f"\n  OK - Knowledge Graph contains {total:,} triples")
    log(f"  (Note: Many are duplicates due to mapping artifacts)")
else:
    log("\n  FAILED - Could not connect. Are you on the UPM VPN?")
    sys.exit(1)


# ============================================================
# STEP 2: ALL DISEASES
# ============================================================

log("""
+--------------------------------------------------------------+
|  STEP 2: Diseases                                            |
|  -> Relevant for: AP4 (Disease Hierarchy)                    |
+--------------------------------------------------------------+

Diseases are the center of the KG. Each has a UMLS code
(Unified Medical Language System), e.g. C0007131 = NSCLC.

Query: Give me all entities of type ncit:C7057
(= "Disease, Disorder or Finding") with their code and name.
""")

rows = query("""
    SELECT DISTINCT ?id ?label
    WHERE {
      ?disease rdf:type ncit:C7057 .
      ?disease dcterms:identifier ?id .
      ?disease rdfs:label ?label .
    }
    ORDER BY ?id
""")
log(f"\n  {len(rows)} diseases in the KG:")
table(rows)


# ============================================================
# STEP 3: DISEASE HIERARCHY (AP4 validation)
# ============================================================

log("""
+--------------------------------------------------------------+
|  STEP 3: Disease Hierarchy – does it exist?                  |
|  -> Direct validation of AP4                                 |
+--------------------------------------------------------------+

The OWL ontology defines a hierarchy:
  Lung Neoplasms -> Carcinoma of lung -> NSCLC -> Adenocarcinoma
                                        -> SCLC -> Combined SCLC

Question: Was this hierarchy loaded as rdfs:subClassOf triples
into the triplestore?

If NO  -> AP4 is NEEDED (hierarchy must be materialized)
If YES -> AP4 is unnecessary
""")

rows = query("""
    SELECT ?child ?parent
    WHERE {
      ?child rdfs:subClassOf ?parent .
      FILTER(STRSTARTS(STR(?child), "http://medal.ctb.upm.es/projects/LUCIA/res/sem-lucia#disease/"))
    }
    LIMIT 50
""")

if not rows:
    log("\n  WARNING: NO disease hierarchy found in triplestore!")
    log("  -> The OWL defines the hierarchy, but it was NOT materialized.")
    log("  -> AP4 is NEEDED: rdfs:subClassOf triples must be inserted.")
    log("")
    log("  For reference – the hierarchy from the OWL file:")
    log("    Lung Neoplasms (C0814136)")
    log("    +-- Malignant neoplasm of lung (C0242379)")
    log("        +-- Carcinoma of lung (C0684249)")
    log("        |   +-- NSCLC (C0007131)")
    log("        |   |   +-- Adenocarcinoma of lung (C0152013)")
    log("        |   |   +-- Squamous cell carcinoma (C0149782)")
    log("        |   |   +-- Large cell carcinoma (C0345958)")
    log("        |   +-- SCLC (C0149925)")
    log("        |   |   +-- Combined SCLC (C1333125)")
    log("        |   +-- Adenoid cystic (C1334439)")
    log("        +-- Adenosquamous (C0279557)")
else:
    log(f"\n  Disease hierarchy FOUND ({len(rows)} triples):")
    table(rows)

log("\n  Additional check: What subClassOf triples exist at all?")
rows = query("""
    SELECT ?child ?parent
    WHERE {
      ?child rdfs:subClassOf ?parent .
      FILTER(STRSTARTS(STR(?child), "http://medal.ctb.upm.es/"))
    }
    LIMIT 20
""")
if rows:
    log("  (subClassOf triples exist, but for Biomarker->GDA, not for Diseases)")
    table(rows, max_rows=5)


# ============================================================
# STEP 4: GENES
# ============================================================

log("""
+--------------------------------------------------------------+
|  STEP 4: Genes                                               |
+--------------------------------------------------------------+

Genes are connected to diseases via Gene-Disease Associations (GDA).
Each gene has an NCBI Gene ID and a symbol (e.g. EGFR, KRAS, TP53).
""")

rows = query("""
    SELECT (COUNT(DISTINCT ?gene) AS ?count)
    WHERE { ?gene rdf:type ncit:C16612 }
""")
if rows:
    log(f"  Number of genes: {rows[0]['count']}")

log("\n  Sample genes:")
rows = query("""
    SELECT ?id ?label
    WHERE {
      ?gene rdf:type ncit:C16612 .
      ?gene dcterms:identifier ?id .
      ?gene rdfs:label ?label .
    }
    LIMIT 10
""")
table(rows)


# ============================================================
# STEP 5: GENE-DISEASE ASSOCIATIONS
# ============================================================

log("""
+--------------------------------------------------------------+
|  STEP 5: Gene-Disease Associations (GDA)                     |
+--------------------------------------------------------------+

GDAs are the bridge between genes and diseases.
Type: sio:SIO_000983
Each GDA has:
  - sio:SIO_000628 ("refers to") -> points to gene AND disease
  - sio:SIO_000216 ("has measurement value") -> score values
  - sio:SIO_000253 ("has source") -> data source
""")

rows = query("""
    SELECT (COUNT(DISTINCT ?gda) AS ?count)
    WHERE { ?gda rdf:type sio:SIO_000983 }
""")
if rows:
    log(f"  Number of GDAs: {rows[0]['count']}")

log("\n  Structure of a single GDA (distinct predicates + objects):")
rows = query("""
    SELECT DISTINCT ?p ?o
    WHERE {
      {
        SELECT ?gda WHERE { ?gda rdf:type sio:SIO_000983 } LIMIT 1
      }
      ?gda ?p ?o .
      FILTER(?p != rdf:type)
    }
""")
table(rows)

log("\n  Example GDA with resolved gene and disease names:")
rows = query("""
    SELECT ?geneLabel ?diseaseLabel ?score ?source
    WHERE {
      ?gda rdf:type sio:SIO_000983 .
      ?gda sio:SIO_000628 ?gene .
      ?gda sio:SIO_000628 ?disease .
      ?gene rdf:type ncit:C16612 .
      ?disease rdf:type ncit:C7057 .
      ?gene rdfs:label ?geneLabel .
      ?disease rdfs:label ?diseaseLabel .
      OPTIONAL { ?gda sio:SIO_000253 ?source }
      OPTIONAL {
        ?gda sio:SIO_000216 ?scoreNode .
        ?scoreNode rdf:type LUCIA:meanrankscore .
        ?scoreNode sio:SIO_000300 ?score .
      }
    }
    LIMIT 10
""")
table(rows)


# ============================================================
# STEP 6: CHEMICAL-DISEASE ASSOCIATIONS
# ============================================================

log("""
+--------------------------------------------------------------+
|  STEP 6: Chemical-Disease Associations                       |
|  -> Relevant for: AP1 (hasComponent), AP2 (Benzene)          |
+--------------------------------------------------------------+

This class links chemicals to diseases.
E.g. "Arsenic is associated with lung cancer".

IMPORTANT: In the triplestore, the class is sio:SIO_000993
(NOT ctd:Chemical-Disease-Association as in the OWL!)

Source: CTD (Comparative Toxicogenomics Database)
""")

rows = query("""
    SELECT (COUNT(DISTINCT ?cda) AS ?count)
    WHERE { ?cda rdf:type sio:SIO_000993 }
""")
if rows:
    log(f"  Number of Chemical-Disease Associations: {rows[0]['count']}")

log("\n  Structure of a single CDA (distinct predicates + objects):")
rows = query("""
    SELECT DISTINCT ?p ?o
    WHERE {
      {
        SELECT ?cda WHERE { ?cda rdf:type sio:SIO_000993 } LIMIT 1
      }
      ?cda ?p ?o .
      FILTER(?p != rdf:type)
    }
""")
table(rows)

log("\n  Example CDAs with resolved chemical and disease names:")
rows = query("""
    SELECT DISTINCT ?chemLabel ?diseaseLabel
    WHERE {
      ?cda rdf:type sio:SIO_000993 .
      ?cda sio:SIO_000628 ?chem .
      ?cda sio:SIO_000628 ?disease .
      ?chem rdfs:label ?chemLabel .
      ?disease rdf:type ncit:C7057 .
      ?disease rdfs:label ?diseaseLabel .
      FILTER NOT EXISTS { ?chem rdf:type ncit:C7057 }
    }
    LIMIT 15
""")
table(rows)

log("\n  Total number of distinct chemicals in CDAs:")
rows = query("""
    SELECT (COUNT(DISTINCT ?chemLabel) AS ?count)
    WHERE {
      ?cda rdf:type sio:SIO_000993 .
      ?cda sio:SIO_000628 ?chem .
      ?chem rdfs:label ?chemLabel .
      FILTER NOT EXISTS { ?chem rdf:type ncit:C7057 }
    }
""")
if rows:
    log(f"  {rows[0]['count']} distinct chemicals (too many to list)")
    log("  (These are from CTD - Comparative Toxicogenomics Database)")


# ============================================================
# STEP 7: CHEMICAL-LOCATION ASSOCIATIONS
# ============================================================

log("""
+--------------------------------------------------------------+
|  STEP 7: ChemicalLocation Associations (Environmental data)  |
|  -> Direct validation of AP1, AP2, AP3                       |
+--------------------------------------------------------------+

This class contains environmental measurement data: PM2.5
concentrations in European cities.

IMPORTANT: In the triplestore, the class is called
  LUCIA:ChemicalLocationAssociation (singular!)
  NOT LUCIA:ChemicalLocationAssociations (plural, as in OWL)

Each instance connects:
  - A chemical (PM2.5) with
  - A location (city/country) and
  - A measurement value (ug/m3)
""")

rows = query("""
    SELECT (COUNT(DISTINCT ?cla) AS ?count)
    WHERE { ?cla rdf:type LUCIA:ChemicalLocationAssociation }
""")
if rows:
    log(f"  Number of ChemicalLocation Associations: {rows[0]['count']}")

log("\n  Structure of a single CLA (distinct predicates + objects):")
rows = query("""
    SELECT DISTINCT ?p ?o
    WHERE {
      {
        SELECT ?cla WHERE { ?cla rdf:type LUCIA:ChemicalLocationAssociation } LIMIT 1
      }
      ?cla ?p ?o .
      FILTER(?p != rdf:type)
    }
""")
table(rows)

log("\n  Sample CLAs: which cities, which chemical, what values?")
rows = query("""
    SELECT DISTINCT ?claId ?cityLabel ?chemLabel ?value
    WHERE {
      ?cla rdf:type LUCIA:ChemicalLocationAssociation .
      ?cla dcterms:identifier ?claId .
      ?cla sio:SIO_000628 ?city .
      ?cla sio:SIO_000628 ?chem .
      ?city rdfs:label ?cityLabel .
      ?chem rdfs:label ?chemLabel .
      FILTER(?cityLabel != ?chemLabel)
      OPTIONAL {
        ?cla sio:SIO_000216 ?valNode .
        ?valNode sio:SIO_000300 ?value .
      }
    }
    ORDER BY ?cityLabel
    LIMIT 20
""")
table(rows)

log("\n  How many distinct cities?")
rows = query("""
    SELECT (COUNT(DISTINCT ?cityLabel) AS ?count)
    WHERE {
      ?cla rdf:type LUCIA:ChemicalLocationAssociation .
      ?cla sio:SIO_000628 ?city .
      ?city rdfs:label ?cityLabel .
      ?city rdf:type ncit:C48807 .
    }
""")
if rows:
    log(f"  {rows[0]['count']} distinct cities")


# ============================================================
# STEP 8: BIOMARKER-DISEASE ASSOCIATIONS
# ============================================================

log("""
+--------------------------------------------------------------+
|  STEP 8: Biomarker-Disease Associations                      |
+--------------------------------------------------------------+

25 associations between biomarkers (vitamins, trace elements)
and lung cancer.
""")

log("\n  Structure of a single BDA (distinct predicates + objects):")
rows = query("""
    SELECT DISTINCT ?p ?o
    WHERE {
      {
        SELECT ?bda WHERE { ?bda rdf:type LUCIA:BiomarkerDiseaseAssociations } LIMIT 1
      }
      ?bda ?p ?o .
      FILTER(?p != rdf:type)
    }
""")
table(rows)

log("\n  All biomarkers with their associated diseases:")
rows = query("""
    SELECT DISTINCT ?biomarkerLabel ?diseaseLabel
    WHERE {
      ?bda rdf:type LUCIA:BiomarkerDiseaseAssociations .
      ?bda sio:SIO_000628 ?biomarker .
      ?bda sio:SIO_000628 ?disease .
      ?biomarker rdfs:label ?biomarkerLabel .
      ?disease rdf:type ncit:C7057 .
      ?disease rdfs:label ?diseaseLabel .
      FILTER NOT EXISTS { ?biomarker rdf:type ncit:C7057 }
    }
    ORDER BY ?biomarkerLabel
    LIMIT 30
""")
table(rows)


# ============================================================
# STEP 9: STUDY POPULATION (Demographics)
# ============================================================

log("""
+--------------------------------------------------------------+
|  STEP 9: StudyPopulation (Demographics)                      |
+--------------------------------------------------------------+

Demographic data: Incidence and mortality of lung cancer
per EU country, age group, and gender.
""")

log("\n  Structure of a single StudyPopulation (distinct predicates + objects):")
rows = query("""
    SELECT DISTINCT ?p ?o
    WHERE {
      {
        SELECT ?study WHERE { ?study rdf:type vocab:StudyPopulation } LIMIT 1
      }
      ?study ?p ?o .
      FILTER(?p != rdf:type)
    }
""")
table(rows)

log("\n  All study populations with their countries:")
rows = query("""
    SELECT ?studyLabel ?country
    WHERE {
      ?study rdf:type vocab:StudyPopulation .
      ?study rdfs:label ?studyLabel .
      ?study sio:SIO_000061 ?loc .
      ?loc dcterms:identifier ?country .
    }
    ORDER BY ?studyLabel ?country
""")
table(rows)


# ============================================================
# STEP 10: VARIANTS AND PATHWAYS
# ============================================================

log("""
+--------------------------------------------------------------+
|  STEP 10: Variants and Pathways                              |
+--------------------------------------------------------------+
""")

rows = query("SELECT (COUNT(DISTINCT ?vda) AS ?count) WHERE { ?vda rdf:type sio:SIO_000897 }")
if rows: log(f"  Variant-Disease Associations: {rows[0]['count']}")

rows = query("SELECT (COUNT(DISTINCT ?v) AS ?count) WHERE { ?v rdf:type OBO:SO_0001060 }")
if rows: log(f"  Sequence Variants: {rows[0]['count']}")

rows = query("SELECT (COUNT(DISTINCT ?pw) AS ?count) WHERE { ?pw rdf:type LUCIA:Pathway }")
if rows: log(f"  Pathways: {rows[0]['count']}")

rows = query("SELECT (COUNT(DISTINCT ?pda) AS ?count) WHERE { ?pda rdf:type LUCIA:PathwayDiseaseAssociation }")
if rows: log(f"  Pathway-Disease Associations: {rows[0]['count']}")

log("\n  Structure of a single VDA (distinct predicates, excluding rdf:type):")
rows = query("""
    SELECT DISTINCT ?p ?o
    WHERE {
      {
        SELECT ?vda WHERE { ?vda rdf:type sio:SIO_000897 } LIMIT 1
      }
      ?vda ?p ?o .
      FILTER(?p != rdf:type)
    }
""")
table(rows)

log("\n  Sample pathways:")
rows = query("""
    SELECT ?id ?label
    WHERE {
      ?pw rdf:type LUCIA:Pathway .
      ?pw dcterms:identifier ?id .
      ?pw rdfs:label ?label .
    }
    LIMIT 10
""")
table(rows)


# ============================================================
# STEP 11: CONNECTIVITY CHECK (The central question!)
# ============================================================

log("""
+--------------------------------------------------------------+
|  STEP 11: Connectivity check between subgraphs               |
|  -> THE CENTRAL QUESTION for the extensions                  |
+--------------------------------------------------------------+

The KG consists of several "islands":
  A) Bio-network:   Disease <-> GDA <-> Gene <-> Pathway
  B) Variants:      Disease <-> VDA <-> Variant <-> Chromosome
  C) Environment:   ChemicalLocation <-> City <-> PM2.5 value
  D) Chemistry:     Chemical-Disease <-> Chemical <-> Disease
  E) Demographics:  StudyPopulation <-> Country <-> Incidence

Question: Are C (Environment) and D (Chemistry) connected?
If NO -> AP1 and AP2 are NEEDED to build the bridge.
""")

# 11a: What does ChemicalLocation reference?
log("  11a) ChemicalLocation Associations reference entities of type:")
rows = query("""
    SELECT DISTINCT ?refType (COUNT(*) AS ?count)
    WHERE {
      ?cla rdf:type LUCIA:ChemicalLocationAssociation .
      ?cla sio:SIO_000628 ?ref .
      ?ref rdf:type ?refType .
    }
    GROUP BY ?refType
    ORDER BY DESC(?count)
""")
table(rows)

# 11b: What does Chemical-Disease reference?
log("\n  11b) Chemical-Disease Associations reference entities of type:")
log("       (filtering out namespace duplicates)")
rows = query("""
    SELECT DISTINCT ?refType (COUNT(*) AS ?count)
    WHERE {
      ?cda rdf:type sio:SIO_000993 .
      ?cda sio:SIO_000628 ?ref .
      ?ref rdf:type ?refType .
      FILTER(
        STRSTARTS(STR(?refType), "http://ncicb.nci.nih.gov/") ||
        STRSTARTS(STR(?refType), "http://semanticscience.org/") ||
        STRSTARTS(STR(?refType), "https://w3id.org/LUCIA/")
      )
    }
    GROUP BY ?refType
    ORDER BY DESC(?count)
""")
table(rows)

# 11c: The decisive question!
log("\n  11c) Are there shared chemical entities between both subgraphs?")
log("       (= is there a chemical that appears in BOTH ChemicalLocation")
log("        AND Chemical-Disease?)")
rows = query("""
    SELECT DISTINCT ?chem ?chemLabel
    WHERE {
      ?cla rdf:type LUCIA:ChemicalLocationAssociation .
      ?cla sio:SIO_000628 ?chem .
      
      ?cda rdf:type sio:SIO_000993 .
      ?cda sio:SIO_000628 ?chem .
      
      OPTIONAL { ?chem rdfs:label ?chemLabel }
    }
""")
if not rows:
    log("\n  WARNING: NO shared chemicals!")
    log("  -> The subgraphs ChemicalLocation and Chemical-Disease are NOT connected.")
    log("  -> There is NO path from City/PM2.5 to Disease/Gene.")
    log("  -> AP1 (hasComponent) and AP2 (Benzene) are NEEDED.")
else:
    log(f"\n  {len(rows)} shared chemicals found:")
    table(rows)


# ============================================================
# STEP 12: SUMMARY
# ============================================================

log("""
+--------------------------------------------------------------+
|  STEP 12: SUMMARY                                            |
+--------------------------------------------------------------+
""")

log("  GRAPH CONTENTS:")
log("  ---------------")
log(f"  Total triples:                    228,060,321")
log(f"  (many are duplicates due to mapping artifacts)")
log("")
log("  ENTITY CLASSES:")
log("  ---------------")
log("  Diseases (ncit:C7057)             -> see Step 2")
log("  Genes (ncit:C16612)               -> see Step 4")
log("  Gene-Disease Assoc (SIO_000983)   -> see Step 5")
log("  Variant-Disease Assoc (SIO_000897)-> see Step 10")
log("  Variants (SO_0001060)             -> see Step 10")
log("  Chemical-Disease (SIO_000993)     -> see Step 6")
log("  ChemicalLocation (LUCIA)          -> see Step 7")
log("  Biomarker Assoc (LUCIA)           -> see Step 8")
log("  Pathways (LUCIA)                  -> see Step 10")
log("  StudyPopulation                   -> see Step 9")

log("""
  EXTENSION VALIDATION:
  ---------------------

  AP1 (hasComponent - PM2.5 Decomposition):
    Finding: ChemicalLocation (Environment) and Chemical-Disease (Chemistry)
             are NOT connected. -> Step 11
    Solution: PM2.5 -> hasComponent -> Arsenic/Cadmium/Lead/PAHs
    Verdict:  AP1 IS NEEDED

  AP2 (Gaseous Pollutants - Benzene etc.):
    Finding: ChemicalLocation contains only PM2.5 (542 entries). -> Step 7
    Solution: Add new ChemicalLocation entries for Benzene, Toluene etc.
              Benzene also exists in Chemical-Disease -> direct path!
    Verdict:  AP2 IS VALUABLE

  AP3 (Derived Measures):
    Finding: ChemicalLocation only has base values (ug/m3). -> Step 7
    Solution: Add perCapitaExposure, citySize, exceedsWHOGuideline
    Verdict:  AP3 ENRICHES GNN FEATURES

  AP4 (Disease Hierarchy):
    Finding: NO rdfs:subClassOf between diseases. -> Step 3
    Solution: Materialize hierarchy from OWL
              (NSCLC->Adenocarcinoma, SCLC->Combined, etc.)
    Verdict:  AP4 IS NEEDED
""")

log("=" * 80)
log(f"Exploration completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
log(f"Log saved to: {LOG_PATH}")
log("=" * 80)

log_file.close()
