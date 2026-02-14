"""
Lung-CABO Knowledge Graph Explorer
===================================
Connect to the LUCIA SPARQL endpoint and explore the full graph.
"""

import urllib.request
import json
import csv
import os

# === CONFIGURATION ===
ENDPOINT = "http://138.4.130.153:5001/execute_query"

PREFIXES = """
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX ncit: <http://ncicb.nci.nih.gov/xml/owl/EVS/Thesaurus.owl#>
PREFIX sio: <http://semanticscience.org/resource/>
PREFIX bao: <http://www.bioassayontology.org/bao#>
PREFIX dcterms: <http://purl.org/dc/terms/>
PREFIX LUCIA: <https://w3id.org/LUCIA/sem-lucia#>
PREFIX OBO: <http://purl.obolibrary.org/obo/>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX ctd: <http://bio2rdf.org/ctd_vocabulary:>
PREFIX wp: <http://vocabularies.wikipathways.org/wp#>
PREFIX id: <http://linkedlifedata.com/resource/umls/id/>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX vocab: <https://w3id.org/biolink/vocab/>
"""


def query(sparql: str, timeout: int = 120):
    """Execute SPARQL query via JSON POST, return rows as list of dicts."""
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
        print(f"  -> {len(rows)} results ({t}s)")
        return rows
    except Exception as e:
        print(f"  -> Error: {e}")
        return []


def save_csv(rows, filename):
    """Save query results to CSV."""
    if not rows:
        print(f"  No data to save for {filename}")
        return
    filepath = os.path.join("notebooks", "data", filename)
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    print(f"  Saved -> {filepath}")


# ============================================================
if __name__ == "__main__":
    print("=" * 60)
    print("Lung-CABO Knowledge Graph Explorer")
    print(f"Endpoint: {ENDPOINT}")
    print("=" * 60)

    # --- 1. Total triples ---
    print("\n[1] Total triples")
    rows = query("SELECT (COUNT(*) AS ?total) WHERE { ?s ?p ?o }")
    if rows:
        print(f"  Total: {rows[0]['total']}")

    # --- 2. All classes with instance counts ---
    print("\n[2] Classes with instance counts")
    rows = query("""
        SELECT ?class (COUNT(?s) AS ?count)
        WHERE { ?s rdf:type ?class }
        GROUP BY ?class
        ORDER BY DESC(?count)
    """)
    save_csv(rows, "graph_classes.csv")
    for r in rows[:15]:
        print(f"  {r['count']:>10}  {r['class']}")

    # --- 3. All properties with usage counts ---
    print("\n[3] Properties with usage counts")
    rows = query("""
        SELECT ?p (COUNT(*) AS ?count)
        WHERE { ?s ?p ?o }
        GROUP BY ?p
        ORDER BY DESC(?count)
        LIMIT 50
    """)
    save_csv(rows, "graph_properties.csv")
    for r in rows[:15]:
        print(f"  {r['count']:>10}  {r['p']}")

    # --- 4. Disease hierarchy (subClassOf) ---
    print("\n[4] Disease hierarchy - subClassOf in triplestore?")
    rows = query("""
        SELECT ?child ?childLabel ?parent ?parentLabel
        WHERE {
          ?child rdfs:subClassOf ?parent .
          ?child rdfs:label ?childLabel .
          ?parent rdfs:label ?parentLabel .
        }
        LIMIT 50
    """)
    if not rows:
        print("  >>> KEINE subClassOf-Triples im Triplestore!")
        print("  >>> AP4: Hierarchie muss materialisiert werden.")
    else:
        save_csv(rows, "disease_hierarchy.csv")
        for r in rows[:10]:
            print(f"  {r.get('childLabel','')} -> subClassOf -> {r.get('parentLabel','')}")

    # --- 5. ChemicalLocationAssociation structure ---
    print("\n[5] ChemicalLocationAssociation - sample triples")
    rows = query("""
        SELECT ?s ?p ?o
        WHERE {
          ?s rdf:type LUCIA:ChemicalLocationAssociations .
          ?s ?p ?o .
        }
        LIMIT 30
    """)
    save_csv(rows, "chemical_location_triples.csv")

    # --- 6. Chemical-Disease Association structure ---
    print("\n[6] Chemical-Disease Association - sample triples")
    rows = query("""
        SELECT ?s ?p ?o
        WHERE {
          ?s rdf:type ctd:Chemical-Disease-Association .
          ?s ?p ?o .
        }
        LIMIT 30
    """)
    save_csv(rows, "chemical_disease_triples.csv")

    # --- 7. StudyPopulation / Demographics ---
    print("\n[7] StudyPopulation - sample triples")
    rows = query("""
        SELECT ?s ?p ?o
        WHERE {
          ?s rdf:type vocab:StudyPopulation .
          ?s ?p ?o .
        }
        LIMIT 30
    """)
    save_csv(rows, "demographics_triples.csv")

    # --- 8. Named graphs ---
    print("\n[8] Named graphs")
    rows = query("SELECT DISTINCT ?g WHERE { GRAPH ?g { ?s ?p ?o } } LIMIT 20")
    if rows:
        for r in rows:
            print(f"  {r['g']}")
    else:
        print("  No named graphs (default graph only)")

    # --- 9. LUCIA-namespace properties ---
    print("\n[9] LUCIA-specific properties")
    rows = query("""
        SELECT DISTINCT ?p (COUNT(*) AS ?count)
        WHERE {
          ?s ?p ?o .
          FILTER(STRSTARTS(STR(?p), "https://w3id.org/LUCIA/"))
        }
        GROUP BY ?p
        ORDER BY DESC(?count)
    """)
    for r in rows:
        print(f"  {r['count']:>10}  {r['p']}")

    # --- 10. Schema overview ---
    print("\n[10] Schema: which types connect via which properties?")
    rows = query("""
        SELECT ?sType ?p (COUNT(*) AS ?count)
        WHERE {
          ?s rdf:type ?sType .
          ?s ?p ?o .
        }
        GROUP BY ?sType ?p
        ORDER BY ?sType DESC(?count)
        LIMIT 100
    """)
    save_csv(rows, "graph_schema.csv")

    print("\n" + "=" * 60)
    print("Done! CSV files saved in notebooks/data/")
    print("=" * 60)