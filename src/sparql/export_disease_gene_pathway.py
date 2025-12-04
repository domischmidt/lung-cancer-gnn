import os
import pandas as pd
from SPARQLWrapper import SPARQLWrapper, JSON

# -----------------------------------------
# Konfiguration
# -----------------------------------------
ENDPOINT = "http://138.4.130.153:8890/sparql"

OUTPUT_DIR = "data/raw"
OUTPUT_FILE = "disease_gene_pathway.csv"

os.makedirs(OUTPUT_DIR, exist_ok=True)


# -----------------------------------------
# Disease–Gene–Pathway SPARQL-Query
# (1:1 deine Version)
# -----------------------------------------
QUERY = r"""
PREFIX rdf:     <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX ncit:    <http://ncicb.nci.nih.gov/xml/owl/EVS/Thesaurus.owl#>
PREFIX sio:     <http://semanticscience.org/resource/>
PREFIX bao:     <http://www.bioassayontology.org/bao#>
PREFIX dcterms: <http://purl.org/dc/terms/>
PREFIX rdfs:    <http://www.w3.org/2000/01/rdf-schema#>
PREFIX CABO:   <https://w3id.org/LUCIA/sem-lucia#>
PREFIX mesh:    <http://phenomebrowser.net/ontologies/mesh/mesh.owl#>

SELECT DISTINCT
?DiseaseCui
?DiseaseName
?GeneId
?GeneName
?GeneSymbol
?PathwayId
?PathwayName
WHERE {

  # Gene–Disease association
  ?gda a sio:SIO_000983 ;
         sio:SIO_000628 ?gene, ?disease .

  # Gene information
  ?gene a ncit:C16612 ;
          dcterms:identifier ?GeneId ;
          rdfs:label ?GeneName ;
          sio:SIO_000068 ?pathwayURI ;       # linked to pathway
          sio:SIO_000205 ?geneSymbolIRI .

  # Gene symbol
  ?geneSymbolIRI a ncit:C43568 ;
                   dcterms:identifier ?GeneSymbol .

  # Pathway information
  ?pathwayURI a CABO:Pathway ;
                dcterms:identifier ?PathwayId ;
                rdfs:label ?PathwayName .

  # Disease information
  ?disease a ncit:C7057 ;
             dcterms:identifier ?DiseaseCui ;
             rdfs:label ?DiseaseName .

  # Optional: restrict to selected diseases (e.g., NSCLC, SCLC)
  FILTER(?DiseaseCui IN ("C0007131"))  # You can replace/add CUIs as needed
  FILTER(!STRSTARTS(STR(?PathwayName), "https"))
}
"""


# -----------------------------------------
# Helper
# -----------------------------------------
def run_query():
    print(f"Running Disease–Gene–Pathway query → {OUTPUT_FILE}")

    sparql = SPARQLWrapper(ENDPOINT)
    sparql.setQuery(QUERY)
    sparql.setReturnFormat(JSON)

    results = sparql.query().convert()

    vars_ = results["head"]["vars"]
    rows = []

    for b in results["results"]["bindings"]:
        row = {v: b[v]["value"] if v in b else None for v in vars_}
        rows.append(row)

    df = pd.DataFrame(rows)
    out_path = os.path.join(OUTPUT_DIR, OUTPUT_FILE)
    df.to_csv(out_path, index=False)

    print(f"Saved {len(df)} rows to {out_path}")


# -----------------------------------------
# Main
# -----------------------------------------
if __name__ == "__main__":
    try:
        run_query()
    except Exception as e:
        print(f"Error running Disease–Gene–Pathway export: {e}")