import os
import pandas as pd
from SPARQLWrapper import SPARQLWrapper, JSON

# -----------------------------------------
# Konfiguration
# -----------------------------------------
ENDPOINT = "http://138.4.130.153:8890/sparql"

OUTPUT_DIR = "data/raw"
OUTPUT_FILE = "disease_gene_fusion.csv"

os.makedirs(OUTPUT_DIR, exist_ok=True)


# -----------------------------------------
# Disease–Gene Fusion SPARQL-Query
# -----------------------------------------
QUERY = r"""
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX ncit: <http://ncicb.nci.nih.gov/xml/owl/EVS/Thesaurus.owl#>
PREFIX sio: <http://semanticscience.org/resource/>
PREFIX bao: <http://www.bioassayontology.org/bao#>
PREFIX dcterms: <http://purl.org/dc/terms/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX mesh: <http://phenomebrowser.net/ontologies/mesh/mesh.owl#>
PREFIX CABO:<https://w3id.org/LUCIA/sem-lucia#>

SELECT DISTINCT
  ?DiseaseCui
  ?DiseaseName
  ?GeneFusion
WHERE {

  # Gene–Disease association
  ?gda sio:SIO_000628 ?gene , ?disease .
  ?gda sio:SIO_000628 ?disease .

  # Disease
  ?disease rdf:type ncit:C7057 ;
           dcterms:identifier ?DiseaseCui ;
           rdfs:label ?DiseaseName .

  # Gene fusion
  ?biomarker rdfs:subClassOf ?gda .
  ?gdalt rdfs:subClassOf ?biomarker .
  ?gdalt sio:SIO_000008 ?gf .

  ?gf rdf:type sio:SIO_001348 ;
      dcterms:identifier ?gfId ;
      rdfs:label ?GeneFusion .
}
ORDER BY ?DiseaseCui
"""


# -----------------------------------------
# Helper
# -----------------------------------------
def run_query():
    print(f"Running Disease–Gene Fusion query → {OUTPUT_FILE}")

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
        print(f"Error running Disease–Gene Fusion export: {e}")