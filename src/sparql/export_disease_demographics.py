import os
import pandas as pd
from SPARQLWrapper import SPARQLWrapper, JSON

# -----------------------------------------
# Konfiguration
# -----------------------------------------
ENDPOINT = "http://138.4.130.153:8890/sparql"

OUTPUT_DIR = "data/raw"
OUTPUT_FILE = "disease_demographics.csv"

os.makedirs(OUTPUT_DIR, exist_ok=True)


# -----------------------------------------
# Disease Demographics & Vital Statistics SPARQL-Query
# (1:1 deine Version)
# -----------------------------------------
QUERY = r"""
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX ncit: <http://ncicb.nci.nih.gov/xml/owl/EVS/Thesaurus.owl#>
PREFIX sio: <http://semanticscience.org/resource/>
PREFIX dcterms: <http://purl.org/dc/terms/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX biolik: <https://w3id.org/biolink/vocab/>
PREFIX LUCIA: <https://w3id.org/LUCIA/sem-lucia#>
PREFIX SNOMED: <http://snomed.info/id/>

SELECT DISTINCT
  ?DiseaseCui
  ?DiseaseName
  (ROUND(?incidence * 100) / 100 AS ?Incidence)
  (ROUND(?mortalityrate * 100) / 100 AS ?MortalityRate)
  (CONCAT(
    REPLACE(STR(?vs_iri), ".*_([A-Z]{2})_\\d.*", "$1"), "_",
    REPLACE(STR(?vs_iri), ".*_[A-Z]{2}_(\\d[^_]+)_.*", "$1"), "_",
    REPLACE(STR(?vs_iri), ".*_[A-Z]{2}_\\d[^_]+_([^_]+)_.*", "$1"), "_",
    REPLACE(STR(?vs_iri), ".*_[A-Z]{2}_\\d[^_]+_[^_]+_([^_]+)$", "$1")
  ) AS ?DemographicGroup)
WHERE {
  ## Enfermedad y relaciones demográficas
  ?disease_iri 
    rdf:type ncit:C7057 ;
    dcterms:identifier ?DiseaseCui ;
    rdfs:label ?DiseaseName ;
    sio:SIO_000300 ?vs_iri .

  ?vs_iri 
    a ncit:C17258 ;
    LUCIA:incidence ?incidence ;
    LUCIA:mortalityrate ?mortalityrate .
}
"""


# -----------------------------------------
# Helper
# -----------------------------------------
def run_query():
    print(f"Running Disease Demographics & Vital Statistics query → {OUTPUT_FILE}")

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
        print(f"Error running Disease Demographics export: {e}")
