import os
import pandas as pd
from SPARQLWrapper import SPARQLWrapper, JSON

# -----------------------------------------
# Konfiguration
# -----------------------------------------
ENDPOINT = "http://138.4.130.153:8890/sparql"

OUTPUT_DIR = "data/raw"
OUTPUT_FILE = "pathway_disease_association.csv"

os.makedirs(OUTPUT_DIR, exist_ok=True)


# -----------------------------------------
# Pathway–Disease SPARQL-Query
# -----------------------------------------
QUERY = r"""
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX dcterms: <http://purl.org/dc/terms/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX sio: <http://semanticscience.org/resource/>
PREFIX CABO: <https://w3id.org/LUCIA/sem-lucia#>
PREFIX wp: <http://vocabularies.wikipathways.org/wp#>
PREFIX ncit: <http://ncicb.nci.nih.gov/xml/owl/EVS/Thesaurus.owl#>

SELECT DISTINCT
  ?PathwayDiseaseAssociation
  ?PathwayId
  ?PathwayName
  ?DiseaseCui
  ?DiseaseName
  ?GeneProductId
  ?GeneProductName
WHERE {
  # Association between Pathway and Disease
  ?pwda a CABO:PathwayDiseaseAssociation ;
        dcterms:identifier ?PathwayDiseaseAssociation ;
        sio:SIO_000628 ?pathway ;
        sio:SIO_000628 ?disease .

  # Pathway information
  ?pathway a CABO:Pathway ;
           dcterms:identifier ?PathwayId ;
           rdfs:label ?PathwayName .

  # Remove Pathway name duplicates
  FILTER ( !isIRI(?PathwayName) && !STRSTARTS(STR(?PathwayName), "http" ))

  # Disease information
  ?disease rdf:type ncit:C7057 ;
           dcterms:identifier ?DiseaseCui ;
           rdfs:label ?DiseaseName .

  # Optional relationship between the pathway and the gene product
  OPTIONAL {
    ?pathway sio:SIO_000028 ?geneProductIRI .
    ?geneProductIRI a wp:GeneProduct ;
                    dcterms:identifier ?GeneProductId ;
                    rdfs:label ?GeneProductName .
  }
}
"""


# -----------------------------------------
# Helper
# -----------------------------------------
def run_query():
    print(f"Running Pathway–Disease Association query → {OUTPUT_FILE}")

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
        print(f"Error running Pathway–Disease Association export: {e}")