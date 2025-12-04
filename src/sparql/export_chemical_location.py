import os
import pandas as pd
from SPARQLWrapper import SPARQLWrapper, JSON

# -----------------------------------------
# Konfiguration
# -----------------------------------------
ENDPOINT = "http://138.4.130.153:8890/sparql"

OUTPUT_DIR = "data/raw"
OUTPUT_FILE = "chemical_location.csv"

os.makedirs(OUTPUT_DIR, exist_ok=True)


# -----------------------------------------
# Chemical–Location SPARQL-Query
# (1:1 deine Version)
# -----------------------------------------
QUERY = r"""
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX ncit: <http://ncicb.nci.nih.gov/xml/owl/EVS/Thesaurus.owl#>
PREFIX sio: <http://semanticscience.org/resource/>
PREFIX dcterms: <http://purl.org/dc/terms/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX biolink: <https://w3id.org/biolink/vocab/>
PREFIX LUCIA: <https://w3id.org/LUCIA/sem-lucia#>
PREFIX SNOMED: <http://snomed.info/id/>

SELECT 
  ?ChemicalId
  ?ChemicalName
  ?CityId
  ?CountryName
  ?CityName
  ?Value
  ?Units
  ?Population 
WHERE {
  ## Asociación chemical–location
  ?cla a LUCIA:ChemicalLocationAssociation ;
       LUCIA:value ?Value ;
       sio:SIO_000008 ?units_iri ;
       sio:SIO_000628 ?chemical_iri , ?city_iri .

  ## Unidades
  ?units_iri rdfs:label ?Units .

  ## Ciudad
  ?city_iri a ncit:C0008848 ;
            dcterms:identifier ?CityId ;
            rdfs:label ?CityName ;
            LUCIA:population ?Population ;
            sio:SIO_000061 ?country_iri .
  FILTER(LCASE(STR(?CityName)) != "unspecified")

  ## Chemical
  ?chemical_iri a ncit:C48807 ;
                dcterms:identifier ?ChemicalId ;
                rdfs:label ?ChemicalName .

  ## Country
  ?country_iri a ncit:C25464 ;
               rdfs:label ?CountryName .
}
"""


# -----------------------------------------
# Helper
# -----------------------------------------
def run_query():
    print(f"Running Chemical–Location Association query → {OUTPUT_FILE}")

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
        print(f"Error running Chemical–Location Association export: {e}")
