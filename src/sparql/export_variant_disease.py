import os
import pandas as pd
from SPARQLWrapper import SPARQLWrapper, JSON

# -----------------------------------------
# Konfiguration
# -----------------------------------------
ENDPOINT = "http://138.4.130.153:8890/sparql"

OUTPUT_DIR = "data/raw"
OUTPUT_FILE = "disease_variant.csv"

os.makedirs(OUTPUT_DIR, exist_ok=True)


# -----------------------------------------
# Variant–Disease SPARQL-Query
# (1:1 deine Version)
# -----------------------------------------
QUERY = r"""
PREFIX rdf:     <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX ncit:    <http://ncicb.nci.nih.gov/xml/owl/EVS/Thesaurus.owl#>
PREFIX sio:     <http://semanticscience.org/resource/>
PREFIX bao:     <http://www.bioassayontology.org/bao#>
PREFIX dcterms: <http://purl.org/dc/terms/>
PREFIX CABO:   <https://w3id.org/LUCIA/sem-lucia#>
PREFIX OBO:     <http://purl.obolibrary.org/obo/>
PREFIX rdfs:    <http://www.w3.org/2000/01/rdf-schema#>
PREFIX geno:    <http://purl.obolibrary.org/obo/>
PREFIX dctype:  <http://purl.org/dc/terms/DCMIType>
PREFIX dcat:    <http://www.w3.org/TR/vocab-dcat#>

SELECT DISTINCT
?DiseaseCui
?DiseaseName
?GeneId
?GeneName
?GeneSymbol
?VariantId
?Consequence
?Chromosome
?ChromosomeStartPosition
?ChromosomeEndPosition
?ReferenceAllele
?AlternativeAllele
?DiseaseSpecificity
?DiseasePleiotropy
?DataSource
WHERE {

  # Main variant-disease association
  ?vda a sio:SIO_000897 ;
         sio:SIO_000253 ?SourceIRI ;
         sio:SIO_000628 ?variant, ?disease ;
         dcterms:identifier ?vdaId .

  OPTIONAL {
    ?Source_iri a dctype:Dataset, dcat:Distribution ;
                  dcterms:title ?DataSource .
  }

  # Variant and gene info
  ?variant a OBO:SO_0001060 ;
             sio:SIO_001403 ?gene ;
             dcterms:identifier ?VariantId ;
             CABO:consequence ?Consequence .

  # Optional: specificity and pleiotropy
  OPTIONAL {
    ?variant sio:SIO_000216 ?diseaseSpecificityIRI, ?diseasePleiotropyIRI .
    ?diseaseSpecificityIRI a sio:SIO_001351 ;
                             sio:SIO_000300 ?DiseaseSpecificity .
    ?diseasePleiotropyIRI a sio:SIO_001352 ;
                            sio:SIO_000300 ?DiseasePleiotropy .
  }

  # Optional: alleles
  OPTIONAL {
    ?variant sio:SIO_000223 ?alternativeAlleleIRI, ?referenceAlleleIRI .
    ?referenceAlleleIRI a geno:GENO_0000152 ;
                          sio:SIO_000300 ?ReferenceAllele .
    ?alternativeAlleleIRI a geno:GENO_0000476 ;
                            sio:SIO_000300 ?AlternativeAllele .
  }

  # Chromosome and position
  ?variant sio:SIO_000061 ?chromosomeIRI,
           ?chromosomeStartPositionIRI,
           ?chromosomeEndPositionIRI .
  ?chromosomeIRI a sio:SIO_000899 ;
                   sio:SIO_000300 ?Chromosome .
  ?chromosomeStartPositionIRI a sio:SIO_000791 ;
                                sio:SIO_000300 ?ChromosomeStartPosition .
  ?chromosomeEndPositionIRI a sio:SIO_000792 ;
                              sio:SIO_000300 ?ChromosomeEndPosition .

  # Gene
  ?gene a ncit:C16612 ;
          dcterms:identifier ?GeneId ;
          rdfs:label ?GeneName ;
          sio:SIO_000205 ?geneSymbolIRI .
  ?geneSymbolIRI a ncit:C43568 ;
                   dcterms:identifier ?GeneSymbol .

  # Disease
  ?disease a ncit:C7057 ;
             dcterms:identifier ?DiseaseCui ;
             rdfs:label ?DiseaseName .

  # --- FILTERS ---

  # Optional: restrict to specific source (can change to "C%" for COSMIC, etc.)
  FILTER(STRSTARTS(?DataSource, "D"))  # Data sources starting with 'D' (e.g., DisGeNET)
}
"""


# -----------------------------------------
# Helper
# -----------------------------------------
def run_query():
    print(f"Running Variant–Disease Association query → {OUTPUT_FILE}")

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
        print(f"Error running Variant–Disease Association export: {e}")
