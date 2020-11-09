import pandas as pd

queryVertical1='''select * FROM <http://dbpedia.org> 
WHERE {{
{{
<{uri}>
?propiedad 
?recurso1.
}}UNION
{{
?recurso1
?propiedad 
<{uri}>. 
}}


FILTER (regex(str(?propiedad), "http://purl.org/dc/terms/subject")
|| regex(str(?propiedad), "http://www.w3.org/1999/02/22-rdf-syntax-ns#type")).

}}
'''
queryVecinos1='''PREFIX dbr: <http://dbpedia.org/resource/>
PREFIX dbo: <http://dbpedia.org/ontology/>
select * FROM <http://dbpedia.org> 
WHERE {{
        {{
        <{uri}>
        ?propiedad
        ?recurso.
        }}
        UNION
        {{
        ?recurso
        ?propiedad
        <{uri}>.
        }}


    FILTER regex(?recurso, dbr:).
    FILTER( 
        (regex(?propiedad, dbo:) && !regex(?propiedad, dbo:wikiPageRedirects)) 
    || regex(?propiedad, rdfs:seeAlso)
    || regex(?propiedad, owl:sameAs) ).

    OPTIONAL
    {{
    ?recurso rdf:type ?person.
    FILTER regex(?person, foaf:Person).
    }}
FILTER(!bound(?person))
}}'''

queryVecinos2='''PREFIX dbr: <http://dbpedia.org/resource/>
PREFIX dbo: <http://dbpedia.org/ontology/>
select * FROM <http://dbpedia.org> 
WHERE {{
    {{
        <{uri}>
        ?propiedad
        ?recursoInter.
        
        ?recursoInter 
        ?propiedad2 
        ?recurso.
    }}UNION{{
        ?recurso
        ?propiedad
        ?recursoInter.
        
       ?recursoInter 
       ?propiedad2 
       <{uri}>.

    }}


   FILTER regex(?recurso, dbr:).
    FILTER( 
        ((regex(?propiedad, dbo:) && !regex(?propiedad, dbo:wikiPageRedirects)) 
    || regex(?propiedad, rdfs:seeAlso)
    || regex(?propiedad, owl:sameAs))  &&
        ((regex(?propiedad2, dbo:) && !regex(?propiedad2, dbo:wikiPageRedirects)) 
    || regex(?propiedad2, rdfs:seeAlso)
    || regex(?propiedad2, owl:sameAs)) ).

     OPTIONAL
    {{
    ?recurso rdf:type ?person.
    FILTER regex(?person, foaf:Person).
    }}
FILTER(!bound(?person)).
OPTIONAL
    {{
    ?recurso rdf:type ?place.
    FILTER regex(?place, dbo:PopulatedPlace).
    }}
FILTER(!bound(?place)).
}}'''

def get_conceptos(ruta, clases, numero_conceptos):
    conceptos= pd.read_csv(ruta,encoding='ISO-8859-1')
    filtro=conceptos.university_id.isin(clases)

    conceptos_dalgo= conceptos[filtro]
    conceptos_dalgo=conceptos_dalgo.sort_values(by='confidence_score', ascending=False)
    conceptos_dalgo=conceptos_dalgo.filter(['university_id', 'entity_id', 'confidence_score'])
    conceptos_dalgo=conceptos_dalgo.dropna()

    conceptos_dalgo=conceptos_dalgo.head(numero_conceptos)
    return conceptos_dalgo

def get_uris(conceptos_dalgo):
    
    uri_template='http://dbpedia.org/resource/{concepto}'
    uris=[]
    for i in conceptos_dalgo['entity_id']:
        concepto=i.replace(' ','_')
        uris.append(uri_template.format(concepto=concepto))
    return uris

def get_conceptos_vecinos(uris,path):
    from SPARQLWrapper import SPARQLWrapper, JSON
    sparql = SPARQLWrapper("http://localhost:8890/sparql")
    expansion=set()
    for uri in uris:
        if path==1:
            sparql.setQuery(queryVecinos1.format(uri=uri))
        elif path==2:
            sparql.setQuery(queryVecinos2.format(uri=uri))
        sparql.setReturnFormat(JSON)
        result = sparql.query().convert()
        for res in result["results"]["bindings"]:
            entity=res['recurso']['value']
            entity=entity.split('/')[-1]
            entity=entity.replace('_',' ')
            expansion.add(entity)
    return expansion

def generate_report(ruta, clases, num, path):
    from prettytable import PrettyTable
    orig = PrettyTable()
    expan = PrettyTable()
    originals= get_conceptos(ruta, clases, num)
    uris= get_uris(originals)
    vecinos=get_conceptos_vecinos(uris,path)

    with open('./expansion_{clases}_path_{path}.txt'.format(clases=clases, path=path), 'a+',encoding="utf-8") as file:
        file.write('**Vecinos con expansion de {num}**\n'.format(num=num))
        orig.add_column("Originals", list(originals['entity_id']))
        expan.add_column("Expansion ", list(vecinos))
        file.write(orig.get_string()+'\n')
        file.write(expan.get_string()+'\n')
        file.write('# of New Concepts: '+ str(len(vecinos))+'\n\n')
    
generate_report("./conceptos_felipe.csv",['ISIS1105'], 5,1)
generate_report("./conceptos_felipe.csv",['ISIS1105'], 10,1)
generate_report("./conceptos_felipe.csv",['ISIS1105'], 15,1)
generate_report("./conceptos_felipe.csv",['ISIS1105'], 24,1)

generate_report("./conceptos_felipe.csv",['ISIS1105'], 5,2)
generate_report("./conceptos_felipe.csv",['ISIS1105'], 10,2)
generate_report("./conceptos_felipe.csv",['ISIS1105'], 15,2)
generate_report("./conceptos_felipe.csv",['ISIS1105'], 24,2)
