import pandas as pd
from SPARQLWrapper import SPARQLWrapper, JSON
from prettytable import PrettyTable

queryPadres='''PREFIX dct: <http://purl.org/dc/terms/>

select ?categoria as ?{salida} FROM <http://dbpedia.org> 
WHERE {{
<{uri}>
?propiedad 
?categoria. 
FILTER (regex(?propiedad, {propiedad})).

}}'''

queryJerarquia2='''PREFIX dct: <http://purl.org/dc/terms/>

select ?cat2 as ?{cat2},?cat3 as ?{cat3}, ?concepto1 as ?{con1} FROM <http://dbpedia.org> 
WHERE {{

<{uri}>
?propiedad1
?cat1.

FILTER (regex(?propiedad1, {propiedad1})).
FILTER (!regex(?cat1, owl:Thing)).
FILTER (!regex(?cat1, skos:Concept)).

{{
?cat1
?propiedad2
?cat2.

FILTER (regex(?propiedad2, {propiedad2})).
}}
UNION{{
?cat3
?propiedad2
?cat1

FILTER (regex(?propiedad2, {propiedad2})).
}}
UNION
{{
?concepto1
?propiedad1
?cat1

FILTER (regex(?propiedad1, {propiedad1})).

}}
}}'''
queryRecursosHijos='''PREFIX dct: <http://purl.org/dc/terms/>
select ?recurso as ?{salida} where{{

?recurso
?propiedad 
<{uri}> 


FILTER (STRSTARTS(str(?propiedad), str({propiedad}))).

}}'''

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
FILTER(!bound(?person)).
OPTIONAL
    {{
    ?recurso rdf:type ?place.
    FILTER regex(?place, dbo:PopulatedPlace).
    }}
FILTER(!bound(?place)).
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



queryCuentaPadres='''PREFIX dct: <http://purl.org/dc/terms/>
select COUNT(?concepto) AS ?cuenta,?{salida} where{{
<{uri}>
?propiedad 
?{salida}. 

?concepto
?propiedad
?{salida}. 
FILTER (STRSTARTS(str(?propiedad), str({propiedad}))).

}}
GROUP BY ?{salida}'''


def get_conceptos_originales(ruta, clases, numero_conceptos):
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

def get_conceptos_vecinos(uris,path,original_concepts):
    expansion=set()
    for uri in uris:
        query=queryVecinos1.format(uri=uri) if path==1 else queryVecinos2.format(uri=uri)
        result=send_query(query)
        for res in result:
            entity=res['recurso']['value']
            entity=entity.split('/')[-1]
            entity=entity.replace('_',' ')
            if not (any(s for s in original_concepts if entity in s) or any(s for s in original_concepts if s in entity)) :
                expansion.add(entity)
    return expansion


def get_categorias_jerarquicos(uris,path, original_concepts):
    if path==2:
        return get_categorias_jerarquicos2(uris, original_concepts)
    else:
        expansion=set()
        for uri in uris:
            query_categorias=queryPadres.format(uri=uri, propiedad='dct:subject', salida='category')
            result= send_query(query_categorias)
            for res in result:
                categoria=res['category']['value']
                categoria=categoria.split('/')[-1]
                categoria=categoria.split(':')[-1]
                expansion.add(categoria)
        return expansion

def get_categorias_jerarquicos2(uris,original_concepts):
    abuelos=set()
    tios=set()
    hermanos=set()
    for uri in uris:
        query_categorias=queryJerarquia2.format(uri=uri, propiedad='dct:subject', cat2='abuelo', cat3='tio', con1='hermano', propiedad1='dct:subject', propiedad2='skos:broader')
        
        result= send_query(query_categorias)
        for res in result:
            if 'abuelo' in res:
                abuelo=res['abuelo']['value']
                abuelo=abuelo.split('/')[-1]
                abuelo=abuelo.split(':')[-1]
                abuelos.add(abuelo)
            if 'tio' in res:
                tio=res['tio']['value']
                tio=tio.split('/')[-1]
                tio=tio.split(':')[-1]
                tios.add(tio)
            if 'hermano' in res:
                hermano=res['hermano']['value']
                hermano=hermano.split('/')[-1]
                hermano=hermano.split(':')[-1]
                hermanos.add(hermano)
    return (abuelos,tios,hermanos)

def get_clases_jerarquicas(uris,path, original_concepts):
    expansion=set()
    for uri in uris:
        query_categorias=queryPadres.format(uri=uri, propiedad='rdf:type', salida='class')
        result= send_query(query_categorias)
        for res in result:
            categoria=res['class']['value']
            categoria=categoria.split('/')[-1]
            categoria=categoria.split(':')[-1]
            expansion.add(categoria)
    return expansion

def get_conceptos_hijos(uris,path, original_concepts):
    expansion=set()
    for uri in uris:
        entity=uri.split('/')[-1]
        uri='http://dbpedia.org/resource/Category:'+entity
        query_categorias=queryRecursosHijos.format(uri=uri, propiedad='dct:subject', salida='recurso')
        result= send_query(query_categorias)
        for res in result:
            categoria=res['recurso']['value']
            categoria=categoria.split('/')[-1]
            categoria=categoria.split(':')[-1]
            expansion.add(categoria)
    return expansion


    
def send_query(query):
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)
    result = sparql.query().convert()
    return result["results"]["bindings"]



def generate_report(ruta, clases, num, path, jerarquica):
    orig = PrettyTable()
    
    #get conceptos
    originals= get_conceptos_originales(ruta, clases, num)
    uris= get_uris(originals)
    
    salida_template='./result/{jerarquica}_{clases}_path{path}.txt'
    salida= salida_template.format( jerarquica='jerarquica',clases=clases, path=path) if jerarquica else salida_template.format(jerarquica='asociativa',clases=clases, path=path) 
    
    with open(salida, 'a+',encoding="utf-8") as file:
        file.write('**Expansion de {num}**\n'.format(num=num))
        orig.add_column("Originals", list(originals['entity_id']))
        file.write(orig.get_string()+'\n')

    if (not jerarquica) or path==1:
        expansion=get_conceptos_vecinos(uris,path,originals)   
        write_file(salida, 'Expansion ', expansion)
    else:
        (abuelos,tios,hermanos)= get_categorias_jerarquicos2(uris,originals)
        write_file(salida, 'Abuelos ', abuelos)
        write_file(salida, 'Tios ', tios)
        write_file(salida, 'Hermanos ', hermanos)

def generate_csv(ruta, clases, num, path, jerarquica):
    #get conceptos
    originals= get_conceptos_originales(ruta, clases, num)
    uris= get_uris(originals)
    
    salida_template='./result/{jerarquica}_{clases}_{num}_path{path}.csv'
    salida= salida_template.format( jerarquica='jerarquica',clases=clases, path=path,num=num) if jerarquica else salida_template.format(jerarquica='asociativa',clases=clases, path=path,num=num) 
    
    if (not jerarquica) or path==1:
        expansion=get_conceptos_vecinos(uris,path,originals)   
        write_csv(salida, ['Expansion'], [list(expansion)])
    else:
        (abuelos,tios,hermanos)= get_categorias_jerarquicos2(uris,originals)
        write_csv(salida, ['Abuelos','Tios', 'Hermanos'], [list(abuelos),list(tios),list(hermanos)])
        
def write_csv(salida,names, expansions):
    print(names)
    print(expansions)
    df= pd.DataFrame(expansions,columns=names)
    df.to_csv(salida)

    
def write_file(salida,  name, expansion):
    expan = PrettyTable()
    with open(salida, 'a+',encoding="utf-8") as file:
        expan.add_column(name, list(expansion))
        file.write(expan.get_string()+'\n')
        file.write('# of New Concepts: '+ str(len(expansion))+'\n\n')

sparql = SPARQLWrapper("http://localhost:8890/sparql")
"""
generate_report("./conceptos_felipe.csv",['ISIS1105'], 5,1,False)
generate_report("./conceptos_felipe.csv",['ISIS1105'], 10,1,False)
generate_report("./conceptos_felipe.csv",['ISIS1105'], 15,1,False)
generate_report("./conceptos_felipe.csv",['ISIS1105'], 24,1,False)

generate_report("./conceptos_felipe.csv",['ISIS1105'], 5,2,False)
generate_report("./conceptos_felipe.csv",['ISIS1105'], 10,2,False)
generate_report("./conceptos_felipe.csv",['ISIS1105'], 15,2,False)
generate_report("./conceptos_felipe.csv",['ISIS1105'], 24,2,False)




generate_report("./conceptos_felipe.csv",['ISIS1105'], 5,1,True)
generate_report("./conceptos_felipe.csv",['ISIS1105'], 10,1,True)
generate_report("./conceptos_felipe.csv",['ISIS1105'], 15,1,True)
generate_report("./conceptos_felipe.csv",['ISIS1105'], 24,1,True)


generate_report("./conceptos_felipe.csv",['ISIS1105'], 5,2,True)
generate_report("./conceptos_felipe.csv",['ISIS1105'], 10,2,True)
generate_report("./conceptos_felipe.csv",['ISIS1105'], 15,2,True)
generate_report("./conceptos_felipe.csv",['ISIS1105'], 24,2,True)


"""
generate_csv("./conceptos_felipe.csv",['ISIS1105'], 5,1,True)
generate_csv("./conceptos_felipe.csv",['ISIS1105'], 10,1,True)
