class Querys:
    def __init__(self):
        self.queryPadres='''PREFIX dct: <http://purl.org/dc/terms/>

        select ?categoria as ?{salida} FROM <http://dbpedia.org> 
        WHERE {{
        <{uri}>
        ?propiedad 
        ?categoria. 
        FILTER (regex(?propiedad, {propiedad})).

        }}'''

        self.queryJerarquia2='''PREFIX dct: <http://purl.org/dc/terms/>

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
        self.queryRecursosHijos='''PREFIX dct: <http://purl.org/dc/terms/>
        select ?recurso as ?{salida} where{{

        ?recurso
        ?propiedad 
        <{uri}> 


        FILTER (STRSTARTS(str(?propiedad), str({propiedad}))).

        }}'''

        self.queryVecinos1='''PREFIX dbr: <http://dbpedia.org/resource/>
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

        self.queryVecinos2='''PREFIX dbr: <http://dbpedia.org/resource/>
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


        self.queryCuentaPadres='''PREFIX dct: <http://purl.org/dc/terms/>
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

