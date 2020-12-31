import pandas as pd
from SPARQLWrapper import SPARQLWrapper, JSON
from prettytable import PrettyTable
from querys import Querys


def get_conceptos_originales(ruta, clases, numero_conceptos):
    conceptos= pd.read_csv(ruta,encoding='ISO-8859-1')
    filtro=conceptos.university_id.isin(clases)

    conceptos_dalgo= conceptos[filtro]
    conceptos_dalgo=conceptos_dalgo.sort_values(by='confidence_score', ascending=False)
    conceptos_dalgo=conceptos_dalgo.filter(['university_id', 'entity_id', 'confidence_score'])
    conceptos_dalgo=conceptos_dalgo.dropna()

    conceptos_dalgo=conceptos_dalgo.head(numero_conceptos)
    return conceptos_dalgo['entity_id']

def get_uris(conceptos_dalgo):
    
    uri_template='http://dbpedia.org/resource/{concepto}'
    uris=[]
    for i in conceptos_dalgo:
        concepto=i.replace(' ','_')
        uris.append(uri_template.format(concepto=concepto))
    return uris

def get_conceptos_vecinos(uris,path, anteriores):
    expansion={}
    for uri in uris:
        query=queryTemplates.queryVecinos1.format(uri=uri) if path==1 else queryTemplates.queryVecinos2.format(uri=uri)
        result=send_query(query)
        for res in result:
            entity=res['recurso']['value']
            entity=entity.split('/')[-1]
            entity=entity.replace('_',' ')
            if entity not in anteriores:
                expansion[entity]=expansion.get(entity,0)+1
    ans={k:v for k, v in sorted(expansion.items(), key=lambda item: item[1], reverse=True)}
    return ans


def get_categorias_jerarquicos(uris, anteriores):
   
    expansion={}
    for uri in uris:
        query_categorias=queryTemplates.queryPadres.format(uri=uri, propiedad='dct:subject', salida='category')
        result= send_query(query_categorias)
        for res in result:
            categoria=res['category']['value']
            categoria=categoria.split('/')[-1]
            categoria=categoria.split(':')[-1]
            categoria=categoria.replace('_',' ')
            if categoria not in anteriores:
                expansion[categoria]=expansion.get(categoria,0)+1         
    ans={k:v for k, v in sorted(expansion.items(), key=lambda item: item[1], reverse=True)}
    return ans

def get_categorias_jerarquicos2(uris, anteriores_ab, anteriores_ti, anteriores_her):
    abuelos={}
    tios={}
    hermanos={}
    for uri in uris:
        query_categorias=queryTemplates.queryJerarquia2.format(uri=uri, propiedad='dct:subject', cat2='abuelo', cat3='tio', con1='hermano', propiedad1='dct:subject', propiedad2='skos:broader')
        
        result= send_query(query_categorias)
        for res in result:
            if 'abuelo' in res:
                abuelo=res['abuelo']['value']
                abuelo=abuelo.split('/')[-1]
                abuelo=abuelo.split(':')[-1]
                abuelo=abuelo.replace('_',' ')
                if abuelo not in anteriores_ab:
                    abuelos[abuelo]=abuelos.get(abuelo,0)+1
            elif 'tio' in res:
                tio=res['tio']['value']
                tio=tio.split('/')[-1]
                tio=tio.split(':')[-1]
                tio=tio.replace('_',' ')
                if tio not in anteriores_ti:
                    tios[tio]=tios.get(tio,0)+1
            elif 'hermano' in res:
                hermano=res['hermano']['value']
                hermano=hermano.split('/')[-1]
                hermano=hermano.split(':')[-1]
                hermano=hermano.replace('_',' ')
                if hermano not in anteriores_her:
                    hermanos[hermano]=hermanos.get(hermano,0)+1
    ans1={k:v for k, v in sorted(abuelos.items(), key=lambda item: item[1], reverse=True)}
    ans2={k:v for k, v in sorted(tios.items(), key=lambda item: item[1], reverse=True)}
    ans3={k:v for k, v in sorted(hermanos.items(), key=lambda item: item[1], reverse=True)}
    return (ans1,ans2,ans3)

def get_clases_jerarquicas(uris,path):
    expansion=set()
    for uri in uris:
        query_categorias=queryTemplates.queryPadres.format(uri=uri, propiedad='rdf:type', salida='class')
        result= send_query(query_categorias)
        for res in result:
            categoria=res['class']['value']
            categoria=categoria.split('/')[-1]
            categoria=categoria.split(':')[-1]
            expansion.add(categoria)
    return expansion

def get_conceptos_hijos(uris,path):
    expansion=set()
    for uri in uris:
        entity=uri.split('/')[-1]
        uri='http://dbpedia.org/resource/Category:'+entity
        query_categorias=queryTemplates.queryRecursosHijos.format(uri=uri, propiedad='dct:subject', salida='recurso')
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



def generate_report(ruta, clases, nums, path, jerarquica):
    originals= get_conceptos_originales(ruta, clases, 24)
    conceptosAnteriores=set(originals)
    abuelosAnteriores=set(originals)
    tiosAnteriores=set(originals)
    hermanosAnteriores=set(originals)

    for num in nums:
        orig = PrettyTable()
        #get conceptos
        originals= get_conceptos_originales(ruta, clases, num)
        uris= get_uris(originals)
        
        salida_template='./result/{jerarquica}_{clases}_path{path}.txt'
        salida= salida_template.format( jerarquica='jerarquica',clases=clases, path=path) if jerarquica else salida_template.format(jerarquica='asociativa',clases=clases, path=path) 
        
        with open(salida, 'a+',encoding="utf-8") as file:
            file.write('**Expansion de {num}**\n'.format(num=num))
            orig.add_column("Originals", list(originals))
            file.write(orig.get_string()+'\n')

        if (not jerarquica) :
            expansion=get_conceptos_vecinos(uris,path,conceptosAnteriores)
            conceptosAnteriores.update(expansion.keys())   
            write_file(salida, 'Expansion ', expansion.keys())
        else:
            if path==1:
                expansion= get_categorias_jerarquicos(uris,conceptosAnteriores)
                conceptosAnteriores.update(expansion.keys())
                write_file(salida, 'Expansion ', expansion.keys())
            elif path==2:
                (abuelos,tios,hermanos)= get_categorias_jerarquicos2(uris,abuelosAnteriores,tiosAnteriores,hermanosAnteriores)
                write_file(salida, 'Abuelos ', abuelos.keys())
                write_file(salida, 'Tios ', tios.keys())
                write_file(salida, 'Hermanos ', hermanos.keys())

                abuelosAnteriores.update(abuelos.keys()) 
                tiosAnteriores.update(tios.keys()) 
                hermanosAnteriores.update(hermanos.keys()) 

def generate_csv(ruta, clases, nums, path, jerarquica):
    #get conceptos
    #Saco todos los conceptos para comparar 
    originals= get_conceptos_originales(ruta, clases, 24)
    conceptosAnteriores=set(originals)
    abuelosAnteriores=set(originals)
    tiosAnteriores=set(originals)
    hermanosAnteriores=set(originals)
    for num in nums:
        originals= get_conceptos_originales(ruta, clases, num)
        uris= get_uris(originals)
        salida_template='./result/{jerarquica}/path{path}/num{num}_{clases}_{tipo}.csv'
        
        
        if not jerarquica:
            if num==24:
                salidaOriginals=salida_template.format(clases=clases, path=path,num=num,tipo='originales', jerarquica='asociativa') 
                write_csv(salidaOriginals, ['Concepto'], [originals])
            salida=salida_template.format(clases=clases, path=path,num=num,jerarquica='asociativa', tipo='expansion')
            expansion=get_conceptos_vecinos(uris,path, conceptosAnteriores)
            conceptosAnteriores.update(expansion.keys())   
            write_csv(salida, ['Concepto','Frecuencia'], [expansion.keys(),expansion.values()])
        else:
            salidaJerarquica=salida_template.format(clases=clases, path=path,num=num,tipo='{nombre}expansion', jerarquica='jerarquica')
            if num==24:
                salidaOriginals=salida_template.format(clases=clases, path=path,num=num,tipo='originales', jerarquica='jerarquica') 
                write_csv(salidaOriginals, ['Concepto'], [originals])
            if path ==1:
                salida=salidaJerarquica.format(nombre='')
                expansion=get_categorias_jerarquicos(uris, conceptosAnteriores)
                conceptosAnteriores.update(expansion.keys())   
                write_csv(salida, ['Concepto','Frecuencia'], [expansion.keys(),expansion.values()])
            elif path ==2:
                (abuelos,tios,hermanos)= get_categorias_jerarquicos2(uris, abuelosAnteriores, tiosAnteriores,hermanosAnteriores)
                salida=salidaJerarquica.format(nombre='abuelos')
                write_csv(salida, ['Abuelo','Frecuencia'], [abuelos.keys(),abuelos.values()])
                salida=salidaJerarquica.format(nombre='tios')
                write_csv(salida, ['Tio','Frecuencia'], [tios.keys(),tios.values()])
                salida=salidaJerarquica.format(nombre='hermanos')
                write_csv(salida, ['Hermano','Frecuencia'], [hermanos.keys(),hermanos.values()])
                
                abuelosAnteriores.update(abuelos.keys()) 
                tiosAnteriores.update(tios.keys()) 
                hermanosAnteriores.update(hermanos.keys()) 
               
        
def write_csv(salida,names, expansions):
   
    df= pd.DataFrame(columns=names)
    for name,expansion in zip(names,expansions):
        df[name]=expansion
    df.to_csv(salida,index=False)


   
def write_file(salida,  name, expansion):
    expan = PrettyTable()
    with open(salida, 'a+',encoding="utf-8") as file:
        expan.add_column(name, list(expansion))
        file.write(expan.get_string()+'\n')
        file.write('# of New Concepts: '+ str(len(expansion))+'\n\n')


queryTemplates= Querys()
sparql = SPARQLWrapper("http://localhost:8890/sparql")

generate_csv("./conceptos_felipe.csv",['ISIS1105'], [5,10,15,24],1,False)
generate_csv("./conceptos_felipe.csv",['ISIS1105'], [5,10,15,24],2,False)

generate_csv("./conceptos_felipe.csv",['ISIS1105'], [5,10,15,24],1,True)
generate_csv("./conceptos_felipe.csv",['ISIS1105'], [5,10,15,24],2,True)



generate_report("./conceptos_felipe.csv",['ISIS1105'], [5,10,15,24],1,False)
generate_report("./conceptos_felipe.csv",['ISIS1105'], [5,10,15,24],2,False)

generate_report("./conceptos_felipe.csv",['ISIS1105'],[5,10,15,24],1,True)
generate_report("./conceptos_felipe.csv",['ISIS1105'], [5,10,15,24],2,True)



