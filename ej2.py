# -*- coding: utf-8 -*-
"""
Gabriela Ballesteros Gómez 

Escribe un programa paralelo que calcule los 3-ciclos de un grafo que se encuentra denido
en múltiples ficheros de entrada.
"""
import sys
from pyspark import SparkContext
sc = SparkContext()

def arista(linea):
    linea_=linea.split(",") #le quitamos las comas, no hacemos strip proque no tiene espacios
    l1,l2= linea_[0], linea_[1]
    if l1<l2 : #las letras en python tienen un número representativo, así que las podemos ordenar 
        return l1,l2
    elif l1>l2: 
        return l2,l1
    else: #l1==l2
        pass 
        
#buscamos las aristas asociadas
def relacionadas(tupla):
    conex = []
    for i in range(len(tupla[1])):
        conex.append(((tupla[0],tupla[1][i]),'existe'))
        for j in range(i+1,len(tupla[1])):
            if tupla[1][i] < tupla[1][j]:
                conex.append(((tupla[1][i],tupla[1][j]),('pendiente',tupla[0])))
            else:
                conex.append(((tupla[1][j],tupla[1][i]),('pendiente',tupla[0])))
    return conex

#elegimos solo los que pueden ser triciclo 
def candidatos(tupla):
    return (len(tupla[1])>= 2 and 'existe' in tupla[1])  
   
#juntamos las ternas 
def ternas(tupla):
    triple = []
    for pos in tupla[1]:
        if pos != 'exists':
            triple.append((pos[1],tupla[0][0], tupla[0][1]))
    return triple
    
def ej2(sc, files):
    rdd = sc.parallelize([])
    for file in files:
        file_rdd = sc.textFile(file)
        rdd = rdd.union(file_rdd)
    adj = rdd.map(arista).filter(lambda x: x is not None).distinct()
    connected = adj.groupByKey().mapValues(list).flatMap(relacionadas)
    triciclos = connected.groupByKey().mapValues(list).filter(candidatos).flatMap(ternas)
    print(triciclos.collect())
    return triciclos.collect()

if __name__ == "__main__":
    lista = []
    if len(sys.argv) <= 2:
        print(f"python3 file")
    else:
        for i in range(len(sys.argv)):
            if i != 0:
                lista.append(sys.argv[i])
        ej2(sc,lista)
