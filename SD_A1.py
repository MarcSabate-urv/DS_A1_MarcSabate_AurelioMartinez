#!/bin/env python

import numpy as np
import time as t
import pickle
import pywren_ibm_cloud as pywren
from cos_backend import COSBackend
#columnas matriz 2
l = 100

#filas matriz 1
n = 100
#m columnas 1 filas 2
m = 10000

rang = 100
work = 5


def incializar(n, m, l, rang, work):
    cos = COSBackend()
    iterdata = []
    num = work
    if (num > n or num > l) and num != n * l: # si es valor no valido (mayor que los validos, diferente a su multiplo)
        num = n
        num2 = l
    else:
        num2 = num

    matrizA = [[(np.random.randint(rang)) for i in range(m)] for j in range(n)]
    matrizB = [[(np.random.randint(rang)) for i in range(l)] for j in range(m)]

    array = np.array_split(matrizA, num)
    array2 = np.array_split(np.transpose(matrizB), num2)
    for i in range(num):
        name = "fil" + str(i)
        cos.put_object('depositoaurelio', name,
                       pickle.dumps(array[i], pickle.HIGHEST_PROTOCOL))
    for j in range(num2):
        name = "col" + str(j)
        cos.put_object('depositoaurelio', name,
                       pickle.dumps(np.transpose(array2[j]), pickle.HIGHEST_PROTOCOL))

    if ( work == (l*n) ):
        for i in range(num):
            for j in range(num2):
                array = []
                array.append("fil" + str(i))
                array.append("col" + str(j))
                iterdata.append([array])
    else:
        for i in range(num):
            array = []
            for j in range(num2):
                array.append("fil" + str(i))
                array.append("col" + str(j))
            iterdata.append([array])

    return iterdata


def mult(array):
    result = []
    cos = COSBackend()
    for i in range(len(array)):
        if (i % 2) != 0:
            continue
        matrix1 = cos.get_object('depositoaurelio', array[i])
        matrix1 = pickle.loads(matrix1)
        matrix2 = cos.get_object('depositoaurelio', array[i + 1])
        matrix2 = pickle.loads(matrix2)
        result = np.append(result, np.dot(matrix1, matrix2))
    return result


def ensamblar(results):
    global n,l,work
    cos = COSBackend()
    if (n%work != 0 or l%work != 0)  and (work < l or work < n):
        array = []
        for result in results:
            array =  np.append(array,result)
        final = np.reshape(array, (n, l))
    else:
        final = np.reshape(results, (n, l))
    cos.put_object('depositoaurelio', 'matrizFinal',
                   pickle.dumps(final, pickle.HIGHEST_PROTOCOL))
    return final

if __name__ == "__main__":
    cos = COSBackend()


    pw = pywren.ibm_cf_executor()
    params = [n, m, l, rang, work]
    pw.call_async(incializar, params)
    iterdata = pw.get_result()
    inicio = t.time()
    pw.map_reduce(mult, iterdata, ensamblar)
    pw.wait()
    pasado = t.time() - inicio
    print(pw.get_result())
    print("Tiempo: {0:2f} secs.".format(pasado))