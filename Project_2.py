from pyspark import SparkContext, SparkConf
import os
import sys
import time

#datasets path on shared group directory on Ukko2. Uncomment the one which you would like to work on.
dataset = "/wrk/group/grp-ddi-2021/datasets/data-2.txt"

conf = (SparkConf()
        .setAppName("spark_test")           ##change app name to your username
        .setMaster("spark://ukko2-10.local.cs.helsinki.fi:7077")
        .set("spark.cores.max", "10")  ##dont be too greedy ;)
        .set("spark.rdd.compress", "true")
        .set("spark.broadcast.compress", "true"))

sc = SparkContext(conf=conf)

data = sc.textFile(dataset)

data = data.map(lambda row: [float(s) for s in row.split()])

def AxAtxA(d):
	
	d = d.zipWithIndex()
	
	A = d.flatMap(lambda K_V:[(j,("A",K_V[1],e)) for j,e in enumerate(K_V[0])]).cache() #RDD in form [column,(Matrix name, row, value)]
	
	join = A.join(A) #Pairs all elements that need to be multiplied with each other together

	pass1 = join.map(lambda K_V:((K_V[1][0][1],K_V[1][1][1]),K_V[1][0][2]*K_V[1][1][2])) #intermediate RDD of A*A^T in form [(row,column),value of A * value of A^T]
	
	pass2 = pass1.reduceByKey(lambda a,b: a+b) #Final RDD of A*A^T
	
	AxAt = pass2.map(lambda K_V:(K_V[0][1],("AxAt",K_V[0][0],K_V[1]))) #Repeat for second multiplication

	A2 = A.map(lambda K_V:(K_V[1][1],("A",K_V[0],K_V[1][2])))
	
	join2 = AxAt.join(A2)

	pass3 = join2.map(lambda K_V:((K_V[1][0][1],K_V[1][1][1]),K_V[1][0][2]*K_V[1][1][2]))
	
	pass4 = pass3.reduceByKey(lambda a,b: a+b)

	with open('result_row_1.txt','w') as f:
		for i in range(1000):
			f.write(f'{pass4.lookup((0,i))}')
			f.write(' ')


AxAtxA(data)
