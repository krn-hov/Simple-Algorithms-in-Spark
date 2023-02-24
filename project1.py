#Karen Hovhannisyan
#Student Number: 015288040

from pyspark import SparkContext, SparkConf
import os
import sys
import time

#datasets path on shared group directory on Ukko2. Uncomment the one which you would like to work on.
dataset = "/wrk/group/grp-ddi-2021/datasets/data-1.txt"

conf = (SparkConf()
        .setAppName("spark_test")           ##change app name to your username
        .setMaster("spark://ukko2-10.local.cs.helsinki.fi:7077")
        .set("spark.cores.max", "10")  ##dont be too greedy ;)
        .set("spark.rdd.compress", "true")
        .set("spark.broadcast.compress", "true"))
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
data = sc.textFile(dataset)

data.cache()

print("getting count\n\n\n")
count = data.count()

data = data.map(lambda s: float(s))


##Question 1
print("getting max\n\n\n")
max = data.max()
print("getting min\n\n\n")
min = data.min()
print("getting mean\n\n\n")
avg = data.mean()
print("getting variance\n\n\n")
var = data.variance()

##Question 2


def partition(rdd,k,min,max,count):

	i = 1
	rdd = rdd
	min = min
	max = max
	k = k
	count = count
	
	upperBound = max
	start1 = time.time()

	while(True):

		start = time.time()
		print(f"\niteration {i} of partition\n")
		print(f"\n{count} floats left\n")

		if(i==5):
			a = 0.5 #using prior knowledge from previous run
		else:
			a = k/count
		
		b = 1-a
		pivot = a*max + b*min

		left = rdd.filter(lambda s: s < pivot).cache()
		right = rdd.filter(lambda s: s > pivot).cache()

		lcount = left.count()
		rcount = count - lcount
		
		

		if(lcount == k):
			print("Mission Accomplished")
			print("\nIt took this many seconds",time.time()-start1)
			return [left.max(),upperBound]            

		elif(lcount > k):
			i+=1
			max = left.max()
			rdd.unpersist()
			rdd = left			
			count = lcount
			upperBound = pivot
			print(time.time()-start)

		elif(lcount < k):
			i+=1
			min = right.min()
			rdd.unpersist()
			rdd = right
			k = k - lcount
			count = rcount
			print(time.time()-start)




if(count % 2 == 0):

	Med_Ind1 = count/2
	Med_Ind2 = Med_Ind1 + 1
	Med1,UB = partition(data,Med_Ind1,min,max,count)

	print(f"median1 = {Med1}\n-------------------------\n*********************************\n-----------------------------\n")

	data1 = data.filter(lambda s: (s>=Med1) and (s<=UB)).cache()
	UBcnt = data1.count()
	print(f"UB:{UB}, UBcnt:{UBcnt}")
	Med2 = partition(data1,2,Med1,UB,UBcnt)[0]

	print(f"median2 = {Med2}\n-------------------------\n*********************************\n-----------------------------\n")

	Med = (Med1+Med2)/2

else:

	Med_Ind = round(count/2)+1
	Med = partition(data,Med_Ind,min,max,count)


print (f"count = {count:.8f}\n")
print ("Question 1\n")
print (f"max = {max:.8f}")
print (f"min = {min:.8f}")
print (f"average = {avg:.8f}")
print (f"variance = {var:.8f}\n")
print ("Question 2\n")
print (f"median = {Med:.8f}")
