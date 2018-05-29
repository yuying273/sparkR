
from pyspark import SparkContext, SparkConf
from pyspark.mllib.classification import LogisticRegressionWithLBFGS
from pyspark.mllib.regression import LabeledPoint
import numpy as np
import math
import time

n = 30
v = 4
p = 2**v-1
m = 9
r = 2**(n-m)

def generate(line,m=m,p=p):
    p = p
    m = 2**m
    np.random.seed(line)
    x = np.random.normal(0, 1, [m, p])
    np.random.seed(line)
    #y = np.random.choice([0, 1], [m, 1])
    y = np.random.binomial(2,0.5,[m,1])
    dataframe = np.hstack((x, y))
    return dataframe

## 
#One important parameter for parallel collections is
#the number of partitions to cut the dataset into. 
#Spark will run one task for each partition of the cluster.
#Typically you want 2-4 partitions for each CPU in your cluster. 
#Normally, Spark tries to set the number of partitions automatically based on your cluster. 
#However, you can also set it manually by passing it as a second parameter to parallelize (e.g. sc.parallelize(data, 10))
start = time.time()
#data = sc.parallelize(range(0,r), 100)
data = sc.parallelize(range(0,r)) ## take 16 minutes to create data ## 
# data.count()
# data.collect() # only if the data can be fit into the memory
outputfile = "/wsc/song273/spark/data/n" + str(n)+"v"+str(v) + "m" + str(m)+"ver"+str(2)
#outputfile1 = "/wsc/song273/spark/data/sequence/n" + str(n)+"v"+str(v) + "m" + str(m)
# a1 = data.map(generate)
# a1.count()
# a1.collect()
data.map(generate).saveAsTextFile(outputfile)
end_time = time.time()
print(end_time - start)
#saveAsSequenceFile(path, compressionCodecClass=None)
#data.map(generate).saveAsSequenceFile(outputfile)
#saveAsPickleFile(path, batchSize=10)
rdd = sc.parallelize([
    "2,Fitness", "3,Footwear", "4,Apparel"
])
rdd.map(lambda x: tuple(x.split(",", 1))).saveAsSequenceFile("testSeq")
# try key-value pair form
data.map(lambda x: (x,generate(x))).saveAsTextFile(outputfile)

