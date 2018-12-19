# cd /var/lib/spark
# ./bin/pyspark --master yarn-client --num-executors 5 --executor-memory 10g --executor-cores 5
# ./bin/pyspark --master yarn-client --num-executors 5 --executor-cores 5

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

def generate(line,m,p):
    p = p
    m = 2**m
    np.random.seed(line)
    x = np.random.normal(0, 1, [m, p])
    np.random.seed(line)
    #y = np.random.choice([0, 1], [m, 1])
    y = np.random.binomial(2,0.5,[m,1])
    dataframe = list(np.hstack((x, y)))
    dataframe = [list(element) for element in dataframe]
    dataframe = tuple([tuple([float(ele) for ele in element]) for element in dataframe])
    return(dataframe)

value = generate(1,m,p)
outputfile1 = "/wsc/song273/spark/data/sequence/n" + str(n)+"v"+str(v) + "m" + str(m)
# a1 = data.map(generate)
# a1.count()
# a1.collect()
data.map(lambda x: (x,value)).saveAsSequenceFile(outputfile1)
## 
#One important parameter for parallel collections is
#the number of partitions to cut the dataset into. 
#Spark will run one task for each partition of the cluster.
#Typically you want 2-4 partitions for each CPU in your cluster. 
#Normally, Spark tries to set the number of partitions automatically based on your cluster. 
#However, you can also set it manually by passing it as a second parameter to parallelize (e.g. sc.parallelize(data, 10))                                                                      
                                                                                                                 
                                                                                                                 
                                                                                                                 
