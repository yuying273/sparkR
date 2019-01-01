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

## simulate data
value = generate(1,m,p)
outputfile1 = "/wsc/song273/spark/data/sequence/n" + str(n)+"v"+str(v) + "m" + str(m)
data = sc.parallelize(range(0,r))
data.map(lambda x: (x,value)).saveAsSequenceFile(outputfile1)

## simulate data, key is integer, value is array [1,2]
outputfile2 = "/wsc/song273/spark/data/sequence/n" + str(n)+"v"+str(v) + "m" + str(m)+"try2"
data = sc.parallelize(range(0,r))
data.map(lambda x: (x,(1,2))).saveAsSequenceFile(outputfile2)

## simulate data, key is integer, value is integer
outputfile3 = "/wsc/song273/spark/data/sequence/n" + str(n)+"v"+str(v) + "m" + str(m)+"try3"
data = sc.parallelize(range(0,r))
data.map(lambda x: (x,1)).saveAsSequenceFile(outputfile3)

######### after simulate the data
# load file: sequenceFile(path, keyClass, valueClass, minPartitions)
inFile = outputfile1
data = sc.sequenceFile(inFile,
  "org.apache.hadoop.io.IntWritable", "org.apache.hadoop.io.ArrayWritable") ## or org.apache.hadoop.io.ArrayWritable
## actions
data.first()
data.count()
data.take(3)
data.collect() ##use only the data size is small
## 
#One important parameter for parallel collections is
#the number of partitions to cut the dataset into. 
#Spark will run one task for each partition of the cluster.
#Typically you want 2-4 partitions for each CPU in your cluster. 
#Normally, Spark tries to set the number of partitions automatically based on your cluster. 
#However, you can also set it manually by passing it as a second parameter to parallelize (e.g. sc.parallelize(data, 10))                                                                      
map{case (x, y): (x, func(y))}             
map(lambda (x,y): (1, y)).mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
.map(lambda key, xy: (key, xy[0]/xy[1])).collectAsMap()
 
sumCount = nums.combineByKey((lambda x: (x,1)),
                             (lambda x, y: (x[0] + y, x[1] + 1)),
                             (lambda x, y: (x[0] + y[0], x[1] + y[1])))
r = sumCount.map(lambda key, xy: (key, xy[0]/xy[1])).collectAsMap()
print(*r) 

sumCount = nums.aggregate((0, 0),
               (lambda acc, value: (acc[0] + value, acc[1] + 1)),
               (lambda acc1, acc2: (acc1[0] + acc2[0], acc1[1] + acc2[1])))
return sumCount[0] / float(sumCount[1])


######### after simulate the data
# load file: sequenceFile(path, keyClass, valueClass, minPartitions)
data = sc.sequenceFile(inFile,
  "org.apache.hadoop.io.Text", "org.apache.hadoop.io.IntWritable") ## or org.apache.hadoop.io.ArrayWritable
## actions
data.first()
data.count()
data.take(3)
data.collect() ##use only the data size is small

## persisting or not?
n practice, you will often use persist() to load a subset of your data into memory and query it repeatedly. For example, if we knew that we wanted to compute multiple results about the README lines that contain Python, we could write the script shown in Example 3-4.

Example 3-4. Persisting an RDD in memory
>>> pythonLines.persist

>>> pythonLines.count()
2

>>> pythonLines.first()
u'## Interactive Python Shell'
To summarize, every Spark program and shell session will work as follows:

Create some input RDDs from external data.

Transform them to define new RDDs using transformations like filter().

Ask Spark to persist() any intermediate RDDs that will need to be reused.

Launch actions such as count() and first() to kick off a parallel computation, which is then optimized and executed by Spark.

