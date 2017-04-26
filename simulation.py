
from pyspark import SparkContext, SparkConf
from pyspark.mllib.classification import LogisticRegressionWithLBFGS
from pyspark.mllib.regression import LabeledPoint
import numpy as np
import math

conf = SparkConf().setMaster("yarn-client").setAppName("logistic").set("spark.executor.memory","1g")
sc = SparkContext(conf=conf)

n = 29
v = 4
p = 2**v-1
m = 8
r = 2**(n-m)

def generate(line):
    p = 15
    m = 2**8
    np.random.seed(line)
    x = np.random.normal(0, 1, [m, p])
    np.random.seed(line)
    #y = np.random.choice([0, 1], [m, 1])
    y = np.random.binomial(2,0.5,[m,1])
    dataframe = np.hstack((x, y))
    return dataframe

data = sc.parallelize(range(0,r), 100)

outputfile = "/wsc/song273/spark/data/n" + str(n)+"v"+str(v) + "m" + str(int(math.log(m,2)))
data.map(generate).saveAsTextFile(outputfile)
