#A better approach is to start from the pair RDD and then use the reduceByKey() transformation to create a new pair RDD.
# The reduceByKey() transformation gathers together pairs 
#that have the same key and applies the function provided to two values at a time, 
#iteratively reducing all of the values to a single value. 
#reduceByKey() operates by applying the function first within each partition on a per-key basis 
#and then across the partitions, allowing it to scale efficiently to large datasets.

## carry out the logistic regression and return the average value for coefficient

from pyspark import SparkContext, SparkConf
from pyspark.ml.regression import GeneralizedLinearRegression
from pyspark.mllib.regression import LabeledPoint
import numpy as np
import math

n = 29
v = 4
p = 2**v-1
m = 8
r = 2**(n-m)

def logisT(value):
  glr = GeneralizedLinearRegression(family="gaussian", link="identity", maxIter=10, regParam=0.3)
  # Fit the model
  model = glr.fit(value)
  # Print the coefficients and intercept for generalized linear regression model
  print("Coefficients: " + str(model.coefficients))
  print("Intercept: " + str(model.intercept))
  return (model.coefficients,1)


inputfile = "/wsc/song273/spark/data/n" + str(n)+"v"+str(v) + "m" + str(int(math.log(m,2)))
rdd = sc.textFile(inputfile)
#Every RDD has a fixed number of partitions that 
#determine the degree of parallelism to use when executing operations on the RDD.
coefs = rdd.mapValues(logisT).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))




## reduceByKey, familiar with the combiner concept from MapReduce
## calling reduceByKey() and foldByKey() will automatically perform combining locally
## on each machine before computing global totals for each key. The user does not need to specify a combiner.
The more general combineByKey() interface allows you to customize combining behavior.
rdd.mapValues(logisT).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))

##
