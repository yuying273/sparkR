#A better approach is to start from the pair RDD and then use the reduceByKey() transformation to create a new pair RDD.
# The reduceByKey() transformation gathers together pairs 
#that have the same key and applies the function provided to two values at a time, 
#iteratively reducing all of the values to a single value. 
#reduceByKey() operates by applying the function first within each partition on a per-key basis 
#and then across the partitions, allowing it to scale efficiently to large datasets.

## carry out the logistic regression and return the average value for coefficient
rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
