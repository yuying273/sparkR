###################################
# start sparkR process in cluster #
###################################
# module add spark/2.0.0
# sparkR or pyspark
###################################
# task: logistic regression in R  #
###################################
#1. simulate data
n <- 2^20
p <- 2^7 - 1
# so value is a 1gb data.frame
value <- matrix(c(rnorm(n*p), sample(c(0,1), n, replace=TRUE)), ncol=p+1)


rst = read.df("/wsc/song273/sk/test/logistics2.txt",source = "text")
#  notes: http://spark.apache.org/docs/latest/sparkr.html
#  notes: https://spark.apache.org/docs/latest/ml-classification-regression.html
# When Spark is configured with YARN, the default file system used is HDFS. 
# If you want to read a file from your local disk, try:
# people <- read.df("file:/usr/lib/spark/examples/src/main/resources/people.json", "json”)

