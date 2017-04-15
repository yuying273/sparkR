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
write.table(value,"logistic.csv", sep = ",", col.names = FALSE, row.names=FALSE)

library(Rhipe)
rhinit()
rhput("logistic.csv", "/wsc/song273/sk/test/logistic.csv")
##########################################################
#2. data csv 
rst = read.df("/wsc/song273/sk/test/logistics2.txt",source = "text")
df <- read.df("/wsc/song273/sk/test/logistic.csv", "csv", header = "false", inferSchema = "true", na.strings = "NA")
model <- glm( v127 ~ v0, data = df, family = "binomial")
names(df) = paste("v", 0:127, sep="")
model <- glm( v127 ~ ., data = df, family = "binomial")   
head(df)
#  notes: http://spark.apache.org/docs/latest/sparkr.html
#  notes: https://spark.apache.org/docs/latest/ml-classification-regression.html
# When Spark is configured with YARN, the default file system used is HDFS. 
# If you want to read a file from your local disk, try:
# people <- read.df("file:/usr/lib/spark/examples/src/main/resources/people.json", "json”)

library(SparkR, lib.loc ="spark/R/lib/")
sparkEnvir <- list(spark.num.executors='5', spark.executor.cores='5')
sparkR.session(master ="yarn-client",sparkHome="spark",sparkConfig=sparkEnvir)
# http://spark.apache.org/docs/preview/running-on-yarn.html
