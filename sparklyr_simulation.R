# in hadoop cluster, the purdue wsc cluster:
### hadoop 2
### R: 3.4.3, system: x86_64, linux-gnu
# load the sparklyr package
library(sparklyr)
library(dplyr)
# create the Spark context
sc <- spark_connect(master = "yarn-client", version = "2.2.0")

