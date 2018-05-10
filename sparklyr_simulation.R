# in hadoop cluster, the purdue wsc cluster:
# The performance as usually depends on the hardware you have and the software settings.
### hadoop 2
### R: 3.4.3, system: x86_64, linux-gnu
# load the sparklyr package
library(sparklyr)
library(dplyr)
config <- spark_config()                            # Create a config to tune memory
config[["sparklyr.shell.driver-memory"]] <- "10G"   # Set driver memory to 10GB
#Sys.setenv(SPARK_HOME = "/usr/local/spark")
#Sys.setenv(HADOOP_CONF_DIR = '/usr/local/hadoop/etc/hadoop/conf')
#Sys.setenv(YARN_CONF_DIR = '/usr/local/hadoop/etc/hadoop/conf')

config$spark.executor.instances <- 4
config$spark.executor.cores <- 4
config$spark.executor.memory <- "4G"

file <- gsub("s3n://commoncrawl/",                  # mapping the S3 bucket url
             "http://commoncrawl.amazonaws.com/",   # into a adownloadable url
             sparkwarc::cc_warc(1)), "warc.gz")     # from the first archive file

# create the Spark context
sc <- spark_connect(master = "yarn-client", version = "2.2.0",config=config) # using custom configs

