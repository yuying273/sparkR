# in hadoop cluster, the purdue wsc cluster:
# The performance as usually depends on the hardware you have and the software settings.
### hadoop 2
### R: 3.4.3, system: x86_64, linux-gnu
# load the sparklyr package
library(sparklyr)
library(dplyr)
config <- spark_config()   # Create a config to tune memory

# no need to set spark_home, hadoop_conf_dir and so on, it is set on wsc cluster, you can check
# Sys.getenv('SPARK_HOME')
#Sys.setenv(SPARK_HOME = "/usr/lib/spark")
#Sys.setenv(HADOOP_CONF_DIR = '/usr/lib/hadoop/etc/hadoop')
#Sys.setenv(YARN_CONF_DIR = '/usr/lib/hadoop/etc/hadoop/conf')

config$spark.executor.instances <- 2 
config$spark.executor.cores <- 5
#config$spark.executor.memory <- "4G"
#config[["sparklyr.shell.driver-memory"]] <- "10G"   # Set driver memory to 10GB
# create the Spark context
sc <- spark_connect(master = "yarn-client", version = "2.2.0",config=config) # using custom configs

#### read the data in
data_tbl = spark_read_csv(sc, "data", "/wsc/song273/pf10n/sparklyr/data/n20v4m10.csv", header = TRUE)
# returns a reference to a Spark DataFrame which can be used as a dplyr table (tbl)
## more details about function spark_read_csv :https://github.com/rstudio/sparklyr/issues/315
## spark_read_csv: with spark_csv_read, data is not loaded into R bur rather into the Java VM. 
#In this case, when the Java process crashes, is also taking down the R session #
data_tbl
# Source:   table<data> [?? x 17]
# Database: spark_connection
#       v0     v1     v2     v3      v4     v5      v6     v7     v8      v9
#    <dbl>  <dbl>  <dbl>  <dbl>   <dbl>  <dbl>   <dbl>  <dbl>  <dbl>   <dbl>
# 1  1.76   0.400  0.979  2.24   1.87   -0.977  0.950  -0.151 -0.103  0.411 
# 2  0.334  1.49  -0.205  0.313 -0.854  -2.55   0.654   0.864 -0.742  2.27  
# 3  0.155  0.378 -0.888 -1.98  -0.348   0.156  1.23    1.20  -0.387 -0.302 
# 4 -0.438 -1.25   0.777 -1.61  -0.213  -0.895  0.387  -0.511 -1.18  -0.0282
# 5 -0.672 -0.360 -0.813 -1.73   0.177  -0.402 -1.63    0.463 -0.907  0.0519
# 6 -0.685 -0.871 -0.579 -0.312  0.0562 -1.17   0.901   0.466 -1.54   1.49  
# 7 -0.403  1.22   0.208  0.977  0.356   0.707  0.0105  1.79   0.127  0.402 
# 8  1.94  -0.414 -0.747  1.92   1.48    1.87   0.906  -0.861  1.91  -0.268 
data_tbl %>%
  group_by(g)

spark_write_parquet(data_tbl,"/wsc/song273/pf10n/sparklyr/parquet/n20v4m10")


#data_tbl = spark_read_parquet(sc,"groupdata2","/wsc/song273/pf10n/sparklyr/parquet/n20v4m10")
path = "/wsc/song273/pf10n/sparklyr/"
for(m in c(9,11,14,15)){
  inputpath = paste(path,"data/n30v4m",m,".csv",sep="")
  data_tbl = spark_read_csv(sc, "data", inputpath, header = TRUE)
  outputpath = paste(path,"parquet/n30v4m",m,seq="")
  spark_write_parquet(data_tbl,outputpath)
}
