## load the library
library(sparklyr)
library(dplyr)
library(magrittr)
## timing
sleep = 60
n=20
v=4
m=10
datapath = paste("/wsc/song273/pf10n/sparklyr/parquet/","n",n,"v",v,"m",m,sep="")
################################# start time ##################################
start_time <- Sys.time()

config <- spark_config()   # Create a config to tune memory

# no need to set spark_home, hadoop_conf_dir and so on, it is set on wsc cluster, you can check
# Sys.getenv('SPARK_HOME')
#Sys.setenv(SPARK_HOME = "/usr/lib/spark")
#Sys.setenv(HADOOP_CONF_DIR = '/usr/lib/hadoop/etc/hadoop')
#Sys.setenv(YARN_CONF_DIR = '/usr/lib/hadoop/etc/hadoop/conf')

#config$spark.executor.instances <- 2
## WSC cluster has 10 node, each node has 20 cores
config[["spark.r.command"]] <-"/apps/wsc/R/3.4.3_gcc-4.8.5_wsc/bin/R"
config$spark.executor.cores <- 20
config$spark.dynamicAllocation.enabled = TRUE
#config$spark.executor.memory <- "4G"
#config[["sparklyr.shell.driver-memory"]] <- "10G"   # Set driver memory to 10GB
# create the Spark context
sc <- spark_connect(master = "yarn-client", version = "2.2.0",config=config) # using custom configs
data_tbl = spark_read_parquet(sc,"data_tbl",datapath)
#result <- data_tbl %>% 
           #mutate(binary_y = as.numeric(activ == "Active")) %>%
           group_by(g)%>% 
           ml_logistic_regression(y~v1+v2+v3+v4+v5+v6+v7,fit_intercept =FALSE,family = "binomial")$Coefficients
coeffs = spark_apply(
  data_tbl,
  function(e) broom::tidy(glm.fit(y~v1+v2+v3+v4+v5+v6+v7,e,)$coef),
  #names = c("term", "estimate", "std.error", "statistic", "p.value"),
  group_by = "g"
)

coeffs


#################################### end time ################################
end_time <- Sys.time()
t = as.numeric(end_time) - as.numeric(start_time)
print(start_time)
print(end_time)
print(t)

## put the whole system on sleep
spark_disconnect(sc)
Sys.sleep(sleep)
