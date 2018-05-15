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
config[["spark.r.command"]] <- "/home/dgc/Rscript"
config$spark.executor.instances <- 199
#config$spark.executor.cores <- 20
config$spark.shuffle.service.enabled = TRUE
config$spark.dynamicAllocation.enabled = TRUE
#config$spark.executor.memory <- "4G"
#config[["sparklyr.shell.driver-memory"]] <- "10G"   # Set driver memory to 10GB
# create the Spark context
sc <- spark_connect(master = "yarn", version = "2.2.0",config=config) # using custom configs
start_time <- Sys.time()
data_tbl = spark_read_parquet(sc,"data_tbl",datapath)
#result <- data_tbl %>% 
           #mutate(binary_y = as.numeric(activ == "Active")) %>%
#           group_by(g)%>% 
#           ml_logistic_regression(y~v0+v1+v2+v3+v4+v5+v6+v7+v8+v9+v10+v11+v12+v13+v14,fit_intercept =FALSE,family = "binomial")$Coefficients

coeffs = spark_apply(
  data_tbl,
  function(e){glm(y~v0+v1+v2+v3+v4+v5+v6+v7+v8+v9+v10+v11+v12+v13+v14,family=binomial(),data=e)$coef},
  #function(e) broom::tidy(glm.fit(y~v0+v1+v2+v3+v4+v5+v6+v7+v8+v9+v10+v11+v12+v13+v14,e,family=binomial())$coef),
  names = c(paste("v",0:14,sep="")),
  group_by = "g"
)%>% 
    summarize_all(mean)

coeffs <- data_tbl %>% 
    spark_apply(
               function(e){head(e,1)},
               #names = c("term", "estimate", "std.error", "statistic", "p.value"),
               group_by = "g") %>% 
    summarize_all(mean)



#################################### end time ################################
end_time <- Sys.time()
t = as.numeric(end_time) - as.numeric(start_time)
print(start_time)
print(end_time)
print(t)

## put the whole system on sleep
spark_disconnect(sc)
Sys.sleep(sleep)
