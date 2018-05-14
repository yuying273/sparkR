## load the library
library(sparklyr)
library(dplyr)

## timing
sleep = 60

## start time
start_time <- Sys.time()
data_tbl = spark_read_parquet(sc,"groupdata2","/wsc/song273/pf10n/sparklyr/parquet/n20v4m10")





## end time
end_time <- Sys.time()
t = as.numeric(end_time) - as.numeric(start_time)
print(start_time)
print(end_time)
print(t)

## put the whole system on sleep
Sys.sleep(sleep)
