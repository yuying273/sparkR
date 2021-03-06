
############################
## setup
library(iotools)
############################
# experiment name
#name <- "multi.factor"
# top level directory for experiment on HDFS
dir.exp = "/hadoop/mnt/wsc/song273/spark/data/csv/"
# directory for this experiment on HDFS
#dir.exp = file.path(dir, name)
# directory for local file system
dir.local = "/home/song273/performance/wsc/dr/result/10node/sparklyr/"

# break time in seconds between jobs
#sleep = 120

############################
## factors
############################

## subset factors 
# log2 number of observations
#n.c = 30
n.c = 30
# number of predictor variables
#v.c = c(4,7)
v.c = 4
# log2 number of observations per subset
#m.vec = seiq(8, 16, by=1)
#r.c = 23:15
r.c = 21
###############################
###############################
## generate csv file in hdfs
###############################
system.time(for(n in n.c){
  for(v in v.c){
    p = 2^v-1
    m.vec = n-r.c
    for(m in m.vec){
      dir.dm = paste(dir.exp,'n',n,'p',p,"m",m,".csv", sep="")
      r = 2^(n - m)
      m = 2^m
      set.seed(1)
      value = as.data.frame(matrix(c(rnorm(m*p), sample(c(0,1), m, replace=TRUE),rep(i,m)), ncol=p+2))
      for(i in 1:r) { 
        #set.seed(r)
        #value = as.data.frame(matrix(c(rnorm(m*p), sample(c(0,1), m, replace=TRUE),rep(i,m)), ncol=p+2))
        ## not working on writing to hdfs
        write.csv.raw(value,file=dir.dm, append=TRUE,col.names=FALSE) 
        print(i)
      }
      #Sys.sleep(time=sleep)
    }
  }
})


