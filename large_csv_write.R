
n = 29
# number of predictor variables
p = 2^4-1
# log2 number of observations per subset
#m.vec = seq(8, 16, by=1)

m.vec =8


for(i in 1:r) { 
fitarima <- forecast.Arima(arima_fits$fit[[i]], h=32) 
write.table(fitarima,file="fitarima.csv", append=TRUE,sep=",",col.names=FALSE,row.names=FALSE) 
}
