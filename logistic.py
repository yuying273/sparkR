
from pyspark.mllib.classification import LogisticRegressionWithLBFGS
from pyspark.mllib.regression import LabeledPoint
from numpy import array
data = sc.textFile("/wsc/song273/sk/test/logistics2.txt")
data.count()
data.first()
def parsePoint(line):
  values = [float(x) for x in line.split(' ')]
  return LabeledPoint(values[127], values[0:127])

parsedData = data.map(parsePoint)
model = LogisticRegressionWithLBFGS.train(parsedData)
## 

