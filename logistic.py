
# cd /var/lib/spark
# ./bin/pyspark --master yarn-client --num-executors 5 --executor-memory 10g --executor-cores 5
# ./bin/pyspark --master yarn-client --num-executors 5 --executor-cores 5
from pyspark import SparkContext
#logFile = "$YOUR_SPARK_HOME/README.md"  # Should be some file on your system
#sc = SparkContext("local", "Simple App")
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
## huayi's code

# cd /var/lib/spark
# ./bin/pyspark --master yarn-client --num-executors 5 --executor-memory 10g --executor-cores 5
# ./bin/pyspark --master yarn-client --num-executors 5 --executor-cores 5
from pyspark import SparkContext, SparkConf
import random
from pyspark.mllib.classification import LogisticRegressionWithLBFGS
from pyspark.mllib.regression import LabeledPoint
import csv
from numpy import random as rd
import numpy as np

conf = SparkConf().setMaster("yarn-client").setAppName("logistic").set("spark.executor.memory","1g")
sc = SparkContext(conf=conf)
--master yarn-client --num-executors 5 --executor-cores 5


def generate(line):
    p = 15
    m = 2 ^ 8
    np.random.seed(line)
    x = np.random.normal(0, 1, [m, p])
    np.random.seed(line)
    y = np.random.choice(0, 1, [m, 1])
    dataframe = np.hstack((x, y))
    return dataframe





#logFile = "$YOUR_SPARK_HOME/README.md"  # Should be some file on your system
#sc = SparkContext("local", "Simple App")

# with open('numbers.csv', 'wb') as fp:
#  a = csv.writer(fp)
#         data = np.zeros((1, 1))
#         data[0, 0] = 1
#  for i in range(100):
#     a.writerows(data)
#                 data[0, 0] += 1
#
# data = sc.textFile("C:/Users/Justin/Desktop/Spark_BoxCox/numbers.csv")

data = sc.parallelize(range(0,100), 2)


genDataFrame = data.map(generate)

