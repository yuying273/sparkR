# Creating an RDD of strings with textFile() in Python
lines = sc.textFile("file:/var/lib/spark/README.md")
## two types of operation on RDD: transformation or action
# Calling the filter() transformation
pythonLines = lines.filter(lambda line: "Python" in line)
# Calling the first() action
pythonLines.first()
# you can define new RDDs any time, 
# but Spark computes them only in a lazy fashion—that is, the first time they are used in an action

# Persisting an RDD in memory
pythonLines.persist
pythonLines.count()
pythonLines.first()

# RDDs also have a collect() function to retrieve the entire RDD
# Keep in mind that your entire dataset must fit in memory on a single machine to use collect() on it,
# so collect() shouldn’t be used on large datasets.
# if only take several RDD: use take()
badLinesRDD.take(10)
# save the contents of an RDD using the saveAsTextFile() action, saveAsSequenceFile()
df.printSchema()

# in Python
from pyspark.sql import Row
from pyspark.sql.types import StructField, StructType, StringType, LongType
myManualSchema = StructType([
  StructField("some", StringType(), True),
  StructField("col", StringType(), True),
  StructField("names", LongType(), False)
])
myRow = Row("Hello", None, 1)
myDf = spark.createDataFrame([myRow], myManualSchema)
myDf.show()

https://stackoverflow.com/questions/50366604/pyspark-create-dataframe-from-random-uniform-disribution
https://stackoverflow.com/questions/48181221/convert-the-map-rdd-into-dataframe  

from pyspark.sql.functions import corr
df.stat.corr("Quantity", "UnitPrice") 
df.select(corr("Quantity", "UnitPrice")).show()

## statistics
df.describe().show()
#Be sure to search the API documentation for more information and functions.
