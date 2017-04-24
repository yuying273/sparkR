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
