
// load a csv file
val spark = org.apache.spark.sql.SparkSession.builder.
        master("yarn").
        appName("Spark CSV Reader").
        getOrCreate;

val df = spark.read.format("com.databricks.spark.csv").option("header", "false").option("inferSchema", "true").
                  load("/wsc/song273/try2.csv")        
df.first()
/////// layout of the data
// number of rows
df.count()
// number of columns
df.columns.size
df.printSchema()
df.show
// groupby the conditional variable, the last column, and count the number of entries of column "_c21"
df.groupBy("_c21").count().show
