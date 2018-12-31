
## load a csv file
val spark = org.apache.spark.sql.SparkSession.builder.
        master("yarn").
        appName("Spark CSV Reader").
        getOrCreate;

val df = spark.read.format("csv").option("header", "false").option("mode", "DROPMALFORMED").load("/wsc/song273/try2.csv")
val df2 = spark.read.format("com.databricks.spark.csv").option("header", "false").option("inferSchema", "true").
                  load("/wsc/song273/try2.csv")        
df.first()
// number of rows
df.count()
// number of columns
df.columns.size

