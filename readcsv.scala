
// load a csv file
val spark = org.apache.spark.sql.SparkSession.builder.
        master("yarn").
        appName("Spark CSV Reader").
        getOrCreate;

val df = spark.read.format("com.databricks.spark.csv").option("header", "false").option("inferSchema", "true").
                  load("/wsc/song273/spark/data/csv/n30p15m9.csv")        
df.first()
/////// layout of the data
// number of rows
df.count()
// number of columns
df.columns.size
df.printSchema()
df.show
// groupby the conditional variable, the last column, and count the number of entries of column "_c16"
df.groupBy("_c16").count().show
// each level has 512 entries
// I have error messages:
df.write.format("parquet").option("path", "/wsc/song273").mode("overwrite").partitionBy("_c21").saveAsTable("myparquet")
//df_writer.partitionBy('col1')\
//         .saveAsTable('test_table', format='parquet', mode='overwrite',
//                      path='/wsc/song273')
//spark.read.parquet(...)
