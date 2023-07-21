from pyspark.sql import SparkSession, row

# We initiate a SparkSession instead of a SparkContext
spark = SparkSession.builder.appName("SparkSQL").getOrCreate()
inputData = spark.read.json("dataFile.json")

# We expose this as a 'virtual database table / view"
inputData.createOrReplaceTempView("myStructuredStuff")

# Then we can run sql commands
myResultDataFrame = spark.sql("SELECT * FROM tbl ORDER BY 1 ")