# %%
from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, LongType

spark = SparkSession.builder.appName("PopularMovies").getOrCreate()

# Create schema when reading u.data
schema = StructType([ \
                     StructField("userID", IntegerType(), True), \
                     StructField("movieID", IntegerType(), True), \
                     StructField("rating", IntegerType(), True), \
                     StructField("timestamp", LongType(), True)]
                     )

# Load up movie data as dataframe
# note: tab seperated, not comma
moviesDF = spark.read.option("sep", "\t").schema(schema).csv("/home/src/raw_data/ml-100k/u.data")

# Some SQL-style magic to sort all movies by popularity in one line!
# order by descending 'count'
topMovieIDs = moviesDF.groupBy("movieID").count().orderBy(func.desc("count"))

# Grab the top 10
topMovieIDs.show(10)

# Stop the session
spark.stop()

# %%
