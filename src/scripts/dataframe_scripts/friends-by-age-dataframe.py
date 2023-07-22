# %%
from pyspark.sql import SparkSession
from pyspark.sql import functions as func



"""
Data:
id, name, age

Task:

For each age, calculate the average number of friends that age has.

"""


#  Initialise session
spark = SparkSession.builder.appName("FriendsByAgeDataFrame").getOrCreate()

# Import data
data = spark.read.option("header", "true").option("inferSchema", "true").csv('/home/src/raw_data/fakefriends-header.csv')

# Discard info we dont need
friends = data.select("age", "friends")

# Calculate average number of friends for a given age and format results and column name
friends = friends.groupBy("age").agg(func.round(func.avg("friends"), 2).alias('friends_avg')).sort("age")


friends.show()

spark.stop()
# %%
