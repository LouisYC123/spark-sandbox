# %%
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

# Read the header with .option("header", "true")
people = spark.read.option("header", "true").option("inferSchema", "true")\
    .csv("/home/src/raw_data/fakefriends-header.csv")


  

people.printSchema()

# %%
print("Let's display the name column:")
people.select("name").show()

# %%
print("Filter out anyone over 21:")
people.filter(people.age < 21).show()

# %%
print("Group by age")
people.groupBy("age").count().show()

# %%
print("Make everyone 10 years older:")
people.select(people.name, people.age + 10).show()

spark.stop()


# %%
