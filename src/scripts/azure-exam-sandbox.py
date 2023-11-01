# %%
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, to_timestamp

spark = SparkSession.builder.appName('azure_exam').getOrCreate()

data = [(1, '2021-07-30 09:35:00'), (2, '2021-07-31 10:15:00')]
df = spark.createDataFrame(data, ['id', 'session_date'])

df = (
    df.withColumn('date', to_date(col('session_date')))
    .withColumn('timestamp', to_timestamp(col('session_date')))
)

df.show()

# %%