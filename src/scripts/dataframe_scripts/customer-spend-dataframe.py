# %%
from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType


spark = SparkSession.builder.appName("CustomerTotals").getOrCreate()

schema = StructType(
    [
        StructField("customer_id", IntegerType(), nullable=True),
        StructField("item_id", IntegerType(), nullable=True),
        StructField("amount", FloatType(),  nullable=True),
    ]
)


# Load dataframe using a schema
data = spark.read.schema(schema).csv("/home/src/raw_data/customer-orders.csv")

# Group by customer id and sum by amount spent
result = data.groupBy("customer_id").agg(func.round(func.sum("amount"), 2).alias("total_spend")).sort("total_spend")

result.show(result.count())

# %%
