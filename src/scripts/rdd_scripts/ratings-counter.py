# %%
"""
You cant create a spark context without SparkConf
SparkConf allows you to configure the spark context
With SparkConf you can configure things like single-computer or multi-cluster set up etc
"""
from pyspark import SparkConf, SparkContext
import collections

"""
Next, we create or Spark context
"""
# setMaster = local (single thread, single machine)
# providing a setAppName makes it clearer in the UI
conf = SparkConf().setMaster("local").setAppName("RatingsHistogram") 
# Then we use our SparkConf object to create our SparkContext
sc = SparkContext(conf=conf)

"""
Then we load up our data
"""
lines = sc.textFile("/home/src/raw_data/ml-100k/u.data")
"""
Then we process our data
"""
# This is extracting the 3rd 'column' from the text file and putting it in a new RDD called 'ratings'
ratings = lines.map(lambda x: x.split()[2])
# countByValue is a built-in method (Same as value_counts())
result = ratings.countByValue()

# Use simple python code to sort and print results
sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.items():
    print(f"{key}: {value}")




