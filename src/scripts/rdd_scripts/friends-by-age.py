from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    age = int(fields[2])
    numFriends = int(fields[3])
    # we will return a tuple to .map(), which creates a key:value 
    return (age, numFriends)

# Parse raw data into 'lines' RDD
lines = sc.textFile("/home/src/raw_data/fakefriends.csv")
# Map our data using our parseLine() function, which creates a key:value RDD
rdd = lines.map(parseLine)
"""
output here is key:value pairs of age:numFriends

age, numFriends
33, 385
33, 2
55, 221
40, 465
"""
# Here, our mapValue() is creating tuples of the value and the number 1
"""
age, numFriends
33, (385, 1)
33, (2, 1)
55, (221, 1)
40, (465, 1)
"""
# Then the reduceByKey() takes the first element of our value tuples and 
# adds the 1st element of the next tuple, then does the same for the 2nd element of the tuple
# This process continues for every values of a given key
"""
age, (totalNumFriends, number of instances of the key)
33, (387, 2)
"""
totalsByAge = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))

# Then we calculate the average for every key
"""
age, averageFriends
33, 193.5
"""
averagesByAge = totalsByAge.mapValues(lambda x: x[0] / x[1])
"""
Remember, Spark is lazy, so nothing actually happens until the first action is called,
for example, collect()
"""
results = averagesByAge.collect()
# Then print our results
for result in results:
    print(result)
