"""Filtering RDDs"""
from pyspark import SparkConf, SparkContext

# Create the spark context
conf = SparkConf().setMaster("local").setAppName("MinTemperatures")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    stationID = fields[0]
    entryType = fields[2]
    temperature = float(fields[3]) * 0.1 * (9.0 / 5.0) + 32.0
    return (stationID, entryType, temperature)

# Load up our data
lines = sc.textFile("/home/src/raw_data/1800.csv")

# Extract 3 columns we need
parsedLines = lines.map(parseLine)

# Allow "TMIN" 'entryType' entries and remove everything else
minTemps = parsedLines.filter(lambda x: "TMIN" in x[1])

stationTemps = minTemps.map(lambda x: (x[0], x[2]))
"""
(stationID, temperature)
"""
# Traverse through every value:nextValue pair, and select the minimum of the two 
# This gives us the minimum temperature for each weather station
minTemps = stationTemps.reduceByKey(lambda x, y: min(x,y))
# remember, nothing happens until we run .collect()
results = minTemps.collect()

for result in results:
    print(result[0] + "\t{:.2f}F".format(result[1]))
