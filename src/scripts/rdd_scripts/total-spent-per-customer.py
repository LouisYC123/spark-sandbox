"""
customer_id, item_id, amount_spent_on_item

total spent per customer
"""
from pyspark import SparkConf, SparkContext

# Create spark context
config = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf=config)
sc.setLogLevel("WARN")


def parse_line(line):
    fields = line.split(',')
    return (int(fields[0]), float(fields[2]))


# Read data and split each comma-delimited line into fields
input = sc.textFile('/home/src/raw_data/customer-orders.csv')
parsedLines = input.map(parse_line)

# Map each line to key/value pairs of customer ID and $ amount and
# use reduceByKey to add up amount spent by customer ID
customerAmount = parsedLines.reduceByKey(lambda x, y: round((x + y), 2))
customerAmount = customerAmount.map(lambda x: (x[1], x[0])).sortByKey()

# collect() the results and print them
results = customerAmount.collect()

for result in results:
    print(result)




