import re
from pyspark import SparkConf, SparkContext

def normalizeWords(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

input = sc.textFile("/home/src/raw_data/Book.txt")
words = input.flatMap(normalizeWords)
# For every word, add up every instance to get a total word count
wordCounts = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)
# Here, we are swapping keys with values so we can sort it in the way we want (sort by values)
wordCountsSorted = wordCounts.map(lambda x: (x[1], x[0])).sortByKey()
# execute using collect()
results = wordCountsSorted.collect()

for result in results:
    count = str(result[0])
    word = result[1].encode('ascii', 'ignore')
    if (word):
        print(word.decode() + ":\t\t" + count)
