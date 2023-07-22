# Intro to Spark

- Spark projects can be written in Python, Scala or Java (Spark is written in Scala)  

- The driver program is the script you write for data processing (i.e - your normal python script)  

- Spark will take the work defined in your script and farm it out to different computer nodes or different CPUs on your own machine  

- It does this using a Cluster Manager.  

- Spark has its own built-in default Cluster Manager  

- It also has a hadoop cluster manager called YARN  

- You can use Amazon EMR (Elastic Map Reduce)

- The Cluster Manager will have in-built fautl tolerance management  

- Spark has a lot in common with Map Reduce but it is faster.  

- Spark utilizes a DAG Engine to optimize workflow  

- Spark is built around the idea of the RDD: Resilient Distributed Dataset

- Componenets of Spark:  

        - Spark Core
        - Spark SQL
        - MLLib
        - GraphX


## The Resilient Distributed Dataset

- Your cluster manager handles all of the details of managing RDDs
- When writing a script, you need 'Spark Context' object  
- This will be created by your driver program  
- It is resonsible for making RDDs resilient and distributed
- The spark shell creates an 'sc' object for you  
- The sc object provides you with methods to create an RDD

Some common operations on RDDs are:
- map  
- flatmap  
- filter  
- distinct  
- sample  
- union, intersection, subtract, cartesian

An RDD also has methods such as:
- collect
- count
- countByValue
- take
- top
- reduce



## Spark Basics 

when using a key:value RDD, a useful function is reduceByKey(), which allows you to combine values in a key:value pair using a function


Note: If you are only operating on values and not keys, make sure you use mapValues() or flatMapValues() so that spark only processes values and not keys. This is more efficient as it allows Spark to maintain the partitioning of the original RDD instead of having to shuffle the data around, which can be very expensive.


### Map v FlatMap

map() will transform each element into a new element, so there is a 1:1 relationship between input and output
FlatMap() 
e.g

“This is my string”

lines = sc.textFile(“my_text_file.txt”)
caps = lines.map(lambda x: x.upper())


output = “THIS IS MY STRING”


flatMap() on the other hand, can create many new elements:


lines = sc.textFile(“my_text_file.txt”)
caps = lines.flatMap(lambda x: x.split() )

output = [“This”,  “is”,  “my”, “string”]


## SparkSQL, DataFrames and DataSets 

DataFrames are now more common than RDDs
You can run SQL on these dataframes
DataFrames can have a schema

SparkSQL lives inside SparkSession
We create a SparkSession instead of a SparkContext

from pyspark.sql import SparkSession, row
spark = SparkSession.builder.appName(“SparkSQL”).getOrCreate()
inputData = spark.read.json(dataFile)


You dont have to use sql though. You can use methods on the DataFrame

myResultDataFrame.show()

myResultDataFrame.select(“someFieldName”)

Under the hood, a DataFrame is actually a DataSet with Row objects

It is really easy to combine SQL and Python syntax in spark by using UDFs



## broadcast()

broadcast objects ‘broadcast’ data to an executor so that they are always there when needed.

you can use sc.broadcast() to ship off whatever you want to all executor nodes

you can use this to send out lookup tables

this is a more efficient approach that avoids large cumbersome joins or merges



## Accumulators
accumulators allows all the executors in your cluster to increment some shared variable in your cluster.



## Running Spark on a cluster

You need to think about how your data will be partitioned. Spark wont distribute the data on its own.  

You can use .partitionBy() on an RDD before running a large operation that benefits from partitioning.  

.partitionBy() will split the data up into chunks  

.join(), cogroup(), groupWith(), leftOuterJoin(), rightOuterJoin(), groupByKey(), reduceByKey(), coombineByKey(), and lookup() all benefit from using .partitionBy() first.  


To few partitions wont take full advantage of your cluster  

Too many results in too much overhead from shuffling data  

rule of thumb, use at least as many partitions as you have cores, or executors, that fit within your avialable memory  

partitionBy(100) is usually a reasonable place to start for large operations  




