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



