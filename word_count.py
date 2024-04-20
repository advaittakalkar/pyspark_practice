from pyspark import SparkContext
from sys import stdin

# common lines
sc = SparkContext("local[*]", "word_count" )

# changing the log level
sc.setLogLevel("ERROR")   # WARN, INFO, ERROR, FATAL

i = sc.textFile("C:/World/Learnings/Big data/5. spark/Week 9/file.txt.txt")

# one input row multiple output
rdd = i.flatMap(lambda x: x.split(" "))

# one input one output
rdd2 = rdd.map(lambda x: (x, 1))

# reduce the number of rows - take two rows and does aggregation on keys
rdd3 = rdd2.reduceByKey(lambda x, y: x + y)

# Action
rdd4 = rdd3.collect()

for a in rdd4:
    print(a)

stdin.readline()   # program will go on running until we stop it, because DAG will be active till the program is running and we want to see the DAG

# congratulations on first program
