from pyspark import SparkContext
from sys import stdin

# common lines
sc = SparkContext("local[*]", "word_count" )

# changing the log level
sc.setLogLevel("ERROR")   # WARN, INFO, ERROR, FATAL

i = sc.textFile("C:/World/Learnings/Big data/5. spark/Week 9/file.txt.txt")

# one input row multiple output
words = i.flatMap(lambda x: x.split(" "))

# converting all to lowercase - map transformation one to one transformation
rdd = words.map(lambda x: (x.lower()))

# one input one output
rdd2 = rdd.map(lambda x: (x, 1))

# reduce the number of rows - take two rows and does aggregation on keys
rdd3 = rdd2.reduceByKey(lambda x, y: x + y )       # .map(lambda x: (x[1],x[0]))

# in sortBy we can specify the key on which we want to sort it. by default its ascending, use false to make it descending
rdd4 = rdd3.sortBy(lambda x: x[1],False).collect()            # .map(lambda x: (x[1],x[0])).collect()


for a in rdd4:
 print(a)






