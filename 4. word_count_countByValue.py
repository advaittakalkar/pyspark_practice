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

final_count = rdd.countByValue()  # countByValue gives the output in dictionary format

# one input one output
# rdd2 = rdd.map(lambda x: (x, 1))

# reduce the number of rows - take two rows and does aggregation on keys
# rdd3 = rdd2.reduceByKey(lambda x, y: x + y)

# Action
# rdd4 = rdd3.collect()

print(final_count)



