from pyspark import SparkContext


# common lines
sc = SparkContext("local[*]", "word_count" )
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

# congratulations on first program
