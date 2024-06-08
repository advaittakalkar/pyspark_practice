from pyspark import SparkContext

sc = SparkContext("local[*]","which_star_is_most_given")

rdd1 = sc.textFile("C:/World/Learnings/Big data/5. spark/Week 9/moviedata.data")

#3rd column is the rating column, you need to take the count of each rating and get the maximum one
# get only the desired column i.e 3rd column

rdd2 = rdd1.map(lambda x: (x.split()[2])).map(lambda x: (x,1)).reduceByKey(lambda x,y: (x+y)).sortBy(lambda x: x[1], False)

for a in rdd2.collect():
    print(a)