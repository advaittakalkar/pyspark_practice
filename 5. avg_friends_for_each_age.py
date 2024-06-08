# id, name, age, no_of_friends
# so we want to calculate average friends for that particular age, so we only want 3rd and 4th column
# we can get age and no_of_friends via function too

from pyspark import SparkContext

sc = SparkContext("local[*]", "avg_friends_for_age")

rdd1 = sc.textFile("C:/World/Learnings/Big data/5. spark/Week 9/friendsdata.csv")

rdd2 = rdd1.map(lambda x: (x.split("::")[2], x.split("::")[3]))

#23 345 -> 23,(345,1)
#23 356 -> 23,(356,1)
# rdd3= rdd2.map(lambda x: (x[0],(x[1],1))) | mapValues ignores the key and just see the values
# mapValues(lambda x: (x,1)) - mapvalues ignore 23 whereas in map we took x[0] i.e key too
rdd3 = rdd2.map(lambda x: (x[0], (float(x[1]),1)))

# reduceByKey ignore the key,so just consider values
rdd4 = rdd3.reduceByKey(lambda x,y: (x[0]+y[0],x[1]+y[1]))

# rdd5 = rdd4.mapValues(lambda x: (x[0]/x[1])) #mapValues ignores the key, 345 is first element

# instead of 21 we can also write below line using map- here need to consider the key so key is 0th element
rdd5 = rdd4.map(lambda x: (x[0], (x[1][0]/x[1][1])))


for a in rdd5.sortBy(lambda x: x[1],False).collect():
    print(a)

