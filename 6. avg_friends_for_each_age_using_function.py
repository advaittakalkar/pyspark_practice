# id, name, age, no_of_friends
# so we want to calculate average friends for that particular age, so we only want 3rd and 4th column
# we can get age and no_of_friends via function too

def parse_line(line):
    fields = line.split("::")
    age = fields[2]
    friends = fields[3]
    return(age,friends)

from pyspark import SparkContext

sc = SparkContext("local[*]", "avg_friends_for_age")

rdd1 = sc.textFile("C:/World/Learnings/Big data/5. spark/Week 9/friendsdata.csv")

rdd2 = rdd1.map(parse_line).mapValues(lambda x: (float(x),1))
# output - (33,345) we want (33,(345,1))

rdd3 = rdd2.reduceByKey(lambda x, y: ((x[0]+y[0],x[1]+y[1])))   # ignoring key here

rdd4 = rdd3.map(lambda x: (x[0],(x[1][0]/x[1][1])))  #x[0] is key here
#rdd4 = rdd3.mapValues(lambda x : (x[0]/x[1]))


for a in rdd4.collect():
    print(a)




