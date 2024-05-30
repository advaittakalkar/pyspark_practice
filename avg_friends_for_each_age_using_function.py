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

rdd2 = rdd1.map(parse_line)
# output - (33,345) we want (33,(345,1))

for a in rdd2.collect():
    print(a)




