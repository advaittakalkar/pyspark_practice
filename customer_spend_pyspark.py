# cust_id XXX amount   we want to calculate the customer who spends the most
from pyspark import SparkContext

sc = SparkContext("local[*]", "cust_spend_most")

i = sc.textFile("C:/World/Learnings/Big data/5. spark/Week 9/customerorders.csv")

# splitting it on "," and eliminate 2nd column, keep only 1 and 3rd
rdd1 = i.map(lambda x: (x.split(",")[0], float(x.split(",")[2])))

rdd2 = rdd1.reduceByKey(lambda x, y: (x+y))

rdd3 = rdd2.sortBy(lambda x: x[1], False)

for a in rdd3.collect():    # collect gives list and you can iterate over the list and print it
    print(a)

    # cust_id = 68 has done most spend
