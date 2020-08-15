'''
This program takes rows of key value pairs (ages and number of friends) and returns value pairs of the integers and the number of times they occurred
'''
# All Spark programs import the configurator and Spark Context libs
from pyspark import SparkConf, SparkContext

# Most Python Spark scripts have these two statements
# This will run on local macine in a single process processor 
# Name the program in Spark
# Create the Spark Context object and named it sc (by convention)
conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf = conf)

# line in and age, numFriends out
# The split method extracts the third column (separated by space) into a new RDD named ratings
def parseLine(line):
    fields = line.split(',')
    age = int(fields[2])
    numFriends = int(fields[3])
    return (age, numFriends)

# Load the RDD file using the sc textfile method to load the data from a local file
# invoke pareline function on each line to return  key/value pair of age/numFriends
# result is rdd of key/value pair of age/numFriends
lines = sc.textFile("fakefriends.csv")
print("lines type: ",type(lines))
rdd = lines.map(parseLine)
print("rdd type: ",type(rdd))

# lambda is a shorthand for passing a function into the map
# call mapValues or flatMapValues rather than map or flatMap when you are working with values only - it is more efficient
# Map means to extract the data we care about
# mapValues creates a 1:1 new rdd but flatMapValues can fan out to a larger output rdd
# rdd.mapValues(lambda x: (x, 1)) returns value (number of friend) and number of times that count of friends occurred(one)
# reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])) give a running total of all ages and occurances of that age
# NOTE: reduceByKey is the first action and this is when Spark will begin to perform actions
# totalsByAge.mapValues(lambda x: x[0] / x[1]) compute average number
# NOTE: after rdd.mapValues is applied the data will be (age,(friends,1)) (33,(23,1)),(10,(546,1))
totalsByAge = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
print("totalsByAge type: ",type(totalsByAge))
# NOTE: This will be (age,(tot-friends,tot-count)) so dividing tot-friends by tot-count results in average number of friends per age
averagesByAge = totalsByAge.mapValues(lambda x: x[0] / x[1])
print("averagesByAge type: ",type(averagesByAge))

# Put the resuts to a Python list and print
results = averagesByAge.collect()
print("results type: ",type(results))
for result in results:
    print(result)
