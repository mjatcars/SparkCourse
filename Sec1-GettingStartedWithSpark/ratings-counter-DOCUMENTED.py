'''
This program takes a column of integers and returns value pairs of the integers and the number of times they occurred
'''
# All Spark programs import the configurator and Spark Context libs
from pyspark import SparkConf, SparkContext 
import collections

# Most Python Spark scripts have these two statements
# This will run on local macine in a single process processor 
# Name the program in Spark
# Create the Spark Context object and named it sc (by convention)
conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf = conf)

# Load the RDD file using the sc file method to load the data from a local file
# Map means to extract the data we care about
# lambda is a shorthand for passing a function into the map
# The split method extracts the third column (separated by space) into a new RDD named ratings
lines = sc.textFile("../ml-100k/u.data")
print("@@@@@@@@@ lines type: ",type(lines))
ratings = lines.map(lambda x: x.split()[2])
print("@@@@@@@@@ ratings type: ",type(ratings))

# count by value and put in a Python dictionary consisting of rating:counts 
result = ratings.countByValue()
print("@@@@@@@@@ result type: ",type(result))
print("result.items(): ",result.items())

# Call Python collections package to sort the dictionary by key and place in sortedesults Python dictionary
# Iterate through the sortedResults dictionary and print the key value pairs
sortedResults = collections.OrderedDict(sorted(result.items()))
print("@@@@@@@@@ sortedResults type: ",type(sortedResults))
for key, value in sortedResults.items():
    print("{} {}".format(key, value))
