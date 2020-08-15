from pyspark import SparkConf, SparkContext # All Spark programs import the configurator and Spark Context libs
import collections

# Most Python Spark scripts have these two statements
# This will run on local macine in a single process processor 
# Name the program in Spark
# Create the Spark Context object and named it sc (by convention)
conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf = conf)


lines = sc.textFile("../ml-100k/u.data")
print(type(lines))
ratings = lines.map(lambda x: x.split()[2])
print(type(ratings))


result = ratings.countByValue()
print(type(result.items()))
print(result.items())
sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.items():
    print("%s %i" % (key, value))
