from pyspark import SparkConf, SparkContext
import collections
print("************************** 1")
conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
print("************************** 2")
sc = SparkContext(conf = conf)
print("************************** 3")

lines = sc.textFile("ml-100k/u.data")
print(type(lines))
print(lines)
print("************************** 4")
ratings = lines.map(lambda x: x.split()[2])
print("************************** 5")
print("RATINGS: ",ratings)
result = ratings.countByValue()
print("************************** 6")

sortedResults = collections.OrderedDict(sorted(result.items()))
print("************************** 7")
for key, value in sortedResults.items():
    print("************************** 8")
    print("%s %i" % (key, value))
print("************************** 9")
