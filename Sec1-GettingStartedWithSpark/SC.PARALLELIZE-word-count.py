'''
sc.parallelize allows you to create an RDD from local data
'''
from pyspark import SparkContext
sc = SparkContext("local", "count app")
words = sc.parallelize (
   ["scala", 
   "java", 
   "hadoop", 
   "spark", 
   "akka",
   "spark vs hadoop", 
   "pyspark",
   "pyspark and spark"]
)

# Return the number of elements in the RDD as Python int
counts = words.count()

print("##################################################")
print ("Number of elements in RDD -> {}".format(counts))
print("##################################################")
