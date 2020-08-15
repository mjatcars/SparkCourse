from pyspark import SparkConf, SparkContext
import re

conf = SparkConf().setMaster("local").setAppName("RDD-demo")
sc = SparkContext(conf = conf)

# sc.parallelize allows you to create an RDD from local data
rdd1 = sc.parallelize([1,5,9,0,23,56,99,87])

# min(), max(), and sum() will result in integer output
print(rdd1.collect())
minVal = rdd1.min()
print(type(minVal))
erprint(minVal)

'''
def parseLine(line):
    fields = line.split(',')
    stationID = fields[0]
    entryType = fields[2]
    temperature = float(fields[3]) * 0.1 * (9.0 / 5.0) + 32.0
    return (stationID, entryType, temperature)

lines = sc.textFile("1800.csv")

# collect(): Return an array that contains all of the elements in this RDD.
linesArray = lines.collect()
print("########## linesArray TYPE: ", type(linesArray))
#print(linesArray)

# print a line of an RDD using collect()
print(lines.collect()[0])

# count(): Return the number of elements in the RDD
print("########## count of lines:", lines.count())

# .map: Return a new RDD by applying a function to all elements of this RDD
parsedLines = lines.map(parseLine)

# filter(): Return a new RDD containing only the elements that satisfy a predicate
minTemps = parsedLines.filter(lambda x: "TMIN" in x[1])

# Use map to extract 1 and 3rd column of source rdd and create new rdd
stationTemps = minTemps.map(lambda x: (x[0], x[2]))
print ("########## stationTemps: ",stationTemps.collect()[0])

# Use .map to change column order. In this case flip the order of the columns
# also, sort by key column
stationTempsFlipSorted = minTemps.map(lambda x: (x[1], x[0])).sortByKey()
print ("########## stationTempsFlipSorted: ",stationTempsFlipSorted.collect()[0])

# get minimum value for each key
minTemps = stationTemps.reduceByKey(lambda x, y: min(x,y))
results = minTemps.collect();

for result in results:
    print(result[0] + "\t{:.2f}F".format(result[1]))


# the return statement tells regular expressions to break up the
#  unicode text into words and convert them all to lowercase
def normalizeWords(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())

# Split each by white space into one word per line
book = sc.textFile("book.txt")
print("########## record count of book", book.count())
print("########## first line of book", book.collect()[0])

# flatMap can result in multiple rows of output from one row of input
# unlike map which is one in one out
words = book.flatMap(normalizeWords)
print("########## record count of words", words.count())
print("########## first words: ",words.collect()[0])

'''
