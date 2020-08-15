import re
from pyspark import SparkConf, SparkContext

# the return statement tells regular expressions to break up the
#  unicode text into words and convert them all to lowercase
def normalizeWords(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

# Split each by white space into one word per line
input = sc.textFile("book.txt")
words = input.flatMap(normalizeWords)

# Instead of using the countByValue() method, we are loading  the  
# words and the number 1 in rdd then summing the counts by key
wordCounts = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)

# switch around the word and the count making the count the key
# then sort by key and write to a list 
wordCountsSorted = wordCounts.map(lambda x: (x[1], x[0])).sortByKey()
results = wordCountsSorted.collect()

# Convert from unicode to ascii and print the word, tab, tab and count 
# and ignore conversion errors
for result in results:
    count = str(result[0])
    word = result[1].encode('ascii', 'ignore')
    if (word):
        print(word.decode() + ":\t\t" + count)

print(type(results))
print(results[0:3])