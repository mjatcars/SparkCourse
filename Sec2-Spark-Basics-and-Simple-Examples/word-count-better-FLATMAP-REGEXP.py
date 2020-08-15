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
wordCounts = words.countByValue()

# Convert from unicode to ascii and print the word and count 
# and ignore conversion errors
for word, count in wordCounts.items():
    cleanWord = word.encode('ascii', 'ignore')
    if (cleanWord):
        print(cleanWord.decode() + " " + str(count))
