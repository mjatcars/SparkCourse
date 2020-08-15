from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

input = sc.textFile("book.txt")

# Split each by white space into one word per line
words = input.flatMap(lambda x: x.split())

# Count the occurences of each individual word
wordCounts = words.countByValue()

# Convert from unicode to ascii and print the word and count 
# and ignore conversion errors
for word, count in wordCounts.items():
    cleanWord = word.encode('ascii', 'ignore')
    if (cleanWord):
        print(cleanWord.decode() + " " + str(count))


