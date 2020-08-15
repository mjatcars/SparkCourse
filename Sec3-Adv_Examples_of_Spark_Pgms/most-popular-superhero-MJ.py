from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("PopularHeroMJ3")
sc = SparkContext(conf = conf)

# this returns the superhero's id and the number of times he appears
# note there may be multiple entries for a superhero so he may have multiple lines
def countCoOccurences(line):
    elements = line.split()
    return (int(elements[0]), len(elements) - 1)

def parseNames(line):
    fields = line.split('\"')
    return (int(fields[0]), fields[1].encode("utf8"))

names = sc.textFile("Marvel-Names.txt")
namesRdd = names.map(parseNames)

lines = sc.textFile("Marvel-Graph.txt")

pairings = lines.map(countCoOccurences)

# Superheros may have multiple lines so aggregating counts by superhero id is required
totalFriendsByCharacter = pairings.reduceByKey(lambda x, y : x + y)
flipped = totalFriendsByCharacter.map(lambda xy : (xy[1], xy[0]))
print(type(flipped))
mostPopular = flipped.max()
print(type(mostPopular))
print(mostPopular)
mostPopularName = namesRdd.lookup(mostPopular[1])[0]

print("\n\n##################################################")
print(str(mostPopularName) + " is the most popular superhero, with " + \
    str(mostPopular[0]) + " co-appearances.")
print("##################################################\n\n")
