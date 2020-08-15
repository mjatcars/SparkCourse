from pyspark import SparkConf, SparkContext

def loadMovieNames():
    movieNames = {}
    with open("../ml-100k/u.ITEM") as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames

conf = SparkConf().setMaster("local").setAppName("PopularMoviesMJ1")
sc = SparkContext(conf = conf)

# broadcast will send loadMovieNames to every node on the cluster
nameDict = sc.broadcast(loadMovieNames())

lines = sc.textFile("../ml-100k/u.data")
movies = lines.map(lambda x: (int(x.split()[1]), 1))
movieCounts = movies.reduceByKey(lambda x, y: x + y)

flipped = movieCounts.map( lambda x : (x[1], x[0]))
sortedMovies = flipped.sortByKey()

# Merge the movie name with the movie count
sortedMoviesWithNames = sortedMovies.map(lambda countMovie : (nameDict.value[countMovie[1]], countMovie[0]))

results = sortedMoviesWithNames.collect()

for result in results:
    print (result)
