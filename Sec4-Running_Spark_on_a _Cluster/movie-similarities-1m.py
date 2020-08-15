import sys
from pyspark import SparkConf, SparkContext
from math import sqrt

#To run on EMR successfully + output results for Star Wars:
#aws s3 cp s3://sundog-spark/MovieSimilarities1M.py ./
#aws s3 cp s3://sundog-spark/ml-1m/movies.dat ./
#spark-submit --executor-memory 2g MovieSimilarities1M.py 260

def loadMovieNames():
    movieNames = {}
    with open("movies.dat", encoding="ISO-8859-1") as f:
        for line in f:
            fields = line.split("::")
            movieNames[int(fields[0])] = fields[1]
    return movieNames

def makePairs(line):
# format of "line": user, ratings. So line[1] are the ratings
    (movie1, rating1) = line[1][0]
    (movie2, rating2) = line[1][1]
    return ((movie1, movie2), (rating1, rating2))

def filterDuplicates(line):

# format of "line": userID, ratings. So line[1] are the ratings
    (movie1, rating1) = line[1][0]
    (movie2, rating2) = line[1][1]
    return movie1 < movie2

def computeCosineSimilarity(ratingPairs):
    numPairs = 0
    sum_xx = sum_yy = sum_xy = 0
    for ratingX, ratingY in ratingPairs:
        sum_xx += ratingX * ratingX
        sum_yy += ratingY * ratingY
        sum_xy += ratingX * ratingY
        numPairs += 1

    numerator = sum_xy
    denominator = sqrt(sum_xx) * sqrt(sum_yy)

    score = 0
    if (denominator):
        score = (numerator / (float(denominator)))

    return (score, numPairs)



conf = SparkConf()
sc = SparkContext(conf = conf)

print("\nLoading movie names...")
nameDict = loadMovieNames()

data = sc.textFile("s3n://sundog-spark/ml-1m/ratings.dat")

# Map ratings to key / value pairs: user ID => movie ID, rating
ratings = data.map(lambda l: l.split("::")).map(lambda l: (int(l[0]), (int(l[1]), float(l[2]))))


# Emit every movie rated together by the same user.
# Self-join to find every combination.
ratingsPartitioned = ratings.partitionBy(100)
joinedRatings = ratingsPartitioned.join(ratingsPartitioned)


# At this point our RDD consists of userID => ((movieID, rating), (movieID, rating))

# Filter out duplicate pairs
uniqueJoinedRatings = joinedRatings.filter(filterDuplicates)


# Now key by (movie1, movie2) pairs.
moviePairs = uniqueJoinedRatings.map(makePairs).partitionBy(100)


# We now have (movie1, movie2) => (rating1, rating2)
# Now collect all ratings for each movie pair and compute similarity
moviePairRatings = moviePairs.groupByKey()


# We now have (movie1, movie2) = > (rating1, rating2), (rating1, rating2) ...
# Can now compute similarities.
moviePairSimilarities = moviePairRatings.mapValues(computeCosineSimilarity).persist()


# Save the results if desired
moviePairSimilarities.sortByKey()

# This line works fine if the script is run once, second time it's run the file already exists, and script fails
#   TODO - find out how to override or delete the file first
# moviePairSimilarities.saveAsTextFile("movie-sims")


# Extract similarities for the movie we care about that are "good".
if (len(sys.argv) > 1):
    scoreThreshold = 0.97
    coOccurenceThreshold = 1000

    movieID = int(sys.argv[1])


    # Filter for movies with this sim that are "good" as defined by
    # our quality thresholds above
    # format of "line" used in lambda: (pair,sim), so "line[0]"=pair, "line[1]"=sim
    filteredResults = moviePairSimilarities.filter(lambda line: \
        (line[0][0] == movieID or line[0][1] == movieID) \
        and line[1][0] > scoreThreshold and line[1][1] > coOccurenceThreshold)

    # Sort by quality score.
    # format of "line" used in lambda: (pair,sim), so "line[0]"=pair, "line[1]"=sim
    results = filteredResults.map(lambda line: (line[1], line[0])).sortByKey(ascending = False).take(10)

    print("Top 10 similar movies for " + nameDict[movieID])

    for result in results:
        (sim, pair) = result

        # Display the similarity result that isn't the movie we're looking at
        similarMovieID = pair[0]

        if (similarMovieID == movieID):
            similarMovieID = pair[1]

        print(nameDict[similarMovieID] + "\tscore: " + str(sim[0]) + "\tstrength: " + str(sim[1]))