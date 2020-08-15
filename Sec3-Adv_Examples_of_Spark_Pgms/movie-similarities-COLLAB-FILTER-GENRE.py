'''
NOTE: MODIFIED BY MJ TO FILTER MATCHES ON GENRE
ITEM BASED COLLABORATIVE FILTERING
Finding similar movies using Spark and the MovieLens dataset
Introoducing caching and persisting RDD's

PROCESS:
- find every pair of movies that were watched by the same person
- measure the similarity of their ratings across all users who watched both 
- sort by movie, then by similarity strength
'''
import sys
from pyspark import SparkConf, SparkContext
from math import sqrt

def loadMovieNamesGenres():
    movieNames = {}
    movieGenres = {}
    with open("../ml-100k/u.ITEM", encoding='ascii', errors='ignore') as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
            movieGenres[int(fields[0])] = fields[5:]

    return movieNames, movieGenres



#Python 3 doesn't let you pass around unpacked tuples,
#so we explicitly extract the ratings now.
def makePairs( userRatings ):
    ratings = userRatings[1]
    (movie1, rating1) = ratings[0]
    (movie2, rating2) = ratings[1]
    return ((movie1, movie2), (rating1, rating2))

# returns boolean directing filter to only take pairs with movie1 id is < movie2 id
# this will rid the set of cases where id's are equal or gt thus eliminating duplicates
def filterDuplicates( userRatings ):
    ratings = userRatings[1]
    (movie1, rating1) = ratings[0]
    (movie2, rating2) = ratings[1]
    return movie1 < movie2

# remove pairs from different genres
def filterMismatchGenre( userRatings ):
    ratings = userRatings[1]
    (movie1, rating1) = ratings[0]
    (movie2, rating2) = ratings[1]
    return genreDict[movie1] == genreDict[movie2]

# ratingPairs=(rating1, rating2), (rating1, rating2) ...
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


# setMaster("local[*]") directs Spark to use all cores
conf = SparkConf().setMaster("local[*]").setAppName("MovieSimilarities4")
sc = SparkContext(conf = conf)


print("\n****************************Loading movie names and genres into separate dictionaries...\n")
nameDict, genreDict = loadMovieNamesGenres()



data = sc.textFile("../ml-100k/u.data")

# Map ratings to key / value pairs: user ID => movie ID, rating
ratings = data.map(lambda l: l.split()).map(lambda l: (int(l[0]), (int(l[1]), float(l[2]))))
#print("########## ratings.count(): ",ratings.count())
#print("########## ratings: ",ratings.collect()[0])

# Emit every movie rated together by the same user.
# Self-join to find every combination.
joinedRatings = ratings.join(ratings)
print("########## joinedRatings.count(): ",joinedRatings.count())
print("########## joinedRatings: ",joinedRatings.collect()[0])

# At this point our RDD consists of userID => ((movieID, rating), (movieID, rating))

# Filter out duplicate pairs
uniqueJoinedRatings = joinedRatings.filter(filterDuplicates)

# Filter out unmatched genres
uniqueJoinedRatingsSameGenre = uniqueJoinedRatings.filter(filterMismatchGenre)


# Now key by (movie1, movie2) pairs.
moviePairs = uniqueJoinedRatingsSameGenre.map(makePairs)

# We now have (movie1, movie2) => (rating1, rating2)
# Now collect all ratings for each movie pair and compute similarity
moviePairRatings = moviePairs.groupByKey()

# We now have (movie1, movie2) = > (rating1, rating2), (rating1, rating2) ...
# Can now compute similarities.
# NOTE: .cache() directs Spark to keep the RDD in memory
# NOTE2: a similar command .perist() saves to disk
moviePairSimilarities = moviePairRatings.mapValues(computeCosineSimilarity).cache()
# output is (movie1, movie2),(score, number of ratings)

# Save the results files (one per core/executor) to folder movie-sims
#moviePairSimilarities.sortByKey()
#moviePairSimilarities.saveAsTextFile("movie-sims")


# Extract similarities for the movie we care about that are "good".
if (len(sys.argv) > 1):

    scoreThreshold = 0.95
    coOccurenceThreshold = 10

    movieID = int(sys.argv[1])

    # Filter for movies with this sim that are "good" as defined by
    # our quality thresholds above
    # data layout = (movie1, movie2),(score, number of ratings)
    filteredResults = moviePairSimilarities.filter(lambda pairSim: \
        (pairSim[0][0] == movieID or pairSim[0][1] == movieID) \
        and pairSim[1][0] > scoreThreshold and pairSim[1][1] > coOccurenceThreshold)

    # Sort by quality score.
    results = filteredResults.map(lambda pairSim: (pairSim[1], pairSim[0])).sortByKey(ascending = False).take(10)
    
    print("\n\n##################################################\n")

    print("Top 10 similar movies for " + nameDict[movieID])
    for result in results:
        (sim, pair) = result
        # Display the similarity result that isn't the movie we're looking at
        similarMovieID = pair[0]
        if (similarMovieID == movieID):
            similarMovieID = pair[1]
        print(nameDict[similarMovieID] + "\tscore: " + str(sim[0]) + "\tstrength: " + str(sim[1]))
    
    print("\n##################################################\n\n")

