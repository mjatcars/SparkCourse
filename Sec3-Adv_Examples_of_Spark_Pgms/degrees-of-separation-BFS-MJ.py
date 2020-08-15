'''
This program is an example of a Breadth Firs Search algorithm.
This program will determine the least degree of separation between superheros.
It will loop thru the records only as many times as required to find the degrees of separation.
When it does find a match it will stop.  
'''
#Boilerplate stuff:
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("DegreesOfSeparationMJ8")
sc = SparkContext(conf = conf)

# The characters we wish to find the degree of separation between:
startCharacterID = 5306 #SpiderMan
targetCharacterID = 2548  #ADAM 3,031 (who?)

# Our accumulator, used to signal when we find the target character during
# our BFS traversal.
hitCounter = sc.accumulator(0)

# change row on incoming file to (heroID, (connections, distance, color))
# and set startChracterID color to gray
def convertToBFS_1a(line):
    fields = line.split()
    heroID = int(fields[0])
    connections = []
    for connection in fields[1:]:
        connections.append(int(connection))

    color = 'WHITE'
    distance = 9999

    if (heroID == startCharacterID):
        color = 'GRAY'
        distance = 0

    return (heroID, (connections, distance, color))


# read the input file and change it to (heroID, (connections, distance, color))
def createStartingRdd_1():
    inputFile = sc.textFile("Marvel-Graph.txt")
    return inputFile.map(convertToBFS_1a)

# Perform processing across each RDD record (node)
# If GRAY  
#    Set connections to GRAY 
#    update results with connections node
#    if targetCharacterID increment hitCounter
#    set color black 
#    save this node to results
#    return results
def bfsMap_2a(node):
    characterID = node[0]
    data = node[1]
    connections = data[0]
    distance = data[1]
    color = data[2]

    results = []

    #If this node needs to be expanded...
    if (color == 'GRAY'):
        for connection in connections:
            newCharacterID = connection
            newDistance = distance + 1
            newColor = 'GRAY'
            if (targetCharacterID == connection):
                hitCounter.add(1)

            # this new entry marks a connection as gray and will be merged
            # with the existing node at the bottom of the main loop
            newEntry = (newCharacterID, ([], newDistance, newColor))
            results.append(newEntry)

        #We've processed this node, so color it black
        color = 'BLACK'

    #Emit the input node so we don't lose it.
    results.append( (characterID, (connections, distance, color)) )
    return results

def bfsReduce_2b(data1, data2):
    edges1 = data1[0]
    edges2 = data2[0]
    distance1 = data1[1]
    distance2 = data2[1]
    color1 = data1[2]
    color2 = data2[2]

    distance = 9999
    color = color1
    edges = []

    # See if one is the original node with its connections.
    # If so preserve them.
    if (len(edges1) > 0):
        edges.extend(edges1)
    if (len(edges2) > 0):
        edges.extend(edges2)

    # Preserve minimum distance
    if (distance1 < distance):
        distance = distance1

    if (distance2 < distance):
        distance = distance2

    # Preserve darkest color
    if (color1 == 'WHITE' and (color2 == 'GRAY' or color2 == 'BLACK')):
        color = color2

    if (color1 == 'GRAY' and color2 == 'BLACK'):
        color = color2

    if (color2 == 'WHITE' and (color1 == 'GRAY' or color1 == 'BLACK')):
        color = color1

    if (color2 == 'GRAY' and color1 == 'BLACK'):
        color = color1

    return (edges, distance, color)

###########################################################################################
# Main program here:
###########################################################################################
# read the input file and change it to (heroID, (connections, distance, color))
iterationRdd = createStartingRdd_1()

print("\n\n##################################################")

# main_loop_2
for iteration in range(0, 10):
    print("Running BFS iteration# " + str(iteration+1))

    # Create new vertices as needed to darken or reduce distances in the
    # reduce stage. If we encounter the node we're looking for as a GRAY
    # node, increment our accumulator to signal that we're done.
    # This in effect is an inner loop processing all rows in RDD
    # set processed row to black
    # add a gray row for the connected nodes (but it's connections will be empty)
    mapped = iterationRdd.flatMap(bfsMap_2a)

    # Note that mapped.count() action here forces the RDD to be evaluated, and
    # that's the only reason our accumulator is actually updated.
    print("Processing " + str(mapped.count()) + " values.")

    if (hitCounter.value > 0):
        print("Hit the target character! From " + str(hitCounter.value) \
            + " different direction(s).")
        break

    # Reducer combines data for each character ID, preserving the darkest
    # color and shortest path.
    iterationRdd = mapped.reduceByKey(bfsReduce_2b)

print("##################################################\n\n")
