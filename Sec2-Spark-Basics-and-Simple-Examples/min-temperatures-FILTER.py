from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("MinTemperatures")
sc = SparkContext(conf = conf)

# Line in -> split line by comma -> stationID, entryType, temperature converted to Ferenheiht type float
def parseLine(line):
    fields = line.split(',')
    stationID = fields[0]
    entryType = fields[2]
    temperature = float(fields[3]) * 0.1 * (9.0 / 5.0) + 32.0
    return (stationID, entryType, temperature)

lines = sc.textFile("1800.csv")
parsedLines = lines.map(parseLine)

# Strip out only lines with minimal temps
minTemps = parsedLines.filter(lambda x: "TMIN" in x[1])

# Remove "TMIN" column
stationTemps = minTemps.map(lambda x: (x[0], x[2]))

# reduceByKey is an aggretate function on the value accoss keys and here
# here we are requesting the min value per key (stationid)
# the way this works is as each record comes in the first rec x is compared to the 
# second rec and the lowest value is taken
minTemps = stationTemps.reduceByKey(lambda x, y: min(x,y))

# copy from rdd to list
results = minTemps.collect();

# FORMAT PRINT: stationid -> tab -> float temp displaying 2 decimal places
for result in results:
    print(result[0] + "\t{:.2f}F".format(result[1]))
