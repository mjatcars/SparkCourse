'''
This program takes a column of integers and returns value pairs of the integers and the number of times they occurred
'''
from pyspark import SparkConf, SparkContext 
import collections


conf = SparkConf().setMaster("local").setAppName("TotalSpentByCustomersMJ7")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    customer = int(fields[0])
    amount = float(fields[2])
    return (customer, amount)

# aggregate amounts by customerid
lines = sc.textFile("customer-orders.csv")
rdd = lines.map(parseLine)
customerTotal = rdd.reduceByKey(lambda x, y: x + y)

# flip customerid for amount making the amount the key so it can be sorted
flipped = customerTotal.map(lambda x: (x[1], x[0]))
customerTotalSorted = flipped.sortByKey()
results = customerTotalSorted.collect()


results = customerTotalSorted.collect()
for result in results:
    print("Customer: {}   Total Orders: ${:.2f}".format(result[1],result[0]))