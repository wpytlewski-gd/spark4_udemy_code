from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("CustomerTotal")
sc = SparkContext(conf=conf)


def parseLine(line):
    fields = line.split(",")
    customerID = int(fields[0])
    amount = float(fields[2])
    return (customerID, amount)


lines = sc.textFile("Materials/Code/customer-orders.csv")
mapedLines = lines.map(parseLine)
totalByCustomer = mapedLines.reduceByKey(lambda x, y: x + y)
totalByCustomerFlipped = totalByCustomer.map(lambda x: (x[1], x[0]))
totalBycustomerSorted = totalByCustomerFlipped.sortByKey()
results = totalBycustomerSorted.collect()
for amount, customer in results:
    print(f"{customer} : {amount:.2f}")
