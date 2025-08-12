from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("CustomerTotal")
sc = SparkContext(conf=conf)


def parseLine(line):
    fields = line.split(",")
    customerID = int(fields[0])
    amount = float(fields[2])
    return (customerID, amount)


lines = sc.textFile("Materials/Code/customer-orders.csv")
parsedLines = lines.map(parseLine)
totalByCustomer = parsedLines.reduceByKey(lambda x, y: x + y)
results = sorted(totalByCustomer.collect())
for customer, amount in results:
    print(f"{customer} : {amount:.2f}")
