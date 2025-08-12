from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import (
    FloatType,
    IntegerType,
    StructField,
    StructType,
)

spark = SparkSession.builder.appName("TotalSpentByCustomer").getOrCreate()

schema = StructType(
    [
        StructField("customerID", IntegerType(), True),
        StructField("transactionID", IntegerType(), True),
        StructField("amount", FloatType(), True),
    ]
)

customersDF = spark.read.schema(schema).csv("Materials/Code/customer-orders.csv")
customersDF = customersDF.select("customerID", "amount")
totalByCustomerDF = (
    customersDF.groupBy("customerID").agg(func.round(func.sum("amount"), 2).alias("totalAmount")).sort("totalAmount")
)
results = totalByCustomerDF.collect()

for result in results:
    print(f"{result[0]} : {result[1]:.2f}")

spark.stop()
