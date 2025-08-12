from pyspark.sql import Row, SparkSession

spark = SparkSession.builder.appName("SparkSQL").getOrCreate()


def mapper(line):
    fields = line.split(",")
    return Row(ID=int(fields[0]), name=str(fields[1].encode("utf-8")), age=int(fields[2]), numFriends=int(fields[3]))


lines = spark.sparkContext.textFile("Materials/Code/fakefriends.csv")
people = lines.map(mapper)

schemaPeople = spark.createDataFrame(people).cache()
schemaPeople.createOrReplaceTempView("people")


grouped = spark.sql("SELECT age, AVG(numFriends) as avgNumFriends FROM people GROUP BY age ORDER BY age")

for row in grouped.collect():
    print(f"{row.age} : {row.avgNumFriends:.0f}")

spark.stop()
