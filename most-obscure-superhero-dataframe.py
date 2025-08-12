from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

spark = SparkSession.builder.appName("MostPopularSuperhero").getOrCreate()

schema = StructType([StructField("id", IntegerType(), True), StructField("name", StringType(), True)])

names = spark.read.schema(schema).option("sep", " ").csv("Materials/Code/Marvel-names.txt")

lines = spark.read.text("Materials/Code/Marvel-graph.txt")

# Small tweak vs. what's shown in the video: we trim each line of whitespace as that could
# throw off the counts.
connections = (
    lines.withColumn("id", func.split(func.trim(func.col("value")), " ")[0])
    .withColumn("connections", func.size(func.split(func.trim(func.col("value")), " ")) - 1)
    .groupBy("id")
    .agg(func.sum("connections").alias("connections"))
)

leastPopular = connections.sort("connections").first()
# leastPopular = connections.agg(func.min("connections")).first()

if leastPopular is not None:
    leastPopularHeros = connections.filter(func.col("connections") == leastPopular[1])

    leastPopularNamedHeros = leastPopularHeros.join(names, "id")
    print(f"The following characters have only {leastPopular[1]} conncetion(s):")
    leastPopularNamedHeros.select("name").show(leastPopularNamedHeros.count())
