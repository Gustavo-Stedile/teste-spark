from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("TestApp") \
    .getOrCreate()

df = spark.read \
    .option("multiLine", "true") \
    .json('people.json')

df.show()
