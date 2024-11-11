from idlelib.pyparse import trans

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("loading-app")\
    .master("local[*]").getOrCreate()

df = spark.read\
    .option("header",True)\
    .option("inferSchema",True)\
    .csv("../data/AAPL.csv")

df.show()
df.printSchema()