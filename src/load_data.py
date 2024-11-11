from shlex import shlex

from pyspark.sql.functions import col
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType

spark = SparkSession.builder.appName("loading-app")\
    .master("local[*]").getOrCreate()

df = spark.read\
    .option("header",True)\
    .option("inferSchema",True)\
    .csv("../data/AAPL.csv")

df.show()
df.printSchema()

#targeting cols in ways

col1=df["Close"]
col2=df.Close
col3=col("Close")

#access wid variables
df.select(col1,col2,col3).show()

#access as col as String
df.select("Close","Open").show()

#casting

data_as_String=df["Date"].cast(StringType()).alias("date")

df.select(df["Date"],data_as_String).show()