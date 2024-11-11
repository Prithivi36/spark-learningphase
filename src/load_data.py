from  pyspark.sql.functions import *
from pyspark.sql.functions import col
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.types import *

spark = SparkSession.builder.appName("loading-app")\
    .master("local[*]").getOrCreate()

df = spark.read\
    .option("header",True)\
    .option("inferSchema",True)\
    .csv("../data/AAPL.csv")

df=df.select(
    df["Date"].cast(DateType()).alias("date"),
    df["Open"].cast(DoubleType()).alias("open"),
    df["Close"].cast(DoubleType()).alias("close")
)

df.show()
df.printSchema()

#targeting cols in ways

col1=df["close"]
col2=df.close
col3=col("close")

#access wid variables
df.select(col1,col2,col3).show()

#access as col as String
df.select("close","open").show()

#casting

data_as_String=df["date"].cast(StringType())

df.select(df["date"],data_as_String).show()

date_plus_2= date_add(df["date"],2)
as_String=date_plus_2.cast(StringType())
concaDate = concat(as_String,lit("its me"))

df.select(df["date"],concaDate.alias("transformer")).show()

df.sort(date_add(df['date'],5).desc()).show()

df.groupby(year(df['date']),month(df['date'])).agg(
    max("close"),avg("open"),sum("open").alias("sum_op")

).sort("sum_op").show()

df.groupby("date").max("open").show()

window = Window.partitionBy(year(df['date'])).orderBy(df['close'].desc())

df.withColumn("rank",row_number().over(window))\
    .filter(col("rank")==1).show()

windo = Window.partitionBy().orderBy(df['date'])

df.withColumn("previous",lag(df["close"],1,0.0).over(windo)) \
    .withColumn("diffBwnDays",df['open']-col("previous"))\
    .show()