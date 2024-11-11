from pyspark.sql import SparkSession

spark = SparkSession.builder\
    .appName("hola_app").master("local[*]")\
    .getOrCreate()

print(spark.version)
print(spark)