import sys
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("InspectXLSX").getOrCreate()

# we can't easily get sheet names without spark-excel, let's use it
df = spark.read.format("com.crealytics.spark.excel") \
    .option("header", "true") \
    .load("/opt/bitnami/spark/data/data-rapor-pendidikan-indonesia-2025-indonesia.xlsx")

print("Columns:", df.columns)
spark.stop()
