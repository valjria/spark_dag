from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import *

spark = SparkSession.builder \
.appName("Clean Dirty Data") \
.master("local[2]") \
.getOrCreate()

df = spark.read.format("csv") \
.option("header",True) \
.option("inferSchema", True) \
.load("file:///tmp/dirty_store_transactions.csv")

df.show(5)


df1 = df.withColumn("STORE_LOCATION", F.regexp_replace(F.col("STORE_LOCATION"),"[^A-Za-z0-9]", ""))

df2 = df1.withColumn("MRP", F.regexp_replace(F.col("MRP"), "\$", "").cast(FloatType())) \
.withColumn("CP", F.regexp_replace(F.col("CP"), "\$", "").cast(FloatType())) \
.withColumn("DISCOUNT", F.regexp_replace(F.col("DISCOUNT"), "\$", "").cast(FloatType())) \
.withColumn("SP", F.regexp_replace(F.col("SP"), "\$", "").cast(FloatType()))

df3 = df2.withColumn("sales_date", F.col("Date").cast(DateType()))

df4 = df3.dropna()
df5 = df4.drop("Date")

psql_conn = "jdbc:postgresql://localhost:5432/traindb?user=train&password=Ankara06"

print("writing to postgresql")
df5.write.mode("overwrite") \
.jdbc(url = psql_conn, table='clean_transactions', properties = {"driver": 'org.postgresql.Driver'})

print("write completed")

