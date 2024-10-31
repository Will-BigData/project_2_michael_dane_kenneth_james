from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from pyspark.sql.functions import *

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Ecommerce Data Generator") \
    .getOrCreate()

# Create Spark Context
sc = spark.sparkContext

rangedf = sc.parallelize(range(0,10000))
df = rangedf.map(lambda x: (x,)).toDF(["x"])

# Convert generated data to a DataFrame

# df.show()
# df.foreach(lambda x: print(x))

#Coalesce the DataFrame to a single partition
df = df.coalesce(1)

# Write DataFrame to CSV
df.write.csv("/project2/data", header=True, mode="overwrite")