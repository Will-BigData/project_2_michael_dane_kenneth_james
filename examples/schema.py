from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from pyspark.sql.functions import to_timestamp

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Ecommerce Data Generator") \
    .getOrCreate()



# Define PySpark schema
schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("customer_name", StringType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("product_name", StringType(), True),
    StructField("product_category", StringType(), True),
    StructField("payment_type", StringType(), True),
    StructField("qty", IntegerType(), True),
    StructField("price", DoubleType(), True),
    StructField("datetime", StringType(), True), 
    StructField("country", StringType(), True),
    StructField("city", StringType(), True),
    StructField("ecommerce_website_name", StringType(), True),
    StructField("payment_txn_id", StringType(), True),
    StructField("payment_txn_success", StringType(), True),
    StructField("failure_reason", StringType(), True)
])

#example data checking
data = [(1, 11, "mike", 1, "blanket", "bedding", "card", 49, 20.22, "2023-04-25 00:00:00", "USA", "Houston", "everythingstore.com", 10, "Y", "N/A")]


# Convert generated data to a DataFrame
df = spark.createDataFrame(data, schema=schema)

#Coalesce the DataFrame to a single partition 
df = df.coalesce(1)

# Convert 'datetime' to TimestampType
df = df.withColumn("datetime", to_timestamp("datetime", "yyyy-MM-dd HH:mm:ss"))


# Write DataFrame to CSV 
df.write.csv("/project2/data", header=True, mode="overwrite")
