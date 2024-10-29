from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

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
    StructField("datetime", TimestampType(), True),
    StructField("country", StringType(), True),
    StructField("city", StringType(), True),
    StructField("ecommerce_website_name", StringType(), True),
    StructField("payment_txn_id", StringType(), True),
    StructField("payment_txn_success", StringType(), True),
    StructField("failure_reason", StringType(), True)
])

#example data
data = [(1, 11, "mike", 1, "blanket", "bedding", "card", 49, 20.22, "dfa")]


# Convert generated data to a DataFrame
df = spark.createDataFrame(data, schema=schema)
    
# Save as CSV
# Write DataFrame to CSV 
df.write.csv("/project2/data", header=True, mode="overwrite")
