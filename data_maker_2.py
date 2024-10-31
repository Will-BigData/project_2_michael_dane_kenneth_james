from func import *
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, TimestampType
from pyspark.sql import Row
from pyspark.sql.functions import to_timestamp
from datetime import datetime

# Initialize Spark session
spark = SparkSession.builder.appName("FurnitureStoreDataset").getOrCreate()



# Example usage in generating records
def generate_records(num_records):
    data = []
    for i in range(num_records):
        product_id, product_name, product_category, price = generate_product()
        customer_id, customer_name, country, city = generate_customers()
        record = Row(
            order_id=i + 1,
            customer_id= customer_id,
            customer_name= customer_name,
            product_id=product_id, 
            product_name=product_name,
            product_category=product_category,
            payment_type=random.choice(["card", "apple pay", "paypal"]),
            qty=random.randint(1, 5),
            price=price,  
            datetime=random_date(start_date, end_date).strftime("%Y-%m-%d %H:%M:%S"),
            country=country,
            city= city,
            ecommerce_website_name= random.choice(websites),
            payment_txn_id=str(random.randint(1000, 9999)),
            payment_txn_success=random.choice(["Y", "N"]),
            failure_reason=random.choice(["N/A", "Insufficient Funds", "Network Error"])
        )
        data.append(record)
    return data

# Generate 100 records
num_records = 9000
data = generate_records(num_records)

# Define schema
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

# Create DataFrame
df = spark.createDataFrame(data, schema=schema)

# Convert 'datetime' to TimestampType
df = df.withColumn("datetime", to_timestamp("datetime", "yyyy-MM-dd HH:mm:ss"))



#Coalesce the DataFrame to a single partition
df = df.coalesce(1)

# Show the DataFrame and schema
df.show()
df.printSchema()

# Write DataFrame to CSV 
df.write.csv("/project2/data", header=True, mode="overwrite")


