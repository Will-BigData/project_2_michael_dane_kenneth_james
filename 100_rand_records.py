import random
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, TimestampType
from pyspark.sql import Row
from pyspark.sql.functions import to_timestamp
from datetime import datetime

# Initialize Spark session
spark = SparkSession.builder.appName("FurnitureStoreDataset").getOrCreate()

# List of furniture products and their categories
products = [
    ("Classic Wooden Bed", "Bedroom Furniture"),
    ("Memory Foam Mattress", "Bedroom Furniture"),
    ("L-Shaped Sofa", "Living Room Furniture"),
    ("Recliner Chair", "Living Room Furniture"),
    ("Oak Dining Table", "Dining Room Furniture"),
    ("Leather Dining Chair", "Dining Room Furniture"),
    ("Wooden Coffee Table", "Living Room Furniture"),
    ("Modular Bookcase", "Home Office Furniture"),
    ("Ergonomic Office Chair", "Home Office Furniture"),
    ("Metal Bed Frame", "Bedroom Furniture"),
    ("Vanity Dresser", "Bedroom Furniture"),
    ("Upholstered Bench", "Entryway Furniture"),
    ("Glass Console Table", "Entryway Furniture"),
    ("TV Stand", "Living Room Furniture"),
    ("Outdoor Patio Set", "Outdoor Furniture"),
    ("Garden Lounge Chair", "Outdoor Furniture"),
    ("Kids Bunk Bed", "Kids Furniture"),
    ("Nursery Rocking Chair", "Kids Furniture"),
    ("Shoe Storage Cabinet", "Entryway Furniture"),
    ("Standing Desk", "Home Office Furniture")
]

# Function to randomly generate a product
def generate_product():
    return random.choice(products)

# Generate a list of records with random data
def generate_records(num_records):
    data = []
    for i in range(num_records):
        product_name, product_category = generate_product()
        record = Row(
            order_id=i + 1,
            customer_id=random.randint(1, 100),
            customer_name=random.choice(["Mike", "Anna", "John", "Sarah", "Tom"]),
            product_id=random.randint(1, 50),
            product_name=product_name,
            product_category=product_category,
            payment_type=random.choice(["card", "cash", "online"]),
            qty=random.randint(1, 5),
            price=round(random.uniform(20, 500), 2),
            datetime=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            country="USA",
            city=random.choice(["Houston", "Austin", "Dallas"]),
            ecommerce_website_name="everythingstore.com",
            payment_txn_id=str(random.randint(1000, 9999)),
            payment_txn_success=random.choice(["Y", "N"]),
            failure_reason=random.choice(["N/A", "Insufficient Funds", "Network Error"])
        )
        data.append(record)
    return data

# Generate 100 records
num_records = 100
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

# Show the DataFrame and schema
df.show()
df.printSchema()

# Write DataFrame to CSV 
df.write.csv("/project2/data", header=True, mode="overwrite")

