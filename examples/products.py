from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import udf, expr
import random

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Random Data Generation") \
    .getOrCreate()

# Product list with product_id, product_name, product_category, and price
products = [
    (1, "Classic Wooden Bed", "Bedroom Furniture", 299.99),
    (2, "Memory Foam Mattress", "Bedroom Furniture", 199.99),
    (3, "L-Shaped Sofa", "Living Room Furniture", 499.99),
    (4, "Recliner Chair", "Living Room Furniture", 249.99),
    (5, "Oak Dining Table", "Dining Room Furniture", 399.99),
    (6, "Leather Dining Chair", "Dining Room Furniture", 89.99),
    (7, "Wooden Coffee Table", "Living Room Furniture", 149.99),
    (8, "Modular Bookcase", "Home Office Furniture", 199.99),
    (9, "Ergonomic Office Chair", "Home Office Furniture", 129.99),
    (10, "Metal Bed Frame", "Bedroom Furniture", 149.99),
    (11, "Vanity Dresser", "Bedroom Furniture", 189.99),
    (12, "Upholstered Bench", "Entryway Furniture", 99.99),
    (13, "Glass Console Table", "Entryway Furniture", 129.99),
    (14, "TV Stand", "Living Room Furniture", 89.99),
    (15, "Outdoor Patio Set", "Outdoor Furniture", 299.99),
    (16, "Garden Lounge Chair", "Outdoor Furniture", 79.99),
    (17, "Kids Bunk Bed", "Kids Furniture", 249.99),
    (18, "Nursery Rocking Chair", "Kids Furniture", 119.99),
    (19, "Shoe Storage Cabinet", "Entryway Furniture", 59.99),
    (20, "Standing Desk", "Home Office Furniture", 249.99),
    (21, "Blanket", "Bedroom Furniture", 29.99)
]

# Convert product list to a DataFrame
product_df = spark.createDataFrame(products, ["product_id", "product_name", "product_category", "price"])

# Custom UDFs for generating fields based on products
@udf("int")
def get_random_product_id():
    return random.choice([product[0] for product in products])

# Create the main DataFrame with random data
num_records = 1000  # Adjust number of records
df = spark.range(0, num_records) \
    .withColumn("product_id", get_random_product_id()) \
    .join(product_df, "product_id", "left")


# Show sample data
df.show(10, truncate=False)