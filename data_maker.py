import random
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, TimestampType
from pyspark.sql import Row
from pyspark.sql.functions import to_timestamp
from datetime import datetime

# Initialize Spark session
spark = SparkSession.builder.appName("FurnitureStoreDataset").getOrCreate()

# List of first names

fnames = ["James","Mary","Michael","Patricia","Robert","Jennifer","John","David","Elizabeth","William","Barbara",
"Richard","Susan","Joseph","Jessica","Thomas","Karen","Christopher","Sarah","Charles","Lisa","Daniel","Nancy","Matthew","Sandra","Anthony","Betty",
"Mark","Ashley","Donald","Emily","Steven","Kimberly","Andrew","Margaret","Paul","Donna","Joshua","Michelle","Kenneth","Carol",
"Kevin","Amanda","Brian","Melissa","Timothy","Deborah","Ronald","Stephanie"]

lnames = ["Anderson", "Baker", "Campbell", "Clark", "Davis", "Edwards", "Foster", "Garcia", "Harris", "Johnson", "King", "Lewis",
"Martinez", "Nelson", "Brine", "Patel", "Ramirez", "Smith", "Taylor", "Thompson", "Upton", "Vasquez", "Walker", "White", "Young",
"Adams", "Bell", "Carter", "Diaz", "Elliott", "Fisher", "Gomez", "Hayes", "Jenkins", "Kelly", "Long", "Morgan", "Nguyen", "Ortiz",
"Price", "Reed", "Sanchez", "Torres", "Wright", "Xu", "Yang", "Zeller", "Bishop", "Harper", "Kim"]

locations = [
    ("United States", "New York City"), ("United States", "Los Angeles"), ("United States", "Chicago"), ("United States", "Miami"), 
    ("France", "Paris"), ("France", "Marseille"), ("France", "Lyon"), ("France", "Nice"),
    ("Japan", "Tokyo"), ("Japan", "Osaka"), ("Japan", "Kyoto"), ("Japan", "Hiroshima"),
    ("Australia", "Sydney"), ("Australia", "Melbourne"), ("Australia", "Brisbane"), ("Australia", "Perth"),
    ("Italy", "Rome"), ("Italy", "Milan"), ("Italy", "Venice"), ("Italy", "Florence"),
    ("Canada", "Toronto"), ("Canada", "Vancouver"), ("Canada", "Montreal"), ("Canada", "Calgary"),
    ("Spain", "Madrid"), ("Spain", "Barcelona"), ("Spain", "Valencia"), ("Spain", "Seville"),
    ("Germany", "Berlin"), ("Germany", "Munich"), ("Germany", "Hamburg"), ("Germany", "Frankfurt")]

# Updated list with hard-coded product_id, product_name, product_category, and price
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
    (21, "Blanket", "Bedroom Furniture", 29.99)  # Added Blanket with price
]

# Function to randomly generate a product with product_id, product_name, product_category, and price
def generate_product():
    product_id, product_name, product_category, price = random.choice(products)
    return product_id, product_name, product_category, price

# Example usage in generating records
def generate_records(num_records):
    data = []
    for i in range(num_records):
        product_id, product_name, product_category, price = generate_product()
        record = Row(
            order_id=i + 1, #change later
            customer_id=random.randint(1, 100), #change later
            customer_name=random.choice(["Mike", "Anna", "John", "Sarah", "Tom"]), #change later
            product_id=product_id, 
            product_name=product_name,
            product_category=product_category,
            payment_type=random.choice(["card", "cash", "online"]),
            qty=random.randint(1, 5),
            price=price,  # Use the hard-coded price
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
randomdf = spark.createDataFrame(data, schema=schema)
trend1df = spark.createDataFrame(data, schema=schema)
trend2df = spark.createDataFrame(data, schema=schema)
trend3df = spark.createDataFrame(data, schema=schema)
trend4df = spark.createDataFrame(data, schema=schema)


# Convert 'datetime' to TimestampType
randomdf = randomdf.withColumn("datetime", to_timestamp("datetime", "yyyy-MM-dd HH:mm:ss"))











#Coalesce the DataFrame to a single partition
randomdf = randomdf.coalesce(1)

# Show the DataFrame and schema
randomdf.show()
randomdf.printSchema()

# Write DataFrame to CSV 
randomdf.write.csv("/project2/data", header=True, mode="overwrite")


