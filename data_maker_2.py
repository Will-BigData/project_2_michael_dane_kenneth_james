from func import *
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, TimestampType
from pyspark.sql import Row
from pyspark.sql.functions import to_timestamp
from datetime import datetime

# Initialize Spark session
spark = SparkSession.builder.appName("FurnitureStoreDataset").getOrCreate()



# generating records
def generate_records(num_records, date_generator, product_type, pay_type, order_id):
    data = []
    for i in range(num_records):
        product_id, product_name, product_category, price = generate_product(product_type)
        customer_id, customer_name, country, city = generate_customers()
        payment_txn_success = random.choice(["Y", "N"])
        bulk_mulitplier = check_city(city)
        if pay_type == True:
            payment = "Internet Banking"
        else:
            payment = random.choice(["Card", "Internet Banking", "UPI", "Wallet"])
        record = Row(
            order_id=order_id+i+1,
            customer_id= customer_id,
            customer_name= customer_name,
            product_id=product_id, 
            product_name=product_name,
            product_category=product_category,
            payment_type=payment,
            qty=random.randint(1, 5) * bulk_mulitplier,
            price=price,  
            datetime=date_generator().strftime("%Y-%m-%d %H:%M:%S"),
            country=country,
            city= city,
            ecommerce_website_name= random.choice(websites),
            payment_txn_id=str(random.randint(1000, 9999)),
            payment_txn_success= payment_txn_success,
            failure_reason= "SUCCESS" if payment_txn_success == "Y" else random.choice(["Insufficient Funds", "Network Error"])
        )
        data.append(record)
    return data

# Generate 10000 random records
num_records = 10000
data = generate_records(num_records, date_generator, products, False,0)

#Generate 300 records of michael's trend
michael_trend = generate_records(300, december_date, product_blanket, False,10000)

data.extend(michael_trend)

#Generate 500 records of Kenny's Trend
kenny_trend = generate_records(500, random_date_in_summer, product_hammock, False, 10300)

data.extend(kenny_trend)

dane_trend = generate_records(300, date_generator, products, True, 10800)

data.extend(dane_trend)

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


