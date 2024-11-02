from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import udf, expr, to_timestamp, rand, concat, lit, lpad, col
import random
from var import product_blanket, product_hammock, customers, payment, products

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Random Data Generation") \
    .getOrCreate()

# Convert product list to a DataFrame
product_df = spark.createDataFrame(products, ["product_id", "product_name", "product_category", "price"])

# Convert customer list to a DataFrame
customer_df = spark.createDataFrame(customers, ["customer_id", "customer_name", "country", "city"])


# Custom UDFs for generating fields based on products
@udf("int")
def get_random_product_id():
    return random.choice([product[0] for product in products])

# Custom UDFs for generating fields based on customers 
@udf("int")
def get_random_customer_id():
    return random.choice([customer[0] for customer in customers])

# Create the 10,000 records
num_records = 10000  # Adjust number of records
df = spark.range(0, num_records)


# Rename the column "id" to "order_id"
df = df.withColumnRenamed("id", "order_id")


#add product_id column 
df = df.withColumn("product_id", get_random_product_id())

# left join to match product_id with product_name, product_category, price
df = df.join(product_df, "product_id", "left") 

# Date range: 1/1/2020 to 1/1/2022
start_datetime = "2020-01-01 00:00:00"
end_datetime = "2022-01-01 00:00:00"


# Calculate the difference in seconds between start and end datetime
date_format = "yyyy-MM-dd HH:mm:ss"
df_diff = spark.sql(f"SELECT unix_timestamp('{end_datetime}', '{date_format}') - unix_timestamp('{start_datetime}', '{date_format}') AS diff").collect()[0][0]

# Generate a random timestamp by adding a random number of seconds to the start_datetime
df = df.withColumn(
    "random_timestamp",
    to_timestamp(expr(f"from_unixtime(unix_timestamp('{start_datetime}') + cast(rand() * {df_diff} as int))"))
)


#add customer_id column 
df = df.withColumn("customer_id", get_random_customer_id())


# left join to match customer_id with customer_name, country, and city
df = df.join(customer_df, "customer_id", "left") 


# Create a new DataFrame with 10,000
df_2 = spark.range(0, num_records).withColumnRenamed("id", "order_id")

# Generate a unique alphanumeric payment_txn_id with prefix "CODE"
df_2 = df_2.withColumn("payment_txn_id", concat(lit("CODE"), lpad(col("order_id").cast("string"), 12, "0")))


#join the new Data Frame with the main data frame
df = df.join(df_2, on = "order_id", how= "inner")












#Coalesce the DataFrame to a single partition
df = df.coalesce(1)

# Show the DataFrame and schema
df.show()
df.printSchema()

# Write DataFrame to CSV 
df.write.csv("/project2/data", header=True, mode="overwrite")