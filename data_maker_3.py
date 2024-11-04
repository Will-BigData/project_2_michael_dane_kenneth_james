from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import udf, expr, to_timestamp, rand, concat, lit, lpad, col, when
import random
from var import *
from func import check_city

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Furniture Store Data Generation 3.0") \
    .getOrCreate()

# Convert product list to a DataFrame
product_df = spark.createDataFrame(products, ["product_id", "product_name", "product_category", "price"])

# Convert customer list to a DataFrame
customer_df = spark.createDataFrame(customers, ["customer_id", "customer_name", "country", "city"])


#michael's trend
michael_df = spark.createDataFrame(product_blanket, ["product_id", "product_name", "product_category", "price"])

#kenny's trend 
kenny_df = spark.createDataFrame(product_hammock, ["product_id", "product_name", "product_category", "price"])



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

# Generate a random timestamp by adding a random number of seconds to the start_datetime for datetime column
df = df.withColumn(
    "datetime",
    to_timestamp(expr(f"from_unixtime(unix_timestamp('{start_datetime}') + cast(rand() * {df_diff} as int))"))
)


#add customer_id column 
df = df.withColumn("customer_id", get_random_customer_id())


# left join to match customer_id with customer_name, country, and city
df = df.join(customer_df, "customer_id", "left") 





#ADDING MY TREND 
df_150 = spark.range(num_records, num_records+150).withColumnRenamed("id", "order_id")


#add product_id column 
df_150 = df_150.withColumn("product_id", lit(21))

# left join to match product_id with product_name, product_category, price
df_150 = df_150.join(michael_df, "product_id", "left") 

# Date range: 12/1/2020 to 12/30/2020
start_datetime = "2020-12-01 00:00:00"
end_datetime = "2020-12-31 23:59:59"


# Calculate the difference in seconds between start and end datetime
date_format = "yyyy-MM-dd HH:mm:ss"
df_diff = spark.sql(f"SELECT unix_timestamp('{end_datetime}', '{date_format}') - unix_timestamp('{start_datetime}', '{date_format}') AS diff").collect()[0][0]

# Generate a random timestamp by adding a random number of seconds to the start_datetime for datetime column
df_150 = df_150.withColumn(
    "datetime",
    to_timestamp(expr(f"from_unixtime(unix_timestamp('{start_datetime}') + cast(rand() * {df_diff} as int))"))
)


#add customer_id column 
df_150 = df_150.withColumn("customer_id", get_random_customer_id())


# left join to match customer_id with customer_name, country, and city
df_150 = df_150.join(customer_df, "customer_id", "left") 



#Stack df_150 below df
df = df.union(df_150)


num_records += 150 
df_150 = spark.range(num_records, num_records+150).withColumnRenamed("id", "order_id")


#add product_id column 
df_150 = df_150.withColumn("product_id", lit(21))

# left join to match product_id with product_name, product_category, price
df_150 = df_150.join(michael_df, "product_id", "left") 

# Date range: 12/1/2021 to 12/30/2021
start_datetime = "2021-12-01 00:00:00"
end_datetime = "2021-12-31 23:59:59"


# Calculate the difference in seconds between start and end datetime
date_format = "yyyy-MM-dd HH:mm:ss"
df_diff = spark.sql(f"SELECT unix_timestamp('{end_datetime}', '{date_format}') - unix_timestamp('{start_datetime}', '{date_format}') AS diff").collect()[0][0]

# Generate a random timestamp by adding a random number of seconds to the start_datetime for datetime column
df_150 = df_150.withColumn(
    "datetime",
    to_timestamp(expr(f"from_unixtime(unix_timestamp('{start_datetime}') + cast(rand() * {df_diff} as int))"))
)


#add customer_id column 
df_150 = df_150.withColumn("customer_id", get_random_customer_id())


# left join to match customer_id with customer_name, country, and city
df_150 = df_150.join(customer_df, "customer_id", "left") 



#Stack df_150 below df
df = df.union(df_150)




#END OF MY TREND









#ADDING THESE COLUMNS AT THE END 


# Define ecommerce_website_name probabilities
choice_column = (
    when(rand() < 0.2, websites[0])
    .when(rand() < 0.4, websites[1])
    .when(rand() < 0.6, websites[2])
    .when(rand() < 0.8, websites[3])
    .otherwise(websites[4])
)



#adding ecommerce_website_name column
df = df.withColumn("ecommerce_website_name",choice_column)




# Create a new DataFrame with 10,000
df_2 = spark.range(0, num_records).withColumnRenamed("id", "order_id")

# Generate a unique alphanumeric payment_txn_id with prefix "CODE"
df_2 = df_2.withColumn("payment_txn_id", concat(lit("CODE"), lpad(col("order_id").cast("string"), 12, "0")))


#join the new Data Frame with the main data frame
df = df.join(df_2, on = "order_id", how= "inner")


# Define payment_txn_success probabilities
choice_column = (
    when(rand() < 0.5, success[0])
    .otherwise(success[1])
)



#adding payment_txn_success column
df = df.withColumn("payment_txn_success",choice_column)

# Define reason probabilities
N_reason = (
    when(rand() < 0.5, reason[0])
    .otherwise(reason[1])
)

#adding faliure_reason column
df = df.withColumn(
    "failure_reason",
    when(col("payment_txn_success") == "Y", "SUCCESS" )
    .otherwise(N_reason) 
)


#adding qty column, first when condition addes James's trend
df = df.withColumn(
    "qty",
    when(
        (col("city") == "Chicago") | 
        (col("city") == "Vancouver") | 
        (col("city") == "Los Angeles") | 
        (col("city") == "Berlin"),
        random.randint(1, 5) * random.randint(4, 7)
    )
    .otherwise(random.randint(1, 5))
)



""" 


# adds between 1 to 2 percent rouge data

# nullify some rows from product_id
column_to_nullify = "product_id"

# Nullify approximately 1% of rows from product_id
df = df.withColumn(
    column_to_nullify,
    when(rand() < 0.01, lit(None)).otherwise(col(column_to_nullify))
)

# nullify some rows from country
column_to_nullify = "country"

# Nullify approximately 1% of rows from country
df = df.withColumn(
    column_to_nullify,
    when(rand() < 0.01, lit(None)).otherwise(col(column_to_nullify))
)

 """


#Coalesce the DataFrame to a single partition
df = df.coalesce(1)

# Show the DataFrame and schema
df.show()
df.printSchema()

# Write DataFrame to CSV 
df.write.csv("/project2/data", header=True, mode="overwrite")