from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("DataCleaning").getOrCreate()

df = spark.read.csv("/project2/trending_data.csv", header=True, inferSchema=True)



#Remove rows with NaN or null values in all columns expect for "failure_reason"
columns_to_check = ['city', 'country', 'customer_id', 'customer_name', 'datetime', 'ecommerce_website_name', 'gender', 'median_age', 'order_id', 'payment_transaction_id', 'payment_transaction_success', 'payment_type', 'population', 'price', 'product_category', 'product_id', 'product_name', 'qty']
df = df.na.drop(subset = columns_to_check)


df.createOrReplaceTempView("table") 

#Filter out rows where price <= 0 and qty <= 0" 
filter_df = spark.sql("SELECT COUNT(*) FROM table WHERE price > 0 and qty > 0")


filter_df.show()