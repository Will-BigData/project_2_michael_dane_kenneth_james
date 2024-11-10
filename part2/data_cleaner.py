from pyspark.sql import SparkSession
from pyspark.sql.functions import when

spark = SparkSession.builder.appName("DataCleaning").getOrCreate()

df = spark.read.csv("/project2/trending_data.csv", header=True, inferSchema=True)



#Remove rows with NaN or null values in all columns expect for "failure_reason"
columns_to_check = ['city', 'country', 'customer_id', 'customer_name', 'datetime', 'ecommerce_website_name', 'gender', 'median_age', 'order_id', 'payment_transaction_id', 'payment_transaction_success', 'payment_type', 'population', 'price', 'product_category', 'product_id', 'product_name', 'qty']
df = df.na.drop(subset = columns_to_check)


df.createOrReplaceTempView("table") 

#Filter out rows where price <= 0 and qty <= 0" 
filter_df = spark.sql("SELECT * FROM table WHERE price > 0 and qty > 0")


filter_df.show()

#Findthe qty sold for each product name
pop_products_df = filter_df.select('product_name', 'price', 'qty', 'payment_transaction_success')
pop_products_df = pop_products_df.filter(pop_products_df.payment_transaction_success == "Y")
pop_products_df = pop_products_df.select('product_name', 'price', 'qty')
pop_products_df = pop_products_df.groupBy("product_name").sum().sort("sum(qty)", ascending=False)
pop_products_df.show()


#Find automobile product sales by gender
car_df = filter_df.select('product_category','product_name','price', 'qty', 'gender', 'payment_transaction_success')
car_df = car_df.filter(car_df.payment_transaction_success == "Y")
car_df = car_df.select('product_category','product_name','price', 'qty', 'gender')
car_df = car_df.withColumn("gross_product", car_df.qty * car_df.price)
car_df = car_df.select('product_category', 'product_name', 'gross_product', 'gender')
car_df = car_df.groupBy('product_name', 'product_category', 'gender').sum()
car_df = car_df.filter(car_df.product_category == "Automobile").sort(["product_name", "sum(gross_product)"], ascending=[False, False])
car_df.show()


#Find and group all transaction Failures
# fail_df = df.select('country','city','qty','payment_transaction_success')
# fail_df = fail_df.filter(fail_df.payment_transaction_success == "N")
# fail_df = fail_df.withColumn("payment_transaction_success", when(fail_df.payment_transaction_success == "N", 1))
# fail_df = fail_df.select(fail_df.country,fail_df.city,fail_df.qty,fail_df.payment_transaction_success.alias("failed_transactions"))



# Write the filtered/cleaned data to CSV 
filter_df.coalesce(1).write.csv("/project2/data", header=True, mode="overwrite")


pop_products_df.coalesce(1).write.csv("/project2/analysis1", header=True, mode="overwrite")

car_df.coalesce(1).write.csv("/project2/analysis2", header=True, mode="overwrite")


