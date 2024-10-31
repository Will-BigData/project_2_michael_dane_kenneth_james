import random
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, TimestampType
from pyspark.sql import Row
from pyspark.sql.functions import to_timestamp
import pyspark.sql.functions as sf

# Initialize Spark session
spark = SparkSession.builder.appName("FurnitureStoreDataset").getOrCreate()

# Create Spark Context
sc = spark.sparkContext

fnames = ["James","Mary","Michael","Patricia","Robert","Jennifer","John","David","Elizabeth","William","Barbara",
"Richard","Susan","Joseph","Jessica","Thomas","Karen","Christopher","Sarah","Charles","Lisa","Daniel","Nancy","Matthew","Sandra","Anthony","Betty",
"Mark","Ashley","Donald","Emily","Steven","Kimberly","Andrew","Margaret","Paul","Donna","Joshua","Michelle","Kenneth","Carol",
"Kevin","Amanda","Brian","Melissa","Timothy","Deborah","Ronald","Stephanie"]

lnames = ["Anderson", "Baker", "Campbell", "Clark", "Davis", "Edwards", "Foster", "Garcia", "Harris", "Johnson", "King", "Lewis",
"Martinez", "Nelson", "Brine", "Patel", "Ramirez", "Smith", "Taylor", "Thompson", "Upton", "Vasquez", "Walker", "White", "Young",
"Adams", "Bell", "Carter", "Diaz", "Elliott", "Fisher", "Gomez", "Hayes", "Jenkins", "Kelly", "Long", "Morgan", "Nguyen", "Ortiz",
"Price", "Reed", "Sanchez", "Torres", "Wright", "Xu", "Yang", "Zeller", "Bishop", "Harper", "Kim"]

customerIDs = sc.parallelize(range(1,301))
df = customerIDs.map(lambda x: (x,)).toDF(["customer_id"])

#creating a function for pyspark to use
def random_name():
    first_name = random.choice(fnames)
    last_name = random.choice(lnames)
    return f"{first_name} {last_name}"



#passing the function to pyspark as a user-defined function so it can see it
random_name_udf = sf.udf(random_name)

#passing the udf to withColumn, which will run the function for every row
customer_table = df.withColumn("customer_name", random_name_udf())
print(customer_table.count())

customer_table_2 = df.withColumn("customer_name", random_name_udf())

customer_all_table = customer_table.union(customer_table_2)

print(customer_all_table.count())

customer_all_table.show(600)