# Import necessary libraries
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("WriteCSVExample").getOrCreate()

# Sample DataFrame creation (assuming you have one)
data = [("Alice", 1), ("Bob", 2), ("Catherine", 3)]
columns = ["Name", "Id"]
df = spark.createDataFrame(data, columns)

# Write DataFrame to CSV
df.write.csv("project2/data.csv", header=True, mode="overwrite")