from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("DataCleaning").getOrCreate()

df = spark.read.csv("project2/data/trending_data.csv", header=True, inferSchema=True)