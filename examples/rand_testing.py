from pyspark.sql import SparkSession
from pyspark.sql.functions import rand, when
from pyspark.sql import functions as F

#testing how to make rand() from pyspark work like python's random.choice()


#Spark session
spark = SparkSession.builder.appName("RandomChoice").getOrCreate()

# Sample DataFrame
df = spark.createDataFrame([(1,), (2,), (3,)], ["id"])


choices = ["Electronics", "Clothing", "Books"]  # List of choices

# Define probabilities
choice_column = (
    when(rand() < 0.33, choices[0])
    .when(rand() < 0.66, choices[1])
    .otherwise(choices[2])
)

# Add random choice column to DataFrame
df = df.withColumn("random_choice", choice_column)
df.show()