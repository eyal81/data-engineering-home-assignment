import findspark
findspark.init()

from pyspark.sql import SparkSession

# Configure and initialize Spark session
spark = SparkSession.builder \
    .appName("PySparkTest") \
    .config("spark.master", "local[*]") \
    .getOrCreate()

# Test Spark Context
print("Spark Context Initiated:", spark.sparkContext)
print("Spark Session Initiated:", spark)

# Example DataFrame operation
data = [("Alice", 1), ("Bob", 2), ("Catherine", 3)]
columns = ["Name", "Id"]
df = spark.createDataFrame(data, columns)
df.show()

# Stop Spark session
spark.stop()