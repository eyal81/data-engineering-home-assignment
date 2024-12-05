import sys
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lag, avg
from pyspark.sql.window import Window
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions

# Read parameters from Glue job
args = getResolvedOptions(sys.argv, ['input_path', 'output_path'])

# Initialize Glue and Spark contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Load Data from S3
data_path = args['input_path']  # S3 path to your CSV file
df = spark.read.csv(data_path, header=True, inferSchema=True)

# Show DataFrame schema and sample data
df.printSchema()
df.show()

# Compute the average daily return of all stocks for every date
window_spec = Window.partitionBy("ticker").orderBy("date")
df = df.withColumn("prev_close", lag(col("close")).over(window_spec))
df = df.withColumn("return", (col("close") - col("prev_close")) / col("prev_close"))
avg_daily_return = df.groupBy("date").agg(avg(col("return")).alias("average_return"))

# Save the result to S3
output_path = args['output_path']  # S3 path to save the output CSV file
avg_daily_return.write.csv(output_path, header=True, mode='overwrite')