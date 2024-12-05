import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lag, avg, stddev
from pyspark.sql.window import Window

# Initialize Spark Session
spark = SparkSession.builder.appName("StocksAnalysis").getOrCreate()

# Load Data
data_path = "stocks_data.csv"  # Local path to your CSV file
df = spark.read.csv(data_path, header=True, inferSchema=True)


df.show()

# Compute the average daily return of all stocks for every date
window_spec = Window.partitionBy("ticker").orderBy("date")
df = df.withColumn("prev_close", lag(col("close")).over(window_spec))
df = df.withColumn("return", (col("close") - col("prev_close")) / col("prev_close"))
avg_daily_return = df.groupBy("date").agg(avg(col("return")).alias("average_return"))

# Save to local file system
avg_daily_return.write.csv('output/average_daily_return/', header=True, mode='overwrite')

# Which stock was traded with the highest worth - closing price * volume - on average?
df = df.withColumn("worth", col("close") * col("volume"))
highest_worth = df.groupBy("ticker").agg(avg(col("worth")).alias("average_worth"))
highest_worth = highest_worth.orderBy(col("average_worth").desc()).limit(1)

# Save to local file system
highest_worth.write.csv('output/highest_worth/', header=True, mode='overwrite')

# Which stock was the most volatile?
volatility = df.groupBy("ticker").agg(stddev(col("return")).alias("stddev"))
most_volatile = volatility.orderBy(col("stddev").desc()).limit(1)

# Save to local file system
most_volatile.write.csv('output/most_volatile/', header=True, mode='overwrite')

# Top 3 30-day return dates
df = df.withColumn("30_day_return", (col("close") - lag(col("close"), 30).over(window_spec)) / lag(col("close"), 30).over(window_spec))
top_30_day_return = df.orderBy(col("30_day_return").desc()).select("ticker", "date").limit(3)

# Save to local file system
top_30_day_return.write.csv('output/top_30_day_return/', header=True, mode='overwrite')

# Stop Spark session
spark.stop()




# # Upload average_daily_return directory
# aws s3 cp output/average_daily_return/ s3://data-engineer-assignment-eyal-moses/average_daily_return/ --recursive
#
# # Upload highest_worth directory
# aws s3 cp output/highest_worth/ s3://data-engineer-assignment-eyal-moses/highest_worth/ --recursive
#
# # Upload most_volatile directory
# aws s3 cp output/most_volatile/ s3://data-engineer-assignment-eyal-moses/most_volatile/ --recursive
#
# # Upload top_30_day_return directory
# aws s3 cp output/top_30_day_return/ s3://data-engineer-assignment-eyal-moses/top_30_day_return/ --recursive





