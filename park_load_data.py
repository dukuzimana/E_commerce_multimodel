# spark_load_data.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp

# Initialize Spark
spark = SparkSession.builder \
    .appName("AUCA E-commerce Analytics") \
    .getOrCreate()

# Paths
users_path = "data/users.json"
products_path = "data/products.json"
sessions_path = "data/sessions_*.json"
transactions_path = "data/transactions.json"

# Load JSON files
users_df = spark.read.json(users_path)
products_df = spark.read.json(products_path)
sessions_df = spark.read.json(sessions_path)
transactions_df = spark.read.json(transactions_path)

# Convert timestamps
sessions_df = sessions_df.withColumn("start_time", to_timestamp("start_time")) \
                         .withColumn("end_time", to_timestamp("end_time"))

transactions_df = transactions_df.withColumn("timestamp", to_timestamp("timestamp"))

# Show sample data
print("Users:")
users_df.show(3)
print("Products:")
products_df.show(3)
print("Sessions:")
sessions_df.show(3)
print("Transactions:")
transactions_df.show(3)

# Save cleaned Spark DataFrames as Parquet for faster reuse
users_df.write.mode("overwrite").parquet("data_clean/users.parquet")
products_df.write.mode("overwrite").parquet("data_clean/products.parquet")
sessions_df.write.mode("overwrite").parquet("data_clean/sessions.parquet")
transactions_df.write.mode("overwrite").parquet("data_clean/transactions.parquet")
