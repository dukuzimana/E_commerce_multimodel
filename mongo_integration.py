# mongo_integration.py
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Mongo Integration").getOrCreate()

# Load cleaned data
users_df = spark.read.parquet("data_clean/users.parquet")
products_df = spark.read.parquet("data_clean/products.parquet")
transactions_df = spark.read.parquet("data_clean/transactions.parquet")

# MongoDB URI
mongo_uri = "mongodb://localhost:27017/ecommerce"

# Write to MongoDB
users_df.write.format("mongo").mode("overwrite").option("uri", f"{mongo_uri}.users").save()
products_df.write.format("mongo").mode("overwrite").option("uri", f"{mongo_uri}.products").save()
transactions_df.write.format("mongo").mode("overwrite").option("uri", f"{mongo_uri}.transactions").save()

# Read back sample
mongo_users = spark.read.format("mongo").option("uri", f"{mongo_uri}.users").load()
print("Sample Users from MongoDB:")
mongo_users.show(3)
