# spark_batch_analytics.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, month, year, sum as spark_sum

spark = SparkSession.builder.appName("Batch Analytics").getOrCreate()

# Load cleaned data
users_df = spark.read.parquet("data_clean/users.parquet")
products_df = spark.read.parquet("data_clean/products.parquet")
sessions_df = spark.read.parquet("data_clean/sessions.parquet")
transactions_df = spark.read.parquet("data_clean/transactions.parquet")

# --- Top Viewed Products ---
viewed_products_df = sessions_df.select(explode("viewed_products").alias("product_id"))
top_viewed = viewed_products_df.groupBy("product_id").count().orderBy(col("count").desc())
print("Top Viewed Products:")
top_viewed.show(10)

# --- Cohort Analysis ---
users_df = users_df.withColumn("reg_month", month("registration_date")) \
                   .withColumn("reg_year", year("registration_date"))

user_tx_df = transactions_df.join(users_df, "user_id")
cohort_revenue = user_tx_df.groupBy("reg_year", "reg_month") \
                           .agg(spark_sum("total").alias("total_revenue")) \
                           .orderBy("reg_year", "reg_month")
print("Cohort Revenue:")
cohort_revenue.show()

# --- Products Frequently Bought Together ---
items_df = transactions_df.select(col("transaction_id"), explode("items").alias("item"))
also_bought_df = items_df.alias("a").join(
    items_df.alias("b"), on="transaction_id"
).filter(col("a.item.product_id") != col("b.item.product_id"))

co_purchase = also_bought_df.groupBy(
    col("a.item.product_id").alias("product_A"),
    col("b.item.product_id").alias("product_B")
).count().orderBy(col("count").desc())

print("Products Frequently Bought Together:")
co_purchase.show(10)
