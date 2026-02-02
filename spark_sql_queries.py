# spark_sql_queries.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col

spark = SparkSession.builder.appName("Spark SQL Analytics").getOrCreate()

# Load cleaned data
products_df = spark.read.parquet("data_clean/products.parquet")
transactions_df = spark.read.parquet("data_clean/transactions.parquet")

# Register temp views
products_df.createOrReplaceTempView("products")
transactions_df.createOrReplaceTempView("transactions")

# SQL Query: Total revenue per category
result = spark.sql("""
SELECT p.category_id, SUM(t.total) as total_revenue
FROM transactions t
LATERAL VIEW explode(t.items) exploded_item AS item
JOIN products p ON exploded_item.item.product_id = p.product_id
GROUP BY p.category_id
ORDER BY total_revenue DESC
""")

result.show(10)
