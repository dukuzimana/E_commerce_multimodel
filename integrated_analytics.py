# integrated_analytics.py
import happybase
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Integrated Analytics").getOrCreate()

# Load MongoDB transactions & users
mongo_uri = "mongodb://localhost:27017/ecommerce"
users_df = spark.read.format("mongo").option("uri", f"{mongo_uri}.users").load()
transactions_df = spark.read.format("mongo").option("uri", f"{mongo_uri}.transactions").load()

# Top 5 users by total spending
top_customers_df = transactions_df.groupBy("user_id") \
                                  .sum("total") \
                                  .orderBy("sum(total)", ascending=False) \
                                  .limit(5)
top_user_ids = [row['user_id'] for row in top_customers_df.collect()]
print("Top Customers from MongoDB:")
top_customers_df.show()

# Connect to HBase
connection = happybase.Connection('localhost')
connection.open()
table = connection.table('user_sessions')

# Fetch sessions for top customers
for user_id in top_user_ids:
    print(f"\nSessions for {user_id}:")
    for key, data in table.scan(row_prefix=user_id):
        print(key, data)
