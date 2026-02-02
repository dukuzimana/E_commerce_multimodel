# hbase_integration.py
import happybase
import json
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("HBase Integration").getOrCreate()
sessions_df = spark.read.parquet("data_clean/sessions.parquet")

# Connect to HBase
connection = happybase.Connection('localhost')
connection.open()

# Create table
table_name = 'user_sessions'
if table_name.encode() not in connection.tables():
    connection.create_table(
        table_name,
        {'info': dict(), 'events': dict()}
    )

table = connection.table(table_name)

# Insert sessions (small subset for demo)
for row in sessions_df.limit(100).collect():  # limit to 100 for demo
    key = f"{row['user_id']}_{int(row['start_time'].timestamp())}"
    table.put(key, {
        'info:start_time': row['start_time'].isoformat(),
        'info:end_time': row['end_time'].isoformat(),
        'info:duration': str(row['duration_seconds']),
        'events:viewed_products': json.dumps(row['viewed_products']),
        'events:cart_contents': json.dumps(row['cart_contents'])
    })

# Sample query for a user
print("Sample HBase sessions for user_000042:")
for key, data in table.scan(row_prefix="user_000042"):
    print(key, data)
