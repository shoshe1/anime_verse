from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, expr
import sys

spark = SparkSession.builder \
    .appName("CheckBronzeData") \
    .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.my_catalog.type", "hadoop") \
    .config("spark.sql.catalog.my_catalog.warehouse", "s3a://warehouse/") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# ✅ You define this: How far back to check
window_minutes = 2

# Compare ingestion_ts or event_ts > now - 2 minutes
time_condition = f"event_ts >= current_timestamp() - interval {window_minutes} minutes"

bronze_tables = [
    "my_catalog.bronze.bronze_pos_transaction_events",
    "my_catalog.bronze.bronze_ticket_booking_events",
    "my_catalog.bronze.bronze_supplier_delivery_events"
]

data_found = False

for table in bronze_tables:
    df = spark.table(table)
    if "event_ts" in df.columns:
        recent_count = df.filter(expr(time_condition)).count()
        if recent_count > 0:
            print(f"✅ Found {recent_count} new rows in {table}")
            data_found = True
            break

if not data_found:
    print("⚠️ No new data in bronze tables (last {} minutes).".format(window_minutes))
    sys.exit(99)  # Special exit code — DAG will detect this and skip
else:
    print("✅ New data found — ready to run Bronze to Silver.")
    sys.exit(0)
