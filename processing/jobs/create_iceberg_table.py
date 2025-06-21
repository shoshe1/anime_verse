from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("IcebergCreateTableTest") \
    .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.my_catalog.type", "hadoop") \
    .config("spark.sql.catalog.my_catalog.warehouse", "s3a://warehouse") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# Drop table if exists
spark.sql("DROP TABLE IF EXISTS my_catalog.bronze.bronze_pos_transaction_events")

# Create Iceberg table with ORC format
spark.sql("""
    CREATE TABLE my_catalog.bronze.bronze_pos_transaction_events (
        transaction_id STRING,
        event_ts TIMESTAMP,
        ingestion_ts TIMESTAMP,
        customer_id STRING,
        store_id STRING,
        product_id STRING,
        quantity_purchased INT,
        unit_price_at_sale FLOAT
    )
    USING ICEBERG
    TBLPROPERTIES ('write.format.default'='orc')
""")

print(" Iceberg table created in MinIO bucket 'warehouse/bronze'")
spark.stop()
