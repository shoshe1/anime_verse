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

print(" Iceberg bronze_pos_transaction_events table created in MinIO bucket 'warehouse/bronze'")

#bronze_ticket_booking_events

spark.sql("DROP TABLE IF EXISTS my_catalog.bronze.bronze_ticket_booking_events")

# Create Iceberg table with ORC format
spark.sql("""
    CREATE TABLE my_catalog.bronze.bronze_ticket_booking_events (
        booking_id STRING,
        event_ts TIMESTAMP,
        ingestion_ts TIMESTAMP,
        customer_id STRING,
        screening_id STRING,
        quantity_purchased INT,
        ticket_quantity INT,
        unit_price_at_sale FLOAT
    )
    USING ICEBERG
    TBLPROPERTIES ('write.format.default'='orc')
""")

print(" Iceberg bronze_ticket_booking_events table created in MinIO bucket 'warehouse/bronze'")

#bronze_supplier_delivery_events 

spark.sql("DROP TABLE IF EXISTS my_catalog.bronze.bronze_supplier_delivery_events ")

# Create Iceberg table with ORC format
spark.sql("""
    CREATE TABLE my_catalog.bronze.bronze_supplier_delivery_events  (
        delivery_id STRING,
        event_ts TIMESTAMP,
        ingestion_ts TIMESTAMP,
        supplier_id STRING,
        product_id STRING,
        quantity_delivered INT,
        unit_cost FLOAT
    )
    USING ICEBERG
    TBLPROPERTIES ('write.format.default'='orc')
""")

print(" Iceberg bronze_supplier_delivery_events  table created in MinIO bucket 'warehouse/bronze'")



#bronze_anime_broadcast_schedule  

spark.sql("DROP TABLE IF EXISTS my_catalog.bronze.bronze_anime_broadcast_schedule  ")

# Create Iceberg table with ORC format
spark.sql("""
    CREATE TABLE my_catalog.bronze.bronze_anime_broadcast_schedule   (
        broadcast_id STRING,
        ingestion_ts TIMESTAMP,
        anime_id STRING,
        title STRING,
        genre STRING,
        studio STRING,
        broadcast_end_ts TIMESTAMP,
        broadcast_start_ts TIMESTAMP,
        season STRING,
    )
    USING ICEBERG
    TBLPROPERTIES ('write.format.default'='orc')
""")

print(" Iceberg bronze_anime_broadcast_schedule   table created in MinIO bucket 'warehouse/bronze'")


#bronze_customer_registration_events   

spark.sql("DROP TABLE IF EXISTS my_catalog.bronze.bronze_customer_registration_events   ")

# Create Iceberg table with ORC format
spark.sql("""
    CREATE TABLE my_catalog.bronze.bronze_customer_registration_events    (
        registration_id STRING,
        event_ts TIMESTAMP,
        customer_id STRING,
        name STRING,
        email STRING,
        ingestion_ts TIMESTAMP,
    )
    USING ICEBERG
    TBLPROPERTIES ('write.format.default'='orc')
""")

print(" Iceberg bronze_customer_registration_events    table created in MinIO bucket 'warehouse/bronze'")

#bronze_concession_purchase_events 

spark.sql("DROP TABLE IF EXISTS my_catalog.bronze.bronze_concession_purchase_events  ")

# Create Iceberg table with ORC format
spark.sql("""
    CREATE TABLE my_catalog.bronze.bronze_concession_purchase_events   (
        purchase_id STRING,
        event_ts TIMESTAMP,
        ingestion_ts TIMESTAMP,
        screening_id STRING,
        item_id STRING,
        item_quantity INT,
        item_unit_price FLOAT
    )
    USING ICEBERG
    TBLPROPERTIES ('write.format.default'='orc')
""")

print(" Iceberg bronze_concession_purchase_events table created in MinIO bucket 'warehouse/bronze'")



spark.stop()
