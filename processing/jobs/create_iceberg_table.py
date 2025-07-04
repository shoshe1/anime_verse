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
        season STRING
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
        ingestion_ts TIMESTAMP
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

# #silver_pos_transactions_clean  

spark.sql("DROP TABLE IF EXISTS my_catalog.silver.silver_pos_transactions_clean")

# Create Iceberg table with ORC format
spark.sql("""
    CREATE TABLE my_catalog.silver.silver_pos_transactions_clean   (
        transaction_id STRING,
        transaction_ts TIMESTAMP,
        customer_id STRING,
        product_id STRING,
        quantity_purchased INT,
        unit_price_at_sale FLOAT,
           linked_anime_id STRING
    )
    USING ICEBERG
    TBLPROPERTIES ('write.format.default'='orc')
""")

print(" Iceberg silver_ticket_bookings_clean table created in MinIO bucket 'warehouse/silver'")


# #silver_ticket_bookings_clean   

# spark.sql("DROP TABLE IF EXISTS my_catalog.silver.silver_ticket_bookings_clean ")

# # Create Iceberg table with ORC format
# spark.sql("""
#     CREATE TABLE my_catalog.silver.silver_ticket_bookings_clean    (
#         booking_id STRING,
#         booking_ts TIMESTAMP,
#         customer_id STRING,
#         screening_id STRING,
#         ticket_quantity INT,
#         ticket_unit_price FLOAT
#         linked_anime_id STRING
#     )
#     USING ICEBERG
#     TBLPROPERTIES ('write.format.default'='orc')
# """)

# print(" Iceberg silver_ticket_bookings_clean table created in MinIO bucket 'warehouse/silver'")

# #silver_inventory_status_per_product_store     

# spark.sql("DROP TABLE IF EXISTS my_catalog.silver.silver_inventory_status_per_product_store   ")

# # Create Iceberg table with ORC format
# spark.sql("""
#     CREATE TABLE my_catalog.silver.silver_inventory_status_per_product_store      (
#         product_id STRING,
#         last_update_ts TIMESTAMP,
#         current_quantity INT
#     )
#     USING ICEBERG
#     TBLPROPERTIES ('write.format.default'='orc')
# """)

# print(" Iceberg silver_inventory_status_per_product_store  table created in MinIO bucket 'warehouse/silver'")

# #silver_concession_purchases_clean      

# spark.sql("DROP TABLE IF EXISTS my_catalog.silver.silver_concession_purchases_clean    ")

# # Create Iceberg table with ORC format
# spark.sql("""
#     CREATE TABLE my_catalog.silver.silver_concession_purchases_clean      (
#         purchase_id STRING,
#         purchase_ts TIMESTAMP,
#         screening_id STRING,
#         item_id STRING,
#         item_quantity INT,
#         item_unit_price FLOAT
#     )
#     USING ICEBERG
#     TBLPROPERTIES ('write.format.default'='orc')
# """)

# print(" Iceberg silver_concession_purchases_clean  table created in MinIO bucket 'warehouse/silver'")

# #silver_customer_master_data       

# spark.sql("DROP TABLE IF EXISTS my_catalog.silver.silver_customer_master_data     ")

# # Create Iceberg table with ORC format
# spark.sql("""
#     CREATE TABLE my_catalog.silver.silver_customer_master_data       (
#         customer_id STRING,
#         registration_ts TIMESTAMP,
#         name STRING,
#         email STRING,
#         item_unit_price FLOAT
#        
#     )
#     USING ICEBERG
#     TBLPROPERTIES ('write.format.default'='orc')
# """)

# print(" Iceberg silver_customer_master_data   table created in MinIO bucket 'warehouse/silver'")


# #silver_product_master_data        

# spark.sql("DROP TABLE IF EXISTS my_catalog.silver.silver_product_master_data      ")

# # Create Iceberg table with ORC format
# spark.sql("""
#     CREATE TABLE my_catalog.silver.silver_product_master_data        (
#         product_id STRING,
#         registration_ts TIMESTAMP,
#         product_name STRING,
#         category STRING,
#         supplier_id STRING,
#         current_unit_cost FLOAT,
#         last_cost_updated_ts TIMESTAMP
#          linked_anime_id STRING
          
#     )
#     USING ICEBERG
#     TBLPROPERTIES ('write.format.default'='orc')
# """)

# print(" Iceberg silver_product_master_data    table created in MinIO bucket 'warehouse/silver'")

# #silver_supplier_master_data         

# spark.sql("DROP TABLE IF EXISTS my_catalog.silver.silver_supplier_master_data       ")

# # Create Iceberg table with ORC format
# spark.sql("""
#     CREATE TABLE my_catalog.silver.silver_supplier_master_data         (
#         supplier_id STRING,
#         product_id STRING,
#         last_cost_update TIMESTAMP,
#         region STRING,
#         current_unit_cost FLOAT
#         linked_anime_id STRING
          
#     )
#     USING ICEBERG
#     TBLPROPERTIES ('write.format.default'='orc')
# """)

# print(" Iceberg silver_supplier_master_data     table created in MinIO bucket 'warehouse/silver'")
# 1. POS Transactions
spark.sql("DROP TABLE IF EXISTS my_catalog.silver.silver_pos_transactions_clean")
spark.sql("""
    CREATE TABLE my_catalog.silver.silver_pos_transactions_clean (
        transaction_id STRING,
        transaction_ts TIMESTAMP,
        customer_id STRING,
        product_id STRING,
        quantity_purchased INT,
        unit_price_at_sale FLOAT,
        linked_anime_id STRING
    )
    USING ICEBERG
    TBLPROPERTIES ('write.format.default'='orc')
""")

# 2. Ticket Bookings
spark.sql("DROP TABLE IF EXISTS my_catalog.silver.silver_ticket_bookings_clean")
spark.sql("""
    CREATE TABLE my_catalog.silver.silver_ticket_bookings_clean (
        booking_id STRING,
        booking_ts TIMESTAMP,
        customer_id STRING,
        screening_id STRING,
        ticket_quantity INT,
        ticket_unit_price FLOAT,
        linked_anime_id STRING
    )
    USING ICEBERG
    TBLPROPERTIES ('write.format.default'='orc')
""")

# 3. Product Master Data
spark.sql("DROP TABLE IF EXISTS my_catalog.silver.silver_product_master_data")
spark.sql("""
    CREATE TABLE my_catalog.silver.silver_product_master_data (
        product_id STRING,
        supplier_id STRING,
        current_unit_cost FLOAT,
        last_cost_update TIMESTAMP,
        linked_anime_id STRING
    )
    USING ICEBERG
    TBLPROPERTIES ('write.format.default'='orc')
""")

# 4. Customer Master Data
spark.sql("DROP TABLE IF EXISTS my_catalog.silver.silver_customer_master_data")
spark.sql("""
    CREATE TABLE my_catalog.silver.silver_customer_master_data (
        customer_id STRING,
        name STRING,
        email STRING,
        preferred_genre STRING,
        registration_ts TIMESTAMP
    )
    USING ICEBERG
    TBLPROPERTIES ('write.format.default'='orc')
""")

# 5. Inventory Movements
spark.sql("DROP TABLE IF EXISTS my_catalog.silver.silver_inventory_movements_clean")
spark.sql("""
    CREATE TABLE my_catalog.silver.silver_inventory_movements_clean (
        product_id STRING,
        linked_anime_id STRING,
        current_quantity INT,
        last_update_ts TIMESTAMP,
        last_event_type STRING,
        last_event_id STRING,
        supplier_id STRING
    )
    USING ICEBERG
    TBLPROPERTIES ('write.format.default'='orc')
""")

# 6. Anime Broadcast Schedule (SCD2)
spark.sql("DROP TABLE IF EXISTS my_catalog.silver.silver_anime_broadcast_schedule_scd2")
spark.sql("""
    CREATE TABLE my_catalog.silver.silver_anime_broadcast_schedule_scd2 (
        anime_id STRING,
        title STRING,
        genre STRING,
        studio STRING,
        broadcast_start_ts TIMESTAMP,
        broadcast_end_ts TIMESTAMP,
        season STRING,
        effective_start_date TIMESTAMP,
        effective_end_date TIMESTAMP,
        is_current_broadcast BOOLEAN
    )
    USING ICEBERG
    TBLPROPERTIES ('write.format.default'='orc')
""")
spark.stop()
