from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, max as _max, row_number, lit, current_timestamp, lower
from pyspark.sql.window import Window

# Initialize Spark session with Iceberg support
spark = SparkSession.builder \
    .appName("BronzeToSilverETL") \
    .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.my_catalog.type", "hadoop") \
       .config("spark.sql.catalog.my_catalog.warehouse", "s3a://warehouse/") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# Load anime titles for product-anime and screening-anime linking
anime_df = spark.table("my_catalog.bronze.bronze_anime_broadcast_schedule").select("anime_id", "title")
anime_lookup = anime_df.withColumn("title", lower(col("title")))

# ------------------ 1. POS Transactions ------------------
pos_df = spark.table("my_catalog.bronze.bronze_pos_transaction_events")
product_anime_map = pos_df.select("product_id").distinct().crossJoin(anime_lookup)
product_anime_map = product_anime_map.withColumn("match", lower(col("product_id")).contains(col("title")))
linked_product_map = product_anime_map.filter("match = true").select("product_id", "anime_id")

pos_clean = pos_df.join(linked_product_map, on="product_id", how="left") \
    .select(
        col("transaction_id"),
        col("event_ts").alias("transaction_ts"),
        col("customer_id"),
        col("product_id"),
        col("quantity_purchased"),
        col("unit_price_at_sale"),
        col("anime_id").alias("linked_anime_id")
    ).dropna(subset=["transaction_id", "product_id"])

pos_clean.writeTo("my_catalog.silver.silver_pos_transactions_clean").overwritePartitions()

# ------------------ 2. Ticket Bookings ------------------
bookings_df = spark.table("my_catalog.bronze.bronze_ticket_booking_events")
screening_anime_map = bookings_df.select("screening_id").distinct().crossJoin(anime_lookup)
screening_anime_map = screening_anime_map.withColumn("match", lower(col("screening_id")).contains(col("title")))
linked_screening_map = screening_anime_map.filter("match = true").select("screening_id", "anime_id")

bookings_clean = bookings_df.join(linked_screening_map, on="screening_id", how="left") \
    .select(
        col("booking_id"),
        col("event_ts").alias("booking_ts"),
        col("customer_id"),
        col("screening_id"),
        col("ticket_quantity"),
        col("unit_price_at_sale").alias("ticket_unit_price"),
        col("anime_id").alias("linked_anime_id")
    ).dropna(subset=["booking_id", "screening_id"])

bookings_clean.writeTo("my_catalog.silver.silver_ticket_bookings_clean").overwritePartitions()

# ------------------ 3. Product Master Data ------------------
delivery_df = spark.table("my_catalog.bronze.bronze_supplier_delivery_events")
window_spec = Window.partitionBy("product_id").orderBy(col("ingestion_ts").desc())
latest_costs = delivery_df.withColumn("rn", row_number().over(window_spec)) \
    .filter(col("rn") == 1) \
    .drop("rn")

latest_costs = latest_costs.join(linked_product_map, on="product_id", how="left")
latest_costs = latest_costs.select(
    col("product_id"),
    col("supplier_id"),
    col("unit_cost").alias("current_unit_cost"),
    col("ingestion_ts").alias("last_cost_update"),
    col("anime_id").alias("linked_anime_id")
)

latest_costs.writeTo("my_catalog.silver.silver_product_master_data").overwritePartitions()

# ------------------ 4. Customer Master Data ------------------
customer_df = spark.table("my_catalog.bronze.bronze_customer_registration_events")
customer_clean = customer_df.select(
    col("customer_id"),
    col("name"),
    col("email"),
    col("event_ts").alias("registration_ts")
).dropna(subset=["customer_id"])

customer_clean.writeTo("my_catalog.silver.silver_customer_master_data").overwritePartitions()

# ------------------ 5. Inventory Movements ------------------
sales_df = pos_clean.select(
    col("product_id"),
    col("transaction_ts").alias("movement_ts"),
    (-col("quantity_purchased")).alias("quantity_change")
)

concession_df = spark.table("my_catalog.bronze.bronze_concession_purchase_events")
concession_movements = concession_df.select(
    col("item_id").alias("product_id"),
    col("event_ts").alias("movement_ts"),
    (-col("item_quantity")).alias("quantity_change")
)

concession_movements = concession_movements.join(linked_product_map, on="product_id", how="left")
sales_df = sales_df.join(linked_product_map, on="product_id", how="left")
delivery_movements = delivery_df.select(
    col("product_id"),
    col("event_ts").alias("movement_ts"),
    col("quantity_delivered").alias("quantity_change")
).join(linked_product_map, on="product_id", how="left")

all_movements = delivery_movements.unionByName(sales_df).unionByName(concession_movements)

inventory_status = all_movements.groupBy("product_id", "anime_id").agg(
    _sum("quantity_change").alias("current_quantity"),
    _max("movement_ts").alias("last_update_ts")
)

inventory_status = inventory_status.withColumn("last_event_type", col("last_update_ts").cast("string")) \
    .withColumn("last_event_id", col("last_update_ts").cast("string")) \
    .withColumn("supplier_id", col("product_id").cast("string")) \
    .withColumnRenamed("anime_id", "linked_anime_id")

inventory_status.writeTo("my_catalog.silver.silver_inventory_movements_clean").overwritePartitions()

# ------------------ 6. Anime Broadcast Schedule SCD2 ------------------
from pyspark.sql.functions import expr

bronze_anime = spark.table("my_catalog.bronze.bronze_anime_broadcast_schedule")
existing_silver = spark.table("my_catalog.silver.silver_anime_broadcast_schedule_scd2")

new_data = bronze_anime.select(
    col("anime_id"),
    col("title"),
    col("genre"),
    col("studio"),
    col("broadcast_start_ts"),
    col("broadcast_end_ts"),
    col("season"),
    col("ingestion_ts").alias("effective_start_date"),
    lit("9999-12-31 23:59:59").cast("timestamp").alias("effective_end_date"),
    lit(True).alias("is_current_broadcast")
)

joined = new_data.alias("new").join(
    existing_silver.filter("is_current_broadcast = true").alias("old"),
    on="anime_id",
    how="left"
)

changed = joined.filter(
    "old.anime_id IS NULL OR "
    "(new.title <> old.title OR new.genre <> old.genre OR new.studio <> old.studio OR "
    "new.broadcast_start_ts <> old.broadcast_start_ts OR new.broadcast_end_ts <> old.broadcast_end_ts OR "
    "new.season <> old.season)"
).select("new.*")

expired = joined.filter("old.anime_id IS NOT NULL").select(
    "old.anime_id", "old.title", "old.genre", "old.studio", "old.broadcast_start_ts", "old.broadcast_end_ts",
    "old.season", "old.effective_start_date",
    current_timestamp().alias("effective_end_date"),
    lit(False).alias("is_current_broadcast")
)

final_scd2 = changed.unionByName(expired)
final_scd2.writeTo("my_catalog.silver.silver_anime_broadcast_schedule_scd2").append()

print("✅ Bronze to Silver ETL completed.")
