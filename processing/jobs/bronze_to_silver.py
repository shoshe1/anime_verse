import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as _sum, max as _max, row_number, lit, current_timestamp,
    lower, regexp_replace, trim, coalesce, split, isnan
)
from pyspark.sql.window import Window


def init_spark():
    """Initialize and return Spark session with Iceberg and MinIO config."""
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
    return spark

def check_not_empty(df, df_name):
    """Validate DataFrame contains data before processing.
    Returns False if empty to skip downstream processing."""
    count = df.count()
    if count == 0:
        print(f"⚠️  Skipping: {df_name} is empty.")
        return False
    print(f"Data quality check passed: {df_name} has {count} rows.")
    return True

def check_no_nulls(df, column_list, df_name):
    """Validate critical columns contain no null or NaN values.
    Essential for maintaining data integrity in silver layer."""
    for col_name in column_list:
        null_count = df.filter(col(col_name).isNull() | isnan(col(col_name))).count()
        if null_count > 0:
            print(f"⚠️  Data quality check failed: {df_name} column {col_name} has {null_count} null or NaN values.")
            return False
    print(f"Data quality check passed: {df_name} columns {column_list} have no nulls.")
    return True

def load_anime_lookup(spark):
    """Load bronze anime broadcast schedule and create normalized lookup. to enable matching between  screaning,products and anime titles.."""
    anime_df = spark.table("my_catalog.bronze.bronze_anime_broadcast_schedule").select("anime_id", "title")
    anime_lookup = anime_df.withColumn("normalized_title", regexp_replace(lower(col("title")), "[^a-z0-9]", ""))
    return anime_lookup

def process_pos_transactions(spark, anime_lookup):
    """Process bronze POS transactions to cleaned silver table with linked anime."""
    pos_df = spark.table("my_catalog.bronze.bronze_pos_transaction_events")
    if pos_df.limit(1).count() == 0:
        print("⚠️ Skipping: bronze_pos_transaction_events is empty. No silver data will be processed for POS.")
        return spark.createDataFrame([], schema=pos_df.schema)

    deduped_pos_df = pos_df.dropDuplicates(["transaction_id"])
    normalized_pos = deduped_pos_df.withColumn("normalized_product_id", regexp_replace(lower(col("product_id")), "[^a-z0-9]", ""))
    normalized_anime = anime_lookup.select("anime_id", "normalized_title")

    cross_joined = normalized_pos.select("product_id", "normalized_product_id").distinct().crossJoin(normalized_anime)
    matched = cross_joined.withColumn("match", col("normalized_product_id").contains(col("normalized_title"))).filter(col("match") == True)

    window_spec = Window.partitionBy("product_id").orderBy(col("normalized_title").desc())
    linked_product_map = matched.withColumn("rn", row_number().over(window_spec)).filter(col("rn") == 1).select("product_id", "anime_id").dropDuplicates(["product_id"])

    pos_clean = deduped_pos_df.join(linked_product_map, on="product_id", how="left") \
        .withColumn("total_price", col("unit_price_at_sale") * col("quantity_purchased")) \
        .select("transaction_id", col("event_ts").alias("transaction_ts"), "customer_id", "product_id", "quantity_purchased", "total_price", col("anime_id").alias("linked_anime_id")) \
        .dropna(subset=["transaction_id", "product_id"]) \
        .dropDuplicates(["transaction_id"])

    if pos_df.limit(1).count() == 0:
        print("⚠️ Skipping: bronze_pos_transaction_events is empty. No silver data will be processed for POS.")
        
        return spark.createDataFrame([], schema=pos_df.schema) 

    existing_silver_df = spark.table("my_catalog.silver.silver_pos_transactions_clean")
    new_rows_df = pos_clean.alias("new").join(existing_silver_df.alias("existing"), on="transaction_id", how="left_anti")
    if not check_not_empty(new_rows_df, "POS Transactions") or not check_no_nulls(new_rows_df, ["transaction_id", "product_id"], "Cleaned POS Transactions"):
        return spark.createDataFrame([], schema=pos_clean.schema)
    
    return new_rows_df

def process_ticket_bookings(spark, anime_lookup):
    """Process bronze ticket bookings to cleaned silver table with linked anime."""
    bookings_df = spark.table("my_catalog.bronze.bronze_ticket_booking_events")
    if bookings_df.limit(1).count() == 0:
        print("⚠️ Skipping: bronze_ticket_booking_events is empty. No silver data will be processed for bookings.")
        return spark.createDataFrame([], schema=bookings_df.schema)
    

    deduped_bookings_df = bookings_df.dropDuplicates(["booking_id"])
    normalized_bookings = deduped_bookings_df.withColumn("normalized_screening_id", regexp_replace(lower(col("screening_id")), "[^a-z0-9]", ""))
    screening_anime_map = normalized_bookings.select("screening_id", "normalized_screening_id").distinct().crossJoin(anime_lookup)

    screening_anime_map = screening_anime_map.withColumn("match", (col("normalized_screening_id").contains(col("normalized_title"))) & (col("normalized_title").rlike("^.{3,}$")))
    window_spec = Window.partitionBy("screening_id").orderBy(col("normalized_title").desc())
    linked_screening_map = screening_anime_map.filter("match = true").withColumn("rn", row_number().over(window_spec)).filter(col("rn") == 1).select("screening_id", "anime_id").dropDuplicates(["screening_id"])

    bookings_clean = deduped_bookings_df.join(linked_screening_map, on="screening_id", how="left") \
        .withColumn("total_price", col("unit_price_at_sale") * col("quantity_purchased")) \
        .select("booking_id", col("event_ts").alias("booking_ts"), "customer_id", "screening_id", "quantity_purchased", "total_price", col("anime_id").alias("linked_anime_id")) \
        .dropna(subset=["booking_id", "screening_id"]) \
        .dropDuplicates(["booking_id"])

    if bookings_clean.limit(1).count() == 0:
        print("⚠️ Skipping: bronze_ticket_booking_events is empty. No silver data will be processed for bookings.")
        return spark.createDataFrame([], schema=bookings_clean.schema)

    existing_silver_df = spark.table("my_catalog.silver.silver_ticket_bookings_clean")
    new_rows_df = bookings_clean.alias("new").join(existing_silver_df.alias("existing"), on="booking_id", how="left_anti")
    if not check_not_empty(new_rows_df, "Ticket Bookings") or not check_no_nulls(new_rows_df, ["booking_id", "screening_id"], "Cleaned Ticket Bookings"):
        return spark.createDataFrame([], schema=bookings_clean.schema) 
    return new_rows_df



def process_product_master_data(spark, linked_product_map, deduped_pos_df, deduped_delivery_df):
    """Combine latest cost, price, and linked anime for product master silver table."""

    # Get latest unit_cost per product
    cost_window = Window.partitionBy("product_id").orderBy(col("ingestion_ts").desc())
    latest_costs = deduped_delivery_df.withColumn("rn", row_number().over(cost_window)) \
        .filter(col("rn") == 1).drop("rn") \
        .withColumnRenamed("ingestion_ts", "cost_ingestion_ts")

    # Get latest unit_price per product
    price_window = Window.partitionBy("product_id").orderBy(col("ingestion_ts").desc())
    latest_prices = deduped_pos_df.withColumn("rn", row_number().over(price_window)) \
        .filter(col("rn") == 1).drop("rn") \
        .withColumnRenamed("ingestion_ts", "price_ingestion_ts")

    costs_with_anime = latest_costs.join(linked_product_map, on="product_id", how="left")

    product_master = costs_with_anime.join(latest_prices, on="product_id", how="outer")

    product_master = product_master.withColumn("category", split(col("product_id"), "_").getItem(1))

    product_master_cleaned = product_master.select(
        "product_id",
        "supplier_id",
        col("unit_cost").alias("current_unit_cost"),
        col("unit_price_at_sale").alias("current_unit_price"),
        col("cost_ingestion_ts").alias("last_cost_update"),
        col("price_ingestion_ts").alias("last_price_update"),
        col("anime_id").alias("linked_anime_id"),
        "category"
    ).dropDuplicates(["product_id"])


    # Load existing Silver table
    existing_silver_df = spark.table("my_catalog.silver.silver_product_master_data")

    # Get only new rows (anti-join)
    new_rows_df = product_master_cleaned.alias("new").join(
        existing_silver_df.alias("existing"),
        on="product_id",
        how="left_anti"
    )

    # Check for nulls in critical fields
    if not check_not_empty(new_rows_df, "Product Master Data") or not check_no_nulls(new_rows_df, ["product_id", "current_unit_cost", "current_unit_price", "linked_anime_id"], "Cleaned Product Master Data"):
        return spark.createDataFrame([], schema=product_master_cleaned.schema)

    return new_rows_df


def process_customer_master_data(spark):
    """Clean and deduplicate customer registration events for silver table."""
    customer_df = spark.table("my_catalog.bronze.bronze_customer_registration_events")
    if customer_df.limit(1).count() == 0:
        print("⚠️ Skipping: bronze_customer_registration_events is empty. No silver data will be processed for customers.")
        return spark.createDataFrame([], schema=customer_df.schema)

    customer_clean = customer_df.withColumn("customer_id", lower(trim(col("customer_id")))).dropna(subset=["customer_id"]).dropDuplicates(["customer_id"])
    if customer_clean.limit(1).count() == 0:
        print("⚠️ Skipping: bronze_customer_registration_events is empty. No silver data will be processed for customers.")
        return spark.createDataFrame([], schema=customer_clean.schema)

    existing_silver_df = spark.table("my_catalog.silver.silver_customer_master_data")
    new_rows_df = customer_clean.alias("new").join(existing_silver_df.alias("existing"), on="customer_id", how="left_anti")
    check_no_nulls(new_rows_df, ["customer_id"], "Customer Master Cleaned")
    return new_rows_df

def process_inventory_movements(spark, deduped_pos_df, linked_product_map, deduped_delivery_df):
    """Aggregate inventory movements from sales and deliveries."""
    sales_df = deduped_pos_df.join(linked_product_map, on="product_id", how="left")
    sales_movements = sales_df.select("product_id", col("event_ts").alias("movement_ts"), (-col("quantity_purchased")).alias("quantity_change"), "anime_id", lit("SALE").alias("movement_type"))

    delivery_movements = deduped_delivery_df.join(linked_product_map, on="product_id", how="left") \
        .select("product_id", col("event_ts").alias("movement_ts"), col("quantity_delivered").alias("quantity_change"), "anime_id", lit("DELIVERY").alias("movement_type"), "supplier_id")

    all_movements = delivery_movements.select("product_id", "movement_ts", "quantity_change", "anime_id", "movement_type") \
        .unionByName(sales_movements.select("product_id", "movement_ts", "quantity_change", "anime_id", "movement_type"))

    if all_movements.limit(1).count() == 0:
        print("⚠️ Skipping: No inventory movements found. No silver data will be processed for inventory.")
        return spark.createDataFrame([], schema=all_movements.schema)

    inventory_status = all_movements.groupBy("product_id", "anime_id").agg(_sum("quantity_change").alias("current_quantity"), _max("movement_ts").alias("last_update_ts"))
    supplier_map = deduped_delivery_df.select("product_id", "supplier_id").distinct()

    inventory_status = inventory_status.join(supplier_map, on="product_id", how="left") \
        .withColumnRenamed("anime_id", "linked_anime_id") \
        .select("product_id", "linked_anime_id", "current_quantity", "last_update_ts", "supplier_id") \
        .dropDuplicates(["product_id"])

    existing_silver_df = spark.table("my_catalog.silver.silver_inventory_movements_clean")
    new_rows_df = inventory_status.alias("new").join(existing_silver_df.alias("existing"), on="product_id", how="left_anti")
    if not check_not_empty(new_rows_df, "Inventory Movements") or not check_no_nulls(new_rows_df, ["product_id", "current_quantity", "linked_anime_id"], "Cleaned Inventory Movements"):        
        return spark.createDataFrame([], schema=inventory_status.schema)
    return new_rows_df


def process_anime_broadcast_schedule_scd2(spark):
    """Perform SCD2 update for anime broadcast schedule."""
    bronze_anime = spark.table("my_catalog.bronze.bronze_anime_broadcast_schedule")
    if bronze_anime.limit(1).count() == 0:
        print("⚠️ Skipping: bronze_anime_broadcast_schedule is empty. No silver data will be processed for anime broadcast schedule.")
        return spark.createDataFrame([], schema=bronze_anime.schema)

    bronze_anime_deduped = bronze_anime.dropDuplicates(["anime_id"])

    try:
        existing_silver = spark.table("my_catalog.silver.silver_anime_broadcast_schedule_scd2")
    except Exception:
        existing_silver = spark.createDataFrame([], schema=bronze_anime_deduped.schema) \
            .withColumn("effective_start_date", lit(None).cast("timestamp")) \
            .withColumn("effective_end_date", lit(None).cast("timestamp")) \
            .withColumn("is_current_broadcast", lit(None).cast("boolean"))

    new_data = bronze_anime_deduped.select(
        col("anime_id").alias("new_anime_id"),
        col("title").alias("new_title"),
        col("genre").alias("new_genre"),
        col("studio").alias("new_studio"),
        col("broadcast_start_ts").alias("new_broadcast_start_ts"),
        col("broadcast_end_ts").alias("new_broadcast_end_ts"),
        col("season").alias("new_season"),
        col("ingestion_ts").alias("new_effective_start_date"),
        lit("9999-12-31 23:59:59").cast("timestamp").alias("new_effective_end_date"),
        lit(True).alias("new_is_current_broadcast")
    )

    old_df = existing_silver.filter("is_current_broadcast = true").select(
        col("anime_id").alias("old_anime_id"),
        col("title").alias("old_title"),
        col("genre").alias("old_genre"),
        col("studio").alias("old_studio"),
        col("broadcast_start_ts").alias("old_broadcast_start_ts"),
        col("broadcast_end_ts").alias("old_broadcast_end_ts"),
        col("season").alias("old_season"),
        col("effective_start_date").alias("old_effective_start_date"),
        col("effective_end_date").alias("old_effective_end_date"),
        col("is_current_broadcast").alias("old_is_current_broadcast")
    )

    joined = new_data.join(old_df, new_data.new_anime_id == old_df.old_anime_id, how="left")

    changed = joined.filter(
        (col("old_anime_id").isNull()) | (
            (coalesce(col("new_title"), lit("")) != coalesce(col("old_title"), lit(""))) |
            (coalesce(col("new_genre"), lit("")) != coalesce(col("old_genre"), lit(""))) |
            (coalesce(col("new_studio"), lit("")) != coalesce(col("old_studio"), lit(""))) |
            (coalesce(col("new_broadcast_start_ts"), lit("1900-01-01")) != coalesce(col("old_broadcast_start_ts"), lit("1900-01-01"))) |
            (coalesce(col("new_broadcast_end_ts"), lit("1900-01-01")) != coalesce(col("old_broadcast_end_ts"), lit("1900-01-01"))) |
            (coalesce(col("new_season"), lit("")) != coalesce(col("old_season"), lit("")))
        )
    ).select(
        col("new_anime_id").alias("anime_id"),
        col("new_title").alias("title"),
        col("new_genre").alias("genre"),
        col("new_studio").alias("studio"),
        col("new_broadcast_start_ts").alias("broadcast_start_ts"),
        col("new_broadcast_end_ts").alias("broadcast_end_ts"),
        col("new_season").alias("season"),
        col("new_effective_start_date").alias("effective_start_date"),
        col("new_effective_end_date").alias("effective_end_date"),
        col("new_is_current_broadcast").alias("is_current_broadcast")
    )

    expired = joined.filter(col("old_anime_id").isNotNull()).select(
        col("old_anime_id").alias("anime_id"),
        col("old_title").alias("title"),
        col("old_genre").alias("genre"),
        col("old_studio").alias("studio"),
        col("old_broadcast_start_ts").alias("broadcast_start_ts"),
        col("old_broadcast_end_ts").alias("broadcast_end_ts"),
        col("old_season").alias("season"),
        col("old_effective_start_date").alias("effective_start_date"),
        current_timestamp().alias("effective_end_date"),
        lit(False).alias("is_current_broadcast")
    )

    final_scd2 = changed.unionByName(expired)

    if not final_scd2.limit(1).count() == 0:
        final_scd2.writeTo("my_catalog.silver.silver_anime_broadcast_schedule_scd2").append()

def process_concession_sales_summary(spark):
    """Aggregate concession purchase events for silver summary."""
    concession_df = spark.table("my_catalog.bronze.bronze_concession_purchase_events") \
        .withColumn("item_quantity", col("item_quantity").cast("int")) \
        .withColumn("item_unit_price", col("item_unit_price").cast("double"))

    summary_df = concession_df.withColumn("item_total", col("item_quantity") * col("item_unit_price")) \
        .groupBy("screening_id") \
        .agg(_sum("item_quantity").alias("total_quantity_purchased"), _sum("item_total").alias("total_revenue"))
    if not summary_df.limit(1).count() == 0:
        summary_df.writeTo("my_catalog.silver.silver_concession_sales_summary").overwritePartitions()
    else:
        print("No data to write — skipping writeTo()")


def main():
    spark = init_spark()
    anime_lookup = load_anime_lookup(spark)

    new_pos_rows = process_pos_transactions(spark, anime_lookup)
    new_pos_rows.writeTo("my_catalog.silver.silver_pos_transactions_clean").overwritePartitions()

    new_booking_rows = process_ticket_bookings(spark, anime_lookup)
    new_booking_rows.writeTo("my_catalog.silver.silver_ticket_bookings_clean").overwritePartitions()

    deduped_pos_df = spark.table("my_catalog.bronze.bronze_pos_transaction_events").dropDuplicates(["transaction_id"])
    deduped_delivery_df = spark.table("my_catalog.bronze.bronze_supplier_delivery_events").dropDuplicates(["delivery_id"])

    normalized_pos = deduped_pos_df.withColumn("normalized_product_id", regexp_replace(lower(col("product_id")), "[^a-z0-9]", ""))
    normalized_anime = anime_lookup.select("anime_id", "normalized_title")
    cross_joined = normalized_pos.select("product_id", "normalized_product_id").distinct().crossJoin(normalized_anime)
    matched = cross_joined.withColumn("match", col("normalized_product_id").contains(col("normalized_title"))).filter(col("match") == True)
    window_spec = Window.partitionBy("product_id").orderBy(col("normalized_title").desc())
    linked_product_map = matched.withColumn("rn", row_number().over(window_spec)).filter(col("rn") == 1).select("product_id", "anime_id").dropDuplicates(["product_id"])

    new_product_master_rows = process_product_master_data(spark, linked_product_map, deduped_pos_df, deduped_delivery_df)
    new_product_master_rows.writeTo("my_catalog.silver.silver_product_master_data").overwritePartitions()

    new_customer_rows = process_customer_master_data(spark)
    new_customer_rows.writeTo("my_catalog.silver.silver_customer_master_data").overwritePartitions()

    new_inventory_rows = process_inventory_movements(spark, deduped_pos_df, linked_product_map, deduped_delivery_df)
    new_inventory_rows.writeTo("my_catalog.silver.silver_inventory_movements_clean").overwritePartitions()

    process_anime_broadcast_schedule_scd2(spark)

    process_concession_sales_summary(spark)

    print("✅ ETL process completed successfully!")

if __name__ == "__main__":
    main()
