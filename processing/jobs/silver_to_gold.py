from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, to_date, monotonically_increasing_id, current_timestamp,
    sha2, concat_ws, coalesce, lit, when
)
from pyspark.sql import functions as F

from functools import reduce
import sys
import logging

def create_spark_session():
    spark = SparkSession.builder \
        .appName("silver to gold ETL") \
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

def load_table(spark, table_name):
    try:
        df = spark.table(table_name)
        return df
    except Exception as e:
        print(f"Error loading table {table_name}: {e}")
        return None

def write_table(df: DataFrame, table_name: str, mode: str = "append", format: str = "iceberg", overwrite_partitions=False):
    if overwrite_partitions:
        df.write.format(format).mode("overwrite").saveAsTable(table_name)
    else:
        df.writeTo(table_name).append()
    print(f"Data written to {table_name} with mode={mode}")

def data_quality_checks(df: DataFrame, table_name: str, key_cols=None, non_null_cols=None, positive_cols=None) -> bool:
    errors = []

    count = df.count()
    print(f"[{table_name}] Row count: {count}")

    if non_null_cols:
        for col_name in non_null_cols:
            null_count = df.filter(col(col_name).isNull()).count()
            if null_count > 0:
                errors.append(f"Column {col_name} has {null_count} null values")
    # Check for duplicate records based on key columns
    if key_cols:
        dup_count = df.groupBy(key_cols).count().filter("count > 1").count()
        if dup_count > 0:
            errors.append(f"Found {dup_count} duplicate rows based on keys {key_cols}")
    # Check for non-positive values in numeric columns
    if positive_cols:
        for col_name in positive_cols:
            neg_count = df.filter(col(col_name) <= 0).count()
            if neg_count > 0:
                errors.append(f"Column {col_name} has {neg_count} non-positive values")
    
    if errors:
        print(f"[{table_name}] Data quality check FAILED:")
        for e in errors:
            print(f" - {e}")
        return False
    else:
        print(f"[{table_name}] Data quality check PASSED")
        return True

def etl_fact_sales(spark):
    """Create sales fact table by joining POS transactions with inventory data"""
    pos_df = load_table(spark, "my_catalog.silver.silver_pos_transactions_clean")
    inventory_df = load_table(spark, "my_catalog.silver.silver_inventory_movements_clean")
    if pos_df is None or inventory_df is None:
        logging.warning("Missing input tables for fact_sales ETL")
    else:
        # Join POS transactions with inventory and create fact table
        fact_sales_df = (
            pos_df
            .join(inventory_df, on="product_id", how="left")
            .withColumn("calendar_date", to_date("transaction_ts"))
            .withColumnRenamed("quantity_purchased", "quantity")
            .withColumnRenamed("total_price", "revenue")
            .withColumn("sale_id", monotonically_increasing_id().cast("string"))
            .select(
                "sale_id", "customer_id", "product_id", "calendar_date", "quantity", "revenue", "current_quantity"
            )
            .withColumnRenamed("current_quantity", "inventory_level_at_sale")
        )

        if not data_quality_checks(fact_sales_df, "fact_sales", key_cols=["sale_id"], non_null_cols=["sale_id", "product_id", "customer_id"]):
            logging.warning("Data quality failed for fact_sales. Aborting.")
        else:

            existing_gold_df = load_table(spark, "my_catalog.gold.fact_sales")
            new_rows_df = fact_sales_df.alias("new").join(
                existing_gold_df.alias("existing"),
                on="sale_id",
                how="left_anti"
            )
        
            # Data validation warnings 
            invalid_inventory_data = new_rows_df.filter(F.col("inventory_level_at_sale") <= 0)
            if invalid_inventory_data.count() > 0:
                logging.warning("Found inventory_level_at_sale with zero or negative prices")
            invalid_revenue_data = new_rows_df.filter(F.col("revenue") <= 0)
            if invalid_revenue_data.count() > 0:
                logging.warning("Found revenue with zero or negative prices")
            invalid_quantity_data = new_rows_df.filter(F.col("quantity") <= 0)
            if invalid_quantity_data.count() > 0:
                logging.warning("Found quantity with zero or negative values")
            write_table(new_rows_df, "my_catalog.gold.fact_sales")

def etl_fact_screenings(spark):
    """Create screenings fact table by combining ticket bookings with concession sales"""
    tickets_df = load_table(spark, "my_catalog.silver.silver_ticket_bookings_clean")
    concession_df = load_table(spark, "my_catalog.silver.silver_concession_sales_summary")
    if tickets_df is None or concession_df is None:
        logging.warning("Missing input tables for fact_screenings ETL. Exiting.")
    else:
         # Join ticket bookings with concession sales by screening
        gold_df = tickets_df.alias("t") \
            .join(concession_df.alias("c"), on="screening_id", how="left") \
            .select(
                col("t.screening_id"),
                col("t.linked_anime_id").alias("anime_id"),
                to_date("t.booking_ts").alias("calendar_date"),
                col("t.total_price").cast("double").alias("ticket_revenue"),
                col("c.total_revenue").cast("double").alias("concession_revenue")
            )

        existing_gold_df = load_table(spark, "my_catalog.gold.fact_screenings")
        new_rows_df = gold_df.alias("new").join(
            existing_gold_df.alias("existing"),
            on="screening_id",
            how="left_anti"
        )
        # Data validation warnings
        invalid_ticket_revenue_data = new_rows_df.filter(F.col("ticket_revenue") <= 0)
        if invalid_ticket_revenue_data.count() > 0:
            logging.warning("Found ticket_revenue with zero or negative prices")
        invalid_concession_revenue_data = new_rows_df.filter(F.col("concession_revenue") <= 0)
        if invalid_concession_revenue_data.count() > 0:
            logging.warning("Found concession_revenue with zero or negative prices")
        
        write_table(new_rows_df, "my_catalog.gold.fact_screenings")

def etl_dim_customer(spark):
    """Create customer dimension with segmentation based on purchase behavior"""
    tickets = load_table(spark, "my_catalog.silver.silver_ticket_bookings_clean")
    products = load_table(spark, "my_catalog.silver.silver_pos_transactions_clean")
    customers = load_table(spark, "my_catalog.silver.silver_customer_master_data")
    if tickets is None or products is None or customers is None:
        logging.warning("Missing input tables for dim_customer ETL. Exiting.")
    else:

        ticket_counts = tickets.groupBy("customer_id").agg(F.count("*").alias("ticket_count"))
        product_counts = products.groupBy("customer_id").agg(F.count("*").alias("product_count"))

        combined_counts = ticket_counts.join(product_counts, on="customer_id", how="outer").fillna(0)

        combined = customers.join(combined_counts, on="customer_id", how="left").fillna(0)
        # Apply customer segmentation logic based on purchase behavior
        segmented_df = combined.withColumn(
            "segment",
            when((col("product_count") < 2) & (col("ticket_count") < 2), "Low Spender")
            .when((col("product_count").between(2, 3)) & (col("ticket_count") < 2), "Moderate - Products")
            .when((col("ticket_count").between(2, 3)) & (col("product_count") < 2), "Moderate - Tickets")
            .when((col("product_count").between(2, 3)) & (col("ticket_count").between(2, 3)), "Moderate - Both")
            .when((col("product_count") > 3) & (col("ticket_count") < 2), "Heavy - Products")
            .when((col("ticket_count") > 3) & (col("product_count") < 2), "Heavy - Tickets")
            .when((col("product_count") > 3) & (col("ticket_count") > 3), "Top Customer")
            .otherwise("Low Spender")
        )

        dim_customer_df = segmented_df.select("customer_id", "name", "segment", "email")

        gold_existing = load_table(spark, "my_catalog.gold.dim_customer")
        filtered_existing = gold_existing.join(dim_customer_df.select("customer_id"), on="customer_id", how="left_anti")
        updated_full = filtered_existing.unionByName(dim_customer_df)

        write_table(updated_full, "my_catalog.gold.dim_customer", mode="overwrite", overwrite_partitions=True)

def etl_customer_segments(spark):
    """Create customer segments summary table with descriptions"""
    dim_customer_df = load_table(spark, "my_catalog.gold.dim_customer")
    if dim_customer_df is None:
        logging.warning("dim_customer table missing for customer_segments ETL.")
    else:
        # Aggregate customer counts by segment and add business descriptions
        segments_summary_df = dim_customer_df.groupBy("segment").agg(
            F.count("*").alias("customer_count")
        ).withColumn(
            "description",
            when(col("segment") == "Low Spender", "Buys few or no products or tickets")
            .when(col("segment") == "Moderate - Products", "Buys 2–3 products, few tickets")
            .when(col("segment") == "Moderate - Tickets", "Buys 2–3 tickets, few products")
            .when(col("segment") == "Moderate - Both", "Buys 2–3 of both tickets and products")
            .when(col("segment") == "Heavy - Products", "Buys >3 products, few tickets")
            .when(col("segment") == "Heavy - Tickets", "Buys >3 tickets, few products")
            .when(col("segment") == "Top Customer", "Buys more than 3 in both products and tickets")
            .otherwise("Uncategorized")
        )

        gold_segments = load_table(spark, "my_catalog.gold.customer_segments")
        filtered_existing_segments = gold_segments.join(segments_summary_df.select("segment"), on="segment", how="left_anti")

        updated_segments_full = filtered_existing_segments.unionByName(segments_summary_df)

        write_table(updated_segments_full, "my_catalog.gold.customer_segments", mode="overwrite", overwrite_partitions=True)




def etl_gold_top_anime_performance_tiles(spark):
    """Create top 5 anime performance dashboard tiles based on total sales"""
    pos_df = spark.table("my_catalog.silver.silver_pos_transactions_clean")
    tickets_df = spark.table("my_catalog.silver.silver_ticket_bookings_clean")
    anime_df = spark.table("my_catalog.silver.silver_anime_broadcast_schedule_scd2").filter("is_current_broadcast = true")

    pos_sales = pos_df.groupBy("linked_anime_id").agg(
        F.sum("total_price").alias("sales_from_pos")
    )

    ticket_sales = tickets_df.groupBy("linked_anime_id").agg(
        F.sum("total_price").alias("sales_from_tickets"),
        F.sum("quantity_purchased").alias("total_attendance")
    )
    # Combine all sales sources and calculate total performance
    performance_df = anime_df.join(
        pos_sales, anime_df.anime_id == pos_sales.linked_anime_id, "left"
    ).join(
        ticket_sales, anime_df.anime_id == ticket_sales.linked_anime_id, "left"
    ).select(
        anime_df.anime_id,
        anime_df.title,
        F.coalesce(F.col("sales_from_pos"), F.lit(0.0)).alias("total_sales"),
        F.coalesce(F.col("total_attendance"), F.lit(0)).alias("total_attendance")
    ).orderBy(F.col("total_sales").desc())

    top_5_animes = performance_df.limit(5)

    top_5_animes.writeTo("my_catalog.gold.gold_top_anime_performance_tiles") \
        .using("iceberg") \
        .tableProperty("write.format.default", "orc") \
        .createOrReplace()

    print("gold_top_anime_performance_tiles updated")

def etl_gold_inventory_summary(spark):
    """Create inventory summary with stock status classification"""
    inventory_df = spark.table("my_catalog.silver.silver_inventory_movements_clean")
    # Transform inventory data with stock level status
    inventory_summary_df = inventory_df.select(
        "product_id",
        F.col("current_quantity").alias("stock_level"),
        F.to_date("last_update_ts").alias("last_update")
    ).withColumn(
        "status",
        F.when(F.col("stock_level") < 30, "Low Stock – Reorder Needed")
         .when(F.col("stock_level") < 51, "Low Stock – Monitor")
         .otherwise("Stock OK")
    )
    if data_quality_checks(inventory_summary_df, "gold_inventory_summary", key_cols=["product_id"], non_null_cols=["product_id", "stock_level", "last_update"]):
        inventory_summary_df = inventory_summary_df.withColumn("ingestion_ts", current_timestamp())
    # Business validation
    invalid_stock_data = inventory_summary_df.filter(F.col("stock_level") < 0)
    if invalid_stock_data.count() > 0:
        logging.warning("Found stock_level with negative values")

    inventory_summary_df.writeTo("my_catalog.gold.gold_inventory_summary") \
        .using("iceberg") \
        .tableProperty("write.format.default", "orc") \
        .createOrReplace()

    print("gold_inventory_summary updated")

def etl_dim_product(spark):
    """Create product dimension with SCD2 implementation for price/cost changes"""
    product_df = spark.table("my_catalog.silver.silver_product_master_data")
    anime_df = spark.table("my_catalog.silver.silver_anime_broadcast_schedule_scd2") \
        .filter(F.col("is_current_broadcast") == True) \
        .select("anime_id", "title")
    # Join products with anime titles for enrichment
    joined_df = product_df.join(
        anime_df,
        product_df.linked_anime_id == anime_df.anime_id,
        "left"
    ).select(
        product_df.product_id,
        anime_df.title.alias("name"),
        product_df.category,
        product_df.current_unit_price.alias("price"),
        product_df.current_unit_cost.alias("cost"),
        product_df.supplier_id,
        product_df.last_cost_update,
        product_df.last_price_update
    )

    final_df = joined_df.withColumn(
        "effective_start_date", 
        F.coalesce(F.col("last_price_update"), F.col("last_cost_update"))
    ).withColumn(
        "effective_end_date", F.lit(None).cast("timestamp")
    ).withColumn(
        "is_current", F.lit(True)
    ).drop("last_price_update").drop("last_cost_update")

    dim_product = spark.table("my_catalog.gold.dim_product")
    current_records = dim_product.filter(F.col("is_current") == True)
    # Full outer join to detect changes and new records
    join_cond = ["product_id"]
    join_df = current_records.alias("old").join(
        final_df.alias("new"),
        on=join_cond,
        how="fullouter"
    )

    def rows_differ():
        """Check if any tracked attributes have changed"""
        conditions = [
            (~F.coalesce(F.col("old.price") == F.col("new.price"), F.lit(False))),
            (~F.coalesce(F.col("old.cost") == F.col("new.cost"), F.lit(False))),
            (~F.coalesce(F.col("old.name") == F.col("new.name"), F.lit(False))),
            (~F.coalesce(F.col("old.category") == F.col("new.category"), F.lit(False))),
            (~F.coalesce(F.col("old.supplier_id") == F.col("new.supplier_id"), F.lit(False)))
        ]
        return reduce(lambda a, b: a | b, conditions)

    diff_df = join_df.withColumn("is_different", rows_differ())
    # Expire old records that have changes
    to_expire = diff_df.filter(
        (F.col("old.is_current") == True) & (F.col("is_different") == True)
    ).select(
        "old.product_id", "old.name", "old.category", "old.price", "old.cost",
        "old.supplier_id", "old.effective_start_date"
    ).withColumn(
        "effective_end_date", F.current_timestamp()
    ).withColumn(
        "is_current", F.lit(False)
    )
    # Create new versions for changed or new records
    new_versions = diff_df.filter(
        (F.col("is_different") == True) | (F.col("old.product_id").isNull())
    ).select(
        "new.product_id", "new.name", "new.category", "new.price", "new.cost",
        "new.supplier_id", "new.effective_start_date"
    ).withColumn(
        "effective_end_date", F.lit(None).cast("timestamp")
    ).withColumn(
        "is_current", F.lit(True)
    )
    # Keep unchanged current records as-is
    unchanged_old = diff_df.filter(
        (F.col("is_different") == False) & (F.col("old.is_current") == True)
    ).select("old.*")

    scd2_snapshot = unchanged_old.union(to_expire).union(new_versions)
    if data_quality_checks(scd2_snapshot, "dim_product", key_cols=["product_id"], non_null_cols=["product_id", "name", "category", "price", "cost", "supplier_id"]):
        scd2_snapshot = scd2_snapshot.withColumn("ingestion_ts", current_timestamp())
    # Data validation
    invalid_price_data = scd2_snapshot.filter(F.col("price") <= 0)
    if invalid_price_data.count() > 0:
        logging.warning("Found price or cost with zero or negative values")
    invalid_cost_data = scd2_snapshot.filter(F.col("cost") <= 0)
    if invalid_cost_data.count() > 0:  
        logging.warning("Found price or cost with zero or negative values")

    scd2_snapshot = scd2_snapshot.drop("ingestion_ts")
    scd2_snapshot.writeTo("my_catalog.gold.dim_product").overwritePartitions()

    print("dim_product updated")

def etl_dim_anime_title_scd2(spark):
    """Maintain anime title dimension with SCD2 for tracking title/metadata changes"""
    from pyspark.sql.functions import sha2, concat_ws, coalesce, lit, current_timestamp

    silver_anime = spark.table("my_catalog.silver.silver_anime_broadcast_schedule_scd2")

    try:
        existing_gold = spark.table("my_catalog.gold.dim_anime_title_scd2")
    except:
        # Create empty schema if table missing
        from pyspark.sql.types import StructType, StructField, StringType, BooleanType, TimestampType
        
        gold_schema = StructType([
            StructField("anime_id", StringType(), True),
            StructField("genre", StringType(), True),
            StructField("title", StringType(), True),
            StructField("studio", StringType(), True),
            StructField("release_date", TimestampType(), True),
            StructField("season", StringType(), True),
            StructField("effective_start_date", TimestampType(), True),
            StructField("effective_end_date", TimestampType(), True),
            StructField("is_current", BooleanType(), True),
            StructField("record_hash", StringType(), True)
        ])
        
        existing_gold = spark.createDataFrame([], schema=gold_schema)
    # Transform silver data with hash for change detection
    silver_transformed = silver_anime.select(
        "anime_id",
        "genre",
        "title",
        "studio",
        F.col("broadcast_start_ts").alias("release_date"),
        "season",
        "effective_start_date",
        "effective_end_date",
        F.col("is_current_broadcast").alias("is_current"),
        sha2(concat_ws("|", 
            coalesce(F.col("anime_id"), lit("")),
            coalesce(F.col("genre"), lit("")),
            coalesce(F.col("title"), lit("")),
            coalesce(F.col("studio"), lit("")),
            coalesce(F.col("broadcast_start_ts"), lit("")).cast("string"),
            coalesce(F.col("season"), lit(""))
        ), 256).alias("record_hash")
    )
    # Find new or changed records using hash comparison
    joined = silver_transformed.alias("silver").join(
        existing_gold.alias("gold"),
        (F.col("silver.anime_id") == F.col("gold.anime_id")) & 
        (F.col("silver.record_hash") == F.col("gold.record_hash")) &
        (F.col("silver.effective_start_date") == F.col("gold.effective_start_date")),
        how="left"
    )
    # Select only new or changed records
    new_or_changed = joined.filter(F.col("gold.anime_id").isNull()) \
        .select(
            "silver.anime_id", "silver.genre", "silver.title", "silver.studio",
            "silver.release_date", "silver.season", "silver.effective_start_date",
            "silver.effective_end_date", "silver.is_current", "silver.record_hash"
        )
    # Only append if there are changes
    if not new_or_changed.isEmpty():
        new_or_changed.writeTo("my_catalog.gold.dim_anime_title_scd2").append()
        print(f"dim_anime_title_scd2 updated with {new_or_changed.count()} new/changed records")
    else:
        print("No new changes for dim_anime_title_scd2")

def etl_ml_features_upcoming_titles(spark):
    """Create ML features table for upcoming anime title performance prediction"""
    anime_dim = spark.table("my_catalog.gold.dim_anime_title_scd2").filter("is_current = true")
    fact_sales = spark.table("my_catalog.gold.fact_sales")
    fact_screenings = spark.table("my_catalog.gold.fact_screenings")
    dim_product = spark.table("my_catalog.gold.dim_product").filter("is_current = true")

    sales_with_titles = fact_sales.join(dim_product, "product_id", "inner")

    pos_sales_by_anime = sales_with_titles.groupBy("name").agg(
        F.sum("revenue").alias("sales_from_pos")
    )

    screening_sales = fact_screenings.groupBy("anime_id").agg(
        F.sum("ticket_revenue").alias("sales_from_tickets"),
        F.avg("ticket_revenue").alias("avg_views"),
        F.sum("concession_revenue").alias("sales_from_concessions")
    )
    # Combine all features for ML model
    ml_features_df = anime_dim.alias("a") \
        .join(screening_sales.alias("s"), F.col("a.anime_id") == F.col("s.anime_id"), "left") \
        .join(pos_sales_by_anime.alias("p"), F.col("a.title") == F.col("p.name"), "left") \
        .select(
            F.col("a.anime_id"),
            F.col("a.title"),
            (F.coalesce(F.col("s.sales_from_tickets"), F.lit(0.0)) +
             F.coalesce(F.col("s.sales_from_concessions"), F.lit(0.0)) +
             F.coalesce(F.col("p.sales_from_pos"), F.lit(0.0))
            ).alias("historical_sales"),
            F.coalesce(F.col("s.avg_views"), F.lit(0.0)).alias("avg_views"),
            F.col("a.season"),
            F.col("a.genre")
        )
    # Data validation 
    invalid_sales_data = ml_features_df.filter(F.col("historical_sales") < 0)
    if invalid_sales_data.count() > 0:
        logging.warning("Found historical_sales with zero or negative values")
    invalid_avg_views_data = ml_features_df.filter(F.col("avg_views") < 0)
    if invalid_avg_views_data.count() > 0:
        logging.warning("Found avg_views with zero or negative values")
    

    ml_features_df.writeTo("my_catalog.gold.ML_features_upcoming_titles") \
        .using("iceberg") \
        .tableProperty("write.format.default", "orc") \
        .createOrReplace()

    print("ML_features_upcoming_titles updated")


def run_etl():
    spark = create_spark_session()
    etl_fact_sales(spark)
    etl_fact_screenings(spark)
    etl_dim_customer(spark)
    etl_customer_segments(spark)
    etl_gold_top_anime_performance_tiles(spark)
    etl_gold_inventory_summary(spark)
    etl_dim_product(spark)
    etl_dim_anime_title_scd2(spark)
    etl_ml_features_upcoming_titles(spark)
    
    print("All ETL jobs completed.")

if __name__ == "__main__":
    run_etl()
