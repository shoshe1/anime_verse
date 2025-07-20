from pyspark.sql import SparkSession

def create_spark_session():
    return SparkSession.builder \
        .appName("IcebergTableCreation") \
        .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.my_catalog.type", "hadoop") \
        .config("spark.sql.catalog.my_catalog.warehouse", "s3a://warehouse") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

def create_table(spark, table_name, ddl, layer):
    try:
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")
        spark.sql(ddl)
        print(f"Successfully created {table_name} table in MinIO bucket 'warehouse/{layer}'")
    except Exception as e:
        print(f"Error creating table {table_name}: {str(e)}")

def main():
    spark = create_spark_session()
    
    # Bronze Layer Tables
    bronze_tables = {
              "bronze_pos_transaction_events": """
            CREATE TABLE my_catalog.bronze.bronze_pos_transaction_events (
                transaction_id STRING,
                event_ts TIMESTAMP,
                ingestion_ts TIMESTAMP,
                customer_id STRING,
                product_id STRING,
                quantity_purchased INT,
                unit_price_at_sale FLOAT
            )
            USING ICEBERG
            TBLPROPERTIES ('write.format.default'='orc')
        """,
        "bronze_supplier_delivery_events": """
            CREATE TABLE my_catalog.bronze.bronze_supplier_delivery_events (
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
        """,
        
        "bronze_ticket_booking_events": """
            CREATE TABLE my_catalog.bronze.bronze_ticket_booking_events (
                booking_id STRING,
                event_ts TIMESTAMP,
                ingestion_ts TIMESTAMP,
                customer_id STRING,
                screening_id STRING,
                quantity_purchased INT,
                unit_price_at_sale FLOAT
            )
            USING ICEBERG
            TBLPROPERTIES ('write.format.default'='orc')
        """,
        
        "bronze_anime_broadcast_schedule": """
            CREATE TABLE my_catalog.bronze.bronze_anime_broadcast_schedule (
                broadcast_id STRING,
                ingestion_ts TIMESTAMP,
                anime_id STRING,
                title STRING,
                season_name STRING,
                season_number INT,
                genre STRING,
                studio STRING,
                broadcast_end_ts TIMESTAMP,
                broadcast_start_ts TIMESTAMP,
                season STRING
            )
            USING ICEBERG
            TBLPROPERTIES ('write.format.default'='orc')
        """,
        
        "bronze_customer_registration_events": """
            CREATE TABLE my_catalog.bronze.bronze_customer_registration_events (
                registration_id STRING,
                customer_id STRING,
                name STRING,
                email STRING
            )
            USING ICEBERG
            TBLPROPERTIES ('write.format.default'='orc')
        """,
        
        "bronze_concession_purchase_events": """
            CREATE TABLE my_catalog.bronze.bronze_concession_purchase_events (
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
        """
    }

    # Silver Layer Tables
    silver_tables = {
        "silver_pos_transactions_clean": """
            CREATE TABLE my_catalog.silver.silver_pos_transactions_clean (
                transaction_id STRING,
                transaction_ts TIMESTAMP,
                customer_id STRING,
                product_id STRING,
                quantity_purchased INT,
                total_price FLOAT,
                linked_anime_id STRING
            )
            USING ICEBERG
            TBLPROPERTIES ('write.format.default'='orc')
        """,
        
        "silver_ticket_bookings_clean": """
            CREATE TABLE my_catalog.silver.silver_ticket_bookings_clean (
                booking_id STRING,
                booking_ts TIMESTAMP,
                customer_id STRING,
                screening_id STRING,
                quantity_purchased INT,
                total_price FLOAT,
                linked_anime_id STRING
            )
            USING ICEBERG
            TBLPROPERTIES ('write.format.default'='orc')
        """,
        
        "silver_product_master_data": """
            CREATE TABLE my_catalog.silver.silver_product_master_data (
                product_id STRING,
                supplier_id STRING,
                current_unit_cost FLOAT,
                current_unit_price FLOAT,
                last_cost_update TIMESTAMP,
                last_price_update TIMESTAMP,
                category STRING,
                linked_anime_id STRING
            )
            USING ICEBERG
            TBLPROPERTIES ('write.format.default'='orc')
        """,
        
        "silver_customer_master_data": """
            CREATE TABLE my_catalog.silver.silver_customer_master_data (
                registration_id STRING,
                customer_id STRING,
                name STRING,
                email STRING
            )
            USING ICEBERG
            TBLPROPERTIES ('write.format.default'='orc')
        """,
        
        "silver_inventory_movements_clean": """
            CREATE TABLE my_catalog.silver.silver_inventory_movements_clean (
                product_id STRING,
                linked_anime_id STRING,
                current_quantity INT,
                last_update_ts TIMESTAMP,
                supplier_id STRING
            )
            USING ICEBERG
            TBLPROPERTIES ('write.format.default'='orc')
        """,
        
        "silver_anime_broadcast_schedule_scd2": """
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
        """,
        "silver_concession_sales_summary": """
           CREATE TABLE my_catalog.silver.silver_concession_sales_summary (
                screening_id STRING,
                total_quantity_purchased INT,
                total_revenue FLOAT
            )
            USING ICEBERG
            TBLPROPERTIES ('write.format.default'='orc')
        """
    }
    gold_tables = {

        "fact_sales": """
                CREATE TABLE my_catalog.gold.fact_sales (
            sale_id STRING,
            customer_id STRING,
            product_id STRING,
            calendar_date DATE,
            quantity INT,
            revenue FLOAT,
            inventory_level_at_sale INT
        )
        USING ICEBERG
        TBLPROPERTIES ('write.format.default'='orc');""",
        "fact_screanings": """ 
        CREATE TABLE my_catalog.gold.fact_screenings (
            screening_id STRING,
            anime_id STRING,
            calendar_date DATE,
            ticket_revenue FLOAT,
            concession_revenue FLOAT
        )
        USING ICEBERG
        TBLPROPERTIES ('write.format.default'='orc');
        

    """,

    "dim_customer": """
    CREATE TABLE my_catalog.gold.dim_customer (
        customer_id STRING,
        name STRING,
        segment STRING,
        email STRING
    )
    USING ICEBERG
        TBLPROPERTIES ('write.format.default'='orc')

    """,
        "customer_segment": """
            CREATE TABLE my_catalog.gold.customer_segments (
        segment STRING,
        customer_count INT,
        description STRING
    )
    USING ICEBERG
        TBLPROPERTIES ('write.format.default'='orc');

    """ ,
    "dim_product": """
    CREATE TABLE my_catalog.gold.dim_product (
        product_id STRING,
        name STRING, -- anime title
        category STRING,
        price FLOAT,
        cost FLOAT,
        supplier_id STRING,
        effective_start_date TIMESTAMP,
        effective_end_date TIMESTAMP,
        is_current BOOLEAN
    )
    USING ICEBERG
    TBLPROPERTIES ('write.format.default'='orc');
""",
        "dim_anime_title_scd2": """
            CREATE TABLE my_catalog.gold.dim_anime_title_scd2 (
    anime_id STRING,                           
    genre STRING,                              
    title STRING,                              
    studio STRING,                           
    release_date TIMESTAMP,                   
    season STRING,                             
    effective_start_date TIMESTAMP,            
    effective_end_date TIMESTAMP,              
    is_current BOOLEAN ,
    record_hash STRING
                       
)
USING ICEBERG
TBLPROPERTIES ('write.format.default'='orc');
        """

    }

    # Create Bronze Tables
    print("Creating Bronze Layer Tables...")
    for table_name, ddl in bronze_tables.items():
        create_table(spark, f"my_catalog.bronze.{table_name}", ddl, "bronze")

    # Create Silver Tables
    print("\nCreating Silver Layer Tables...")
    for table_name, ddl in silver_tables.items():
        create_table(spark, f"my_catalog.silver.{table_name}", ddl, "silver")
    # Create Gold Tables
    print("\nCreating gold Layer Tables...")
    for table_name, ddl in gold_tables.items():
        create_table(spark, f"my_catalog.gold.{table_name}", ddl, "gold")

    spark.stop()

if __name__ == "__main__":
    main()