from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType,TimestampType

spark = SparkSession.builder \
    .appName("anime Bronze batch") \
    .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.my_catalog.type", "hadoop")\
    .config("spark.sql.catalog.my_catalog.warehouse", "s3a://warehouse/") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# Define schema
schema = StructType([
   StructField("broadcast_id", StringType(), True),
        StructField("ingestion_ts", TimestampType(), True),
        StructField("anime_id", StringType(), True),
        StructField("title", StringType(), True),
        StructField("genre", StringType(), True),
        StructField("studio", StringType(), True),
        StructField("broadcast_start_ts", TimestampType(), True),
        StructField("broadcast_end_ts", TimestampType(), True),
        StructField("season", StringType(), True)])
# Define the Iceberg table name
full_table_name = "my_catalog.bronze.bronze_anime_broadcast_schedule"
csv_file_path = "batch_data/anime_broadcast_schedule.csv"
try:
        # 2. Read the CSV file into a Spark DataFrame using the provided custom schema
        print(f"Reading CSV from: {csv_file_path} with custom schema")
        df = spark.read \
            .option("header", "true") \
            .schema(schema) \
            .csv(csv_file_path)

        print("CSV DataFrame Schema (based on custom schema):")
        df.printSchema()

        print("CSV DataFrame Content (first 5 rows):")
        df.show(5)

        # 3. Write the DataFrame to an Iceberg table
        #    Since the table is already created, we use mode("append") to add new data.
        #    If the schema of the DataFrame does not match the Iceberg table,
        #    you might need to explicitly cast or reorder columns, or use schema evolution features.
        print(f"Appending DataFrame to existing Iceberg table: {full_table_name}")
        df.writeTo(full_table_name) \
        .append() \
        .using("iceberg") \
        .table(full_table_name) # This line is redundant if full_table_name is already passed to writeTo()

        print(f"Data successfully appended to Iceberg table: {full_table_name}")

        #  Verify the data by reading from the Iceberg table
        print(f"Reading data back from Iceberg table: {full_table_name}")
        iceberg_df = spark.read.format("iceberg").load(full_table_name)
        iceberg_df.show()

        print(f"Schema of Iceberg table ({full_table_name}):")
        iceberg_df.printSchema()

except Exception as e:
        print(f"An error occurred: {e}")
        import traceback
        traceback.print_exc()
finally:
        # Stop the SparkSession
        spark.stop()
        print("SparkSession stopped.")

