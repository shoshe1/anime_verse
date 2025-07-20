from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, col
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType

spark = SparkSession.builder \
    .appName("anime Bronze batch") \
    .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.my_catalog.type", "hadoop") \
    .config("spark.sql.catalog.my_catalog.warehouse", "s3a://warehouse/") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

schema = StructType([
    StructField("broadcast_id", IntegerType(), True),
    StructField("anime_id", IntegerType(), True),
    StructField("title", StringType(), True),
    StructField("season_name", StringType(), True),
    StructField("season_number", IntegerType(), True),
    StructField("genre", StringType(), True),
    StructField("studio", StringType(), True),
    StructField("broadcast_start_ts", StringType(), True),
    StructField("broadcast_end_ts", StringType(), True),
    StructField("season", StringType(), True)
])

csv_file_path = "batch_data/anime_broadcast_schedule.csv"
full_table_name = "my_catalog.bronze.bronze_anime_broadcast_schedule"

try:
    print(f"Reading CSV from: {csv_file_path} with schema")
    df = spark.read \
        .option("header", True) \
        .schema(schema) \
        .csv(csv_file_path)

    # Cast timestamps properly after read
    df = df.withColumn("broadcast_start_ts", col("broadcast_start_ts").cast(TimestampType())) \
           .withColumn("broadcast_end_ts", col("broadcast_end_ts").cast(TimestampType())) \
           .withColumn("ingestion_ts", current_timestamp())

    print("DataFrame schema:")
    df.printSchema()

    print("First 5 rows of DataFrame:")
    df.show(5, truncate=False)

    print(f"Appending data to Iceberg table: {full_table_name}")
    df.writeTo(full_table_name).append()

except Exception as e:
    print(f"Error occurred: {e}")
    import traceback
    traceback.print_exc()
finally:
    spark.stop()
    print("Spark session stopped.")
