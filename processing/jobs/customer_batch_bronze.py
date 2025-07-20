from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType,TimestampType

spark = SparkSession.builder \
    .appName("customer Bronze batch") \
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
         StructField("registration_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
        ])
full_table_name = "my_catalog.bronze.bronze_customer_registration_events"
csv_file_path = "batch_data/customer_registrations.csv"
try:
        
        print(f"Reading CSV from: {csv_file_path} with custom schema")
        df = spark.read \
            .option("header", "true") \
            .schema(schema) \
            .csv(csv_file_path)

        print("CSV DataFrame Schema (based on custom schema):")
        df.printSchema()

        print("CSV DataFrame Content (first 5 rows):")
        df.show(5)

        print(f"Appending DataFrame to existing Iceberg table: {full_table_name}")
        df.writeTo(full_table_name) \
        .append() \
        .using("iceberg") \
        .table(full_table_name) 
        print(f"Data successfully appended to Iceberg table: {full_table_name}")

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

