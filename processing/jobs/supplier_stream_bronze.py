from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

spark = SparkSession.builder \
    .appName("Supplier Bronze Stream") \
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
    StructField("delivery_id", StringType()),
    StructField("event_ts", StringType()),
    StructField("ingestion_ts", StringType()),
    StructField("supplier_id", StringType()),
    StructField("product_id", StringType()),
    StructField("quantity_delivered", IntegerType()),
    StructField("unit_cost", FloatType())
])

# Read from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "supllier_topic") \
    .option("failOnDataLoss", "false") \
    .load()

# Parse JSON
json_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("event_ts", col("event_ts").cast("timestamp")) \
    .withColumn("ingestion_ts", col("ingestion_ts").cast("timestamp"))

# Write to Iceberg table

query = json_df.writeStream \
    .format("iceberg") \
    .outputMode("append") \
    .option("checkpointLocation", "s3a://warehouse/bronze/checkpoints/supplier_stream") \
    .toTable("my_catalog.bronze.bronze_supplier_delivery_events")

query.awaitTermination(100)