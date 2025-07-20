from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

spark = SparkSession.builder \
    .appName("concession Bronze Stream") \
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
    StructField("purchase_id", StringType()),
    StructField("event_ts", StringType()),
    StructField("ingestion_ts", StringType()),
    StructField("screening_id", StringType()),
    StructField("item_id", StringType()),
    StructField("item_quantity", IntegerType()),
    StructField("item_unit_price", FloatType())
])

# Read from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "concession_topic") \
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
    .option("checkpointLocation", "s3a://warehouse/bronze/checkpoints/concession_stream") \
    .toTable("my_catalog.bronze.bronze_concession_purchase_events")

query.awaitTermination(100)