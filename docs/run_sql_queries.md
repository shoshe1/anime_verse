## Running Spark SQL CLI for Table Validation

To run Spark SQL queries against your Iceberg tables stored in MinIO, follow these steps:

1. Make sure your `processing` Docker container with Spark is running.

2. Start an interactive Spark SQL shell inside the `spark-master` container with the required Iceberg and MinIO configurations by running:

```bash
docker exec -it spark-master /opt/bitnami/spark/bin/spark-sql \
  --conf spark.sql.catalog.my_catalog=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.my_catalog.type=hadoop \
  --conf spark.sql.catalog.my_catalog.warehouse=s3a://warehouse \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
  --conf spark.hadoop.fs.s3a.access.key=minioadmin \
  --conf spark.hadoop.fs.s3a.secret.key=minioadmin \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem


## Sample Spark SQL Queries for Gold Layer Validation

Run these queries in the Spark SQL CLI or your preferred SQL interface to verify data in the bronze layer tables:
 SELECT * FROM my_catalog.bronze.bronze_supplier_delivery_events LIMIT 10;
 SELECT * FROM my_catalog.bronze.bronze_anime_broadcast_schedule LIMIT 10;
 SELECT * FROM my_catalog.bronze.bronze_concession_purchase_events LIMIT 10;
 SELECT * FROM my_catalog.bronze.bronze_customer_registration_events LIMIT 10;
 SELECT * FROM my_catalog.bronze.bronze_pos_transaction_events LIMIT 10;
 SELECT * FROM my_catalog.bronze.bronze_ticket_booking_events LIMIT 10;


Run these queries in the Spark SQL CLI or your preferred SQL interface to verify data in the silver layer tables:
SELECT * FROM my_catalog.silver.silver_anime_broadcast_schedule_scd2 ;
SELECT * FROM my_catalog.silver.silver_customer_master_data ;
SELECT * FROM my_catalog.silver.silver_inventory_movements_clean ;
SELECT * FROM my_catalog.silver.silver_pos_transactions_clean;
SELECT * FROM my_catalog.silver.silver_product_master_data ;
SELECT * FROM my_catalog.silver.silver_ticket_bookings_clean ;
SELECT * FROM my_catalog.silver.silver_concession_sales_summary ;

Run these queries in the Spark SQL CLI or your preferred SQL interface to verify data in the gold layer tables:
SELECT * FROM my_catalog.gold.fact_sales ;
SELECT * FROM my_catalog.gold.fact_screenings ;
SELECT * FROM my_catalog.gold.customer_segments ;
SELECT * FROM my_catalog.gold.dim_customer ;
SELECT * FROM my_catalog.gold.gold_top_anime_performance_tiles ;
SELECT * FROM my_catalog.gold.gold_inventory_summary ;
SELECT * FROM my_catalog.gold.dim_product ;
SELECT * FROM my_catalog.gold.dim_anime_title_scd2 ;
SELECT * FROM my_catalog.gold.ML_features_upcoming_titles ;


to show the table schema run:
#for bronze:
DESCRIBE my_catalog.bronze.bronze_supplier_delivery_events ;
DESCRIBE my_catalog.bronze.bronze_anime_broadcast_schedule ;
DESCRIBE my_catalog.bronze.bronze_concession_purchase_events;
DESCRIBE my_catalog.bronze.bronze_customer_registration_events ;
DESCRIBE my_catalog.bronze.bronze_pos_transaction_events;
DESCRIBE  my_catalog.bronze.bronze_ticket_booking_events;

#for silver:
DESCRIBE my_catalog.silver.silver_anime_broadcast_schedule_scd2 ;
DESCRIBE my_catalog.silver.silver_customer_master_data ;
DESCRIBE my_catalog.silver.silver_inventory_movements_clean;
DESCRIBE my_catalog.silver.silver_pos_transactions_clean ;
DESCRIBE my_catalog.silver.silver_product_master_data;
DESCRIBE my_catalog.silver.silver_ticket_bookings_clean;
DESCRIBE my_catalog.silver.silver_concession_sales_summary;

#for gold:
DESCRIBE my_catalog.gold.fact_sales ;
DESCRIBE my_catalog.gold.fact_screenings ;
DESCRIBE my_catalog.gold.customer_segments ;
DESCRIBE my_catalog.gold.dim_customer ;
DESCRIBE my_catalog.gold.gold_top_anime_performance_tiles ;
DESCRIBE my_catalog.gold.gold_inventory_summary ;
DESCRIBE my_catalog.gold.dim_product ;
DESCRIBE my_catalog.gold.dim_anime_title_scd2 ;
DESCRIBE my_catalog.gold.ML_features_upcoming_titles ;