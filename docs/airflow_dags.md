# Airflow DAGs Overview

## `spark_create_table_and_run_batch_jobs`
- Creates Iceberg tables schema in MinIO storage.
- Runs batch processing ETL jobs to populate Silver and Gold tables from batch data.
- Must complete before running streaming DAG.

## `streaming_jobs_and_silver_gold_creation_every_30_minutes`
- Runs streaming ingestion from Kafka to Bronze layer.
- Triggers transformations from Bronze to Silver and Silver to Gold tables.
- Scheduled every 30 minutes, but can be manually controlled to manage system load.
