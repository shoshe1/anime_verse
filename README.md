AnimeVerse Data Engineering Project
Project Purpose

This project implements a multi-layer data pipeline for AnimeVerse, a combined anime retail and cinema business. The pipeline integrates multiple data sources (streaming and batch), processes and cleans data through Bronze, Silver, and Gold layers, and supports analytics dashboards and machine learning models for customer segmentation, sales forecasting, and product recommendations.

Key features include Slowly Changing Dimensions (SCD Type 2) handling for evolving dimension tables, late-arriving supplier data integration, and comprehensive data quality checks to ensure reliable insights.
How to Run
Prerequisites

    Docker and Docker Compose installed

    Sufficient local machine resources (recommend 8GB+ RAM)

Setup Shared Network

Create the shared Docker network used by all components:

docker network create animeverse-net

Starting Components

Run each component in separate terminals in the following order:

cd orchestration
docker compose up

cd ..
cd processing
docker compose up

cd ..
cd streaming
docker compose up

cd ..
cd storage/minio
docker compose up

Airflow DAG Execution Order

    Run the spark_create_table_and_run_batch_jobs DAG to create Iceberg tables and load batch data. This DAG must complete successfully first.

    Then run the streaming_jobs_and_silver_gold_creation_every_30_minutes DAG for streaming ingestion and incremental Silver and Gold table processing. This DAG is configured to run every 30 minutes and can be rescheduled to accommodate resource limitations.


Running Considerations

    Running many Spark jobs in parallel can overload local machine resources.

    Streaming jobs are run for 2–3 minutes and then stopped to free resources before running Silver and Gold batch jobs.

    The streaming DAG is configured for rescheduling to continue incremental ingestion safely.

    A dedicated Airflow DAG creates Iceberg tables and runs batch jobs first, which must finish before Silver and Gold layer jobs run.

    This sequential execution strategy ensures stable local development and prevents crashes.