from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries':0,
    'retry_delay': timedelta(minutes=1.5),
    'email_on_failure': False,
    'email_on_retry': False,
}

with DAG(
    dag_id='spark_data_pipeline_bronze_to_silver',
    default_args=default_args,
    description='Airflow DAG to orchestrate Spark jobs for Bronze → Silver pipeline',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['spark', 'kafka', 'iceberg', 'bronze', 'silver', 'pipeline'],
) as dag:

    spark_base_command = "docker exec spark-master spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1"

    create_iceberg_table_job = BashOperator(
        task_id='create_iceberg_table_job',
        bash_command=(
            "docker exec spark-master spark-submit "
            "--jars /opt/bitnami/spark/jars/spark-sql-kafka-0-10_2.12-3.4.1.jar,"
            "/opt/bitnami/spark/jars/kafka-clients-3.3.2.jar "
            "/opt/bitnami/spark/jobs/create_iceberg_table.py"
        ),
    )
    
    run_pos_stream_bronze_job = BashOperator(
        task_id='run_pos_stream_bronze_job',
        bash_command=("docker exec spark-master spark-submit "
            "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 "
            "/opt/bitnami/spark/jobs/pos_stream_bronze.py")
    )

    run_supplier_stream_bronze_job = BashOperator(
        task_id='run_supplier_stream_bronze_job',
        bash_command=f"{spark_base_command} /opt/bitnami/spark/jobs/supplier_stream_bronze.py",
    )

    run_ticket_stream_bronze_job = BashOperator(
        task_id='run_ticket_stream_bronze_job',
        bash_command=f"{spark_base_command} /opt/bitnami/spark/jobs/ticket_stream_bronze.py",
    )

    run_consession_stream_bronze_job = BashOperator(
        task_id='run_consession_stream_bronze_job',
        bash_command=f"{spark_base_command} /opt/bitnami/spark/jobs/consession_stream_bronze.py",
    )

    run_anime_batch_bronze_job = BashOperator(
        task_id='run_anime_batch_bronze_job',
        bash_command=f"{spark_base_command} /opt/bitnami/spark/jobs/anime_batch_bronze.py",
    )

    run_customer_batch_bronze_job = BashOperator(
        task_id='run_customer_batch_bronze_job',
        bash_command=f"{spark_base_command} /opt/bitnami/spark/jobs/customer_batch_bronze.py",
    )

    # run_bronze_to_silver_job = BashOperator(
    #     task_id='run_bronze_to_silver_job',
    #     bash_command=f"{spark_base_command} /opt/bitnami/spark/jobs/bronze_to_silver.py",
    # )

    # DAG dependencies
    create_iceberg_table_job >>[ 
        run_pos_stream_bronze_job ,
        run_supplier_stream_bronze_job ,
        run_ticket_stream_bronze_job ,
        run_consession_stream_bronze_job ,
        run_anime_batch_bronze_job ,run_customer_batch_bronze_job ]
