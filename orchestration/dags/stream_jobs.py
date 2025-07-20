from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries':1 ,
    'retry_delay': timedelta(minutes=1),
    'email_on_failure': False,
    'email_on_retry': False,
}

with DAG(
    dag_id='streaming_jobs_and_silver_gold_creation_every_30_minutes',
    default_args=default_args,
    description='Run  streaming Spark jobsand coverting logic',
    schedule_interval=timedelta(minutes=30), 
    max_active_runs=1,
    catchup=False,
    tags=['spark', 'streaming'],
) as dag:

    spark_base_command = """
    docker exec spark-master spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    --conf spark.dynamicAllocation.enabled=false \
    --conf spark.executor.instances=1 \
    --conf spark.executor.memory=512m \
    --conf spark.executor.cores=1 \
    --conf spark.driver.memory=512m \
    --conf spark.task.cpus=1 \
    --conf spark.cores.max=1 \
    --conf spark.ui.port=4040 \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 \
    """

    run_pos_stream_bronze_job = BashOperator(
        task_id='run_pos_stream_bronze_job',
        bash_command=spark_base_command + "/opt/bitnami/spark/jobs/pos_stream_bronze.py"
    )

    run_supplier_stream_bronze_job = BashOperator(
        task_id='run_supplier_stream_bronze_job',
        bash_command=spark_base_command + "/opt/bitnami/spark/jobs/supplier_stream_bronze.py",
    )

    run_ticket_stream_bronze_job = BashOperator(
        task_id='run_ticket_stream_bronze_job',
        bash_command=spark_base_command + "/opt/bitnami/spark/jobs/ticket_stream_bronze.py",
    )

    run_consession_stream_bronze_job = BashOperator(
        task_id='run_consession_stream_bronze_job',
        bash_command=spark_base_command + "/opt/bitnami/spark/jobs/consession_stream_bronze.py",
    )
    bronze_to_silver = BashOperator(
        task_id='bronze_to_silver_transform',
        bash_command=spark_base_command + "/opt/bitnami/spark/jobs/bronze_to_silver.py"
    )
    silver_to_gold = BashOperator(
        task_id='silver_to_gold_transform',
        bash_command=spark_base_command + "/opt/bitnami/spark/jobs/silver_to_gold.py"
    )
    # Optional: Run jobs in parallel
    [
        run_pos_stream_bronze_job,
        run_supplier_stream_bronze_job,
        run_ticket_stream_bronze_job,
        run_consession_stream_bronze_job
    ] >> bronze_to_silver >> silver_to_gold
