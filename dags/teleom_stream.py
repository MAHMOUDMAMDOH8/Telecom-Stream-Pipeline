from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import socket
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='telecom_Stream_ingestion_pipeline',
    default_args=default_args,
    description='A DAG for streaming telecom data from Kafka to Snowflake',
    schedule= '*/2 * * * *',  
    catchup=False,
    tags=['kafka'],
) as dag:




    cleaning_job_task = SparkSubmitOperator(
        task_id="Spark_Cleaning_Job",
        application="/opt/airflow/scripts/Spark/cleaning_job.py",
        conn_id="spark_conn",  
        verbose=True,
        executor_memory="2g",
        driver_memory="2g",
        num_executors=1,
        total_executor_cores=1,
        executor_cores=1,
    )

    HDFS_to_Snow = SparkSubmitOperator(
        task_id="HDFS_to_Snow",
        application="/opt/airflow/scripts/Spark/upload_to_snowflake.py",
        conn_id="spark_conn",  
        verbose=True,
        executor_memory="2g",
        driver_memory="2g",
        num_executors=1,
        total_executor_cores=1,
        executor_cores=1,
    )

    trigger_dbt_dag = TriggerDagRunOperator(
        task_id='trigger_dbt_dag',
        trigger_dag_id='dbt_pipeline',  
    )


    cleaning_job_task >> HDFS_to_Snow >> trigger_dbt_dag


