from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2026, 2, 28),
    'retries': 1,
}

with DAG(
    'spark_job',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:

    bronze_task = SparkSubmitOperator(
        task_id='run_bronze_job',
        application='/opt/airflow/spark_jobs/spark_bronze.py',
        name='bronze_job',
        conn_id='spark_default',
        packages="io.delta:delta-spark_2.12:3.2.0,com.microsoft.sqlserver:mssql-jdbc:11.2.0.jre11",
        conf={
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        },
        verbose=True
    )

    silver_task = SparkSubmitOperator(
        task_id='run_silver_job',
        application='/opt/airflow/spark_jobs/spark_silver.py',
        name='silver_job',
        conn_id='spark_default',
        packages="io.delta:delta-spark_2.12:3.2.0,com.microsoft.sqlserver:mssql-jdbc:11.2.0.jre11",
        conf={
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        },
        verbose=True
    )

    bronze_task >> silver_task