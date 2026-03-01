from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id='hello_airflow',
    start_date=datetime(2026,2,24),
    schedule_interval='@daily',
    catchup=False
) as dag:
    t1 = BashOperator(
        task_id='print_hello',
        bash_command='echo "Hello_Airflow!"'
    )
    t2 = BashOperator(
        task_id='say_goodbye',
        bash_command='echo "Goodbye from Example DAG!"'
    )
    t1 >> t2