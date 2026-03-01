from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id='example_dag',
    start_date=datetime(2026, 2, 24),
    schedule_interval='@daily',
    catchup=False
) as dag:

    t1 = BashOperator(
        task_id='say_hello',
        bash_command='echo "Hello from Example DAG!"'
    )

    t2 = BashOperator(
        task_id='say_goodbye',
        bash_command='echo "Goodbye from Example DAG!"'
    )

    t1 >> t2

    
