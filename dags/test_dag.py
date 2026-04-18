from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def print_hello():
    print("Hello from your test DAG 10!")
    return "Test successful 2!"

with DAG(
    dag_id='simple_test_dag',
    start_date=datetime(2023, 1, 1),
    schedule=None,  # This ensures it only runs when triggered manually
    catchup=False,
    tags=['test'],
) as dag:

    hello_task = PythonOperator(
        task_id='hello_task',
        python_callable=print_hello,
    )
