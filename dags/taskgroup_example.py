from airflow import DAG # type: ignore
from airflow.models.baseoperator import chain # type: ignore
from airflow.utils.task_group import TaskGroup # type: ignore
from airflow.operators.python import PythonOperator # type: ignore
from datetime import datetime

with DAG('taskgroup', 
  start_date=datetime(2023, 1, 1), 
  schedule='@daily', 
  catchup=False):

    start = PythonOperator(
        task_id='start',
        python_callable=lambda: print('start'),
    )

    with TaskGroup('sources') as sources:

        with TaskGroup('A') as a:
            task_process_a = PythonOperator(
                task_id='process_a',
                python_callable=lambda: print('process_a'),
            )
            task_store_a = PythonOperator(
                task_id='store_a',
                python_callable=lambda: print('store_a'),
            )

            task_process_a >> task_store_a

        with TaskGroup('B') as b:
            task_process_b = PythonOperator(
                task_id='process_b',
                python_callable=lambda: print('process_b'),
            )

            task_store_b = PythonOperator(
                task_id='store_b',
                python_callable=lambda: print('store_b'),
            )

            task_process_b >> task_store_b

    end = PythonOperator(
        task_id='end',
        python_callable=lambda: print('end'),
    )

    chain(start, sources, end)